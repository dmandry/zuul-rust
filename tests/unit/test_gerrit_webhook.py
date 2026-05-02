# Copyright 2025 Acme Gating, LLC
# Copyright 2026 Electrifex Technologies Pvt Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import socket

import requests

from zuul.driver.gerrit.gerritconnection import GerritWebController

from tests.base import (
    ZuulTestCase,
    ZuulWebFixture,
)


class TestGerritWebhook(ZuulTestCase):
    """Functional tests for Gerrit webhook event source."""

    config_file = 'zuul-gerrit-webhook.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'

    def setUp(self):
        super().setUp()
        self.web = self.useFixture(
            ZuulWebFixture(self.config, self.test_config,
                           self.additional_event_queues, self.upstream_root,
                           self.poller_events,
                           self.git_url_with_auth, self.addCleanup,
                           self.test_root))
        host = '127.0.0.1'
        while True:
            port = self.web.port
            try:
                with socket.create_connection((host, port)):
                    break
            except ConnectionRefusedError:
                pass
        self.web_port = port

    def _post_event(self, event, token=None):
        url = ('http://127.0.0.1:%s/api/connection/gerrit/payload'
               % self.web_port)
        if token:
            url += '?token=%s' % token
        return requests.post(url, json=event,
                             headers={'Content-Type': 'application/json'})

    def test_webhook_patchset_created_triggers_pipeline(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        event = A.getPatchsetCreatedEvent(1)

        # Confirm the connection is configured to use webhooks as the
        # event source (and not stream-events, which is disabled in
        # the fixture); this ensures the build below is triggered by
        # the webhook payload rather than any other code path.
        self.assertEqual(self.fake_gerrit.EVENT_SOURCE_WEBHOOK,
                         self.fake_gerrit.event_source)

        resp = self._post_event(event, token='test-webhook-secret')
        self.assertEqual(200, resp.status_code)
        self.assertEqual({'ok': True}, resp.json())

        self.waitUntilSettled()
        self.assertEqual('SUCCESS',
                         self.getJobFromHistory('project-test1').result)

    def test_webhook_valid_token_accepted(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        event = A.getPatchsetCreatedEvent(1)

        resp = self._post_event(event, token='test-webhook-secret')
        self.assertEqual(200, resp.status_code)
        self.waitUntilSettled()

    def test_webhook_missing_token_rejected(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        event = A.getPatchsetCreatedEvent(1)

        resp = self._post_event(event, token=None)
        self.assertEqual(401, resp.status_code)

    def test_webhook_wrong_token_rejected(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        event = A.getPatchsetCreatedEvent(1)

        resp = self._post_event(event, token='wrongtoken')
        self.assertEqual(401, resp.status_code)

    def test_webhook_oversized_body_rejected(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        event = A.getPatchsetCreatedEvent(1)
        # Pad the event past the controller's maximum body size.
        event['padding'] = 'x' * (GerritWebController.MAX_BODY_BYTES + 1)

        resp = self._post_event(event, token='test-webhook-secret')
        self.assertEqual(413, resp.status_code)
