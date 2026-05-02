# Copyright 2025 Acme Gating, LLC
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
import unittest
from unittest import mock

import cherrypy
import requests

from zuul.driver.gerrit.gerritconnection import GerritWebController

from tests.base import (
    ZuulTestCase,
    ZuulWebFixture,
)


class TestGerritWebControllerTokenValidation(unittest.TestCase):
    """Unit tests for GerritWebController token validation logic."""

    def _make_controller(self, token=None):
        connection = mock.MagicMock()
        connection.connection_config.get.side_effect = (
            lambda key, default=None: token if key == 'webhook_token'
            else default
        )
        zuul_web = mock.MagicMock()
        with mock.patch(
                'zuul.driver.gerrit.gerritconnection.ConnectionEventQueue'):
            return GerritWebController(zuul_web, connection)

    def test_no_token_configured_accepts_all(self):
        ctrl = self._make_controller(token=None)
        # Should not raise for any input when no token is configured
        ctrl._validateToken({}, None)
        ctrl._validateToken({}, 'anything')
        ctrl._validateToken({'x-gerrit-token': 'something'}, None)

    def test_valid_token_via_query_param(self):
        ctrl = self._make_controller(token='mysecret')
        ctrl._validateToken({}, 'mysecret')

    def test_valid_token_via_header(self):
        ctrl = self._make_controller(token='mysecret')
        ctrl._validateToken({'x-gerrit-token': 'mysecret'}, None)

    def test_query_param_takes_precedence_over_header(self):
        ctrl = self._make_controller(token='mysecret')
        # correct token in param, wrong token in header — should pass
        ctrl._validateToken({'x-gerrit-token': 'wrongtoken'}, 'mysecret')

    def test_missing_token_raises_401(self):
        ctrl = self._make_controller(token='mysecret')
        with self.assertRaises(cherrypy.HTTPError) as ctx:
            ctrl._validateToken({}, None)
        self.assertEqual(ctx.exception.status, 401)

    def test_wrong_token_raises_401(self):
        ctrl = self._make_controller(token='mysecret')
        with self.assertRaises(cherrypy.HTTPError) as ctx:
            ctrl._validateToken({}, 'wrongtoken')
        self.assertEqual(ctx.exception.status, 401)

    def test_wrong_header_token_raises_401(self):
        ctrl = self._make_controller(token='mysecret')
        with self.assertRaises(cherrypy.HTTPError) as ctx:
            ctrl._validateToken({'x-gerrit-token': 'wrongtoken'}, None)
        self.assertEqual(ctx.exception.status, 401)


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

    def test_webhook_token_via_header(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        event = A.getPatchsetCreatedEvent(1)

        url = ('http://127.0.0.1:%s/api/connection/gerrit/payload'
               % self.web_port)
        resp = requests.post(url, json=event, headers={
            'Content-Type': 'application/json',
            'X-Gerrit-Token': 'test-webhook-secret',
        })
        self.assertEqual(200, resp.status_code)
