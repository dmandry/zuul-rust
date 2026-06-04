# Copyright 2014 Hewlett-Packard Development Company, L.P.
# Copyright 2014 Rackspace Australia
# Copyright 2021-2026 Acme Gating, LLC
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

import collections
import os
import urllib.parse
import socket
import textwrap
import time
import json
import jwt
import sys
import subprocess
import threading
from unittest import skip, mock

from kazoo.exceptions import NoNodeError
import requests
import testtools

from zuul.lib.statsd import normalize_statsd_name
import zuul.model
from zuul.zk.event_queues import TENANT_EVENT_STATE
from zuul.zk.locks import tenant_write_lock
import zuul.web

from tests.base import ZuulTestCase, AnsibleZuulTestCase
from tests.base import (
    FIXTURE_DIR,
    ZuulWebFixture,
    iterate_timeout,
    okay_tracebacks,
)
from tests.unit.test_launcher import LauncherBaseTestCase
from tests.base import simple_layout, return_data


class FakeConfig(object):

    def __init__(self, config):
        self.config = config or {}

    def has_option(self, section, option):
        return option in self.config.get(section, {})

    def get(self, section, option):
        return self.config.get(section, {}).get(option)


class WebMixin:
    config_ini_data = {}
    stats_interval = None
    default_token_groups = None

    def startWebServer(self):
        self.zuul_ini_config = FakeConfig(self.config_ini_data)
        # Start the web server
        self.web = self.useFixture(
            ZuulWebFixture(self.config, self.test_config,
                           self.additional_event_queues, self.upstream_root,
                           self.poller_events,
                           self.git_url_with_auth, self.addCleanup,
                           self.test_root,
                           info=zuul.model.WebInfo.fromConfig(
                               self.zuul_ini_config),
                           stats_interval=self.stats_interval))

        self.executor_server.hold_jobs_in_build = True

        self.host = 'localhost'
        self.port = self.web.port
        # Wait until web server is started
        while True:
            try:
                with socket.create_connection((self.host, self.port)):
                    break
            except ConnectionRefusedError:
                pass
        self.base_url = "http://{host}:{port}".format(
            host=self.host, port=self.port)

    def get_url(self, url, *args, **kwargs):
        return requests.get(
            urllib.parse.urljoin(self.base_url, url), *args, **kwargs)

    def post_url(self, url, *args, **kwargs):
        return requests.post(
            urllib.parse.urljoin(self.base_url, url), *args, **kwargs)

    def put_url(self, url, *args, **kwargs):
        return requests.put(
            urllib.parse.urljoin(self.base_url, url), *args, **kwargs)

    def delete_url(self, url, *args, **kwargs):
        return requests.delete(
            urllib.parse.urljoin(self.base_url, url), *args, **kwargs)

    def options_url(self, url, *args, **kwargs):
        return requests.options(
            urllib.parse.urljoin(self.base_url, url), *args, **kwargs)

    def _getToken(self, admin=None, groups=None):
        if admin is None:
            admin = ['tenant-one']
        if groups is None:
            groups = self.default_token_groups
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': admin,
                 },
                 'exp': int(time.time()) + 3600}
        if groups:
            authz['groups'] = groups
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        return token


class BaseTestWeb(ZuulTestCase, WebMixin):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def setUp(self):
        super().setUp()
        self.startWebServer()

    def add_base_changes(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

    def tearDown(self):
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        super(BaseTestWeb, self).tearDown()


class TestWeb(BaseTestWeb):

    def test_web_index(self):
        "Test that we can retrieve the index page"
        resp = self.get_url('api')
        data = resp.json()
        # no point checking the whole thing; just make sure _something_ we
        # expect is here
        self.assertIn('info', data)

    def test_web_status(self):
        "Test that we can retrieve JSON status info"
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.executor_server.release('project-merge')
        self.waitUntilSettled()

        resp = self.get_url("api/tenant/tenant-one/status")
        self.assertIn('Content-Length', resp.headers)
        self.assertIn('Content-Type', resp.headers)
        self.assertEqual(
            'application/json; charset=utf-8', resp.headers['Content-Type'])
        self.assertIn('Access-Control-Allow-Origin', resp.headers)
        self.assertIn('Cache-Control', resp.headers)
        self.assertIn('Last-Modified', resp.headers)
        self.assertTrue(resp.headers['Last-Modified'].endswith(' GMT'))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        data = resp.json()
        status_jobs = []
        self.assertEqual(
            data["connection_event_queues"]["gerrit"]["length"], 0)

        for p in data['pipelines']:
            self.assertEqual(p["trigger_events"], 0)
            self.assertEqual(p["result_events"], 0)
            self.assertEqual(p["management_events"], 0)
            self.assertIn('manager', p, p)
            self.assertTrue(len(p.get('triggers', [])) > 0, p)
            for q in p['change_queues']:
                if p['name'] in ['gate', 'conflict']:
                    self.assertEqual(q['window'], 20)
                else:
                    self.assertEqual(q['window'], 0)
                # This test uses unbranched queues so validate that the branch
                # information is missing.
                self.assertIsNone(q['branch'])
                for head in q['heads']:
                    for item in head:
                        self.assertIn(
                            'review.example.com/org/project',
                            item['refs'][0]['project_canonical'])
                        self.assertTrue(item['active'])
                        change = item['refs'][0]
                        self.assertIn(change['id'], ('1,1', '2,1', '3,1'))
                        for job in item['jobs']:
                            status_jobs.append(job)
        self.assertEqual('project-merge', status_jobs[0]['name'])
        # TODO(mordred) pull uuids from self.builds
        self.assertEqual(
            'stream/{uuid}?logfile=console.log'.format(
                uuid=status_jobs[0]['uuid']),
            status_jobs[0]['url'])
        self.assertEqual(
            'finger://{hostname}/{uuid}'.format(
                hostname=self.executor_server.hostname,
                uuid=status_jobs[0]['uuid']),
            status_jobs[0]['finger_url'])
        self.assertEqual(
            'https://zuul.example.com/t/tenant-one/build/{uuid}'.format(
                uuid=status_jobs[0]['uuid']),
            status_jobs[0]['report_url'])
        self.assertEqual('project-test1', status_jobs[1]['name'])
        self.assertEqual(
            'stream/{uuid}?logfile=console.log'.format(
                uuid=status_jobs[1]['uuid']),
            status_jobs[1]['url'])
        self.assertEqual(
            'finger://{hostname}/{uuid}'.format(
                hostname=self.executor_server.hostname,
                uuid=status_jobs[1]['uuid']),
            status_jobs[1]['finger_url'])
        self.assertEqual(
            'https://zuul.example.com/t/tenant-one/build/{uuid}'.format(
                uuid=status_jobs[1]['uuid']),
            status_jobs[1]['report_url'])

        self.assertEqual('project-test2', status_jobs[2]['name'])
        self.assertEqual(
            'stream/{uuid}?logfile=console.log'.format(
                uuid=status_jobs[2]['uuid']),
            status_jobs[2]['url'])
        self.assertEqual(
            'finger://{hostname}/{uuid}'.format(
                hostname=self.executor_server.hostname,
                uuid=status_jobs[2]['uuid']),
            status_jobs[2]['finger_url'])
        self.assertEqual(
            'https://zuul.example.com/t/tenant-one/build/{uuid}'.format(
                uuid=status_jobs[2]['uuid']),
            status_jobs[2]['report_url'])

        # check job dependencies
        self.assertIsNotNone(status_jobs[0]['dependencies'])
        self.assertIsNotNone(status_jobs[1]['dependencies'])
        self.assertIsNotNone(status_jobs[2]['dependencies'])
        self.assertEqual(len(status_jobs[0]['dependencies']), 0)
        self.assertEqual(len(status_jobs[1]['dependencies']), 1)
        self.assertEqual(len(status_jobs[2]['dependencies']), 1)
        self.assertIn('project-merge', status_jobs[1]['dependencies'])
        self.assertIn('project-merge', status_jobs[2]['dependencies'])
        hostname = normalize_statsd_name(socket.getfqdn())
        self.assertReportedStat(
            f'zuul.web.server.{hostname}.threadpool.idle', kind='g')
        self.assertReportedStat(
            f'zuul.web.server.{hostname}.threadpool.queue', kind='g')

    def test_web_components(self):
        "Test that we can retrieve the list of connected components"
        resp = self.get_url("api/components")
        data = resp.json()

        # The list should contain one of each kind: executor, scheduler, web
        self.assertEqual(len(data), 4)
        self.assertEqual(len(data["executor"]), 1)
        self.assertEqual(len(data["launcher"]), 1)
        self.assertEqual(len(data["scheduler"]), self.scheduler_count)
        self.assertEqual(len(data["web"]), 1)

        # Each component should contain hostname and state information
        for key in ["hostname", "state", "version"]:
            self.assertIn(key, data["executor"][0])
            self.assertIn(key, data["launcher"][0])
            self.assertIn(key, data["scheduler"][0])
            self.assertIn(key, data["web"][0])

    def test_web_tenants(self):
        "Test that we can retrieve a JSON tenant list"
        resp = self.get_url("api/tenants")
        self.assertIn('Content-Length', resp.headers)
        self.assertIn('Content-Type', resp.headers)
        self.assertEqual(
            'application/json; charset=utf-8', resp.headers['Content-Type'])
        data = resp.json()

        expected = [
            {
                'name': 'tenant-one',
            },
        ]
        self.assertEqual(expected, data)

    def test_web_connections_list(self):
        data = self.get_url('api/connections').json()
        connection = {
            'driver': 'gerrit',
            'name': 'gerrit',
            'baseurl': 'https://review.example.com',
            'canonical_hostname': 'review.example.com',
            'server': 'review.example.com',
            'ssh_server': 'review.example.com',
            'port': 29418,
        }
        self.assertEqual([connection], data)

    def test_web_bad_url(self):
        # do we redirect to index.html
        resp = self.get_url("status/foo")
        self.assertEqual(200, resp.status_code)

    def test_web_find_change(self):
        # can we filter by change id
        self.add_base_changes()
        data = self.get_url("api/tenant/tenant-one/status/change/1,1").json()

        self.assertEqual(1, len(data), data)
        self.assertEqual("org/project", data[0]['refs'][0]['project'])

        data = self.get_url("api/tenant/tenant-one/status/change/2,1").json()

        self.assertEqual(1, len(data), data)
        self.assertEqual("org/project1", data[0]['refs'][0]['project'],
                         data)

    @simple_layout('layouts/nodeset-alternatives.yaml')
    def test_web_find_job_nodeset_alternatives(self):
        # test a complex nodeset
        data = self.get_url('api/tenant/tenant-one/job/test-job').json()

        self.assertEqual([
            {'abstract': False,
             'ansible_split_streams': None,
             'ansible_version': None,
             'attempts': 3,
             'branches': None,
             'cleanup_run': [],
             'deduplicate': 'auto',
             'dependencies': [],
             'description': None,
             'exclude_projects': None,
             'extra_variables': {},
             'files': [],
             'final': False,
             'failure_output': [],
             'group_variables': {},
             'host_variables': {},
             'image_build_name': None,
             'include_projects': None,
             'include_vars': [],
             'intermediate': False,
             'irrelevant_files': [],
             'match_on_config_updates': True,
             'name': 'test-job',
             'nodeset_alternatives': [{'alternatives': [],
                                       'groups': [],
                                       'name': 'fast-nodeset',
                                       'nodes': [{'aliases': [],
                                                  'comment': None,
                                                  'hold_job': None,
                                                  'id': None,
                                                  'label': 'fast-label',
                                                  'name': 'controller',
                                                  'requestor': None,
                                                  'state': 'unknown',
                                                  'tenant_name': None,
                                                  'user_data': None}]},
                                      {'alternatives': [],
                                       'groups': [],
                                       'name': '',
                                       'nodes': [{'aliases': [],
                                                  'comment': None,
                                                  'hold_job': None,
                                                  'id': None,
                                                  'label': 'slow-label',
                                                  'name': 'controller',
                                                  'requestor': None,
                                                  'state': 'unknown',
                                                  'tenant_name': None,
                                                  'user_data': None}]}],
             'override_checkout': None,
             'parent': 'base',
             'post_review': None,
             'post_run': [],
             'pre_run': [],
             'protected': None,
             'provides': [],
             'required_projects': [],
             'requires': [],
             'roles': [{'implicit': True,
                        'project_canonical_name':
                        'review.example.com/org/common-config',
                        'target_name': 'common-config',
                        'type': 'zuul'}],
             'run': [],
             'semaphores': [],
             'source_context': {'branch': 'master',
                                'path': 'zuul.yaml',
                                'project': 'org/common-config'},
             'tags': [],
             'type': 'regular',
             'pre_timeout': None,
             'timeout': None,
             'post_timeout': None,
             'variables': {},
             'variant_description': '',
             'voting': True,
             'workspace_scheme': 'golang',
             'workspace_checkout': True,
            }], data)

    def test_web_find_job(self):
        # can we fetch the variants for a single job
        data = self.get_url('api/tenant/tenant-one/job/project-test1').json()

        common_config_role = {
            'implicit': True,
            'project_canonical_name': 'review.example.com/common-config',
            'target_name': 'common-config',
            'type': 'zuul',
        }
        source_ctx = {
            'branch': 'master',
            'path': 'zuul.yaml',
            'project': 'common-config',
        }
        run = [{
            'path': 'playbooks/project-test1.yaml',
            'roles': [{
                'implicit': True,
                'project_canonical_name': 'review.example.com/common-config',
                'target_name': 'common-config',
                'type': 'zuul'
            }],
            'secrets': [],
            'semaphores': [],
            'source_context': source_ctx,
            'cleanup': False,
        }]

        self.assertEqual([
            {
                'name': 'project-test1',
                'abstract': False,
                'ansible_split_streams': None,
                'ansible_version': None,
                'attempts': 4,
                'branches': None,
                'deduplicate': 'auto',
                'dependencies': [],
                'description': None,
                'files': [],
                'image_build_name': None,
                'intermediate': False,
                'irrelevant_files': [],
                'match_on_config_updates': True,
                'final': False,
                'failure_output': [],
                'nodeset': {
                    'alternatives': [],
                    'groups': [],
                    'name': '',
                    'nodes': [{'comment': None,
                               'hold_job': None,
                               'id': None,
                               'label': 'label1',
                               'name': 'controller',
                               'aliases': [],
                               'requestor': None,
                               'state': 'unknown',
                               'tenant_name': None,
                               'user_data': None}],
                },
                'override_checkout': None,
                'parent': 'base',
                'post_review': None,
                'protected': None,
                'provides': [],
                'required_projects': [],
                'requires': [],
                'roles': [common_config_role],
                'run': run,
                'pre_run': [],
                'post_run': [],
                'cleanup_run': [],
                'semaphores': [],
                'source_context': source_ctx,
                'tags': [],
                'type': 'regular',
                'pre_timeout': None,
                'timeout': None,
                'post_timeout': None,
                'variables': {},
                'exclude_projects': None,
                'extra_variables': {},
                'group_variables': {},
                'host_variables': {},
                'include_projects': None,
                'include_vars': [],
                'variant_description': '',
                'voting': True,
                'workspace_scheme': 'golang',
                'workspace_checkout': True,
            }, {
                'name': 'project-test1',
                'abstract': False,
                'ansible_split_streams': None,
                'ansible_version': None,
                'attempts': 3,
                'branches': ['stable'],
                'deduplicate': 'auto',
                'dependencies': [],
                'description': None,
                'files': [],
                'image_build_name': None,
                'intermediate': False,
                'irrelevant_files': [],
                'match_on_config_updates': True,
                'final': False,
                'failure_output': [],
                'nodeset': {
                    'alternatives': [],
                    'groups': [],
                    'name': '',
                    'nodes': [{'comment': None,
                               'hold_job': None,
                               'id': None,
                               'label': 'label2',
                               'name': 'controller',
                               'aliases': [],
                               'requestor': None,
                               'state': 'unknown',
                               'tenant_name': None,
                               'user_data': None}],
                },
                'override_checkout': None,
                'parent': 'base',
                'post_review': None,
                'protected': None,
                'provides': [],
                'required_projects': [],
                'requires': [],
                'roles': [common_config_role],
                'run': run,
                'pre_run': [],
                'post_run': [],
                'cleanup_run': [],
                'semaphores': [],
                'source_context': source_ctx,
                'tags': [],
                'type': 'regular',
                'pre_timeout': None,
                'timeout': None,
                'post_timeout': None,
                'variables': {},
                'exclude_projects': None,
                'extra_variables': {},
                'group_variables': {},
                'host_variables': {},
                'include_projects': None,
                'include_vars': [],
                'variant_description': 'stable',
                'voting': True,
                'workspace_scheme': 'golang',
                'workspace_checkout': True,
            }], data)

        data = self.get_url('api/tenant/tenant-one/job/test-job').json()
        run[0]['path'] = 'playbooks/project-merge.yaml'
        self.assertEqual([
            {
                'abstract': False,
                'ansible_split_streams': None,
                'ansible_version': None,
                'attempts': 3,
                'branches': None,
                'deduplicate': 'auto',
                'dependencies': [],
                'description': None,
                'files': [],
                'image_build_name': None,
                'final': False,
                'failure_output': [],
                'intermediate': False,
                'irrelevant_files': [],
                'match_on_config_updates': True,
                'name': 'test-job',
                'override_checkout': None,
                'parent': 'base',
                'post_review': None,
                'protected': None,
                'provides': [],
                'required_projects': [
                    {'override_branch': None,
                     'override_checkout': None,
                     'project_name': 'org/project'}],
                'requires': [],
                'roles': [common_config_role],
                'run': run,
                'pre_run': [],
                'post_run': [],
                'cleanup_run': [],
                'semaphores': [],
                'source_context': source_ctx,
                'tags': [],
                'type': 'regular',
                'pre_timeout': None,
                'timeout': None,
                'post_timeout': None,
                'variables': {},
                'exclude_projects': None,
                'extra_variables': {},
                'group_variables': {},
                'host_variables': {},
                'include_projects': None,
                'include_vars': [],
                'variant_description': '',
                'voting': True,
                'workspace_scheme': 'golang',
                'workspace_checkout': True,
            }], data)

    def test_find_job_complete_playbooks(self):
        # can we fetch the variants for a single job
        data = self.get_url('api/tenant/tenant-one/job/complete-job').json()

        def expected_pb(path, cleanup=False):
            return {
                'cleanup': cleanup,
                'path': path,
                'roles': [{
                    'implicit': True,
                    'project_canonical_name':
                    'review.example.com/common-config',
                    'target_name': 'common-config',
                    'type': 'zuul'
                }],
                'secrets': [],
                'semaphores': [],
                'source_context': {
                    'branch': 'master',
                    'path': 'zuul.yaml',
                    'project': 'common-config',
                }
            }
        self.assertEqual([
            expected_pb("playbooks/run.yaml")
        ], data[0]['run'])
        self.assertEqual([
            expected_pb("playbooks/pre-run.yaml")
        ], data[0]['pre_run'])
        self.assertEqual([
            expected_pb("playbooks/post-run-01.yaml"),
            expected_pb("playbooks/post-run-02.yaml"),
        ], data[0]['post_run'])
        self.assertEqual([
            expected_pb("playbooks/cleanup-run.yaml")
        ], data[0]['cleanup_run'])

    def test_web_nodes_list(self):
        # can we fetch the nodes list
        self.add_base_changes()
        data = self.get_url('api/tenant/tenant-one/nodes').json()
        self.assertGreater(len(data), 0)
        self.assertEqual("test-provider", data[0]["provider"])
        self.assertEqual("label1", data[0]["type"])

    def test_web_labels_list(self):
        # can we fetch the labels list
        data = self.get_url('api/tenant/tenant-one/labels').json()
        expected_list = [{'name': 'label1'}]
        self.assertEqual(expected_list, data)

    def test_web_pipeline_list(self):
        # can we fetch the list of pipelines
        data = self.get_url('api/tenant/tenant-one/pipelines').json()

        gerrit_trigger = {'name': 'gerrit', 'driver': 'gerrit'}
        timer_trigger = {'name': 'timer', 'driver': 'timer'}
        expected_list = [
            {'name': 'check', 'triggers': [gerrit_trigger]},
            {'name': 'gate', 'triggers': [gerrit_trigger]},
            {'name': 'post', 'triggers': [gerrit_trigger]},
            {'name': 'periodic', 'triggers': [timer_trigger]},
        ]
        self.assertEqual(expected_list, data)

    def test_web_project_list(self):
        # can we fetch the list of projects
        data = self.get_url('api/tenant/tenant-one/projects').json()

        expected_list = [
            {'name': 'common-config', 'type': 'config'},
            {'name': 'org/project', 'type': 'untrusted'},
            {'name': 'org/project1', 'type': 'untrusted'},
            {'name': 'org/project2', 'type': 'untrusted'}
        ]
        for p in expected_list:
            p["canonical_name"] = "review.example.com/%s" % p["name"]
            p["connection_name"] = "gerrit"
        self.assertEqual(expected_list, data)

    def test_web_project_get(self):
        # can we fetch project details
        data = self.get_url(
            'api/tenant/tenant-one/project/org/project1').json()

        jobs = [[{'abstract': False,
                  'ansible_split_streams': None,
                  'ansible_version': None,
                  'attempts': 3,
                  'branches': None,
                  'deduplicate': 'auto',
                  'dependencies': [],
                  'description': None,
                  'exclude_projects': None,
                  'files': [],
                  'final': False,
                  'failure_output': [],
                  'image_build_name': None,
                  'intermediate': False,
                  'irrelevant_files': [],
                  'match_on_config_updates': True,
                  'name': 'project-merge',
                  'override_checkout': None,
                  'parent': 'base',
                  'post_review': None,
                  'protected': None,
                  'provides': [],
                  'required_projects': [],
                  'requires': [],
                  'roles': [],
                  'run': [],
                  'pre_run': [],
                  'post_run': [],
                  'cleanup_run': [],
                  'semaphores': [],
                  'source_context': {
                      'branch': 'master',
                      'path': 'zuul.yaml',
                      'project': 'common-config'},
                  'tags': [],
                  'type': 'regular',
                  'pre_timeout': None,
                  'timeout': None,
                  'post_timeout': None,
                  'variables': {},
                  'extra_variables': {},
                  'group_variables': {},
                  'host_variables': {},
                  'include_vars': [],
                  'include_projects': None,
                  'variant_description': '',
                  'voting': True,
                  'workspace_scheme': 'golang',
                  'workspace_checkout': True,
                  }],
                [{'abstract': False,
                  'ansible_split_streams': None,
                  'ansible_version': None,
                  'attempts': 3,
                  'branches': None,
                  'deduplicate': 'auto',
                  'dependencies': [{'name': 'project-merge',
                                    'soft': False}],
                  'description': None,
                  'exclude_projects': None,
                  'files': [],
                  'final': False,
                  'failure_output': [],
                  'image_build_name': None,
                  'intermediate': False,
                  'irrelevant_files': [],
                  'match_on_config_updates': True,
                  'name': 'project-test1',
                  'override_checkout': None,
                  'parent': 'base',
                  'post_review': None,
                  'protected': None,
                  'provides': [],
                  'required_projects': [],
                  'requires': [],
                  'roles': [],
                  'run': [],
                  'pre_run': [],
                  'post_run': [],
                  'cleanup_run': [],
                  'semaphores': [],
                  'source_context': {
                      'branch': 'master',
                      'path': 'zuul.yaml',
                      'project': 'common-config'},
                  'tags': [],
                  'type': 'regular',
                  'pre_timeout': None,
                  'timeout': None,
                  'post_timeout': None,
                  'variables': {},
                  'extra_variables': {},
                  'group_variables': {},
                  'host_variables': {},
                  'include_vars': [],
                  'include_projects': None,
                  'variant_description': '',
                  'voting': True,
                  'workspace_scheme': 'golang',
                  'workspace_checkout': True,
                  }],
                [{'abstract': False,
                  'ansible_split_streams': None,
                  'ansible_version': None,
                  'attempts': 3,
                  'branches': None,
                  'deduplicate': 'auto',
                  'dependencies': [{'name': 'project-merge',
                                    'soft': False}],
                  'description': None,
                  'exclude_projects': None,
                  'files': [],
                  'final': False,
                  'failure_output': [],
                  'image_build_name': None,
                  'intermediate': False,
                  'irrelevant_files': [],
                  'match_on_config_updates': True,
                  'name': 'project-test2',
                  'override_checkout': None,
                  'parent': 'base',
                  'post_review': None,
                  'protected': None,
                  'provides': [],
                  'required_projects': [],
                  'requires': [],
                  'roles': [],
                  'run': [],
                  'pre_run': [],
                  'post_run': [],
                  'cleanup_run': [],
                  'semaphores': [],
                  'source_context': {
                      'branch': 'master',
                      'path': 'zuul.yaml',
                      'project': 'common-config'},
                  'tags': [],
                  'type': 'regular',
                  'pre_timeout': None,
                  'timeout': None,
                  'post_timeout': None,
                  'variables': {},
                  'extra_variables': {},
                  'group_variables': {},
                  'host_variables': {},
                  'include_vars': [],
                  'include_projects': None,
                  'variant_description': '',
                  'voting': True,
                  'workspace_scheme': 'golang',
                  'workspace_checkout': True,
                  }],
                [{'abstract': False,
                  'ansible_split_streams': None,
                  'ansible_version': None,
                  'attempts': 3,
                  'branches': None,
                  'deduplicate': 'auto',
                  'dependencies': [{'name': 'project-merge',
                                    'soft': False}],
                  'description': None,
                  'exclude_projects': None,
                  'files': [],
                  'final': False,
                  'failure_output': [],
                  'image_build_name': None,
                  'intermediate': False,
                  'irrelevant_files': [],
                  'match_on_config_updates': True,
                  'name': 'project1-project2-integration',
                  'override_checkout': None,
                  'parent': 'base',
                  'post_review': None,
                  'protected': None,
                  'provides': [],
                  'required_projects': [],
                  'requires': [],
                  'roles': [],
                  'run': [],
                  'pre_run': [],
                  'post_run': [],
                  'cleanup_run': [],
                  'semaphores': [],
                  'source_context': {
                      'branch': 'master',
                      'path': 'zuul.yaml',
                      'project': 'common-config'},
                  'tags': [],
                  'type': 'regular',
                  'pre_timeout': None,
                  'timeout': None,
                  'post_timeout': None,
                  'variables': {},
                  'extra_variables': {},
                  'group_variables': {},
                  'host_variables': {},
                  'include_vars': [],
                  'include_projects': None,
                  'variant_description': '',
                  'voting': True,
                  'workspace_scheme': 'golang',
                  'workspace_checkout': True,
                  }]]

        self.assertEqual(
            {
                'canonical_name': 'review.example.com/org/project1',
                'connection_name': 'gerrit',
                'name': 'org/project1',
                'metadata': {
                    'is_template': False,
                    'default_branch': 'master',
                    'merge_mode': 'merge-resolve',
                    'queue_name': 'integrated',
                },
                'configs': [{
                    'source_context': {'branch': 'master',
                                       'path': 'zuul.yaml',
                                       'project': 'common-config'},
                    'is_template': False,
                    'templates': [],
                    'default_branch': None,
                    'queue_name': 'integrated',
                    'merge_mode': None,
                    'pipelines': [{
                        'name': 'check',
                        'jobs': jobs,
                        'parameters': [],
                    }, {
                        'name': 'gate',
                        'jobs': jobs,
                        'parameters': [],
                    }, {'name': 'post',
                        'jobs': [[
                            {'abstract': False,
                             'ansible_split_streams': None,
                             'ansible_version': None,
                             'attempts': 3,
                             'branches': None,
                             'deduplicate': 'auto',
                             'dependencies': [],
                             'description': None,
                             'exclude_projects': None,
                             'files': [],
                             'final': False,
                             'failure_output': [],
                             'image_build_name': None,
                             'intermediate': False,
                             'irrelevant_files': [],
                             'match_on_config_updates': True,
                             'name': 'project-post',
                             'override_checkout': None,
                             'parent': 'base',
                             'post_review': None,
                             'post_run': [],
                             'cleanup_run': [],
                             'pre_run': [],
                             'protected': None,
                             'provides': [],
                             'required_projects': [],
                             'requires': [],
                             'roles': [],
                             'run': [],
                             'semaphores': [],
                             'source_context': {'branch': 'master',
                                                'path': 'zuul.yaml',
                                                'project': 'common-config'},
                             'tags': [],
                             'type': 'regular',
                             'pre_timeout': None,
                             'timeout': None,
                             'post_timeout': None,
                             'variables': {},
                             'extra_variables': {},
                             'group_variables': {},
                             'host_variables': {},
                             'include_vars': [],
                             'include_projects': None,
                             'variant_description': '',
                             'voting': True,
                             'workspace_scheme': 'golang',
                             'workspace_checkout': True,
                             }
                        ]],
                        'parameters': [],
                    }
                    ]
                }]
            }, data)

    def test_web_project_get_no_config(self):
        # can we fetch project details for projects with no project stanza
        self.commitConfigUpdate(
            'common-config',
            'layouts/empty-check.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        layout_scheduler = self.scheds.first.sched.local_layout_state.get(
            'tenant-one')
        for _ in iterate_timeout(10, "local layout of zuul-web to be updated"):
            layout_web = self.web.web.local_layout_state.get('tenant-one')
            if layout_web == layout_scheduler:
                break

        data = self.get_url(
            'api/tenant/tenant-one/project/org/project1').json()
        self.assertEqual(
            {
                'canonical_name': 'review.example.com/org/project1',
                'configs': [],
                'metadata': {},
                'connection_name': 'gerrit',
                'name': 'org/project1'
            },
            data)

    def test_web_keys(self):
        with open(os.path.join(FIXTURE_DIR, 'public.pem'), 'rb') as f:
            public_pem = f.read()

        resp = self.get_url("api/tenant/tenant-one/key/org/project.pub")
        self.assertEqual(resp.content, public_pem)
        self.assertIn('text/plain', resp.headers.get('Content-Type'))

        resp = self.get_url("api/tenant/non-tenant/key/org/project.pub")
        self.assertEqual(404, resp.status_code)

        resp = self.get_url("api/tenant/tenant-one/key/org/no-project.pub")
        self.assertEqual(404, resp.status_code)

        with open(os.path.join(FIXTURE_DIR, 'ssh.pub'), 'rb') as f:
            public_ssh = f.read()

        resp = self.get_url("api/tenant/tenant-one/project-ssh-key/"
                            "org/project.pub")
        self.assertEqual(resp.content, public_ssh)
        self.assertIn('text/plain', resp.headers.get('Content-Type'))

        resp = self.get_url("api/tenant/tenant-one/tenant-ssh-key/"
                            "ssh.pub")
        self.assertEqual(resp.content, public_ssh)
        self.assertIn('text/plain', resp.headers.get('Content-Type'))

    def test_web_404_on_unknown_tenant(self):
        resp = self.get_url("api/tenant/non-tenant/status")
        self.assertEqual(404, resp.status_code)

    def test_autohold_info_404_on_invalid_id(self):
        resp = self.get_url("api/tenant/tenant-one/autohold/12345")
        self.assertEqual(404, resp.status_code)

    def test_autohold_delete_401_on_invalid_id(self):
        resp = self.delete_url("api/tenant/tenant-one/autohold/12345")
        self.assertEqual(401, resp.status_code)

    def test_autohold_info(self):
        self.addAutohold('tenant-one', 'review.example.com/org/project',
                         'project-test2', '.*', 'reason text', 1, 600)

        # Use autohold-list API to retrieve request ID
        resp = self.get_url(
            "api/tenant/tenant-one/autohold")
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))
        request_id = autohold_requests[0]['id']

        # Now try the autohold-info API
        resp = self.get_url("api/tenant/tenant-one/autohold/%s" % request_id)
        self.assertEqual(200, resp.status_code, resp.text)
        request = resp.json()

        self.assertEqual(request_id, request['id'])
        self.assertEqual('tenant-one', request['tenant'])
        self.assertIn('org/project', request['project'])
        self.assertEqual('project-test2', request['job'])
        self.assertEqual(".*", request['ref_filter'])
        self.assertEqual(1, request['max_count'])
        self.assertEqual(0, request['current_count'])
        self.assertEqual("reason text", request['reason'])
        self.assertEqual([], request['nodes'])

        # Scope the request to tenant-two, not found
        resp = self.get_url("api/tenant/tenant-two/autohold/%s" % request_id)
        self.assertEqual(404, resp.status_code, resp.text)

    def test_autohold_list(self):
        """test listing autoholds through zuul-web"""
        self.addAutohold('tenant-one', 'review.example.com/org/project',
                         'project-test2', '.*', 'reason text', 1, 600)
        resp = self.get_url(
            "api/tenant/tenant-one/autohold")
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()

        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))
        ah_request = autohold_requests[0]

        self.assertEqual('tenant-one', ah_request['tenant'])
        self.assertIn('org/project', ah_request['project'])
        self.assertEqual('project-test2', ah_request['job'])
        self.assertEqual(".*", ah_request['ref_filter'])
        self.assertEqual(1, ah_request['max_count'])
        self.assertEqual(0, ah_request['current_count'])
        self.assertEqual("reason text", ah_request['reason'])
        self.assertEqual([], ah_request['nodes'])

        # filter by project
        resp = self.get_url(
            "api/tenant/tenant-one/autohold?project=org/project2")
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertEqual([], autohold_requests)
        resp = self.get_url(
            "api/tenant/tenant-one/autohold?project=org/project")
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()

        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))
        ah_request = autohold_requests[0]

        self.assertEqual('tenant-one', ah_request['tenant'])
        self.assertIn('org/project', ah_request['project'])
        self.assertEqual('project-test2', ah_request['job'])
        self.assertEqual(".*", ah_request['ref_filter'])
        self.assertEqual(1, ah_request['max_count'])
        self.assertEqual(0, ah_request['current_count'])
        self.assertEqual("reason text", ah_request['reason'])
        self.assertEqual([], ah_request['nodes'])

        # Unknown tenants return 404
        resp = self.get_url(
            "api/tenant/tenant-fifty/autohold")
        self.assertEqual(404, resp.status_code, resp.text)

    def test_admin_routes_404_by_default(self):
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(404, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(404, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(404, resp.status_code)

    def test_jobs_list(self):
        jobs = self.get_url("api/tenant/tenant-one/jobs").json()
        self.assertEqual(len(jobs), 10)

        resp = self.get_url("api/tenant/non-tenant/jobs")
        self.assertEqual(404, resp.status_code)

    def test_jobs_list_variants(self):
        resp = self.get_url("api/tenant/tenant-one/jobs").json()
        for job in resp:
            if job['name'] in ["base", "noop"]:
                variants = None
            elif job['name'] == 'project-test1':
                variants = [
                    {'parent': 'base'},
                    {'branches': ['stable'], 'parent': 'base'},
                ]
            else:
                variants = [{'parent': 'base'}]
            self.assertEqual(variants, job.get('variants'))

    def test_jobs_list_tags(self):
        resp = self.get_url("api/tenant/tenant-one/jobs").json()

        post_job = None
        for job in resp:
            if job['name'] == 'project-post':
                post_job = job
                break
        self.assertIsNotNone(post_job)
        self.assertEqual(['post'], post_job.get('tags'))

    def test_web_nonexistent_job(self):
        resp = self.get_url("api/tenant/tenant-one/job/nopenopenope")
        self.assertEqual(404, resp.status_code)

    def test_web_job_noop(self):
        job = self.get_url("api/tenant/tenant-one/job/noop").json()
        self.assertEqual("noop", job[0]["name"])

    @simple_layout('layouts/special-characters-job.yaml')
    def test_web_job_special_characters(self):
        job = self.get_url("api/tenant/tenant-one/job/a%40b%2Fc").json()
        self.assertEqual("a@b/c", job[0]["name"])

    def test_freeze_jobs(self):
        # Test can get a list of the jobs for a given project+pipeline+branch.
        resp = self.get_url(
            "api/tenant/tenant-one/pipeline/check"
            "/project/org/project1/branch/master/freeze-jobs")

        freeze_jobs = [{
            'name': 'project-merge',
            'dependencies': [],
        }, {
            'name': 'project-test1',
            'dependencies': [{
                'name': 'project-merge',
                'soft': False,
            }],
        }, {
            'name': 'project-test2',
            'dependencies': [{
                'name': 'project-merge',
                'soft': False,
            }],
        }, {
            'name': 'project1-project2-integration',
            'dependencies': [{
                'name': 'project-merge',
                'soft': False,
            }],
        }]
        self.assertEqual(freeze_jobs, resp.json())

    def test_freeze_jobs_set_includes_all_jobs(self):
        # When freezing a job set we want to include all jobs even if they
        # have certain matcher requirements (such as required files) since we
        # can't otherwise evaluate them.

        resp = self.get_url(
            "api/tenant/tenant-one/pipeline/gate"
            "/project/org/project/branch/master/freeze-jobs")
        expected = {
            'name': 'project-testfile',
            'dependencies': [{
                'name': 'project-merge',
                'soft': False,
            }],
        }
        self.assertIn(expected, resp.json())

    def test_freeze_job(self):

        resp = self.get_url(
            "api/tenant/tenant-one/pipeline/check"
            "/project/org/project1/branch/master/freeze-job/"
            "project-test1")

        job_params = {
            'job': 'project-test1',
            'ansible_split_streams': None,
            'ansible_version': '9',
            'timeout': None,
            'pre_timeout': None,
            'post_timeout': None,
            'items': [],
            'projects': [],
            'branch': 'master',
            'cleanup_playbooks': [],
            'failure_output': [],
            'nodeset': {
                'alternatives': [],
                'groups': [],
                'name': '',
                'nodes': [
                    {'aliases': [],
                     'comment': None,
                     'hold_job': None,
                     'id': None,
                     'label': 'label1',
                     'name': 'controller',
                     'requestor': None,
                     'state': 'unknown',
                     'tenant_name': None,
                     'user_data': None}]},
            'override_branch': None,
            'override_checkout': None,
            'repo_state_keys': [],
            'playbooks': [{
                'cleanup': False,
                'nesting_level': 1,
                'connection': 'gerrit',
                'project': 'common-config',
                'branch': 'master',
                'trusted': True,
                'roles': [{
                    'target_name': 'common-config',
                    'type': 'zuul',
                    'project_canonical_name':
                        'review.example.com/common-config',
                    'implicit': True,
                    'project_default_branch': 'master',
                    'connection': 'gerrit',
                    'project': 'common-config',
                }],
                'secrets': {},
                'semaphores': [],
                'path': 'playbooks/project-test1.yaml',
            }],
            'pipeline_builds_images': False,
            'pre_playbooks': [],
            'post_playbooks': [],
            'ssh_keys': ['REDACTED'],
            'vars': {},
            'extra_vars': {},
            'host_vars': {},
            'group_vars': {},
            'secret_vars': None,
            'zuul': {
                '_inheritance_path': [
                    '<Job base explicit: None implied: '
                    '{MatchAny:{ImpliedBranchMatcher:master}} '
                    'source: common-config/zuul.yaml@master#53>',
                    '<Job project-test1 explicit: None '
                    'implied: '
                    '{MatchAny:{ImpliedBranchMatcher:master}} '
                    'source: common-config/zuul.yaml@master#66>',
                    '<Job project-test1 explicit: None '
                    'implied: None source: '
                    'common-config/zuul.yaml@master#138>'],
                'build': '00000000000000000000000000000000',
                'buildset': None,
                'branch': 'master',
                'ref': None,
                'build_refs': [
                    {'branch': 'master',
                     'change_url': None,
                     'project': {
                         'canonical_hostname': 'review.example.com',
                         'canonical_name':
                         'review.example.com/org/project1',
                         'name': 'org/project1',
                         'short_name': 'project1',
                         'src_dir': 'src/review.example.com/org/project1',
                     },
                     'src_dir': 'src/review.example.com/org/project1'}],
                'buildset_refs': [
                    {'branch': 'master',
                     'change_url': None,
                     'project': {
                         'canonical_hostname': 'review.example.com',
                         'canonical_name':
                         'review.example.com/org/project1',
                         'name': 'org/project1',
                         'short_name': 'project1',
                         'src_dir': 'src/review.example.com/org/project1',
                     },
                     'src_dir': 'src/review.example.com/org/project1'}],
                'pipeline': 'check',
                'post_review': False,
                'job': 'project-test1',
                'voting': True,
                'project': {
                    'name': 'org/project1',
                    'short_name': 'project1',
                    'canonical_hostname': 'review.example.com',
                    'canonical_name': 'review.example.com/org/project1',
                    'src_dir': 'src/review.example.com/org/project1',
                },
                'tenant': 'tenant-one',
                'pre_timeout': None,
                'timeout': None,
                'post_timeout': None,
                'jobtags': [],
                'branch': 'master',
                'projects': {},
                'items': [],
                'change_url': None,
                'child_jobs': [],
                'event_id': None,
                'include_vars': [],
            },
            'workspace_scheme': 'golang',
            'workspace_checkout': True,
        }

        self.assertEqual(job_params, resp.json())

    @simple_layout('layouts/noop-job.yaml')
    def test_freeze_noop_job(self):

        resp = self.get_url(
            "api/tenant/tenant-one/pipeline/gate"
            "/project/org/noop-project/branch/master/freeze-job/"
            "noop")

        job_params = {
            'ansible_split_streams': None,
            'ansible_version': '9',
            'branch': 'master',
            'extra_vars': {},
            'group_vars': {},
            'host_vars': {},
            'items': [],
            'job': 'noop',
            'failure_output': [],
            'nodeset': {'alternatives': [],
                        'groups': [], 'name': '', 'nodes': []},
            'override_branch': None,
            'override_checkout': None,
            'pipeline_builds_images': False,
            'pre_timeout': None,
            'post_timeout': None,
            'projects': [],
            'repo_state_keys': [],
            'secret_vars': None,
            'ssh_keys': ['REDACTED'],
            'timeout': None,
            'vars': {},
            'workspace_scheme': 'golang',
            'workspace_checkout': True,
            'zuul': {
                '_inheritance_path': [
                    '<Job noop explicit: None implied: None '
                    'source: None#0>',
                    '<Job noop explicit: None implied: None '
                    'source: org/common-config/zuul.yaml@master#22>'],
                'branch': 'master',
                'build': '00000000000000000000000000000000',
                'buildset': None,
                'change_url': None,
                'child_jobs': [],
                'event_id': None,
                'include_vars': [],
                'items': [],
                'job': 'noop',
                'jobtags': [],
                'pipeline': 'gate',
                'post_review': False,
                'project': {
                    'canonical_hostname': 'review.example.com',
                    'canonical_name': 'review.example.com/org/noop-project',
                    'name': 'org/noop-project',
                    'short_name': 'noop-project',
                    'src_dir': 'src/review.example.com/org/noop-project'},
                'projects': {},
                'ref': None,
                'build_refs': [
                    {'branch': 'master',
                     'change_url': None,
                     'project': {
                         'canonical_hostname': 'review.example.com',
                         'canonical_name':
                         'review.example.com/org/noop-project',
                         'name': 'org/noop-project',
                         'short_name': 'noop-project',
                         'src_dir':
                         'src/review.example.com/org/noop-project',
                     },
                     'src_dir':
                     'src/review.example.com/org/noop-project'}],
                'buildset_refs': [
                    {'branch': 'master',
                     'change_url': None,
                     'project': {
                         'canonical_hostname': 'review.example.com',
                         'canonical_name':
                         'review.example.com/org/noop-project',
                         'name': 'org/noop-project',
                         'short_name': 'noop-project',
                         'src_dir':
                         'src/review.example.com/org/noop-project',
                     },
                     'src_dir':
                     'src/review.example.com/org/noop-project'}],
                'tenant': 'tenant-one',
                'pre_timeout': None,
                'timeout': None,
                'post_timeout': None,
                'voting': True}}

        self.assertEqual(job_params, resp.json())

    def test_web_system_events(self):
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        data = self.get_url('api/tenant/tenant-one/system-events').json()
        self.assertEqual(1, len(data))
        self.assertEqual("tenant reconfiguration", data[0]["event_type"])
        self.assertEqual("Tenant reconfiguration due to external request",
                         data[0]["description"])


class TestWebStatsReporting(BaseTestWeb):
    stats_interval = 1

    def test_default_statsd_reporting(self):
        # Let the system settle out then wait 2x the reporting interval
        # before we check that things have reported.
        self.waitUntilSettled()
        time.sleep(2)
        hostname = normalize_statsd_name(socket.getfqdn())
        self.assertReportedStat(
            f'zuul.web.server.{hostname}.threadpool.idle',
            value='10', kind='g')
        self.assertReportedStat(
            f'zuul.web.server.{hostname}.threadpool.queue',
            value='0', kind='g')


class TestWebProviders(LauncherBaseTestCase, WebMixin):
    config_file = 'zuul-connections-nodepool.conf'
    tenant_config_file = 'config/multi-tenant-provider/main.yaml'

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_web_providers(self):
        self.waitUntilSettled()
        self.startWebServer()
        resp = self.get_url('api/tenant/tenant-one/providers')
        data = resp.json()
        self.assertEqual(1, len(data))

        cc = 'review.example.com%2Forg%2Fcommon-config'
        expected = [
            {'canonical_name': f'{cc}/aws-us-east-1-main',
             'flavors': [
                 {'canonical_name': f'{cc}/normal',
                  'name': 'normal'},
                 {'canonical_name': f'{cc}/large',
                  'name': 'large'},
                 {'canonical_name': f'{cc}/dedicated',
                  'name': 'dedicated'},
                 {'canonical_name': f'{cc}/invalid',
                  'name': 'invalid'}],
             'images': [
                 {'canonical_name': f'{cc}/debian',
                  'name': 'debian',
                  'type': 'cloud'}],
             'labels': [
                 {'canonical_name': f'{cc}/debian-normal',
                  'name': 'debian-normal'},
                 {'canonical_name': f'{cc}/debian-large',
                  'name': 'debian-large'},
                 {'canonical_name': f'{cc}/debian-dedicated',
                  'name': 'debian-dedicated'},
                 {'canonical_name': f'{cc}/debian-invalid',
                  'name': 'debian-invalid'}],
             'name': 'aws-us-east-1-main'}]
        self.assertEqual(expected, data)

    @simple_layout('layouts/nodepool-image.yaml', enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_web_images(self, mock_image_upload_run):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.startWebServer()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        resp = self.get_url('api/tenant/tenant-one/images')
        data = resp.json()
        self.assertEqual(4, len(data))
        self.assertNotIn('build_artifacts', data[0])
        self.assertEqual(1, len(data[1]['build_artifacts']))
        self.assertEqual(1, len(data[2]['build_artifacts']))
        self.assertEqual(1, len(data[1]['build_artifacts'][0]['uploads']))
        self.assertEqual(1, len(data[2]['build_artifacts'][0]['uploads']))
        ba = data[1]['build_artifacts'][0]
        self.assertFalse(ba['is_locked'])
        self.assertIsNone(ba['lock_holder'])
        upload = data[1]['build_artifacts'][0]['uploads'][0]
        self.assertFalse(upload['is_locked'])
        self.assertIsNone(upload['lock_holder'])
        # The builds have a lot of random data
        data[1].pop('build_artifacts')
        data[2].pop('build_artifacts')
        expected = [
            {'branch': 'master',
             'canonical_name':
             'review.example.com%2Forg%2Fcommon-config/debian',
             'name': 'debian',
             'project_canonical_name': 'review.example.com/org/common-config',
             'type': 'cloud'},
            {'branch': 'master',
             'canonical_name':
             'review.example.com%2Forg%2Fcommon-config/debian-local',
             'name': 'debian-local',
             'project_canonical_name': 'review.example.com/org/common-config',
             'type': 'zuul'},
            {'branch': 'master',
             'canonical_name':
             'review.example.com%2Forg%2Fcommon-config/ubuntu-local',
             'name': 'ubuntu-local',
             'project_canonical_name': 'review.example.com/org/common-config',
             'type': 'zuul'},
            {'branch': 'master',
             'canonical_name':
             'review.example.com%2Forg%2Fcommon-config/fedora-local',
             'name': 'fedora-local',
             'project_canonical_name': 'review.example.com/org/common-config',
             'type': 'zuul'},
        ]
        self.assertEqual(expected, data)

    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_web_image_delete(self, mock_image_upload_run):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.startWebServer()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        resp = self.get_url('api/tenant/tenant-one/images')
        data = resp.json()
        self.assertEqual(4, len(data))
        self.assertNotIn('build_artifacts', data[0])
        self.assertEqual(1, len(data[1]['build_artifacts']))
        self.assertEqual(1, len(data[2]['build_artifacts']))
        self.assertEqual(1, len(data[1]['build_artifacts'][0]['uploads']))
        self.assertEqual(1, len(data[2]['build_artifacts'][0]['uploads']))
        art = data[1]['build_artifacts'][0]
        # Test that unauthenticated access fails
        resp = self.delete_url(
            f"api/tenant/tenant-one/image-build-artifact/{art['uuid']}"
        )
        self.assertEqual(401, resp.status_code, resp.text)
        # Test that the wrong tenant fails, even with auth
        token = self._getToken(['tenant-two'])
        resp = self.delete_url(
            f"api/tenant/tenant-two/image-build-artifact/{art['uuid']}",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(404, resp.status_code, resp.text)
        # Do it again with auth
        token = self._getToken(['tenant-one'])
        resp = self.delete_url(
            f"api/tenant/tenant-one/image-build-artifact/{art['uuid']}",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(204, resp.status_code, resp.text)
        for _ in iterate_timeout(10, "artifact to be deleted"):
            resp = self.get_url('api/tenant/tenant-one/images')
            data = resp.json()
            if 'build_artifacts' not in data[1]:
                break

    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_web_upload_delete(self, mock_image_upload_run):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.startWebServer()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        resp = self.get_url('api/tenant/tenant-one/images')
        data = resp.json()
        self.assertEqual(4, len(data))
        self.assertNotIn('build_artifacts', data[0])
        self.assertEqual(1, len(data[1]['build_artifacts']))
        self.assertEqual(1, len(data[2]['build_artifacts']))
        self.assertEqual(1, len(data[1]['build_artifacts'][0]['uploads']))
        self.assertEqual(1, len(data[2]['build_artifacts'][0]['uploads']))
        upload = data[1]['build_artifacts'][0]['uploads'][0]
        # Test that unauthenticated access fails
        resp = self.delete_url(
            f"api/tenant/tenant-one/image-upload/{upload['uuid']}"
        )
        self.assertEqual(401, resp.status_code, resp.text)
        # Test that the wrong tenant fails, even with auth
        token = self._getToken(['tenant-two'])
        resp = self.delete_url(
            f"api/tenant/tenant-two/image-upload/{upload['uuid']}",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(404, resp.status_code, resp.text)
        # Do it again with auth
        token = self._getToken(['tenant-one'])
        resp = self.delete_url(
            f"api/tenant/tenant-one/image-upload/{upload['uuid']}",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(204, resp.status_code, resp.text)
        for _ in iterate_timeout(30, "artifact to be deleted"):
            resp = self.get_url('api/tenant/tenant-one/images')
            data = resp.json()
            if 'build_artifacts' not in data[1]:
                break

    @simple_layout('layouts/nodepool-image-validate.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="ami-785db401")
    def test_web_upload_validate(self, mock_image_upload_run):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.executor_server.hold_jobs_in_build = True
        self.startWebServer()
        self.waitUntilSettled()

        self.executor_server.release('build-debian-local-image')
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
        ])

        nodes = self.launcher.api.getProviderNodes()
        self.assertEqual(len(nodes), 2)
        for node in nodes:
            self.assertEqual(node.create_state['image_external_id'],
                             mock_image_upload_run.return_value)

        self.assertEqual(len(self.builds), 2)
        for build in self.builds:
            build.should_fail = True

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='validate-debian-local-image', result='FAILURE'),
            dict(name='validate-debian-local-image', result='FAILURE'),
        ])

        resp = self.get_url('api/tenant/tenant-one/images')
        data = resp.json()
        self.assertEqual(1, len(data))
        self.assertEqual(1, len(data[0]['build_artifacts']))
        self.assertEqual(2, len(data[0]['build_artifacts'][0]['uploads']))
        upload = data[0]['build_artifacts'][0]['uploads'][0]
        self.assertFalse(upload['validated'])
        self.assertIsNotNone(upload['build_uuid'])

        # Now retrigger validation
        url = f"api/tenant/tenant-one/image-upload/{upload['uuid']}/validate"
        # Test that unauthenticated access fails
        resp = self.post_url(url)
        self.assertEqual(401, resp.status_code, resp.text)
        # Do it again with auth
        token = self._getToken(['tenant-one'])
        resp = self.post_url(
            url,
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        self.waitUntilSettled("image validate")
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='validate-debian-local-image', result='FAILURE'),
            dict(name='validate-debian-local-image', result='FAILURE'),
            dict(name='validate-debian-local-image', result='SUCCESS'),
        ])

        resp = self.get_url('api/tenant/tenant-one/images')
        data = resp.json()
        self.assertEqual(1, len(data))
        self.assertEqual(1, len(data[0]['build_artifacts']))
        self.assertEqual(2, len(data[0]['build_artifacts'][0]['uploads']))
        for new_upload in data[0]['build_artifacts'][0]['uploads']:
            if new_upload['uuid'] == upload['uuid']:
                break
        else:
            raise Exception("Upload not found")
        self.assertTrue(new_upload['validated'])
        self.assertIsNotNone(new_upload['build_uuid'])

    @simple_layout('layouts/nodepool-image.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @okay_tracebacks('Upload test failure')
    def test_web_upload_retry(self):
        self.startWebServer()
        self.executor_server.hold_jobs_in_build = False
        self._ubuntu_images = []

        def _upload_run(this, *args, **kw):
            # Fail on first upload of this image
            if 'ubuntu-local' in this.image_name:
                upload_id = this.metadata['zuul_upload_uuid']
                upload = self.launcher.image_upload_registry.getItem(upload_id)
                artifact_id = upload.artifact_uuid
                if artifact_id not in self._ubuntu_images:
                    self._ubuntu_images.append(artifact_id)
                if self._ubuntu_images[0] == artifact_id:
                    self._addFinishedUpload(upload_id)
                    raise Exception("Upload test failure")
            return "test_external_id"

        with mock.patch.object(
                zuul.driver.aws.awsendpoint.AwsImageUploadJob,
                'run',
                _upload_run):
            self.addImageBuildEvent(
                'tenant-one',
                'review.example.com/org/common-config',
                'master',
                ['ubuntu-local'],
            )
            self.waitUntilSettled()

            self.assertHistory([
                dict(name='build-ubuntu-local-image', result='SUCCESS'),
            ])
            cname = 'review.example.com%2Forg%2Fcommon-config/ubuntu-local'
            self.waitForUploads(cname, failed=1)
            # The web cache can be slightly out of sync with the
            # launcher, so wait for it to catch up.
            for _ in iterate_timeout(10, "failed upload"):
                resp = self.get_url('api/tenant/tenant-one/images')
                data = resp.json()
                images = [i for i in data if i['canonical_name'] == cname]
                if not images:
                    continue
                artifacts = images[0]['build_artifacts']
                if not len(artifacts) == 1:
                    continue
                uploads = artifacts[0]['uploads']
                if not len(uploads) == 1:
                    continue
                if uploads[0]['state'] != 'failed':
                    continue
                upload = uploads[0]
                break

        def _upload_run(this, *args, **kw):
            # Fail on first upload of this image
            return "test_external_id"

        with mock.patch.object(
                zuul.driver.aws.awsendpoint.AwsImageUploadJob,
                'run',
                _upload_run):
            # Now retrigger upload
            url = f"api/tenant/tenant-one/image-upload/{upload['uuid']}/upload"
            # Test that unauthenticated access fails
            resp = self.post_url(url)
            self.assertEqual(401, resp.status_code, resp.text)
            # Do it again with auth
            token = self._getToken(['tenant-one'])
            resp = self.post_url(
                url,
                headers={'Authorization': 'Bearer %s' % token})
            self.assertEqual(200, resp.status_code, resp.text)
            self.waitUntilSettled("image retry")

            self.waitForUploads(cname, ready=1)
            for _ in iterate_timeout(10, "ready upload"):
                resp = self.get_url('api/tenant/tenant-one/images')
                data = resp.json()
                images = [i for i in data if i['canonical_name'] == cname]
                if not images:
                    continue
                artifacts = images[0]['build_artifacts']
                if not len(artifacts) == 1:
                    continue
                uploads = artifacts[0]['uploads']
                if not len(uploads) == 1:
                    continue
                if uploads[0]['state'] != 'ready':
                    continue
                break

    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_web_image_post(self, mock_image_upload_run):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.startWebServer()
        self.executor_server.hold_jobs_in_build = False
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        resp = self.get_url('api/tenant/tenant-one/images')
        data = resp.json()
        self.assertEqual(4, len(data))
        self.assertNotIn('build_artifacts', data[0])
        self.assertEqual(1, len(data[1]['build_artifacts']))
        self.assertEqual(1, len(data[2]['build_artifacts']))
        self.assertEqual(1, len(data[1]['build_artifacts'][0]['uploads']))
        self.assertEqual(1, len(data[2]['build_artifacts'][0]['uploads']))
        # Test that unauthenticated access fails
        resp = self.post_url(
            "api/tenant/tenant-one/image/ubuntu-local/build"
        )
        self.assertEqual(401, resp.status_code, resp.text)
        # Do it again with auth
        token = self._getToken(['tenant-one'])
        resp = self.post_url(
            "api/tenant/tenant-one/image/ubuntu-local/build",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        self.waitUntilSettled("image rebuild")
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        # Try again with the wrong tenant
        token = self._getToken(['tenant-two'])
        resp = self.post_url(
            "api/tenant/tenant-two/image/ubuntu-local/build",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        self.waitUntilSettled("image rebuild")
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_web_image_post_duplicate(self, mock_image_upload_run):
        # Test that we can enqueue multiple items for the same project
        # if they are different image builds.
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.startWebServer()
        self.executor_server.hold_jobs_in_build = False
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        self.executor_server.hold_jobs_in_build = True
        token = self._getToken(['tenant-one'])
        resp = self.post_url(
            "api/tenant/tenant-one/image/ubuntu-local/build",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        resp = self.post_url(
            "api/tenant/tenant-one/image/ubuntu-local/build",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        resp = self.post_url(
            "api/tenant/tenant-one/image/debian-local/build",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        self.waitUntilSettled("queues empty")
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled("image rebuild")
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-debian-local-image', result='SUCCESS'),
        ], ordered=False)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_web_flavors(self):
        self.waitUntilSettled()
        self.startWebServer()
        resp = self.get_url('api/tenant/tenant-one/flavors')
        data = resp.json()
        self.assertEqual(4, len(data))

        cc = 'review.example.com%2Forg%2Fcommon-config'
        expected = [
            {'canonical_name': f'{cc}/normal',
             'description': None,
             'name': 'normal'},
            {'canonical_name': f'{cc}/large',
             'description': None,
             'name': 'large'},
            {'canonical_name': f'{cc}/dedicated',
             'description': None,
             'name': 'dedicated'},
            {'canonical_name': f'{cc}/invalid',
             'description': None,
             'name': 'invalid'},
        ]
        self.assertEqual(expected, data)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_web_labels(self):
        self.waitUntilSettled()
        self.startWebServer()
        resp = self.get_url('api/tenant/tenant-one/labels')
        data = resp.json()
        self.assertEqual(5, len(data))

        cc = 'review.example.com%2Forg%2Fcommon-config'
        expected = [
            {'name': 'label1'},
            {'canonical_name': f'{cc}/debian-normal',
             'description': None,
             'name': 'debian-normal'},
            {'canonical_name': f'{cc}/debian-large',
             'description': None,
             'name': 'debian-large'},
            {'canonical_name': f'{cc}/debian-dedicated',
             'description': None,
             'name': 'debian-dedicated'},
            {'canonical_name': f'{cc}/debian-invalid',
             'description': None,
             'name': 'debian-invalid'},
        ]
        self.assertEqual(expected, data)

    @simple_layout('layouts/nodepool-min-ready.yaml', enable_nodepool=True)
    def test_web_nodes_list_niz(self):
        self.waitUntilSettled()
        self.startWebServer()
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        data = self.get_url('api/tenant/tenant-one/nodes').json()
        self.assertEqual(len(data), 2)
        pname = 'review.example.com%2Forg%2Fcommon-config/aws-us-east-1-main'
        for node in data:
            self.assertEqual(pname, node["provider"])
        labels = collections.Counter(x['label'] for x in data)
        self.assertEqual({'debian-normal': 1, 'debian-large': 1},
                         labels)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_web_nodes_hold_delete(self):
        # This tests 3 things (since they are lifecycle related):
        # * Setting the node state to hold
        # * Setting the node state to delete
        # * Deleting the request
        self.waitUntilSettled()
        self.startWebServer()
        ctx = self.createZKContext(None)

        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.state,
                         zuul.model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request.nodes), 1)

        nodes = self.get_url('api/tenant/tenant-one/nodes').json()
        self.assertEqual(len(nodes), 1)

        # Test that unauthenticated access fails
        resp = self.put_url(
            f"api/tenant/tenant-one/nodes/{nodes[0]['uuid']}",
            json={'state': 'hold'},
        )
        self.assertEqual(401, resp.status_code)

        token = self._getToken(['tenant-one'])
        resp = self.put_url(
            f"api/tenant/tenant-one/nodes/{nodes[0]['uuid']}",
            headers={'Authorization': 'Bearer %s' % token},
            json={'state': 'hold'},
        )
        self.assertEqual(201, resp.status_code)
        self.waitUntilSettled()

        # Now work on deleting the request so that it can move to the
        # hold state.
        requests = self.get_url(
            'api/tenant/tenant-one/nodeset-requests').json()
        self.assertEqual(len(requests), 1)
        self.assertEqual(request.uuid, requests[0]['uuid'])
        # Test that unauthenticated access fails
        resp = self.delete_url(
            f"api/tenant/tenant-one/nodeset-requests/{requests[0]['uuid']}",
        )
        self.assertEqual(401, resp.status_code)

        resp = self.delete_url(
            f"api/tenant/tenant-one/nodeset-requests/{requests[0]['uuid']}",
            headers={'Authorization': 'Bearer %s' % token},
        )
        self.assertEqual(204, resp.status_code)
        self.waitUntilSettled()

        for _ in iterate_timeout(10, "request to be deleted"):
            try:
                request.refresh(ctx)
            except NoNodeError:
                break

        node = self.launcher.api.nodes_cache.getItem(nodes[0]['uuid'])
        for _ in iterate_timeout(10, "node to hold"):
            node.refresh(ctx)
            if node.state == node.State.HOLD:
                break

        resp = self.put_url(
            f"api/tenant/tenant-one/nodes/{nodes[0]['uuid']}",
            headers={'Authorization': 'Bearer %s' % token},
            json={'state': 'used'},
        )
        self.assertEqual(201, resp.status_code)
        self.waitUntilSettled()

        for _ in iterate_timeout(10, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_web_nodeset_list(self):
        self.waitUntilSettled()
        self.hold_nodeset_requests_in_queue = True
        self.startWebServer()
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        data = self.get_url('api/tenant/tenant-one/nodeset-requests').json()
        self.assertEqual(len(data), 1)
        for k in ['uuid', 'buildset_uuid', 'request_time',
                  'zuul_event_id']:
            # This asserts the keys are present, but the data are
            # random so we don't check below.
            del data[0][k]
        expected = {
            'state': 'test-hold',
            'pipeline_name': 'gate',
            'job_name': 'check-job',
            'labels': ['debian-normal'],
            'priority': 100,
            'image_names': None,
            'image_upload_uuid': None,
            'relative_priority': 0,
            'provider_node_data': [],
            'is_locked': False,
            'lock_holder': None,
        }
        self.assertEqual(expected, data[0])
        self.hold_nodeset_requests_in_queue = False
        self.releaseNodesetRequests()
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()


class TestWebStatusDisplayBranch(BaseTestWeb):
    tenant_config_file = 'config/change-queues/main.yaml'

    def add_changes(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        C = self.fake_gerrit.addFakeChange('org/project3', 'master', 'C')
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        D = self.fake_gerrit.addFakeChange('org/project4', 'master', 'D')
        D.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(D.addApproval('Approved', 1))

    def test_web_status_display_branch(self):
        "Test that we can retrieve JSON status info with branch name"
        self.add_changes()
        self.waitUntilSettled()
        resp = self.get_url("api/tenant/tenant-one/status")
        self.executor_server.release()
        data = resp.json()

        for p in data['pipelines']:
            if p['name'] == 'gate':
                for q in p['change_queues']:
                    # 'per-branch: true' is configured for all gate queues
                    self.assertEqual(q['branch'], 'master')

    def test_web_status_no_empty_queues(self):
        "Test that we the status JSON doesn't contain empty change queues"
        self.executor_server.hold_jobs_in_build = False

        # Run some builds so the change queues are used
        self.add_changes()
        self.waitUntilSettled()

        resp = self.get_url("api/tenant/tenant-one/status")
        data = resp.json()
        for p in data['pipelines']:
            if p['name'] == 'gate':
                self.assertEqual([], p['change_queues'])


class TestWebMultiTenant(BaseTestWeb):
    tenant_config_file = 'config/multi-tenant/main.yaml'

    def test_tenant_reconfigure_command(self):
        # The 'zuul-scheduler tenant-reconfigure' and full-reconfigure
        # are used to correct problems, and as such they clear the
        # branch cache.  Until the reconfiguration is complete,
        # zuul-web will be unable to load configuration for any tenant
        # which has projects that have been cleared from the branch
        # cache.  This test verifies that we retry that operation
        # after encountering missing branch errors.
        sched = self.scheds.first.sched
        web = self.web.web
        # Don't perform any automatic config updates on zuul web so
        # that we can control the sequencing.
        self.web.web._system_config_running = False
        self.web.web.system_config_cache_wake_event.set()
        self.web.web.system_config_thread.join()

        first_state = sched.tenant_layout_state.get('tenant-one')
        self.assertEqual(first_state,
                         web.local_layout_state.get('tenant-one'))

        data = self.get_url('api/tenant/tenant-one/jobs').json()
        self.assertEqual(len(data), 4)

        # Reconfigure tenant-one so that the layout state will be
        # different and we can start a layout update in zuul-web
        # later.
        self.log.debug("Reconfigure tenant-one")
        self.scheds.first.tenantReconfigure(['tenant-one'])
        self.waitUntilSettled()
        self.log.debug("Done reconfigure tenant-one")

        second_state = sched.tenant_layout_state.get('tenant-one')
        self.assertEqual(second_state,
                         sched.local_layout_state.get('tenant-one'))
        self.assertEqual(first_state,
                         web.local_layout_state.get('tenant-one'))

        self.log.debug("Grab write lock for tenant-two")
        with tenant_write_lock(self.zk_client, 'tenant-two') as lock:
            # Start a reconfiguration of tenant-two; allow it to
            # proceed past the point that the branch cache is cleared
            # and is waiting on the lock we hold.
            self.scheds.first.tenantReconfigure(
                ['tenant-two'], command_socket=True)
            for _ in iterate_timeout(30, "reconfiguration to start"):
                if 'RECONFIG' in lock.contenders():
                    break
            # Now that the branch cache is cleared as part of the
            # tenant-two reconfiguration, allow zuul-web to
            # reconfigure tenant-one.  This should produce an error
            # because of the missing branch cache.
            self.log.debug("Web update layout 1")
            self.web.web.updateSystemConfig()
            self.assertFalse(self.web.web.updateLayout())
            self.log.debug("Web update layout done")

            self.assertEqual(second_state,
                             sched.local_layout_state.get('tenant-one'))
            self.assertEqual(first_state,
                             web.local_layout_state.get('tenant-one'))

            # Make sure we can still access tenant-one's config via
            # zuul-web
            data = self.get_url('api/tenant/tenant-one/jobs').json()
            self.assertEqual(len(data), 4)
        self.log.debug("Release write lock for tenant-two")
        for _ in iterate_timeout(30, "reconfiguration to finish"):
            if 'RECONFIG' not in lock.contenders():
                break

        self.log.debug("Web update layout 2")
        self.web.web.updateSystemConfig()
        self.web.web.updateLayout()
        self.log.debug("Web update layout done")

        # Depending on tenant order, we may need to run one more time
        self.log.debug("Web update layout 3")
        self.web.web.updateSystemConfig()
        self.assertTrue(self.web.web.updateLayout())
        self.log.debug("Web update layout done")

        self.assertEqual(second_state,
                         sched.local_layout_state.get('tenant-one'))
        self.assertEqual(second_state,
                         web.local_layout_state.get('tenant-one'))
        data = self.get_url('api/tenant/tenant-one/jobs').json()
        self.assertEqual(len(data), 4)

    def test_web_labels_allowed_list(self):
        labels = ["tenant-one-label", "fake", "tenant-two-label"]
        self.fake_nodepool.registerLauncher(labels, "FakeLauncher2")
        # Tenant-one has label restriction in place on tenant-two
        res = self.get_url('api/tenant/tenant-one/labels').json()
        self.assertEqual([{'name': 'fake'}, {'name': 'tenant-one-label'}], res)
        # Tenant-two has label restriction in place on tenant-one
        expected = ["label1", "fake", "tenant-two-label"]
        res = self.get_url('api/tenant/tenant-two/labels').json()
        self.assertEqual(
            list(map(lambda x: {'name': x}, sorted(expected))), res)

    def test_tenant_add_remove(self):
        "Test that tenants are correctly added/removed to/from the layout"
        # Disable tenant list caching
        self.web.web.api.cache_expiry = 0
        resp = self.get_url("api/tenants")
        data = resp.json()
        self.assertEqual(sorted(d["name"] for d in data),
                         sorted(["tenant-one", "tenant-two", "tenant-three"]))

        self.newTenantConfig('config/multi-tenant/main-reconfig.yaml')
        self.scheds.first.smartReconfigure(command_socket=True)
        self.waitUntilSettled()

        for _ in iterate_timeout(
                10, "tenants to be updated from zuul-web"):
            if ('tenant-three' not in self.web.web.local_layout_state and
                    'tenant-four' in self.web.web.local_layout_state):
                break

        self.assertNotIn('tenant-three', self.web.web.abide.tenants)
        self.assertIn('tenant-four', self.web.web.abide.tenants)

        resp = self.get_url("api/tenants")
        data = resp.json()
        self.assertEqual(sorted(d["name"] for d in data),
                         sorted(["tenant-one", "tenant-two", "tenant-four"]))


class TestWebGlobalSemaphores(BaseTestWeb):
    tenant_config_file = 'config/global-semaphores-config/main.yaml'

    def test_web_semaphores(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertBuilds([
            dict(name='test-global-semaphore', changes='1,1'),
            dict(name='test-common-semaphore', changes='1,1'),
            dict(name='test-project1-semaphore', changes='1,1'),
            dict(name='test-global-semaphore', changes='2,1'),
            dict(name='test-common-semaphore', changes='2,1'),
            dict(name='test-project2-semaphore', changes='2,1'),
        ])

        tenant1_buildset_uuid = self.builds[0].parameters['zuul']['buildset']
        data = self.get_url('api/tenant/tenant-one/semaphores').json()

        expected = [
            {'name': 'common-semaphore',
             'global': False,
             'max': 10,
             'holders': {
                 'count': 1,
                 'this_tenant': [
                     {'buildset_uuid': tenant1_buildset_uuid,
                      'job_name': 'test-common-semaphore'}
                 ],
                 'other_tenants': 0
             }},
            {'name': 'global-semaphore',
             'global': True,
             'max': 100,
             'holders': {
                 'count': 2,
                 'this_tenant': [
                     {'buildset_uuid': tenant1_buildset_uuid,
                      'job_name': 'test-global-semaphore'}
                 ],
                 'other_tenants': 1
             }},
            {'name': 'project1-semaphore',
             'global': False,
             'max': 11,
             'holders': {
                 'count': 1,
                 'this_tenant': [
                     {'buildset_uuid': tenant1_buildset_uuid,
                      'job_name': 'test-project1-semaphore'}
                 ],
                 'other_tenants': 0
             }}
        ]
        self.assertEqual(expected, data)


class TestEmptyConfig(BaseTestWeb):
    tenant_config_file = 'config/empty-config/main.yaml'

    def test_empty_config_startup(self):
        # Test that we can bootstrap a tenant with an empty config

        resp = self.get_url("api/tenant/tenant-one/jobs").json()
        self.assertEqual(len(resp), 1)
        self.commitConfigUpdate(
            'common-config',
            'config/empty-config/git/common-config/new-zuul.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        layout_scheduler = self.scheds.first.sched.local_layout_state.get(
            'tenant-one')
        for _ in iterate_timeout(10, "local layout of zuul-web to be updated"):
            layout_web = self.web.web.local_layout_state.get('tenant-one')
            if layout_web == layout_scheduler:
                break

        resp = self.get_url("api/tenant/tenant-one/jobs").json()
        self.assertEqual(len(resp), 3)


class TestWebSecrets(BaseTestWeb):
    tenant_config_file = 'config/secrets/main.yaml'

    def test_web_find_job_secret(self):
        data = self.get_url('api/tenant/tenant-one/job/project1-secret').json()
        run = data[0]['run']
        secret = {'name': 'project1_secret', 'alias': 'secret_name'}
        self.assertEqual([secret], run[0]['secrets'])

    def test_freeze_job_redacted(self):
        # Test that ssh_keys and secrets are redacted
        resp = self.get_url(
            "api/tenant/tenant-one/pipeline/check"
            "/project/org/project1/branch/master/freeze-job/"
            "project1-secret").json()
        self.assertEqual(
            {'secret_name': 'REDACTED'}, resp['playbooks'][0]['secrets'])
        self.assertEqual('REDACTED', resp['ssh_keys'][0])


class TestInfo(BaseTestWeb):

    config_file = 'zuul-sql-driver-mysql.conf'

    def setUp(self):
        super(TestInfo, self).setUp()
        web_config = self.config_ini_data.get('web', {})
        self.websocket_url = web_config.get('websocket_url')
        self.stats_url = web_config.get('stats_url')
        statsd_config = self.config_ini_data.get('statsd', {})
        self.stats_prefix = statsd_config.get('prefix')

    def _expected_info(self, niz=None):
        ret = {
            "info": {
                "capabilities": {
                    "job_history": True,
                    "auth": {
                        "realms": {},
                        "default_realm": None,
                        "read_protected": False,
                        "auth_log_file_requests": False,
                    }
                },
                "stats": {
                    "url": self.stats_url,
                    "prefix": self.stats_prefix,
                    "type": "graphite",
                },
                "websocket_url": self.websocket_url,
            }
        }
        if niz is not None:
            ret['info']['capabilities']['auth']['niz'] = niz
        return ret

    def test_info(self):
        resp = self.get_url("api/info")
        info = resp.json()

        self.assertTrue("Cache-Control" in resp.headers)
        self.assertEqual(
            resp.headers["Cache-Control"],
            f"public, max-age={self.web.web.static_cache_expiry}"
        )
        self.assertTrue("Last-Modified" in resp.headers)
        self.assertEqual(
            resp.headers["Last-Modified"],
            self.web.web.start_time.strftime('%a, %d %b %Y %X GMT')
        )

        self.assertEqual(
            info, self._expected_info())

    def test_tenant_info(self):
        resp = self.get_url("api/tenant/tenant-one/info")
        info = resp.json()

        self.assertTrue("Cache-Control" in resp.headers)
        self.assertEqual(
            resp.headers["Cache-Control"],
            f"public, max-age={self.web.web.static_cache_expiry}"
        )
        self.assertTrue("Last-Modified" in resp.headers)
        self.assertEqual(
            resp.headers["Last-Modified"],
            self.web.web.start_time.strftime('%a, %d %b %Y %X GMT')
        )

        expected_info = self._expected_info(niz=False)
        expected_info['info']['tenant'] = 'tenant-one'
        self.assertEqual(
            info, expected_info)


class TestWebCapabilitiesInfo(TestInfo):

    config_file = 'zuul-admin-web-oidc.conf'

    def _expected_info(self, niz=None):
        info = super(TestWebCapabilitiesInfo, self)._expected_info()
        info['info']['capabilities']['auth'] = {
            'realms': {
                'myOIDC1': {
                    'authority': 'http://oidc1',
                    'client_id': 'zuul',
                    'type': 'JWT',
                    'scope': 'openid profile',
                    'driver': 'OpenIDConnect',
                    'load_user_info': True,
                },
                'myOIDC2': {
                    'authority': 'http://oidc2',
                    'client_id': 'zuul',
                    'type': 'JWT',
                    'scope': 'openid profile email special-scope',
                    'driver': 'OpenIDConnect',
                    'load_user_info': True,
                },
                'zuul.example.com': {
                    'authority': 'zuul_operator',
                    'client_id': 'zuul.example.com',
                    'type': 'JWT',
                    'driver': 'HS256',
                }
            },
            'default_realm': 'myOIDC1',
            'read_protected': False,
            'auth_log_file_requests': False,
        }
        if niz is not None:
            info['info']['capabilities']['auth']['niz'] = niz
        return info


class TestTenantAuthRealmInfo(TestWebCapabilitiesInfo):

    tenant_config_file = 'config/authorization/rules-templating/main.yaml'

    def test_tenant_info(self):
        expected_info = self._expected_info(niz=False)
        info = self.get_url("api/tenant/tenant-zero/info").json()
        expected_info['info']['tenant'] = 'tenant-zero'
        expected_info['info']['capabilities']['auth']['default_realm'] =\
            'myOIDC1'
        self.assertEqual(expected_info,
                         info,
                         info)
        info = self.get_url("api/tenant/tenant-one/info").json()
        expected_info['info']['tenant'] = 'tenant-one'
        expected_info['info']['capabilities']['auth']['default_realm'] =\
            'myOIDC1'
        self.assertEqual(expected_info,
                         info,
                         info)
        info = self.get_url("api/tenant/tenant-two/info").json()
        expected_info['info']['tenant'] = 'tenant-two'
        expected_info['info']['capabilities']['auth']['default_realm'] =\
            'myOIDC2'
        self.assertEqual(expected_info,
                         info,
                         info)


class TestRootAuth(TestWebCapabilitiesInfo):

    tenant_config_file = 'config/authorization/api-root/main.yaml'

    def test_info(self):
        # This overrides the test in TestInfo
        expected_info = self._expected_info()
        info = self.get_url("api/info").json()
        expected_info['info']['capabilities']['auth']['default_realm'] =\
            'myOIDC2'
        self.assertEqual(expected_info, info)

    def test_tenant_info(self):
        expected_info = self._expected_info(niz=False)
        info = self.get_url("api/tenant/tenant-zero/info").json()
        expected_info['info']['tenant'] = 'tenant-zero'
        expected_info['info']['capabilities']['auth']['default_realm'] =\
            'myOIDC1'
        self.assertEqual(expected_info,
                         info,
                         info)
        info = self.get_url("api/tenant/tenant-one/info").json()
        expected_info['info']['tenant'] = 'tenant-one'
        expected_info['info']['capabilities']['auth']['default_realm'] =\
            'myOIDC1'
        self.assertEqual(expected_info,
                         info,
                         info)
        info = self.get_url("api/tenant/tenant-two/info").json()
        expected_info['info']['tenant'] = 'tenant-two'
        expected_info['info']['capabilities']['auth']['default_realm'] =\
            'myOIDC2'
        self.assertEqual(expected_info,
                         info,
                         info)
        # Re-check tenant zero to make sure that the dict mutation
        # that happens when accessing the other tenants has not
        # affected it (it has no tenant default realm so does not
        # override what is in the original capabilities dict).
        info = self.get_url("api/tenant/tenant-zero/info").json()
        expected_info['info']['tenant'] = 'tenant-zero'
        expected_info['info']['capabilities']['auth']['default_realm'] =\
            'myOIDC1'
        self.assertEqual(expected_info,
                         info,
                         info)


class TestTenantInfoConfigBroken(BaseTestWeb):

    tenant_config_file = 'config/broken/main.yaml'

    def test_tenant_info_broken_config(self):
        tenant_status = self.get_url(
            "api/tenant/tenant-broken/tenant-status").json()
        self.assertEqual(tenant_status['config_error_count'], 2)

        config_errors = self.get_url(
            "api/tenant/tenant-broken/config-errors").json()
        self.assertEqual(
            len(config_errors), 2)

        self.assertEqual(
            config_errors[0]['source_context']['project'], 'org/project3')
        self.assertIn('Zuul encountered an error while accessing the repo '
                      'org/project3',
                      config_errors[0]['error'])

        self.assertEqual(
            config_errors[1]['source_context']['project'], 'org/project2')
        self.assertEqual(
            config_errors[1]['source_context']['branch'], 'master')
        self.assertEqual(
            config_errors[1]['source_context']['path'], '.zuul.yaml')
        self.assertIn('Zuul encountered a syntax error',
                      config_errors[1]['error'])

        config_errors = self.get_url(
            "api/tenant/tenant-broken/"
            "config-errors?project=org/project2").json()
        self.assertEqual(
            len(config_errors), 1)
        config_errors = self.get_url(
            "api/tenant/tenant-broken/config-errors?branch=master").json()
        self.assertEqual(
            len(config_errors), 1)
        config_errors = self.get_url(
            "api/tenant/tenant-broken/config-errors?severity=error").json()
        self.assertEqual(
            len(config_errors), 2)
        config_errors = self.get_url(
            "api/tenant/tenant-broken/config-errors?name=Unknown").json()
        self.assertEqual(
            len(config_errors), 2)
        config_errors = self.get_url(
            "api/tenant/tenant-broken/config-errors?skip=1&limit=1").json()
        self.assertEqual(
            len(config_errors), 1)

        resp = self.get_url("api/tenant/non-tenant/config-errors")
        self.assertEqual(404, resp.status_code)


class TestBrokenConfigCache(ZuulTestCase, WebMixin):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_broken_config_cache(self):
        # Delete the cached config files from ZK to simulate a
        # scheduler encountering an error in reconfiguration.
        path = '/zuul/config/cache/review.example.com%2Forg%2Fproject'
        self.assertIsNotNone(self.zk_client.client.exists(path))
        self.zk_client.client.delete(path, recursive=True)
        self.startWebServer()
        config_errors = self.get_url(
            "api/tenant/tenant-one/config-errors").json()
        self.assertIn('Configuration files missing',
                      config_errors[0]['error'])

    @okay_tracebacks('_cacheTenantYAMLBranch')
    def test_web_cache_new_branch(self):
        # This tests web startup right after a new branch is
        # created.
        first = self.scheds.first
        lock1 = first.sched.layout_update_lock
        lock2 = first.sched.run_handler_lock
        with lock1, lock2:
            self.create_branch('org/project1', 'stable')
            self.fake_gerrit.addEvent(
                self.fake_gerrit.getFakeBranchCreatedEvent(
                    'org/project1', 'stable'))
            self.startWebServer()
            # startWebServer will only return after the initial layout
            # update, so if we're here, the test has already passed.
            # Verify we have a layout for good measure.
            tenant = self.web.web.abide.tenants.get('tenant-one')
            self.assertIsNotNone(tenant)
        self.waitUntilSettled()


class TestWebSocketInfo(TestInfo):

    config_ini_data = {
        'web': {
            'websocket_url': 'wss://ws.example.com'
        }
    }


class TestGraphiteUrl(TestInfo):

    config_ini_data = {
        'statsd': {
            'prefix': 'example'
        },
        'web': {
            'stats_url': 'https://graphite.example.com',
        }
    }


class TestBuildInfo(BaseTestWeb):
    config_file = 'zuul-sql-driver-mysql.conf'
    tenant_config_file = 'config/sql-driver/main.yaml'

    def test_web_list_builds(self):
        # Generate some build records in the db.
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        builds = self.get_url("api/tenant/tenant-one/builds").json()
        self.assertEqual(len(builds), 6)

        uuid = builds[0]['uuid']
        build = self.get_url("api/tenant/tenant-one/build/%s" % uuid).json()
        self.assertEqual(build['job_name'], builds[0]['job_name'])

        resp = self.get_url("api/tenant/tenant-one/build/1234")
        self.assertEqual(404, resp.status_code)

        builds_query = self.get_url("api/tenant/tenant-one/builds?"
                                    "project=org/project&"
                                    "project=org/project1").json()
        self.assertEqual(len(builds_query), 6)
        self.assertEqual(builds_query[0]['nodeset'], 'test-nodeset')

        resp = self.get_url("api/tenant/non-tenant/builds")
        self.assertEqual(404, resp.status_code)

        extrema = [int(builds[-1]['_id']), int(builds[0]['_id'])]
        idx_min = min(extrema)
        idx_max = max(extrema)
        builds_query = self.get_url("api/tenant/tenant-one/builds?"
                                    "idx_max=%i" % idx_min).json()
        self.assertEqual(len(builds_query), 1, builds_query)
        builds_query = self.get_url("api/tenant/tenant-one/builds?"
                                    "idx_min=%i" % idx_min).json()
        self.assertEqual(len(builds_query), len(builds), builds_query)
        builds_query = self.get_url("api/tenant/tenant-one/builds?"
                                    "idx_max=%i" % idx_max).json()
        self.assertEqual(len(builds_query), len(builds), builds_query)
        builds_query = self.get_url("api/tenant/tenant-one/builds?"
                                    "idx_min=%i" % idx_max).json()
        self.assertEqual(len(builds_query), 1, builds_query)

    def test_web_filter_builds_by_queue_item_uuid(self):
        # Hold the builds on the executor, so we can look up the
        # QueueItem UUID from the pipeline manager.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        manager = tenant.layout.pipeline_managers.get('gate')
        queue = None
        for queue_candidate in manager.state.queues:
            if queue_candidate.name == 'org/project':
                queue = queue_candidate
                break
        queue_item = queue.queue[0]

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # Filter the builds for the QueueItem UUID via the zuul-web API
        builds = self.get_url(
            f"api/tenant/tenant-one/builds?queue_item_uuid={queue_item.uuid}"
        ).json()
        self.assertEqual(len(builds), 3)
        self.assertEqual(builds[0]["queue_item_uuid"], queue_item.uuid)

        buildsets = self.get_url(
            "api/tenant/tenant-one/buildsets?"
            f"queue_item_uuid={queue_item.uuid}"
        ).json()
        self.assertEqual(len(buildsets), 1)
        self.assertEqual(buildsets[0]["queue_item_uuid"], queue_item.uuid)

    def test_web_substring_search_builds(self):
        # Generate some build records in the db.
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        builds = self.get_url("api/tenant/tenant-one/builds?"
                              "project=org/project*&"
                              "branch=*aster").json()
        self.assertEqual(len(builds), 6)

        builds = self.get_url("api/tenant/tenant-one/builds?"
                              "job_name=*-merge").json()
        self.assertEqual(len(builds), 2)

        builds = self.get_url("api/tenant/tenant-one/builds?"
                              "job_name=*-merge&"
                              "project=org/project1").json()
        self.assertEqual(len(builds), 1)

        builds = self.get_url("api/tenant/tenant-one/builds?"
                              "pipeline=gat*&"
                              "project=org/project1&"
                              "job_name=*-merge&"
                              "job_name=*test*").json()
        self.assertEqual(len(builds), 3)
        self.assertTrue(
            all(b["ref"]["project"] == "org/project1" for b in builds))
        self.assertEqual("project-test2", builds[0]["job_name"])
        self.assertEqual("project-test1", builds[1]["job_name"])
        self.assertEqual("project-merge", builds[2]["job_name"])

        # sql wildcard chars (% and _), as well as the escape char ($) should
        # be treated literally, i.e. they are escpaed.
        # The below query would return builds for
        # branch LIKE "%master%" OR "%mast_r"
        # if they would not be escaped
        builds = self.get_url("api/tenant/tenant-one/builds?"
                              "branch=%master%&"
                              "branch=%mast_r").json()
        self.assertEqual(len(builds), 0)

    def test_web_list_skipped_builds(self):
        # Test the exclude_result filter
        # Generate some build records in the db.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.executor_server.failJob('project-merge', A)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        builds = self.get_url("api/tenant/tenant-one/builds").json()
        builds.sort(key=lambda x: x['job_name'])
        self.assertEqual(len(builds), 3)
        self.assertEqual(builds[0]['job_name'], 'project-merge')
        self.assertEqual(builds[1]['job_name'], 'project-test1')
        self.assertEqual(builds[2]['job_name'], 'project-test2')
        self.assertEqual(builds[0]['result'], 'FAILURE')
        self.assertEqual(builds[1]['result'], 'SKIPPED')
        self.assertEqual(builds[2]['result'], 'SKIPPED')
        self.assertIsNone(builds[0]['error_detail'])
        self.assertEqual(builds[1]['error_detail'],
                         'Skipped due to failed job project-merge')
        self.assertEqual(builds[2]['error_detail'],
                         'Skipped due to failed job project-merge')

        builds = self.get_url("api/tenant/tenant-one/builds?"
                              "exclude_result=SKIPPED").json()
        self.assertEqual(len(builds), 1)
        self.assertEqual(builds[0]['job_name'], 'project-merge')
        self.assertEqual(builds[0]['result'], 'FAILURE')

        builds = self.get_url("api/tenant/tenant-one/builds?"
                              "result=SKIPPED&result=FAILURE").json()
        builds.sort(key=lambda x: x['job_name'])
        self.assertEqual(len(builds), 3)
        self.assertEqual(builds[0]['job_name'], 'project-merge')
        self.assertEqual(builds[1]['job_name'], 'project-test1')
        self.assertEqual(builds[2]['job_name'], 'project-test2')
        self.assertEqual(builds[0]['result'], 'FAILURE')
        self.assertEqual(builds[1]['result'], 'SKIPPED')
        self.assertEqual(builds[2]['result'], 'SKIPPED')

    def test_web_badge(self):
        # Generate some build records in the db.
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # Now request badge for the buildsets
        result = self.get_url("api/tenant/tenant-one/badge")

        self.log.error(result.content)
        result.raise_for_status()
        self.assertTrue(result.text.startswith('<svg '))
        self.assertIn('passing', result.text)

        # Generate a failing record
        self.executor_server.hold_jobs_in_build = True
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.executor_server.failJob('project-merge', C)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # Request again badge for the buildsets
        result = self.get_url("api/tenant/tenant-one/badge")
        self.log.error(result.content)
        result.raise_for_status()
        self.assertTrue(result.text.startswith('<svg '))
        self.assertIn('failing', result.text)

    def test_web_list_buildsets(self):
        # Generate some build records in the db.
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        buildsets = self.get_url("api/tenant/tenant-one/buildsets").json()
        self.assertEqual(2, len(buildsets))
        project_bs = [
            x for x in buildsets
            if x["refs"][0]["project"] == "org/project"
        ][0]

        buildset = self.get_url(
            "api/tenant/tenant-one/buildset/%s" % project_bs['uuid']).json()
        self.assertEqual(3, len(buildset["builds"]))

        self.assertEqual(1, len(buildset["events"]))
        self.assertEqual('triggered', buildset["events"][0]['event_type'])
        self.assertEqual('Triggered by GerritChange org/project 1,1',
                         buildset["events"][0]['description'])

        project_test1_build = [x for x in buildset["builds"]
                               if x["job_name"] == "project-test1"][0]
        self.assertEqual('SUCCESS', project_test1_build['result'])

        project_test2_build = [x for x in buildset["builds"]
                               if x["job_name"] == "project-test2"][0]
        self.assertEqual('SUCCESS', project_test2_build['result'])

        project_merge_build = [x for x in buildset["builds"]
                               if x["job_name"] == "project-merge"][0]
        self.assertEqual('SUCCESS', project_merge_build['result'])

        extrema = [int(buildsets[-1]['_id']), int(buildsets[0]['_id'])]
        idx_min = min(extrema)
        idx_max = max(extrema)
        buildsets_query = self.get_url("api/tenant/tenant-one/buildsets?"
                                       "idx_max=%i" % idx_min).json()
        self.assertEqual(len(buildsets_query), 1, buildsets_query)
        buildsets_query = self.get_url("api/tenant/tenant-one/buildsets?"
                                       "idx_min=%i" % idx_min).json()
        self.assertEqual(len(buildsets_query), len(buildsets), buildsets_query)
        buildsets_query = self.get_url("api/tenant/tenant-one/buildsets?"
                                       "idx_max=%i" % idx_max).json()
        self.assertEqual(len(buildsets_query), len(buildsets), buildsets_query)
        buildsets_query = self.get_url("api/tenant/tenant-one/buildsets?"
                                       "idx_min=%i" % idx_max).json()
        self.assertEqual(len(buildsets_query), 1, buildsets_query)

    def test_web_list_buildsets_exclude_result(self):
        # Test the exclude_result filter
        # Generate some build records in the db.
        self.executor_server.hold_jobs_in_build = False
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.executor_server.failJob('project-merge', A)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        buildsets = self.get_url("api/tenant/tenant-one/buildsets").json()
        self.assertEqual(2, len(buildsets))
        self.assertEqual("org/project1", buildsets[0]["refs"][0]["project"])
        self.assertEqual("org/project", buildsets[1]["refs"][0]["project"])
        buildsets = self.get_url("api/tenant/tenant-one/buildsets?"
                                 "exclude_result=FAILURE").json()
        self.assertEqual(1, len(buildsets))
        self.assertEqual("org/project1", buildsets[0]["refs"][0]["project"])

    def test_web_substring_search_buildsets(self):
        # Generate some build records in the db.
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        buildsets = self.get_url("api/tenant/tenant-one/buildsets?"
                                 "project=org/*&"
                                 "branch=*ster*&"
                                 "pipeline=check&"
                                 "pipeline=*ate").json()
        self.assertEqual(2, len(buildsets))
        self.assertEqual("org/project1", buildsets[0]["refs"][0]["project"])
        self.assertEqual("org/project", buildsets[1]["refs"][0]["project"])

    def test_web_list_build_times(self):
        # Generate some build records in the db.
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        builds = self.get_url("api/tenant/tenant-one/build-times").json()
        self.assertEqual(len(builds), 6)

    @simple_layout('layouts/empty-check.yaml')
    def test_build_error(self):
        conf = textwrap.dedent(
            """
            - job:
                name: test-job
                run: playbooks/dne.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - test-job
            """)

        file_dict = {'.zuul.yaml': conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        builds = self.get_url("api/tenant/tenant-one/builds").json()
        self.assertIn('Unable to find playbook',
                      builds[0]['error_detail'])


class TestArtifacts(BaseTestWeb, AnsibleZuulTestCase):
    config_file = 'zuul-sql-driver-mysql.conf'
    tenant_config_file = 'config/sql-driver/main.yaml'

    def test_artifacts(self):
        # Generate some build records in the db.
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        build_query = self.get_url("api/tenant/tenant-one/builds?"
                                   "project=org/project&"
                                   "job_name=project-test1").json()
        self.assertEqual(len(build_query), 1)
        self.assertEqual(len(build_query[0]['artifacts']), 3)
        arts = build_query[0]['artifacts']
        arts.sort(key=lambda x: x['name'])
        self.assertEqual(build_query[0]['artifacts'], [
            {'url': 'http://example.com/docs',
             'name': 'docs'},
            {'url': 'http://logs.example.com/build/relative/docs',
             'name': 'relative',
             'metadata': {'foo': 'bar'}},
            {'url': 'http://example.com/tarball',
             'name': 'tarball'},
        ])

    def test_buildset_artifacts(self):
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        buildsets = self.get_url("api/tenant/tenant-one/buildsets").json()
        project_bs = [
            x for x in buildsets
            if x["refs"][0]["project"] == "org/project"
        ][0]
        buildset = self.get_url(
            "api/tenant/tenant-one/buildset/%s" % project_bs['uuid']).json()
        self.assertEqual(3, len(buildset["builds"]))

        test1_build = [x for x in buildset["builds"]
                       if x["job_name"] == "project-test1"][0]
        arts = test1_build['artifacts']
        arts.sort(key=lambda x: x['name'])
        self.assertEqual([
            {'url': 'http://example.com/docs',
             'name': 'docs'},
            {'url': 'http://logs.example.com/build/relative/docs',
             'name': 'relative',
             'metadata': {'foo': 'bar'}},
            {'url': 'http://example.com/tarball',
             'name': 'tarball'},
        ], test1_build['artifacts'])


class TestTenantScopedWebApi(BaseTestWeb):
    config_file = 'zuul-admin-web.conf'

    def test_admin_routes_no_token(self):
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)

    def test_bad_key_JWT_token(self):
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='OnlyZuulNoDana',
                           algorithm='HS256')
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            headers={'Authorization': 'Bearer %s' % token},
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)

    def test_bad_format_JWT_token(self):
        token = 'thisisnotwhatatokenshouldbelike'
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            headers={'Authorization': 'Bearer %s' % token},
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)

    def test_expired_JWT_token(self):
        authz = {'iss': 'zuul_operator',
                 'sub': 'testuser',
                 'aud': 'zuul.example.com',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) - 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            headers={'Authorization': 'Bearer %s' % token},
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)

    def test_valid_JWT_bad_tenants(self):
        authz = {'iss': 'zuul_operator',
                 'sub': 'testuser',
                 'aud': 'zuul.example.com',
                 'zuul': {
                     'admin': ['tenant-six', 'tenant-ten', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            headers={'Authorization': 'Bearer %s' % token},
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(403, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(403, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(403, resp.status_code)

        # For autohold-delete, we first must make sure that an autohold
        # exists before the delete attempt.
        good_authz = {'iss': 'zuul_operator',
                      'aud': 'zuul.example.com',
                      'sub': 'testuser',
                      'zuul': {'admin': ['tenant-one', ]},
                      'exp': int(time.time()) + 3600}
        args = {"reason": "some reason",
                "count": 1,
                'job': 'project-test2',
                'change': None,
                'ref': None,
                'node_hold_expiration': None}
        good_token = jwt.encode(good_authz,
                                key='ThisIsABadSecretOnlyUsedForTesting',
                                algorithm='HS256')
        req = self.post_url(
            'api/tenant/tenant-one/project/org/project/autohold',
            headers={'Authorization': 'Bearer %s' % good_token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)
        resp = self.get_url(
            "api/tenant/tenant-one/autohold",
            headers={'Authorization': 'Bearer %s' % good_token})
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))
        request = autohold_requests[0]
        resp = self.delete_url(
            "api/tenant/tenant-one/autohold/%s" % request['id'],
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(403, resp.status_code)

    def _test_autohold(self, args, code=200):
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.post_url(
            'api/tenant/tenant-one/project/org/project/autohold',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(code, req.status_code, req.text)
        if code != 200:
            return
        data = req.json()
        self.assertEqual(True, data)

        # Check result
        resp = self.get_url(
            "api/tenant/tenant-one/autohold",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))
        request = autohold_requests[0]
        return request

    def test_autohold_default_ref(self):
        """Test that autohold can be set through the admin web interface
        with default ref values"""
        args = {"reason": "some reason",
                "count": 1,
                'job': 'project-test2',
                'change': None,
                'ref': None,
                'node_hold_expiration': None}
        request = self._test_autohold(args)
        self.assertEqual('tenant-one', request['tenant'])
        # The matcher expects a canonical project name
        self.assertEqual('review.example.com/org/project', request['project'])
        self.assertEqual('project-test2', request['job'])
        self.assertEqual(".*", request['ref_filter'])
        self.assertEqual("some reason", request['reason'])
        self.assertEqual(1, request['max_count'])

    def test_autohold_invalid_ref_filter(self):
        """Test that incorrect ref filters are rejected"""
        args = {"reason": "some reason",
                "count": 1,
                'job': 'project-test2',
                'change': None,
                'ref': '*abc',
                'node_hold_expiration': None}
        self._test_autohold(args, code=400)

    def test_autohold_change(self):
        """Test that autohold can be set through the admin web interface
        with a change supplied"""
        args = {"reason": "some reason",
                "count": 1,
                'job': 'project-test2',
                'change': '1',
                'ref': None,
                'node_hold_expiration': None}
        request = self._test_autohold(args)
        self.assertEqual('tenant-one', request['tenant'])
        # The matcher expects a canonical project name
        self.assertEqual('review.example.com/org/project', request['project'])
        self.assertEqual('project-test2', request['job'])
        self.assertEqual("refs/changes/01/1/.*", request['ref_filter'])
        self.assertEqual("some reason", request['reason'])
        self.assertEqual(1, request['max_count'])

    def test_autohold_ref(self):
        """Test that autohold can be set through the admin web interface
        with a change supplied"""
        args = {"reason": "some reason",
                "count": 1,
                'job': 'project-test2',
                'change': None,
                'ref': 'refs/tags/foo',
                'node_hold_expiration': None}
        request = self._test_autohold(args)
        self.assertEqual('tenant-one', request['tenant'])
        # The matcher expects a canonical project name
        self.assertEqual('review.example.com/org/project', request['project'])
        self.assertEqual('project-test2', request['job'])
        self.assertEqual("refs/tags/foo", request['ref_filter'])
        self.assertEqual("some reason", request['reason'])
        self.assertEqual(1, request['max_count'])

    def test_autohold_change_and_ref(self):
        """Test that autohold can be set through the admin web interface
        with a change supplied"""
        args = {"reason": "some reason",
                "count": 1,
                'job': 'project-test2',
                'change': '1',
                'ref': 'refs/tags/foo',
                'node_hold_expiration': None}
        self._test_autohold(args, code=400)

    def _init_autohold_delete(self, authz):
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')

        self.addAutohold('tenant-one', 'review.example.com/org/project',
                         'project-test2', '.*', 'reason text', 1, 600)

        # Use autohold-list API to retrieve request ID
        resp = self.get_url(
            "api/tenant/tenant-one/autohold",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))
        request_id = autohold_requests[0]['id']
        return request_id, token

    def test_autohold_delete_wrong_tenant(self):
        """Make sure authorization rules are applied"""
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        request_id, _ = self._init_autohold_delete(authz)
        # now try the autohold-delete API
        bad_authz = {'iss': 'zuul_operator',
                     'aud': 'zuul.example.com',
                     'sub': 'testuser',
                     'zuul': {
                         'admin': ['tenant-two', ]
                     },
                     'exp': int(time.time()) + 3600}
        bad_token = jwt.encode(bad_authz,
                               key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
        resp = self.delete_url(
            "api/tenant/tenant-one/autohold/%s" % request_id,
            headers={'Authorization': 'Bearer %s' % bad_token})
        # Throw a "Forbidden" error, because user is authenticated but not
        # authorized for tenant-one
        self.assertEqual(403, resp.status_code, resp.text)

    def test_autohold_delete_invalid_id(self):
        """Make sure authorization rules are applied"""
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        bad_token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
        resp = self.delete_url(
            "api/tenant/tenant-one/autohold/invalidid",
            headers={'Authorization': 'Bearer %s' % bad_token})
        self.assertEqual(404, resp.status_code, resp.text)

    def test_autohold_delete(self):
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        request_id, token = self._init_autohold_delete(authz)
        resp = self.delete_url(
            "api/tenant/tenant-one/autohold/%s" % request_id,
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(204, resp.status_code, resp.text)
        # autohold-list should be empty now
        resp = self.get_url(
            "api/tenant/tenant-one/autohold",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertEqual([], autohold_requests)

    def _test_enqueue(self, use_trigger=False):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)

        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        path = "api/tenant/%(tenant)s/project/%(project)s/enqueue"
        enqueue_args = {'tenant': 'tenant-one',
                        'project': 'org/project', }
        change = {'change': '1,1',
                  'pipeline': 'gate', }
        if use_trigger:
            change['trigger'] = 'gerrit'
        req = self.post_url(path % enqueue_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=change)
        # The JSON returned is the same as the client's output
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertEqual(True, data)
        self.waitUntilSettled()

    def test_enqueue_with_deprecated_trigger(self):
        """Test that the admin web interface can enqueue a change"""
        # TODO(mhu) remove in a few releases
        self._test_enqueue(use_trigger=True)

    def test_enqueue(self):
        """Test that the admin web interface can enqueue a change"""
        self._test_enqueue()

    def _test_enqueue_ref(self, use_trigger=False):
        """Test that the admin web interface can enqueue a ref"""
        p = "review.example.com/org/project"
        upstream = self.getUpstreamRepos([p])
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setMerged()
        A_commit = str(upstream[p].commit('master'))
        self.log.debug("A commit: %s" % A_commit)

        path = "api/tenant/%(tenant)s/project/%(project)s/enqueue"
        enqueue_args = {'tenant': 'tenant-one',
                        'project': 'org/project', }
        ref = {'ref': 'master',
               'oldrev': '90f173846e3af9154517b88543ffbd1691f31366',
               'newrev': A_commit,
               'pipeline': 'post', }
        if use_trigger:
            ref['trigger'] = 'gerrit'
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.post_url(path % enqueue_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=ref)
        self.assertEqual(200, req.status_code, req.text)
        # The JSON returned is the same as the client's output
        data = req.json()
        self.assertEqual(True, data)
        self.waitUntilSettled()

    def test_enqueue_ref_with_deprecated_trigger(self):
        """Test that the admin web interface can enqueue a ref"""
        # TODO(mhu) remove in a few releases
        self._test_enqueue_ref(use_trigger=True)

    def test_enqueue_ref(self):
        """Test that the admin web interface can enqueue a ref"""
        self._test_enqueue_ref()

    def test_dequeue(self):
        """Test that the admin web interface can dequeue a change"""
        start_builds = len(self.builds)
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.executor_server.hold_jobs_in_build = True
        self.commitConfigUpdate('common-config', 'layouts/timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        for _ in iterate_timeout(30, 'Wait for a build on hold'):
            if len(self.builds) > start_builds:
                break
        self.waitUntilSettled()

        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        path = "api/tenant/%(tenant)s/project/%(project)s/dequeue"
        dequeue_args = {'tenant': 'tenant-one',
                        'project': 'org/project', }
        change = {'ref': 'refs/heads/stable',
                  'pipeline': 'periodic', }
        req = self.post_url(path % dequeue_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=change)
        # The JSON returned is the same as the client's output
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertEqual(True, data)
        self.waitUntilSettled()

        self.commitConfigUpdate('common-config',
                                'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(self.countJobResults(self.history, 'ABORTED'), 1)

    def test_OPTIONS(self):
        """Ensure that protected endpoints handle CORS preflight requests
        properly"""
        # Note that %tenant, %project are not relevant here. The client is
        # just checking what the endpoint allows.
        endpoints = [
            {'action': 'promote',
             'path': 'api/tenant/my-tenant/promote',
             'allowed_methods': ['POST', ]},
            {'action': 'enqueue',
             'path': 'api/tenant/my-tenant/project/my-project/enqueue',
             'allowed_methods': ['POST', ]},
            {'action': 'enqueue_ref',
             'path': 'api/tenant/my-tenant/project/my-project/enqueue',
             'allowed_methods': ['POST', ]},
            {'action': 'autohold',
             'path': 'api/tenant/my-tenant/project/my-project/autohold',
             'allowed_methods': ['GET', 'POST', ]},
            {'action': 'autohold_by_request_id',
             'path': 'api/tenant/my-tenant/autohold/123',
             'allowed_methods': ['GET', 'DELETE', ]},
            {'action': 'dequeue',
             'path': 'api/tenant/my-tenant/project/my-project/enqueue',
             'allowed_methods': ['POST', ]},
            {'action': 'authorizations',
             'path': 'api/tenant/my-tenant/authorizations',
             'allowed_methods': ['GET', ]},
        ]
        for endpoint in endpoints:
            preflight = self.options_url(
                endpoint['path'],
                headers={'Access-Control-Request-Method': 'GET',
                         'Access-Control-Request-Headers': 'Authorization'})
            self.assertEqual(
                204,
                preflight.status_code,
                "%s failed: %s" % (endpoint['action'], preflight.text))
            self.assertEqual(
                '*',
                preflight.headers.get('Access-Control-Allow-Origin'),
                "%s failed: %s" % (endpoint['action'], preflight.headers))
            self.assertEqual(
                'Authorization, Content-Type',
                preflight.headers.get('Access-Control-Allow-Headers'),
                "%s failed: %s" % (endpoint['action'], preflight.headers))
            allowed_methods = preflight.headers.get(
                'Access-Control-Allow-Methods').split(', ')
            self.assertTrue(
                'OPTIONS' in allowed_methods,
                "%s has allowed methods: %s" % (endpoint['action'],
                                                allowed_methods))
            for method in endpoint['allowed_methods']:
                self.assertTrue(
                    method in allowed_methods,
                    "%s has allowed methods: %s,"
                    " expected: %s" % (endpoint['action'],
                                       allowed_methods,
                                       endpoint['allowed_methods']))

    def test_promote(self):
        """Test that a change can be promoted via the admin web interface"""
        # taken from test_client_promote in test_scheduler
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        C.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))

        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'gate')
        enqueue_times = {}
        for item in items:
            enqueue_times[str(item.changes[0])] = item.enqueue_time

        # REST API
        args = {'pipeline': 'gate',
                'changes': ['2,1', '3,1']}
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 3600,
                 'iat': int(time.time())}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.post_url(
            'api/tenant/tenant-one/promote',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertEqual(True, data)

        # ensure that enqueue times are durable
        items = self.getAllItems('tenant-one', 'gate')
        for item in items:
            self.assertEqual(
                enqueue_times[str(item.changes[0])], item.enqueue_time)

        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 6)
        self.assertEqual(self.builds[0].name, 'project-test1')
        self.assertEqual(self.builds[1].name, 'project-test2')
        self.assertEqual(self.builds[2].name, 'project-test1')
        self.assertEqual(self.builds[3].name, 'project-test2')
        self.assertEqual(self.builds[4].name, 'project-test1')
        self.assertEqual(self.builds[5].name, 'project-test2')

        self.assertTrue(self.builds[0].hasChanges(B))
        self.assertFalse(self.builds[0].hasChanges(A))
        self.assertFalse(self.builds[0].hasChanges(C))

        self.assertTrue(self.builds[2].hasChanges(B))
        self.assertTrue(self.builds[2].hasChanges(C))
        self.assertFalse(self.builds[2].hasChanges(A))

        self.assertTrue(self.builds[4].hasChanges(B))
        self.assertTrue(self.builds[4].hasChanges(C))
        self.assertTrue(self.builds[4].hasChanges(A))

        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.data['status'], 'MERGED')
        self.assertEqual(C.reported, 2)
        self.assertEqual(self.countJobResults(self.history, 'ABORTED'), 1)
        self.assertEqual(len(self.history), 10)

    def test_promote_no_change(self):
        """Test that jobs are not unecessarily restarted when promoting"""
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        C.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))

        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'gate')
        enqueue_times = {}
        for item in items:
            enqueue_times[str(item.changes[0])] = item.enqueue_time

        # REST API
        args = {'pipeline': 'gate',
                'changes': ['1,1', '2,1', '3,1']}
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 3600,
                 'iat': int(time.time())}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.post_url(
            'api/tenant/tenant-one/promote',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertEqual(True, data)

        # ensure that enqueue times are durable
        items = self.getAllItems('tenant-one', 'gate')
        for item in items:
            self.assertEqual(
                enqueue_times[str(item.changes[0])], item.enqueue_time)

        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 6)
        self.assertEqual(self.builds[0].name, 'project-test1')
        self.assertEqual(self.builds[1].name, 'project-test2')
        self.assertEqual(self.builds[2].name, 'project-test1')
        self.assertEqual(self.builds[3].name, 'project-test2')
        self.assertEqual(self.builds[4].name, 'project-test1')
        self.assertEqual(self.builds[5].name, 'project-test2')

        self.assertTrue(self.builds[0].hasChanges(A))
        self.assertFalse(self.builds[0].hasChanges(B))
        self.assertFalse(self.builds[0].hasChanges(C))

        self.assertTrue(self.builds[2].hasChanges(A))
        self.assertTrue(self.builds[2].hasChanges(B))
        self.assertFalse(self.builds[2].hasChanges(C))

        self.assertTrue(self.builds[4].hasChanges(A))
        self.assertTrue(self.builds[4].hasChanges(B))
        self.assertTrue(self.builds[4].hasChanges(C))

        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.data['status'], 'MERGED')
        self.assertEqual(C.reported, 2)
        # The promote should be a noop, so no canceled jobs
        self.assertEqual(self.countJobResults(self.history, 'ABORTED'), 0)
        self.assertEqual(len(self.history), 9)

    def test_promote_check(self):
        """Test that a change can be promoted via the admin web interface"""
        self.executor_server.hold_jobs_in_build = True
        # Make a patch series so that we have some non-live items in
        # the pipeline and we can make sure they are not promoted.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.setDependsOn(B, 1)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'check')
        items = [i for i in items if i.live]
        enqueue_times = {}
        for item in items:
            enqueue_times[str(item.changes[0])] = item.enqueue_time

        # REST API
        args = {'pipeline': 'check',
                'changes': ['2,1', '3,1']}
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 3600,
                 'iat': int(time.time())}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.post_url(
            'api/tenant/tenant-one/promote',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertEqual(True, data)
        self.waitUntilSettled()

        # ensure that enqueue times are durable
        items = self.getAllItems('tenant-one', 'check')
        items = [i for i in items if i.live]
        for item in items:
            self.assertEqual(
                enqueue_times[str(item.changes[0])], item.enqueue_time)

        # We can't reliably test for side effects in the check
        # pipeline since the change queues are independent, so we
        # directly examine the queues.
        items = self.getAllItems('tenant-one', 'check')
        queue_items = [(item.changes[0].number, item.live) for item in items]
        expected = [('1', False),
                    ('2', True),
                    ('1', False),
                    ('2', False),
                    ('3', True),
                    ('1', True)]
        self.assertEqual(expected, queue_items)

        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release()
        self.waitUntilSettled()
        # No jobs should be canceled
        self.assertEqual(self.countJobResults(self.history, 'ABORTED'), 0)
        self.assertEqual(len(self.history), 9)

    def test_tenant_authorizations_override(self):
        """Test that user gets overriden tenant authz if allowed"""
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one'],
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.get_url(
            'api/tenant/tenant-one/authorizations',
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertTrue('zuul' in data)
        self.assertTrue(data['zuul']['admin'], data)
        self.assertTrue(data['zuul']['scope'] == ['tenant-one'], data)
        # change tenant
        authz['zuul']['admin'] = ['tenant-whatever', ]
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.get_url(
            'api/tenant/tenant-one/authorizations',
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertTrue('zuul' in data)
        self.assertTrue(data['zuul']['admin'] is False, data)
        self.assertTrue(data['zuul']['scope'] == ['tenant-one'], data)

    def test_state_post(self):
        self.executor_server.hold_jobs_in_build = False
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 3600,
                 'iat': int(time.time())}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        args = {
            'trigger_queue_paused': True,
            'reason': 'test trigger paused',
        }
        req = self.post_url(
            'api/tenant/tenant-one/state',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)
        time.sleep(1)

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        time.sleep(5)

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(0, len(self.builds))

        args = {
            'trigger_queue_paused': True,
            'result_queue_paused': True,
            'reason': "test result paused",
        }
        req = self.post_url(
            'api/tenant/tenant-one/state',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)

        time.sleep(5)
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(0, len(self.builds))

        args = {
            'trigger_queue_paused': False,
            'result_queue_paused': False,
            'reason': None,
        }
        req = self.post_url(
            'api/tenant/tenant-one/state',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)

        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-merge', result='SUCCESS', changes='2,1'),
            dict(name='project-test1', result='SUCCESS', changes='2,1'),
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
        ], ordered=False)

        args = {
            'trigger_queue_discarding': True,
            'reason': 'test discarding',
        }
        req = self.post_url(
            'api/tenant/tenant-one/state',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)
        time.sleep(1)

        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-merge', result='SUCCESS', changes='2,1'),
            dict(name='project-test1', result='SUCCESS', changes='2,1'),
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
        ], ordered=False)

    @simple_layout('layouts/parameterized-builds.yaml')
    def test_web_parameterized_build(self):
        token = self._getToken(['tenant-one'])
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/freeze-pipeline",
            json={
                'ref': 'refs/heads/master',
                'pipeline': 'on-demand',
            })
        data = resp.json()
        self.assertEqual([
            {'default': None,
             'description': 'Which cluster to deploy to',
             'name': 'target_cluster',
             'required': False,
             'type': 'selection',
             'values': ['production', 'staging', 'dev']},
            {'default': None,
             'description': 'What version to deploy',
             'name': 'version',
             'required': False,
             'type': 'string',
             'values': None},
        ], data['parameters'])

        expected_params = {
            'target_cluster': 'production',
            'version': '1.0',
        }
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={
                'ref': 'refs/heads/master',
                'pipeline': 'on-demand',
                'parameters': expected_params,
            })
        self.assertEqual(200, resp.status_code)
        self.waitUntilSettled()
        params = self.builds[0].job.variables
        self.assertEqual(expected_params, params)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='deploy-app', result='SUCCESS', ref='refs/heads/master'),
        ], ordered=False)

        buildsets = self.get_url("api/tenant/tenant-one/buildsets").json()
        self.assertEqual(1, len(buildsets))
        buildset = self.get_url(
            "api/tenant/tenant-one/buildset/%s" % buildsets[0]['uuid']).json()
        self.assertEqual(expected_params, buildset['parameters'])
        self.assertEqual('testuser', buildset['user_id'])

    @simple_layout('layouts/parameterized-builds.yaml')
    def test_web_parameterized_build_change(self):
        token = self._getToken(['tenant-one'])
        expected_params = {
            'target_cluster': 'production',
            'version': '1.0',
        }
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={
                'ref': 'refs/changes/01/1/1',
                'pipeline': 'on-demand',
                'parameters': expected_params,
            })
        self.assertEqual(400, resp.status_code)
        self.assertIn('supported only with branches or tags', resp.text)


class TestTenantScopedWebApiWithAccessRules(TestTenantScopedWebApi):
    config_file = 'zuul-admin-web.conf'
    tenant_config_file = 'config/single-tenant/main-access-rules.yaml'

    def test_tenant_authorizations_override(self):
        """Test that user gets overriden tenant authz if allowed"""
        # This test behaves differently than the one in the superclass
        # (see below).
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one'],
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.get_url(
            'api/tenant/tenant-one/authorizations',
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertTrue('zuul' in data)
        self.assertTrue(data['zuul']['admin'], data)
        self.assertTrue(data['zuul']['scope'] == ['tenant-one'], data)
        # change tenant
        authz['zuul']['admin'] = ['tenant-whatever', ]
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        # The superclass verifies that the authorizations returned
        # correctly do not include the new tenant.  However in this
        # case, we expect a 403 because our token doesn't match the
        # user access rule.
        req = self.get_url(
            'api/tenant/tenant-one/authorizations',
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(403, req.status_code, req.text)


class TestTenantScopedWebApiWithAuthRules(BaseTestWeb):
    config_file = 'zuul-admin-web-no-override.conf'
    tenant_config_file = 'config/authorization/single-tenant/main.yaml'

    def test_override_not_allowed(self):
        """Test that authz cannot be overriden if config does not allow it"""
        args = {"reason": "some reason",
                "count": 1,
                'job': 'project-test2',
                'change': None,
                'ref': None,
                'node_hold_expiration': None}
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.post_url(
            'api/tenant/tenant-one/project/org/project/autohold',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(401, req.status_code, req.text)

    def test_tenant_level_rule(self):
        """Test that authz rules defined at tenant level are checked"""
        path = "api/tenant/%(tenant)s/project/%(project)s/enqueue"

        def _test_project_enqueue_with_authz(i, project, authz, expected):
            f_ch = self.fake_gerrit.addFakeChange(project, 'master',
                                                  '%s %i' % (project, i))
            f_ch.addApproval('Code-Review', 2)
            f_ch.addApproval('Approved', 1)
            change = {'trigger': 'gerrit',
                      'change': '%i,1' % i,
                      'pipeline': 'gate', }
            enqueue_args = {'tenant': 'tenant-one',
                            'project': project, }

            token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
            req = self.post_url(path % enqueue_args,
                                headers={'Authorization': 'Bearer %s' % token},
                                json=change)
            self.assertEqual(expected, req.status_code, req.text)
            self.waitUntilSettled()

        i = 0
        for p in ['org/project', 'org/project1', 'org/project2']:
            i += 1
            # Authorized sub
            authz = {'iss': 'zuul_operator',
                     'aud': 'zuul.example.com',
                     'sub': 'venkman',
                     'exp': int(time.time()) + 3600}
            _test_project_enqueue_with_authz(i, p, authz, 200)
            i += 1
            # Unauthorized sub
            authz = {'iss': 'zuul_operator',
                     'aud': 'zuul.example.com',
                     'sub': 'vigo',
                     'exp': int(time.time()) + 3600}
            _test_project_enqueue_with_authz(i, p, authz, 403)
            i += 1
            # unauthorized issuer
            authz = {'iss': 'columbia.edu',
                     'aud': 'zuul.example.com',
                     'sub': 'stantz',
                     'exp': int(time.time()) + 3600}
            _test_project_enqueue_with_authz(i, p, authz, 401)
        self.waitUntilSettled()

    def test_group_rule(self):
        """Test a group rule"""
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)

        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'melnitz',
                 'groups': ['ghostbusters', 'secretary'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        path = "api/tenant/%(tenant)s/project/%(project)s/enqueue"
        enqueue_args = {'tenant': 'tenant-one',
                        'project': 'org/project2', }
        change = {'trigger': 'gerrit',
                  'change': '1,1',
                  'pipeline': 'gate', }
        req = self.post_url(path % enqueue_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=change)
        self.assertEqual(200, req.status_code, req.text)
        self.waitUntilSettled()

    def test_depth_claim_rule(self):
        """Test a rule based on a complex claim"""
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)

        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'zeddemore',
                 'vehicle': {
                     'car': 'ecto-1'},
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        path = "api/tenant/%(tenant)s/project/%(project)s/enqueue"
        enqueue_args = {'tenant': 'tenant-one',
                        'project': 'org/project', }
        change = {'trigger': 'gerrit',
                  'change': '1,1',
                  'pipeline': 'gate', }
        req = self.post_url(path % enqueue_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=change)
        self.assertEqual(200, req.status_code, req.text)
        self.waitUntilSettled()

    def test_boolean_claim_rule(self):
        """Test a rule for a boolean claim"""
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)

        path = "api/tenant/%(tenant)s/project/%(project)s/enqueue"
        enqueue_args = {'tenant': 'tenant-one',
                        'project': 'org/project', }
        change = {'trigger': 'gerrit',
                  'change': '1,1',
                  'pipeline': 'gate', }

        def _test_project_enqueue_with_authz(authz, expected):
            token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
            req = self.post_url(path % enqueue_args,
                                headers={'Authorization': 'Bearer %s' % token},
                                json=change)
            self.assertEqual(expected, req.status_code, req.text)
            self.waitUntilSettled()

        valid_authz = {'iss': 'zuul_operator',
                       'aud': 'zuul.example.com',
                       'sub': 'zeddemore',
                       'is_ghost': False,
                       'exp': int(time.time()) + 3600}
        _test_project_enqueue_with_authz(valid_authz, 200)

        invalid_authz = {'iss': 'zuul_operator',
                         'aud': 'zuul.example.com',
                         'sub': 'zeddemore',
                         'is_ghost': True,
                         'exp': int(time.time()) + 3600}
        _test_project_enqueue_with_authz(invalid_authz, 403)

    def test_user_actions_action_override(self):
        """Test that user with 'zuul.admin' claim does NOT get it back"""
        admin_tenants = ['tenant-zero', ]
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {'admin': admin_tenants},
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.get_url('/api/tenant/tenant-one/authorizations',
                           headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(401, req.status_code, req.text)

    def test_user_actions(self):
        """Test that users get the right 'zuul.actions' trees"""
        users = [
            {'authz': {'iss': 'zuul_operator',
                       'aud': 'zuul.example.com',
                       'sub': 'vigo'},
             'zuul.admin': []},
            {'authz': {'iss': 'zuul_operator',
                       'aud': 'zuul.example.com',
                       'sub': 'venkman'},
             'zuul.admin': ['tenant-one', ]},
            {'authz': {'iss': 'zuul_operator',
                       'aud': 'zuul.example.com',
                       'sub': 'stantz'},
             'zuul.admin': []},
            {'authz': {'iss': 'zuul_operator',
                       'aud': 'zuul.example.com',
                       'sub': 'zeddemore',
                       'vehicle': {
                           'car': 'ecto-1'
                       }},
             'zuul.admin': ['tenant-one', ]},
            {'authz': {'iss': 'zuul_operator',
                       'aud': 'zuul.example.com',
                       'sub': 'melnitz',
                       'groups': ['secretary', 'ghostbusters']},
             'zuul.admin': ['tenant-one', ]},
        ]

        for test_user in users:
            authz = test_user['authz']
            authz['exp'] = int(time.time()) + 3600
            token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
            req = self.get_url('/api/tenant/tenant-one/authorizations',
                               headers={'Authorization': 'Bearer %s' % token})
            self.assertEqual(200, req.status_code, req.text)
            data = req.json()
            self.assertTrue('zuul' in data,
                            "%s got %s" % (authz['sub'], data))
            self.assertTrue('admin' in data['zuul'],
                            "%s got %s" % (authz['sub'], data))
            self.assertEqual('tenant-one' in test_user['zuul.admin'],
                             data['zuul']['admin'],
                             "%s got %s" % (authz['sub'], data))
            self.assertEqual(['tenant-one', ],
                             data['zuul']['scope'],
                             "%s got %s" % (authz['sub'], data))

            req = self.get_url('/api/tenant/unknown/authorizations',
                               headers={'Authorization': 'Bearer %s' % token})
            self.assertEqual(404, req.status_code, req.text)

    def test_authorizations_no_header(self):
        """Test that missing Authorization header results in HTTP 401"""
        req = self.get_url('/api/tenant/tenant-one/authorizations')
        self.assertEqual(401, req.status_code, req.text)


class TestTenantScopedWebApiTokenWithExpiry(BaseTestWeb):
    config_file = 'zuul-admin-web-token-expiry.conf'

    def test_iat_claim_mandatory(self):
        """Test that the 'iat' claim is mandatory when
        max_validity_time is set"""
        authz = {'iss': 'zuul_operator',
                 'sub': 'testuser',
                 'aud': 'zuul.example.com',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            headers={'Authorization': 'Bearer %s' % token},
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)

    def test_token_from_the_future(self):
        authz = {'iss': 'zuul_operator',
                 'sub': 'testuser',
                 'aud': 'zuul.example.com',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 7200,
                 'iat': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            headers={'Authorization': 'Bearer %s' % token},
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)

    def test_token_expired(self):
        authz = {'iss': 'zuul_operator',
                 'sub': 'testuser',
                 'aud': 'zuul.example.com',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 3600,
                 'iat': int(time.time())}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        time.sleep(10)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/autohold",
            headers={'Authorization': 'Bearer %s' % token},
            json={'job': 'project-test1',
                  'count': 1,
                  'reason': 'because',
                  'node_hold_expiration': 36000})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'change': '2,1',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)
        resp = self.post_url(
            "api/tenant/tenant-one/project/org/project/enqueue",
            headers={'Authorization': 'Bearer %s' % token},
            json={'trigger': 'gerrit',
                  'ref': 'abcd',
                  'newrev': 'aaaa',
                  'oldrev': 'bbbb',
                  'pipeline': 'check'})
        self.assertEqual(401, resp.status_code)

    def test_autohold(self):
        """Test that autohold can be set through the admin web interface"""
        args = {"reason": "some reason",
                "count": 1,
                'job': 'project-test2',
                'change': None,
                'ref': None,
                'node_hold_expiration': None}
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ],
                 },
                 'exp': int(time.time()) + 3600,
                 'iat': int(time.time())}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.post_url(
            'api/tenant/tenant-one/project/org/project/autohold',
            headers={'Authorization': 'Bearer %s' % token},
            json=args)
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertEqual(True, data)

        # Check result
        resp = self.get_url(
            "api/tenant/tenant-one/autohold")
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))

        ah_request = autohold_requests[0]
        self.assertEqual('tenant-one', ah_request['tenant'])
        self.assertIn('org/project', ah_request['project'])
        self.assertEqual('project-test2', ah_request['job'])
        self.assertEqual(".*", ah_request['ref_filter'])
        self.assertEqual("some reason", ah_request['reason'])


class TestTenantScopedWebApiWithRBAC(BaseTestWeb):
    config_file = 'zuul-admin-web-no-override.conf'
    tenant_config_file = 'config/authorization/single-tenant/rbac.yaml'

    def test_rbac_read_access(self):
        """Test explicit read access"""
        # Generate some build records in the db.
        self.add_base_changes()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        path = 'api/tenant/tenant-one/builds' \
               '?project=org/project&job_name=project-test1'

        def _test_builds_get_with_authz(authz, expected):
            token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
            req = self.get_url(path,
                               headers={'Authorization': 'Bearer %s' % token})
            if expected == 200:
                build_query = req.json()
                self.assertEqual(len(build_query), 1)
                self.assertEqual(build_query[0]['job_name'], 'project-test1')
                self.assertEqual(build_query[0]['pipeline'], 'gate')
                self.assertEqual(build_query[0]['result'], 'SUCCESS')
            else:
                self.assertEqual(expected, req.status_code, req.text)

        # Authorized sub
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'venkman',
                 'exp': int(time.time()) + 3600}
        _test_builds_get_with_authz(authz, 200)
        # Unauthorized sub
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'exp': int(time.time()) + 3600}
        _test_builds_get_with_authz(authz, 403)
        # Authorized group
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        _test_builds_get_with_authz(authz, 200)

    def test_rbac_enqueue_permissions(self):
        """Test that rbac rules defined on tenant are checked"""
        path = "api/tenant/%(tenant)s/project/%(project)s/enqueue"

        def _test_project_enqueue_with_authz(i, project, authz, expected):
            f_ch = self.fake_gerrit.addFakeChange(project, 'master',
                                                  '%s %i' % (project, i))
            f_ch.addApproval('Code-Review', 2)
            f_ch.addApproval('Approved', 1)
            change = {'trigger': 'gerrit',
                      'change': '%i,1' % i,
                      'pipeline': 'gate', }
            enqueue_args = {'tenant': 'tenant-one',
                            'project': project, }

            token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
            req = self.post_url(path % enqueue_args,
                                headers={'Authorization': 'Bearer %s' % token},
                                json=change)
            self.assertEqual(expected, req.status_code, req.text)
            self.waitUntilSettled()

        i = 0
        for p in ['org/project', 'org/project1', 'org/project2']:
            i += 1
            # Authorized sub
            authz = {'iss': 'zuul_operator',
                     'aud': 'zuul.example.com',
                     'sub': 'venkman',
                     'exp': int(time.time()) + 3600}
            _test_project_enqueue_with_authz(i, p, authz, 200)
            i += 1
            # Unauthorized sub
            authz = {'iss': 'zuul_operator',
                     'aud': 'zuul.example.com',
                     'sub': 'vigo',
                     'exp': int(time.time()) + 3600}
            _test_project_enqueue_with_authz(i, p, authz, 403)
            i += 1
            # Authorized group
            authz = {'iss': 'zuul_operator',
                     'aud': 'zuul.example.com',
                     'sub': 'vigo',
                     'groups': ['ghostbusters'],
                     'exp': int(time.time()) + 3600}
            _test_project_enqueue_with_authz(i, p, authz, 200)
            i += 1
            # unauthorized project
            authz = {'iss': 'zuul_operator',
                     'aud': 'zuul.example.com',
                     'sub': 'vigo',
                     'groups': ['ghostbusters2'],
                     'exp': int(time.time()) + 3600}
            _test_project_enqueue_with_authz(i, p, authz, 403)
        self.waitUntilSettled()

    def test_rbac_dequeue(self):
        """Test that rbac rules for dequeue actions are respected"""
        start_builds = len(self.builds)
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.executor_server.hold_jobs_in_build = True
        self.commitConfigUpdate('common-config', 'layouts/timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        for _ in iterate_timeout(30, 'Wait for a build on hold'):
            if len(self.builds) > start_builds:
                break
        self.waitUntilSettled()

        # First check non matching role conditions (ref does not match)
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters2'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        path = "api/tenant/%(tenant)s/project/%(project)s/dequeue"
        dequeue_args = {'tenant': 'tenant-one',
                        'project': 'org/project', }
        change = {'ref': 'refs/heads/stable',
                  'pipeline': 'periodic', }
        req = self.post_url(path % dequeue_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=change)
        # Request has failed and jobs are still held.
        self.assertEqual(403, req.status_code, req.text)
        self.waitUntilSettled()
        assert len(self.builds) > start_builds

        # Now check that the request succeeds with matching role conditions
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        path = "api/tenant/%(tenant)s/project/%(project)s/dequeue"
        dequeue_args = {'tenant': 'tenant-one',
                        'project': 'org/project', }
        change = {'ref': 'refs/heads/stable',
                  'pipeline': 'periodic', }
        req = self.post_url(path % dequeue_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=change)
        self.assertEqual(200, req.status_code, req.text)
        data = req.json()
        self.assertEqual(True, data)
        self.waitUntilSettled()

        self.commitConfigUpdate('common-config',
                                'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(self.countJobResults(self.history, 'ABORTED'), 1)

    def test_rbac_authorizations(self):
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        # Check root authorizations
        path = "api/authorizations"
        req = self.post_url(path,
                            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, req.status_code)
        auths = req.json()['zuul']
        self.assertEqual(auths['scope'], ['<root>'])
        self.assertTrue(auths['permissions']['read'])

        # Check tenant scoped authorizations
        path = "api/tenant/tenant-one/authorizations"
        req = self.post_url(path,
                            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, req.status_code)
        auths = req.json()['zuul']
        self.assertEqual(auths['scope'], ['tenant-one'])
        self.assertTrue(auths['permissions']['read'])
        self.assertTrue(auths['permissions']['enqueue'])
        self.assertTrue(auths['permissions']['dequeue'])
        self.assertTrue(auths['permissions']['autohold'])
        self.assertTrue(auths['permissions']['set-tenant-state'])
        self.assertTrue(auths['permissions']['build-image'])
        self.assertTrue(auths['permissions']['delete-image-build-artifact'])

        # Ensure we 403 when using an invalid user
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'notauser',
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        path = "api/tenant/tenant-one/authorizations"
        req = self.post_url(path,
                            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(403, req.status_code)

    def test_rbac_autohold(self):
        """Test autoholds work with rbac authz"""

        def _test_autohold_with_authz(project, job, authz, expected):
            path = '/api/tenant/%(tenant)s/project/%(project)s/autohold'
            autohold = {'job': job,
                        'count': 1,
                        'change': None,
                        'ref': '.*',
                        'reason': 'Testing RBAC',
                        'node_hold_expiration': 0}
            autohold_args = {'tenant': 'tenant-one',
                             'project': project, }

            token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
            req = self.post_url(path % autohold_args,
                                headers={'Authorization': 'Bearer %s' % token},
                                json=autohold)
            self.assertEqual(expected, req.status_code, req.text)
            self.waitUntilSettled()

        # Unauthorized sub
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'exp': int(time.time()) + 3600}
        _test_autohold_with_authz('org/project', 'project-test1', authz, 403)
        # Unauthorized group
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters2'],
                 'exp': int(time.time()) + 3600}
        _test_autohold_with_authz('org/project', 'project-test1', authz, 403)
        # Authorized group
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        _test_autohold_with_authz('org/project', 'project-test1', authz, 200)

        # Check the resulting autohold exists (and get its id for deletion)
        path = '/api/tenant/%(tenant)s/project/%(project)s/autohold'
        autohold_args = {'tenant': 'tenant-one',
                         'project': 'org/project'}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.get_url(path % autohold_args,
                           headers={'Authorization': 'Bearer %s' % token})
        request_id = req.json()[0]['id']
        self.assertEqual('0000000000', request_id)

        def _test_delete_autohold_with_authz(request_id, authz, expected):
            path = '/api/tenant/%(tenant)s/autohold/%(request_id)s'
            delete_args = {'tenant': 'tenant-one',
                           'request_id': request_id}
            token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                               algorithm='HS256')
            req = self.get_url(path % delete_args,
                               headers={'Authorization': 'Bearer %s' % token})
            self.assertEqual(expected, req.status_code)

        # Now try to delete the autohold
        # Unauthorized sub
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'exp': int(time.time()) + 3600}
        _test_delete_autohold_with_authz(request_id, authz, 403)
        # Authorized group
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        _test_delete_autohold_with_authz(request_id, authz, 200)

    def test_rbac_set_state(self):
        """Test rbac roles are honored by state_post requests"""
        path = '/api/tenant/%(tenant)s/state'
        state_post_args = {'tenant': 'tenant-one'}
        state_post = {'trigger_queue_paused': True,
                      'reason': 'rbac testing'}
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters2'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        # Check a request that doesn't match any roles due to bad credentials
        req = self.post_url(path % state_post_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=state_post)
        self.assertEqual(403, req.status_code, req.text)

        # Now check a request that does match credentials
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.post_url(path % state_post_args,
                            headers={'Authorization': 'Bearer %s' % token},
                            json=state_post)
        self.assertEqual(200, req.status_code, req.text)
        # Check the tenant state in zookeeper
        tqs_path = TENANT_EVENT_STATE.format(tenant='tenant-one')
        tqs = zuul.model.TenantEventState(tqs_path)
        ctx = self.createZKContext(None)
        tqs.refresh(ctx)
        tqs = tqs.toDict()
        self.assertTrue(tqs['trigger_queue_paused'])
        self.assertEqual(tqs['reason'], 'rbac testing')

    def test_rbac_build_image(self):
        path = 'api/tenant/%(tenant)s/image/%(image)s/build'
        image_build_args = {'tenant': 'tenant-one',
                            'image': 'testimage'}
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters2'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        # Check a request that doesn't match any roles due to bad credentials
        req = self.post_url(path % image_build_args,
                            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(403, req.status_code, req.text)
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        # Now check a request with valid credentials
        image_build_args = {'tenant': 'tenant-one',
                            'image': 'testimage'}
        req = self.post_url(path % image_build_args,
                            headers={'Authorization': 'Bearer %s' % token})
        # The 404 response with the image not found error indicates we
        # successfully authenticated and were authorized, but the image
        # simply isn't in this zuul tenant.
        self.assertEqual(404, req.status_code, req.text)
        self.assertIn('Image not found in tenant', req.text)

    def test_rbac_delete_image_build_artifact(self):
        path = '/api/tenant/%(tenant)s/image-build-artifact/%(artifact_id)s'
        delete_args = {'tenant': 'tenant-one',
                       'artifact_id': '123456'}
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters2'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        # Check a request that doesn't match any roles due to bad credentials
        req = self.delete_url(path % delete_args,
                              headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(403, req.status_code, req.text)
        # Now check a request with valid credentials
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        req = self.delete_url(path % delete_args,
                              headers={'Authorization': 'Bearer %s' % token})
        # The 404 response with the image build artifact not found error
        # indicates we successfully authenticated and were authorized, but the
        # image build artifact simply isn't in this zuul tenant.
        self.assertEqual(404, req.status_code, req.text)
        self.assertIn('Image build artifact not found in tenant', req.text)

    def test_rbac_invalid_json(self):
        """Test behavior of bad requests prior to auth validation"""
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'vigo',
                 'groups': ['ghostbusters'],
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        path = '/api/tenant/%(tenant)s/project/%(project)s/autohold'
        autohold_args = {'tenant': 'tenant-one',
                         'project': 'org/project', }
        autohold = {'job': 'project-test1',
                    'count': 1,
                    'change': None,
                    'ref': '.*',
                    'reason': 'Testing RBAC',
                    'node_hold_expiration': 0}
        headers = {
            'Authorization': 'Bearer %s' % token,
            'Content-Type': 'application/json'
        }
        full_path = urllib.parse.urljoin(self.base_url, path % autohold_args)
        raw_json = json.dumps(autohold)

        # Ensure everything is happy if the json is valid
        req = requests.post(full_path, headers=headers, data=raw_json)
        self.assertEqual(200, req.status_code)

        # Edit the raw_json to make it invalid json
        raw_json = raw_json[1:-1]
        req = requests.post(full_path, headers=headers, data=raw_json)
        self.assertEqual(400, req.status_code)
        self.assertIn('Invalid JSON document', req.text)


class TestHeldAttributeInBuildInfo(BaseTestWeb):
    config_file = 'zuul-sql-driver-mysql.conf'
    tenant_config_file = 'config/sql-driver/main.yaml'

    def test_autohold_and_retrieve_held_build_info(self):
        """Ensure the "held" attribute can be used to filter builds"""
        self.addAutohold('tenant-one', 'review.example.com/org/project',
                         'project-test2', '.*', 'reason text', 1, 600)

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.executor_server.failJob('project-test2', B)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        all_builds_resp = self.get_url("api/tenant/tenant-one/builds?"
                                       "project=org/project")
        held_builds_resp = self.get_url("api/tenant/tenant-one/builds?"
                                        "project=org/project&"
                                        "held=1")
        self.assertEqual(200,
                         all_builds_resp.status_code,
                         all_builds_resp.text)
        self.assertEqual(200,
                         held_builds_resp.status_code,
                         held_builds_resp.text)
        all_builds = all_builds_resp.json()
        held_builds = held_builds_resp.json()
        self.assertEqual(len(held_builds), 1, all_builds)
        held_build = held_builds[0]
        self.assertEqual('project-test2', held_build['job_name'], held_build)
        self.assertEqual(True, held_build['held'], held_build)


class TestWebMulti(BaseTestWeb):
    config_file = 'zuul-gerrit-ssh.conf'

    def test_web_connections_list_multi(self):
        data = self.get_url('api/connections').json()
        port = self.web.connections.connections['gerrit'].web_server.port
        url = f'http://localhost:{port}'
        gerrit_connection = {
            'driver': 'gerrit',
            'name': 'gerrit',
            'baseurl': url,
            'canonical_hostname': 'review.example.com',
            'server': 'review.example.com',
            'ssh_server': 'ssh-review.example.com',
            'port': 29418,
        }
        github_connection = {
            'baseurl': 'https://api.github.com',
            'canonical_hostname': 'github.com',
            'driver': 'github',
            'name': 'github',
            'server': 'github.com',
            'repo_cache': None,
        }
        self.assertEqual([gerrit_connection, github_connection], data)


# TODO Remove this class once REST support is removed from Zuul CLI
class TestCLIViaWebApi(BaseTestWeb):
    config_file = 'zuul-admin-web.conf'

    def test_autohold(self):
        """Test that autohold can be set with the CLI through REST"""
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '--zuul-url', self.base_url, '--auth-token', token,
             'autohold', '--reason', 'some reason',
             '--tenant', 'tenant-one', '--project', 'org/project',
             '--job', 'project-test2', '--count', '1'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output[0])
        # Check result
        resp = self.get_url(
            "api/tenant/tenant-one/autohold")
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))
        request = autohold_requests[0]
        self.assertEqual('tenant-one', request['tenant'])
        self.assertIn('org/project', request['project'])
        self.assertEqual('project-test2', request['job'])
        self.assertEqual(".*", request['ref_filter'])
        self.assertEqual("some reason", request['reason'])
        self.assertEqual(1, request['max_count'])

    def test_enqueue(self):
        """Test that the CLI can enqueue a change via REST"""
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)

        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '--zuul-url', self.base_url, '--auth-token', token,
             'enqueue', '--tenant', 'tenant-one',
             '--project', 'org/project',
             '--pipeline', 'gate', '--change', '1,1'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output[0])
        self.waitUntilSettled()

    def test_enqueue_ref(self):
        """Test that the CLI can enqueue a ref via REST"""
        p = "review.example.com/org/project"
        upstream = self.getUpstreamRepos([p])
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setMerged()
        A_commit = str(upstream[p].commit('master'))
        self.log.debug("A commit: %s" % A_commit)

        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '--zuul-url', self.base_url, '--auth-token', token,
             'enqueue-ref', '--tenant', 'tenant-one',
             '--project', 'org/project',
             '--pipeline', 'post', '--ref', 'master',
             '--oldrev', '90f173846e3af9154517b88543ffbd1691f31366',
             '--newrev', A_commit],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output[0])
        self.waitUntilSettled()

    def test_dequeue(self):
        """Test that the CLI can dequeue a change via REST"""
        start_builds = len(self.builds)
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.executor_server.hold_jobs_in_build = True
        self.commitConfigUpdate('common-config', 'layouts/timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        for _ in iterate_timeout(30, 'Wait for a build on hold'):
            if len(self.builds) > start_builds:
                break
        self.waitUntilSettled()

        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '--zuul-url', self.base_url, '--auth-token', token,
             'dequeue', '--tenant', 'tenant-one', '--project', 'org/project',
             '--pipeline', 'periodic', '--ref', 'refs/heads/stable'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output[0])
        self.waitUntilSettled()

        self.commitConfigUpdate('common-config',
                                'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(self.countJobResults(self.history, 'ABORTED'), 1)

    def test_promote(self):
        "Test that the RPC client can promote a change"
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        C.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))

        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'gate')
        enqueue_times = {}
        for item in items:
            enqueue_times[str(item.changes[0])] = item.enqueue_time

        # Promote B and C using the cli
        authz = {'iss': 'zuul_operator',
                 'aud': 'zuul.example.com',
                 'sub': 'testuser',
                 'zuul': {
                     'admin': ['tenant-one', ]
                 },
                 'exp': int(time.time()) + 3600}
        token = jwt.encode(authz, key='ThisIsABadSecretOnlyUsedForTesting',
                           algorithm='HS256')
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '--zuul-url', self.base_url, '--auth-token', token,
             'promote', '--tenant', 'tenant-one',
             '--pipeline', 'gate', '--changes', '2,1', '3,1'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output[0])
        self.waitUntilSettled()

        # ensure that enqueue times are durable
        items = self.getAllItems('tenant-one', 'gate')
        for item in items:
            self.assertEqual(
                enqueue_times[str(item.changes[0])], item.enqueue_time)

        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 6)
        self.assertEqual(self.builds[0].name, 'project-test1')
        self.assertEqual(self.builds[1].name, 'project-test2')
        self.assertEqual(self.builds[2].name, 'project-test1')
        self.assertEqual(self.builds[3].name, 'project-test2')
        self.assertEqual(self.builds[4].name, 'project-test1')
        self.assertEqual(self.builds[5].name, 'project-test2')

        self.assertTrue(self.builds[0].hasChanges(B))
        self.assertFalse(self.builds[0].hasChanges(A))
        self.assertFalse(self.builds[0].hasChanges(C))

        self.assertTrue(self.builds[2].hasChanges(B))
        self.assertTrue(self.builds[2].hasChanges(C))
        self.assertFalse(self.builds[2].hasChanges(A))

        self.assertTrue(self.builds[4].hasChanges(B))
        self.assertTrue(self.builds[4].hasChanges(C))
        self.assertTrue(self.builds[4].hasChanges(A))

        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.data['status'], 'MERGED')
        self.assertEqual(C.reported, 2)


class TestWebStartup(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'
    config_ini_data = {}

    def _start_web(self):
        # Start the web server
        self.web = ZuulWebFixture(
            self.config, self.test_config, self.additional_event_queues,
            self.upstream_root, self.poller_events,
            self.git_url_with_auth, self.addCleanup, self.test_root,
            info=zuul.model.WebInfo.fromConfig(self.zuul_ini_config))
        self.useFixture(self.web)

    def get_url(self, url, *args, **kwargs):
        return requests.get(
            urllib.parse.urljoin(self.base_url, url), *args, **kwargs)

    def createScheduler(self):
        pass

    def realCreateScheduler(self):
        super().createScheduler()

    @skip("This test is not reliable in the gate")
    def test_web_startup(self):
        self.zuul_ini_config = FakeConfig(self.config_ini_data)
        self.web = None
        t = threading.Thread(target=self._start_web)
        t.daemon = True
        t.start()

        for _ in iterate_timeout(30, 'Wait for web to begin startup'):
            if self.web and getattr(self.web, 'web', None):
                break

        self.web.web.system_config_cache_wake_event.wait()

        self.realCreateScheduler()
        self.scheds.execute(
            lambda app: app.start(self.validate_tenants))

        t.join()

        self.host = 'localhost'
        # Wait until web server is started
        while True:
            if self.web is None:
                time.sleep(0.1)
                continue
            self.port = self.web.port
            try:
                with socket.create_connection((self.host, self.port)):
                    break
            except ConnectionRefusedError:
                pass
        self.base_url = "http://{host}:{port}".format(
            host=self.host, port=self.port)

        # If the config didn't load correctly, we won't have the jobs
        jobs = self.get_url("api/tenant/tenant-one/jobs").json()
        self.assertEqual(len(jobs), 10)


class TestWebUnprotectedBranches(ZuulTestCase, WebMixin):
    config_file = 'zuul-github-driver.conf'
    tenant_config_file = 'config/unprotected-branches/main.yaml'

    def test_no_protected_branches(self):
        """Regression test to check that zuul-web doesn't display
        config errors when no protected branch exists."""
        self.startWebServer()
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        layout = tenant.layout

        # project2 should have no parsed branch
        self.assertNotIn('project2-job', layout.jobs)

        # Zuul-web should not display any config errors
        config_errors = self.get_url(
            "api/tenant/tenant-one/config-errors").json()
        self.assertEqual(len(config_errors), 0)


class TestWebApiAccessRules(BaseTestWeb):
    # Test read-level access restrictions
    config_file = 'zuul-admin-web.conf'
    tenant_config_file = 'config/access-rules/main.yaml'

    routes = [
        '/api/connections',
        '/api/components',
        '/api/tenants',
        '/api/authorizations',
        '/api/tenant/{tenant}/status',
        '/api/tenant/{tenant}/status/change/{change}',
        '/api/tenant/{tenant}/jobs',
        '/api/tenant/{tenant}/job/{job_name}',
        '/api/tenant/{tenant}/projects',
        '/api/tenant/{tenant}/project/{project}',
        ('/api/tenant/{tenant}/pipeline/{pipeline}/'
         'project/{project}/branch/{branch}/freeze-jobs'),
        '/api/tenant/{tenant}/pipelines',
        '/api/tenant/{tenant}/semaphores',
        '/api/tenant/{tenant}/labels',
        '/api/tenant/{tenant}/nodes',
        '/api/tenant/{tenant}/key/{project}.pub',
        '/api/tenant/{tenant}/project-ssh-key/{project}.pub',
        # console-stream is tested by test_auth_websocket_streaming
        # '/api/tenant/{tenant}/console-stream',
        '/api/tenant/{tenant}/badge',
        '/api/tenant/{tenant}/builds',
        '/api/tenant/{tenant}/build/{uuid}',
        '/api/tenant/{tenant}/buildsets',
        '/api/tenant/{tenant}/buildset/{uuid}',
        '/api/tenant/{tenant}/config-errors',
        '/api/tenant/{tenant}/authorizations',
        '/api/tenant/{tenant}/project/{project}/autohold',
        '/api/tenant/{tenant}/autohold',
        '/api/tenant/{tenant}/autohold/{request_id}',
        '/api/tenant/{tenant}/autohold/{request_id}',
        '/api/tenant/{tenant}/project/{project}/enqueue',
        '/api/tenant/{tenant}/project/{project}/dequeue',
        '/api/tenant/{tenant}/promote',
    ]

    info_routes = [
        '/api/info',
        '/api/tenant/{tenant}/info',
    ]

    def test_read_routes_no_token(self):
        for route in self.routes:
            url = route.format(tenant='tenant-one',
                               project='org/project',
                               change='1,1',
                               job_name='testjob',
                               pipeline='check',
                               branch='master',
                               uuid='1',
                               request_id='1')
            resp = self.get_url(url)
            self.assertEqual(
                401,
                resp.status_code,
                "get %s failed: %s" % (url, resp.text))

    def test_read_info_routes_no_token(self):
        for route in self.info_routes:
            url = route.format(tenant='tenant-one',
                               project='org/project',
                               change='1,1',
                               job_name='testjob',
                               pipeline='check',
                               branch='master',
                               uuid='1',
                               request_id='1')
            resp = self.get_url(url)
            self.assertEqual(
                200,
                resp.status_code,
                "get %s failed: %s" % (url, resp.text))
            info = resp.json()
            self.assertTrue(
                info['info']['capabilities']['auth']['read_protected'])


class TestWebApiRBACReadAccess(TestWebApiAccessRules):
    # Test read-level access restrictions
    config_file = 'zuul-admin-web.conf'
    tenant_config_file = 'config/access-rules/main-rbac.yaml'


class TestWebOIDCJobs(BaseTestWeb):
    tenant_config_file = 'config/secrets/main.yaml'

    def test_web_oidc_jobs(self):
        # Test that we correctly load the job config of an OIDC job in
        # zuul-web (without scheduler access).
        self.commitConfigUpdate(
            'org/project2',
            'config/secrets/git/org_project2/zuul-oidc-single.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        layout_scheduler = self.scheds.first.sched.local_layout_state.get(
            'tenant-one')
        for _ in iterate_timeout(10, "local layout of zuul-web to be updated"):
            layout_web = self.web.web.local_layout_state.get('tenant-one')
            if layout_web == layout_scheduler:
                break
        jobs = self.get_url("api/tenant/tenant-one/jobs").json()
        names = set(x['name'] for x in jobs)
        self.assertIn('project2-oidc-secret', names)


class TestWebOIDCEndpoints(BaseTestWeb):
    tenant_config_file = 'config/multi-tenant/main.yaml'

    def test_global_webroot(self):
        """
        Test that OIDC endpoinds accessed from the global webroot
        should return correct content
        """
        self._test_oidc_endpoints(None, 'https://zuul.example.com')
        # If tenant config does not specify webroot,
        # it should default to global webroot
        self._test_oidc_endpoints('tenant-two', 'https://zuul.example.com')
        # If tenant does exist, it should default to global webroot
        self._test_oidc_endpoints(
            'tenant-not-exist', 'https://zuul.example.com')

    def test_tenant_webroot(self):
        """
        Test tht OIDC endpoints accessed from the tenant webroot
        should return correct content
        """
        self._test_oidc_endpoints(
            "tenant-one", 'https://tenant-one.example.com')

    def test_multiple_jwt_keys(self):
        """
        Test multiple keys returned in jwks endpoint
        """
        keystore = self.web.web.keystore
        # Make sure the fist signning key is created
        keystore.getLatestOidcSigningKeys(algorithm="RS256")

        jwks_data = self.get_url('oidc/jwks').json()
        self.assertEqual(len(jwks_data["keys"]), 1)
        for idx, key in enumerate(jwks_data["keys"]):
            self._validate_oidc_key(key, expected_kid=f'RS256-{idx}')

        # call the rotateOidcSigningKeys method to create the second key
        time.sleep(2)
        keystore.rotateOidcSigningKeys(
            algorithm="RS256", rotation_interval=1, max_ttl=100)

        # Both api and web cache should be refreshed
        for _ in iterate_timeout(10, 'cache to sync'):
            test_keys = keystore.getOidcSigningKeyData(algorithm="RS256")
            if len(test_keys.keys) == 2:
                break

        jwks_data = self.get_url('oidc/jwks').json()
        self.assertEqual(len(jwks_data["keys"]), 2)
        for idx, key in enumerate(jwks_data["keys"]):
            self._validate_oidc_key(key, expected_kid=f'RS256-{idx}')

    def _test_oidc_endpoints(self, tenant_name, expected_webroot):
        if tenant_name:
            root = f'oidc/tenant/{tenant_name}/'
            jwks = f'{root}jwks'
        else:
            root = ''
            jwks = 'oidc/jwks'
        well_known_url = f'{root}.well-known'

        # Test that the openid-configuration content is correct
        config_data = self.get_url(
            f'{well_known_url}/openid-configuration').json()
        self.assertEqual(config_data['issuer'], expected_webroot)
        self.assertEqual(config_data['jwks_uri'],
                         f'{expected_webroot}/oidc/jwks')
        self.assertEqual(config_data['claims_supported'],
                         ['aud', 'iat', 'iss', 'name', 'sub', 'custom'])
        self.assertEqual(config_data['response_types_supported'], ['id_token'])
        self.assertEqual(config_data['id_token_signing_alg_values_supported'],
                         ['RS256'])
        self.assertEqual(config_data['subject_types_supported'], ['public'])

        # Test that the jwks content is correct
        jwks_data = self.get_url(jwks).json()
        self.assertEqual(len(jwks_data["keys"]), 1)
        key = jwks_data["keys"][0]
        self._validate_oidc_key(key)

        # Test that these similar urls don't work
        well_known_url = f'{root}oidc/.well-known'
        with testtools.ExpectedException(requests.exceptions.JSONDecodeError):
            config_data = self.get_url(
                f'{well_known_url}/openid-configuration').json()
        with testtools.ExpectedException(requests.exceptions.JSONDecodeError):
            config_data = self.get_url('jwks').json()

        # Test backwards compat (remove after zuul 13)
        well_known_url = 'oidc/.well-known'
        if tenant_name:
            well_known_url = f'oidc/{tenant_name}/.well-known'
        jwks_data = self.get_url(
            f'{well_known_url}/jwks').json()
        self.assertEqual(len(jwks_data["keys"]), 1)

    def _validate_oidc_key(self, key, expected_kid='RS256-0'):
        jwk = jwt.PyJWK.from_dict(key)
        self.assertEqual(jwk.key_type, 'RSA')
        self.assertEqual(jwk.algorithm_name, 'RS256')
        self.assertEqual(jwk.key_id, expected_kid)
        self.assertEqual(jwk.public_key_use, 'sig')
        self.assertIsInstance(jwk.Algorithm, jwt.algorithms.RSAAlgorithm)
