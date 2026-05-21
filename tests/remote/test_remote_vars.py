# Copyright 2018 Red Hat, Inc.
# Copyright 2026 Acme Gating, LLC
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

import os
import textwrap

from tests.base import AnsibleZuulTestCase


class FunctionalVarsMixIn:
    tenant_config_file = 'config/remote-vars/main.yaml'
    # This should be overriden in child classes.
    ansible_version = 'X'
    wait_timeout = 120

    def _setUp(self):
        self.fake_nodepool.remote_ansible = True

        ansible_remote = os.environ.get('ZUUL_REMOTE_IPV4')
        self.assertIsNotNone(ansible_remote)

    def _run_job(self, job_name, result, expect_error=None):
        # Keep the jobdir around so we can inspect contents if an
        # assert fails. It will be cleaned up anyway as it is contained
        # in a tmp dir which gets cleaned up after the test.
        self.executor_server.keep_jobdir = True

        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        conf = textwrap.dedent(
            """
            - job:
                name: {job_name}
                run: playbooks/{job_name}.yaml
                ansible-version: {version}
                roles:
                  - zuul: org/common-config
                nodeset:
                  nodes:
                    - name: controller
                      label: whatever

            - project:
                check:
                  jobs:
                    - {job_name}
            """.format(job_name=job_name, version=self.ansible_version))

        file_dict = {'zuul.yaml': conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        job = self.getJobFromHistory(job_name)
        with self.jobLog(job):
            build = self.history[-1]
            self.assertEqual(build.result, result)

            if expect_error:
                path = os.path.join(self.jobdir_root, build.uuid,
                                    'work', 'logs', 'job-output.txt')
                with open(path, 'r') as f:
                    self.assertIn(expect_error, f.read())

    def test_zuul_user_dir(self):
        self._run_job('user-dir', 'SUCCESS')


class TestVars9(AnsibleZuulTestCase, FunctionalVarsMixIn):
    ansible_version = '9'

    def setUp(self):
        super().setUp()
        self._setUp()


class TestVars11(AnsibleZuulTestCase, FunctionalVarsMixIn):
    ansible_version = '11'

    def setUp(self):
        super().setUp()
        self._setUp()
