# Copyright 2026 Red Hat, Inc.
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

import textwrap

from tests.base import ZuulTestCase


class TestTemplateConfig(ZuulTestCase):
    """Tests for template-config configuration parsing"""
    tenant_config_file = 'config/single-tenant/main.yaml'

    def _submit_change(self, nodes_yaml, nodeset_name='test-nodeset',
                       job_name='test-job'):
        """Submit a change with nodeset config and assert it succeeds.

        :param nodes_yaml: YAML string for nodes section
        :param nodeset_name: Name of the nodeset
        :param job_name: Name of the job
        """
        nodes_config = textwrap.indent(
            textwrap.dedent(nodes_yaml).strip(), '      ')
        in_repo_conf = (
            f"- nodeset:\n"
            f"    name: {nodeset_name}\n"
            f"    nodes:\n"
            f"{nodes_config}\n"
            f"\n"
            f"- job:\n"
            f"    name: {job_name}\n"
            f"    nodeset: {nodeset_name}\n"
            f"    run: playbooks/noop.yaml\n"
            f"\n"
            f"- project:\n"
            f"    check:\n"
            f"      jobs:\n"
            f"        - {job_name}\n"
        )
        playbook = "- hosts: all\n  tasks: []\n"
        file_dict = {
            'zuul.yaml': in_repo_conf,
            'playbooks/noop.yaml': playbook,
        }
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        return A

    def test_nodeset_without_template_config(self):
        """Test that nodesets without template-config key still work"""
        A = self._submit_change(
            """
            - name: controller
              label: ubuntu-focal
            """,
            job_name='test-no-config-job',
        )
        self.assertIn('SUCCESS', A.messages[0])

    def test_nodeset_with_empty_template_config(self):
        """Test that explicit empty template-config dict is accepted"""
        A = self._submit_change(
            """
            - name: controller
              label: ubuntu-focal
              template-config: {}
            """,
            job_name='test-empty-config-job',
        )
        self.assertIn('SUCCESS', A.messages[0])

    def test_template_config_rejects_unknown_keys(self):
        """Test that unknown keys in template-config are rejected"""
        A = self._submit_change(
            """
            - name: controller
              label: ubuntu-focal
              template-config:
                some-unknown-key: some-value
                another-bad-key: other-value
            """,
            job_name='test-unknown-key-job',
        )
        self.assertIn('syntax error', A.messages[0])
        self.assertIn('some-unknown-key', A.messages[0])
