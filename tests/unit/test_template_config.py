# Copyright 2025 Red Hat, Inc.
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
    """Tests for generic template-config configuration parsing"""
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
        self.assertIn('SUCCESS', A.messages[0])

    def test_nodeset_with_template_config(self):
        """Test that template-config is accepted including hyphenated keys"""
        self._submit_change(
            """
            - name: controller
              label: ubuntu-focal
              template-config:
                custom_key: custom-value
                another_key: 42
                some-hyphenated-key: value1
            """,
            job_name='test-config-job',
        )

    def test_nodeset_without_template_config(self):
        """Test that nodesets without template-config key still work"""
        self._submit_change(
            """
            - name: controller
              label: ubuntu-focal
            """,
            job_name='test-no-config-job',
        )

    def test_nodeset_with_empty_template_config(self):
        """Test that explicit empty template-config dict is accepted"""
        self._submit_change(
            """
            - name: controller
              label: ubuntu-focal
              template-config: {}
            """,
            job_name='test-empty-config-job',
        )

    def test_nodeset_with_multiple_nodes_different_config(self):
        """Test multiple nodes can have different template-config"""
        self._submit_change(
            """
            - name: controller
              label: ubuntu-focal
              template-config:
                role: controller
                priority: high
            - name: compute1
              label: ubuntu-focal
              template-config:
                role: compute
                priority: low
            - name: compute2
              label: ubuntu-focal
            """,
            job_name='test-multi-nodes-job',
        )

    def test_template_config_with_complex_values(self):
        """Test that template-config supports complex nested values"""
        self._submit_change(
            """
            - name: controller
              label: ubuntu-focal
              template-config:
                string-value: "test"
                int-value: 123
                bool-value: true
                list-value:
                  - item1
                  - item2
                dict-value:
                  nested-key: nested-value
                  another: 456
            """,
            job_name='test-complex-config',
        )

    def test_nodeset_inline_with_template_config(self):
        """Test inline nodeset with template-config"""
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test-inline-job
                run: playbooks/noop.yaml
                nodeset:
                  nodes:
                    - name: controller
                      label: ubuntu-focal
                      template-config:
                        inline-prop: inline-value

            - project:
                check:
                  jobs:
                    - test-inline-job
            """
        )
        playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """
        )
        file_dict = {
            'zuul.yaml': in_repo_conf,
            'playbooks/noop.yaml': playbook,
        }
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('SUCCESS', A.messages[0])

    def test_template_config_passed_to_node(self):
        """Test that template-config from YAML is set on node objects.

        Submits a change defining a nodeset with template-config and
        verifies the configloader correctly parses and populates
        Node.template_config by inspecting the frozen job's nodeset.
        """
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: test-nodeset-verify
                nodes:
                  - name: controller
                    label: ubuntu-focal
                    template-config:
                      test_key: test_value
                      test_number: 42
                      hyphen-key: hyphen_value

            - job:
                name: test-verify-job
                nodeset: test-nodeset-verify
                run: playbooks/noop.yaml

            - project:
                check:
                  jobs:
                    - test-verify-job
            """
        )
        playbook = textwrap.dedent(
            """
            - hosts: all
              tasks:
                - debug:
                    msg: "Testing template config"
            """
        )
        file_dict = {
            'zuul.yaml': in_repo_conf,
            'playbooks/noop.yaml': playbook,
        }
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('SUCCESS', A.messages[0])

        # Verify template_config on the frozen job's nodeset
        build = self.getJobFromHistory('test-verify-job')
        nodeset = build.job.nodeset
        self.assertIsNotNone(nodeset)
        nodes = nodeset.getNodes()
        self.assertEqual(len(nodes), 1)
        node = nodes[0]

        # Verify template_config is set on the node
        # (configloader converts hyphens to underscores in keys)
        self.assertIn('test_key', node.template_config)
        self.assertEqual(node.template_config['test_key'], 'test_value')
        self.assertIn('test_number', node.template_config)
        self.assertEqual(node.template_config['test_number'], 42)
        self.assertIn('hyphen_key', node.template_config)
        self.assertEqual(node.template_config['hyphen_key'], 'hyphen_value')
