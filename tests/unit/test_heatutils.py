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
from unittest.mock import Mock

import openstack
import yaml

from tests.base import BaseTestCase
from zuul.driver.heat import heatutils
from zuul.exceptions import TemplatePolicyViolation


class TestHeatTemplateParser(BaseTestCase):
    """Test Heat template parser functionality."""

    @staticmethod
    def _build_template(servers=None, volumes=None, networks=0,
                        extra_resources=None, parameters=None):
        tpl = {'heat_template_version': 'rocky'}

        if parameters:
            tpl['parameters'] = parameters

        resources = {}

        for i, flavor in enumerate(servers or []):
            resources[f'server{i}'] = {
                'type': 'OS::Nova::Server',
                'properties': {'flavor': flavor},
            }

        for i, size in enumerate(volumes or []):
            resources[f'vol{i}'] = {
                'type': 'OS::Cinder::Volume',
                'properties': {'size': size},
            }

        for i in range(networks):
            resources[f'net{i}'] = {'type': 'OS::Neutron::Net'}

        if extra_resources:
            for rname, rtype in extra_resources.items():
                resources[rname] = {'type': rtype}

        if resources:
            tpl['resources'] = resources

        return yaml.safe_dump(tpl, default_flow_style=False, sort_keys=False)

    def setUp(self):
        super().setUp()
        self.mock_endpoint = Mock()
        self._parser = heatutils.HeatTemplateParser
        self.unsupported_types = heatutils.UNSUPPORTED_TYPES

    def test_via_get_param(self):
        """Test get_param resolution for flavors and volume sizes."""
        flavors = {
            'm1.small': Mock(vcpus=1, ram=2048),
            'm1.large': Mock(vcpus=4, ram=8192),
        }
        self.mock_endpoint._findFlavor.side_effect = flavors.get
        parser = self._parser(self.mock_endpoint, {})

        # Flavor resolution priority
        cases = [
            # (param_default, runtime_params, expected_flavor)
            (None, {'instance_flavor': 'm1.large'}, 'm1.large'),
            ('m1.small', {'instance_flavor': 'm1.large'}, 'm1.large'),
            ('m1.small', None, 'm1.small'),
        ]
        for default, runtime, expected in cases:
            self.mock_endpoint._findFlavor.reset_mock()
            param_def = {'type': 'string'}
            if default:
                param_def['default'] = default
            template = self._build_template(
                parameters={'instance_flavor': param_def},
                servers=[{'get_param': 'instance_flavor'}])

            quota = parser.parse(template, runtime_parameters=runtime)

            expected_flavor = flavors[expected]
            self.assertEqual(quota.quota['cores'], expected_flavor.vcpus)
            self.assertEqual(quota.quota['ram'], expected_flavor.ram)
            self.mock_endpoint._findFlavor.assert_called_once_with(expected)

        # Volume size resolution
        template = self._build_template(
            parameters={'volume_size': {'type': 'number'}},
            volumes=[{'get_param': 'volume_size'}])
        quota = parser.parse(template, runtime_parameters={'volume_size': 100})

        self.assertEqual(quota.quota['volumes'], 1)
        self.assertEqual(quota.quota['volume-gb'], 100)

    def test_missing_required_properties(self):
        """Test that resources missing required properties raise ValueError."""
        parser = self._parser(self.mock_endpoint, {})
        cases = [
            ("missing required 'flavor' property", textwrap.dedent("""\
                resources:
                  server:
                    type: OS::Nova::Server
                    properties:
                      image: test-image
            """)),
            ("missing required 'size' property", textwrap.dedent("""\
                resources:
                  volume:
                    type: OS::Cinder::Volume
                    properties:
                      description: test volume
            """)),
        ]
        for expected_msg, template in cases:
            with self.assertRaises(ValueError) as cm:
                parser.parse(template)
            self.assertIn(expected_msg, str(cm.exception))

    def test_unresolvable_references(self):
        """Test that unresolvable get_param references raise ValueError."""
        parser = self._parser(self.mock_endpoint, {})
        cases = [
            (self._build_template(
                parameters={'instance_flavor': {'type': 'string'}},
                servers=[{'get_param': 'instance_flavor'}]),
             'Unable to resolve flavor reference'),
            (self._build_template(
                parameters={'volume_size': {'type': 'number'}},
                volumes=[{'get_param': 'volume_size'}]),
             'Unable to resolve volume size reference'),
        ]
        for template, expected_msg in cases:
            with self.assertRaises(ValueError) as cm:
                parser.parse(template, runtime_parameters=None)
            self.assertIn(expected_msg, str(cm.exception))

    def test_direct_flavor_reference(self):
        """Test direct flavor string resolution and lookup failure."""
        parser = self._parser(self.mock_endpoint, {})

        # Successful lookup
        self.mock_endpoint._findFlavor.return_value = Mock(vcpus=2, ram=4096)
        template = self._build_template(servers=['m1.medium'])
        quota = parser.parse(template)
        self.assertEqual(quota.quota['cores'], 2)
        self.assertEqual(quota.quota['ram'], 4096)

        # Flavor not found in cloud
        self.mock_endpoint._findFlavor.side_effect = (
            openstack.exceptions.NotFoundException(
                "Flavor m1.nonexistent not found"))
        template = self._build_template(servers=['m1.nonexistent'])
        with self.assertRaises(ValueError) as cm:
            parser.parse(template)
        self.assertIn('m1.nonexistent', str(cm.exception))

    def test_multiple_servers_with_runtime_params(self):
        """Test parsing multiple servers with runtime parameters."""
        template = self._build_template(
            parameters={'bastion_flavor': {'type': 'string'},
                        'worker_flavor': {'type': 'string'}},
            servers=[{'get_param': 'bastion_flavor'},
                     {'get_param': 'worker_flavor'}])

        def find_flavor(name):
            if name == 'm1.small':
                return Mock(vcpus=1, ram=2048)
            elif name == 'm1.large':
                return Mock(vcpus=4, ram=8192)

        self.mock_endpoint._findFlavor.side_effect = find_flavor

        parser = self._parser(self.mock_endpoint, {})
        runtime_params = {
            'bastion_flavor': 'm1.small',
            'worker_flavor': 'm1.large',
        }

        quota = parser.parse(template, runtime_parameters=runtime_params)

        self.assertEqual(quota.quota['instances'], 2)
        self.assertEqual(quota.quota['cores'], 5)  # 1 + 4
        self.assertEqual(quota.quota['ram'], 10240)  # 2048 + 8192

    def test_rejects_external_id(self):
        """Test that external_id is rejected with and without policy."""
        template = textwrap.dedent("""\
            resources:
              existing_net:
                type: OS::Neutron::Net
                external_id: some-net-uuid
        """)
        # Rejected with policy
        parser = self._parser(self.mock_endpoint, {})
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('external_id', str(cm.exception))
        self.assertIn('existing_net', str(cm.exception))

        # Also rejected without policy (security checks always run)
        parser = self._parser(self.mock_endpoint, template_policy=None)
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('external_id', str(cm.exception))

    def test_deletion_policy(self):
        """Test that forbidden deletion policies are rejected."""
        parser = self._parser(self.mock_endpoint, {})

        for policy in ('retain', 'RETAIN', 'Retain',
                       'snapshot', 'SNAPSHOT', 'Snapshot'):
            template = textwrap.dedent(f"""\
                resources:
                  res:
                    type: OS::Neutron::Net
                    deletion_policy: {policy}
            """)
            with self.assertRaises(TemplatePolicyViolation,
                                   msg=f"Expected rejection for '{policy}'"):
                parser.parse(template)

        # 'Delete' is allowed
        template = textwrap.dedent("""\
            resources:
              net:
                type: OS::Neutron::Net
                deletion_policy: Delete
        """)
        quota = parser.parse(template)
        self.assertEqual(quota.quota['networks'], 1)

    def test_rejects_get_file(self):
        """Test that get_file usage anywhere in the template is rejected."""
        template = textwrap.dedent("""\
            resources:
              foo:
                properties:
                  write_files:
                    - content: {get_file: setup.sh}
                      path: /opt/setup.sh
        """)
        parser = self._parser(self.mock_endpoint, {})
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('get_file', str(cm.exception))

    def test_rejects_unsupported_resource_types(self):
        """Test that unsupported resource types are rejected."""
        parser = self._parser(self.mock_endpoint, template_policy=None)

        for res_type in self.unsupported_types:
            template = self._build_template(
                networks=1, extra_resources={'r': res_type})
            with self.assertRaises(TemplatePolicyViolation) as cm:
                parser.parse(template, validate_policy=False)
            self.assertIn(res_type, str(cm.exception))

    def test_intrinsic_function_policy(self):
        """Test allowed-intrinsic-functions policy enforcement."""
        template = textwrap.dedent("""\
            resources:
              foo:
                properties:
                  name:
                    str_replace:
                      template: prefix-$suffix
                      params:
                        $suffix: {get_param: suffix}
                  items: {list_join: [',', ['a', 'b']]}
                  network: {get_resource: net}
                  security_groups:
                    - if:
                        - use_sg
                        - {get_resource: sg}
                        - ''
        """)

        def policy(allowed):
            return {'allowed_intrinsic_functions': allowed}

        # Disallowed function is rejected (including nested 'if')
        parser = self._parser(self.mock_endpoint, policy(
            ['get_param', 'get_resource', 'str_replace', 'list_join']))
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn("'if'", str(cm.exception))

        # Exact matches pass
        parser = self._parser(self.mock_endpoint, policy(
            ['get_param', 'get_resource', 'str_replace', 'list_join', 'if']))
        parser.parse(template)

        # Regex patterns pass
        parser = self._parser(self.mock_endpoint, policy(
            ['get_(param|resource)', 'str_replace(_strict)?', 'list_.*',
             'if']))
        parser.parse(template)

        # Empty list means no restriction
        parser = self._parser(self.mock_endpoint, policy([]))
        parser.parse(template)

        # Absent policy means no restriction
        parser = self._parser(self.mock_endpoint, template_policy=None)
        parser.parse(template)


class TestValidatePolicy(BaseTestCase):
    """Test the standalone validatePolicy function."""

    def test_rejects_disallowed_intrinsic_functions(self):
        """Test that validatePolicy checks intrinsic functions."""
        template = textwrap.dedent("""\
            resources:
              foo:
                properties:
                  cmd:
                    str_replace:
                      template: echo NAME
                      params:
                        NAME: {get_attr: [server, name]}
        """)
        policy = {
            'allowed_intrinsic_functions': ['get_param'],
        }
        with self.assertRaises(TemplatePolicyViolation) as cm:
            heatutils.validatePolicy(policy, template)
        self.assertIn('get_attr', str(cm.exception))
