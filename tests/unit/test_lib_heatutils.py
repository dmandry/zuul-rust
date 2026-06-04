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

from unittest.mock import Mock

import openstack
import yaml

from tests.base import BaseTestCase
from zuul.lib.heatutils import (
    HeatTemplateParser,
    TemplatePolicyViolation,
    UNSUPPORTED_TYPES,
    validatePolicy,
)


class TestHeatTemplateParser(BaseTestCase):
    """Test Heat template parser functionality."""

    @staticmethod
    def _build_template(servers=None, volumes=None, networks=0,
                        floating_ips=0, routers=0, resource_groups=0,
                        extra_resources=None, parameters=None):
        tpl = {'heat_template_version': 'rocky'}

        if parameters:
            params = {}
            for name, attrs in parameters.items():
                p = {'type': attrs['type']}
                if 'default' in attrs:
                    p['default'] = attrs['default']
                params[name] = p
            tpl['parameters'] = params

        resources = {}

        for i, flavor in enumerate(servers or []):
            rname = f'server{i}' if len(servers) > 1 else 'server'
            resources[rname] = {
                'type': 'OS::Nova::Server',
                'properties': {'flavor': flavor},
            }

        for i, size in enumerate(volumes or []):
            rname = f'vol{i}' if len(volumes) > 1 else 'vol'
            resources[rname] = {
                'type': 'OS::Cinder::Volume',
                'properties': {'size': size},
            }

        for i in range(networks):
            rname = f'net{i}' if networks > 1 else 'net'
            resources[rname] = {'type': 'OS::Neutron::Net'}

        for i in range(floating_ips):
            rname = f'fip{i}' if floating_ips > 1 else 'fip'
            resources[rname] = {'type': 'OS::Neutron::FloatingIP'}

        for i in range(routers):
            rname = f'router{i}' if routers > 1 else 'router'
            resources[rname] = {'type': 'OS::Neutron::Router'}

        for i in range(resource_groups):
            rname = f'rg{i}' if resource_groups > 1 else 'rg'
            resources[rname] = {'type': 'OS::Heat::ResourceGroup'}

        if extra_resources:
            for rname, rtype in extra_resources.items():
                resources[rname] = {'type': rtype}

        if resources:
            tpl['resources'] = resources

        return yaml.safe_dump(tpl, default_flow_style=False, sort_keys=False)

    def setUp(self):
        super().setUp()
        self.mock_endpoint = Mock()

    def test_parse_flavor_via_get_param(self):
        """Test get_param resolution priority for flavors."""
        flavors = {
            'm1.small': Mock(vcpus=1, ram=2048),
            'm1.large': Mock(vcpus=4, ram=8192),
        }
        self.mock_endpoint._findFlavor.side_effect = flavors.get
        parser = HeatTemplateParser(self.mock_endpoint, {})

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

    def test_parse_volume_size_via_get_param(self):
        """Test volume size resolution via get_param."""
        template = self._build_template(
            parameters={'volume_size': {'type': 'number'}},
            volumes=[{'get_param': 'volume_size'}])
        parser = HeatTemplateParser(self.mock_endpoint, {})
        runtime_params = {'volume_size': 100}

        quota = parser.parse(template, runtime_parameters=runtime_params)

        self.assertEqual(quota.quota['volumes'], 1)
        self.assertEqual(quota.quota['volume-gb'], 100)

    def test_parse_fails_missing_required_properties(self):
        """Test that resources missing required properties raise ValueError."""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        cases = [
            ("missing required 'flavor' property", """\
heat_template_version: 2015-04-30
resources:
  server:
    type: OS::Nova::Server
    properties:
      image: test-image
"""),
            ("missing required 'size' property", """\
heat_template_version: 2015-04-30
resources:
  volume:
    type: OS::Cinder::Volume
    properties:
      description: test volume
"""),
        ]
        for expected_msg, template in cases:
            with self.assertRaises(ValueError) as cm:
                parser.parse(template)
            self.assertIn(expected_msg, str(cm.exception))

    def test_parse_fails_unresolvable_references(self):
        """Test that unresolvable get_param references raise ValueError."""
        parser = HeatTemplateParser(self.mock_endpoint, {})
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

    def test_parse_direct_flavor_reference(self):
        """Test direct flavor string resolution and lookup failure."""
        parser = HeatTemplateParser(self.mock_endpoint, {})

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

    def test_parse_multiple_servers_with_runtime_params(self):
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

        parser = HeatTemplateParser(self.mock_endpoint, {})
        runtime_params = {
            'bastion_flavor': 'm1.small',
            'worker_flavor': 'm1.large',
        }

        quota = parser.parse(template, runtime_parameters=runtime_params)

        self.assertEqual(quota.quota['instances'], 2)
        self.assertEqual(quota.quota['cores'], 5)  # 1 + 4
        self.assertEqual(quota.quota['ram'], 10240)  # 2048 + 8192

    def test_parse_rejects_external_id(self):
        """Test that resources with external_id are rejected."""
        template = """\
heat_template_version: rocky
resources:
  existing_net:
    type: OS::Neutron::Net
    external_id: some-net-uuid
"""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('external_id', str(cm.exception))
        self.assertIn('existing_net', str(cm.exception))

    def test_parse_rejects_deletion_policy_retain(self):
        """Test that deletion_policy: Retain is rejected."""
        template = """\
heat_template_version: rocky
resources:
  kept_net:
    type: OS::Neutron::Net
    deletion_policy: Retain
"""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('deletion_policy', str(cm.exception))
        self.assertIn('kept_net', str(cm.exception))

    def test_parse_rejects_deletion_policy_snapshot(self):
        """Test that deletion_policy: Snapshot is rejected."""
        template = """\
heat_template_version: rocky
resources:
  snapped_vol:
    type: OS::Cinder::Volume
    deletion_policy: Snapshot
    properties:
      size: 10
"""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('deletion_policy', str(cm.exception))
        self.assertIn('snapped_vol', str(cm.exception))

    def test_parse_rejects_deletion_policy_case_insensitive(self):
        """Test that deletion_policy check is case-insensitive."""
        for policy in ('retain', 'RETAIN', 'Retain',
                       'snapshot', 'SNAPSHOT', 'Snapshot'):
            template = f"""\
heat_template_version: rocky
resources:
  res:
    type: OS::Neutron::Net
    deletion_policy: {policy}
"""
            parser = HeatTemplateParser(self.mock_endpoint, {})
            with self.assertRaises(TemplatePolicyViolation,
                                   msg=f"Expected rejection for '{policy}'"):
                parser.parse(template)

    def test_parse_allows_deletion_policy_delete(self):
        """Test that deletion_policy: Delete is allowed."""
        template = """\
heat_template_version: rocky
resources:
  net:
    type: OS::Neutron::Net
    deletion_policy: Delete
"""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        quota = parser.parse(template)
        self.assertEqual(quota.quota['networks'], 1)

    def test_parse_rejects_get_file(self):
        """Test that get_file usage anywhere in the template is rejected."""
        template = """\
heat_template_version: rocky
resources:
  config:
    type: OS::Heat::CloudConfig
    properties:
      cloud_config:
        write_files:
          - content: {get_file: setup.sh}
            path: /opt/setup.sh
"""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('get_file', str(cm.exception))

    def test_parse_rejects_get_file_top_level(self):
        """Test that get_file at the top level of a resource is rejected."""
        template = """\
heat_template_version: rocky
resources:
  script:
    type: OS::Heat::SoftwareConfig
    properties:
      config: {get_file: my_script.sh}
"""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('get_file', str(cm.exception))

    def test_parse_runs_security_checks_without_policy(self):
        """Test that parse runs security checks without template policy."""
        template = """\
heat_template_version: rocky
resources:
  leaked:
    type: OS::Neutron::Net
    external_id: leftover-uuid
"""
        parser = HeatTemplateParser(self.mock_endpoint, template_policy=None)
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('external_id', str(cm.exception))

    def test_parse_rejects_unsupported_resource_types(self):
        """Test that unsupported resource types are rejected."""
        self.mock_endpoint._findFlavor.return_value = Mock(vcpus=1, ram=2048)
        parser_no_policy = HeatTemplateParser(
            self.mock_endpoint, template_policy=None)

        for res_type in UNSUPPORTED_TYPES:
            template = self._build_template(
                servers=['m1.small'], networks=1,
                extra_resources={'r': res_type})
            with self.assertRaises(TemplatePolicyViolation) as cm:
                parser_no_policy.parse(template, validate_policy=False)
            self.assertIn(res_type, str(cm.exception))

    def test_parse_rejects_disallowed_intrinsic_functions(self):
        """Test that intrinsic functions not in allow-list are rejected."""
        template = """\
heat_template_version: rocky
resources:
  server:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
      name:
        str_replace:
          template: prefix-$suffix
          params:
            $suffix: {get_param: name_suffix}
"""
        policy = {
            'allowed_intrinsic_functions': ['get_param', 'get_resource'],
        }
        parser = HeatTemplateParser(self.mock_endpoint, policy)
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn('str_replace', str(cm.exception))

    def test_parse_allows_matching_intrinsic_functions(self):
        """Test that intrinsic functions matching allow-list pass."""
        self.mock_endpoint._findFlavor.return_value = Mock(vcpus=1, ram=2048)
        template = """\
heat_template_version: rocky
parameters:
  name_suffix:
    type: string
    default: test
resources:
  server:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
      name:
        str_replace:
          template: prefix-$suffix
          params:
            $suffix: {get_param: name_suffix}
"""
        policy = {
            'allowed_intrinsic_functions': [
                'get_param', 'get_resource', 'str_replace'],
        }
        parser = HeatTemplateParser(self.mock_endpoint, policy)
        quota = parser.parse(template)
        self.assertEqual(quota.quota['instances'], 1)

    def test_parse_intrinsic_functions_regex_patterns(self):
        """Test that intrinsic function patterns support regex."""
        self.mock_endpoint._findFlavor.return_value = Mock(vcpus=1, ram=2048)
        template = """\
heat_template_version: rocky
parameters:
  suffix:
    type: string
    default: test
resources:
  server:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
      name:
        str_replace:
          template: prefix-$suffix
          params:
            $suffix: {get_param: suffix}
      metadata:
        items: {list_join: [',', ['a', 'b']]}
"""
        policy = {
            'allowed_intrinsic_functions': [
                'get_(param|resource|attr)',
                'str_replace(_strict)?',
                'list_.*',
            ],
        }
        parser = HeatTemplateParser(self.mock_endpoint, policy)
        quota = parser.parse(template)
        self.assertEqual(quota.quota['instances'], 1)

    def test_parse_intrinsic_functions_empty_list_allows_all(self):
        """Test that empty allowed-intrinsic-functions means no restriction."""
        self.mock_endpoint._findFlavor.return_value = Mock(vcpus=1, ram=2048)
        template = """\
heat_template_version: rocky
parameters:
  suffix:
    type: string
    default: test
resources:
  server:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
      name:
        str_replace:
          template: prefix-$suffix
          params:
            $suffix: {get_param: suffix}
"""
        policy = {
            'allowed_intrinsic_functions': [],
        }
        parser = HeatTemplateParser(self.mock_endpoint, policy)
        quota = parser.parse(template)
        self.assertEqual(quota.quota['instances'], 1)

    def test_parse_intrinsic_functions_no_policy_allows_all(self):
        """Test that absent template policy allows all intrinsic functions."""
        self.mock_endpoint._findFlavor.return_value = Mock(vcpus=1, ram=2048)
        template = """\
heat_template_version: rocky
parameters:
  suffix:
    type: string
    default: test
resources:
  server:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
      name:
        str_replace:
          template: prefix-$suffix
          params:
            $suffix: {get_param: suffix}
"""
        parser = HeatTemplateParser(self.mock_endpoint, template_policy=None)
        quota = parser.parse(template)
        self.assertEqual(quota.quota['instances'], 1)

    def test_validate_policy_rejects_disallowed_intrinsic_functions(self):
        """Test that validatePolicy checks intrinsic functions."""
        template = """\
heat_template_version: rocky
resources:
  config:
    type: OS::Heat::CloudConfig
    properties:
      cloud_config:
        runcmd:
          - str_replace:
              template: echo NAME
              params:
                NAME: {get_attr: [server, name]}
"""
        policy = {
            'allowed_intrinsic_functions': ['get_param'],
        }
        with self.assertRaises(TemplatePolicyViolation) as cm:
            validatePolicy(policy, template)
        self.assertIn('get_attr', str(cm.exception))

    def test_intrinsic_function_detection_nested(self):
        """Test that intrinsic functions are detected in nested structures."""
        template = """\
heat_template_version: rocky
resources:
  port:
    type: OS::Neutron::Port
    properties:
      network: {get_resource: net}
      security_groups:
        - if:
            - use_sg
            - {get_resource: sg}
            - ''
  net:
    type: OS::Neutron::Net
"""
        policy = {
            'allowed_intrinsic_functions': ['get_resource'],
        }
        parser = HeatTemplateParser(self.mock_endpoint, policy)
        with self.assertRaises(TemplatePolicyViolation) as cm:
            parser.parse(template)
        self.assertIn("'if'", str(cm.exception))
