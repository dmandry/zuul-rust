# Copyright 2024 Acme Gating, LLC
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

import contextlib
import logging
import os
import time
import unittest
from unittest import mock

import fixtures

from zuul.driver.openstack import OpenstackDriver
from zuul.driver.openstack.openstackmodel import OpenstackProviderNode
from zuul.driver.openstack.openstackprovider import OpenstackProvider

from tests.fake_openstack import (
    FakeOpenstackCloud,
    FakeOpenstackFloatingIp,
    FakeOpenstackPort,
    FakeOpenstackProviderEndpoint,
)
from tests.base import (
    FIXTURE_DIR,
    ResponsesFixture,
    ZuulTestCase,
    driver_config,
    iterate_timeout,
    return_data,
    simple_layout,
)
from tests.unit.test_launcher import ImageMocksFixture
from tests.unit.test_cloud_driver import BaseCloudDriverTest


class BaseOpenstackDriverTest(ZuulTestCase):
    cloud_test_image_format = 'qcow2'
    cloud_test_provider_name = 'openstack-main'
    config_file = 'zuul-connections-nodepool.conf'
    debian_return_data = {
        'zuul': {
            'artifacts': [
                {
                    'name': 'qcow2 image',
                    'url': 'http://example.com/image.qcow2',
                    'metadata': {
                        'type': 'zuul_image',
                        'image_name': 'debian-local',
                        'format': 'qcow2',
                        'sha256': ImageMocksFixture.qcow2_sha256,
                        'md5sum': ImageMocksFixture.qcow2_md5sum,
                    }
                },
            ]
        }
    }
    openstack_needs_floating_ip = False
    openstack_auto_attach_floating_ip = True

    def _makeFakeCloud(self, region):
        fake_cloud = FakeOpenstackCloud(
            region,
            needs_floating_ip=self.openstack_needs_floating_ip,
            auto_attach_floating_ip=self.openstack_auto_attach_floating_ip,
        )
        fake_cloud.max_instances =\
            self.test_config.driver.openstack.get('max_instances', 100)
        fake_cloud.max_cores =\
            self.test_config.driver.openstack.get('max_cores', 100)
        fake_cloud.max_ram =\
            self.test_config.driver.openstack.get('max_ram', 1000000)
        fake_cloud.max_volumes =\
            self.test_config.driver.openstack.get('max_volumes', 100)
        fake_cloud.max_volume_gb =\
            self.test_config.driver.openstack.get('max_volume_gb', 100)
        self.fake_cloud[region] = fake_cloud

    def setUp(self):
        self.initTestConfig()
        responses_fixture = self.useFixture(ResponsesFixture())
        self.useFixture(ImageMocksFixture(responses_fixture))
        clouds_yaml = os.path.join(FIXTURE_DIR, 'clouds.yaml')
        self.useFixture(
            fixtures.EnvironmentVariable('OS_CLIENT_CONFIG_FILE', clouds_yaml))

        self.fake_cloud = {}
        self._makeFakeCloud(None)
        self._makeFakeCloud('region2')
        self.patch(OpenstackDriver, '_endpoint_class',
                   FakeOpenstackProviderEndpoint)
        self.patch(FakeOpenstackProviderEndpoint,
                   '_fake_cloud', self.fake_cloud)
        super().setUp()

    @contextlib.contextmanager
    def _block_futures(self):
        with (mock.patch(
                'zuul.driver.openstack.openstackendpoint.'
                'OpenstackProviderEndpoint._completeApi', return_value=None)):
            yield


class TestOpenstackDriver(BaseOpenstackDriverTest, BaseCloudDriverTest):
    def _assertProviderNodeAttributes(self, pnode):
        super()._assertProviderNodeAttributes(pnode)
        self.assertEqual('fakecloud', pnode.cloud)
        self.assertEqual('region1', pnode.region)
        if checks := self.test_config.driver.openstack.get('node_checks'):
            checks(self, pnode)

    @simple_layout('layouts/openstack/nodepool.yaml', enable_nodepool=True)
    def test_openstack_node_lifecycle(self):
        self._test_node_lifecycle('debian-normal')

    def check_more_attrs(self, pnode):
        self.assertEqual('foo', pnode.az)

    @simple_layout('layouts/openstack/more.yaml', enable_nodepool=True)
    @driver_config('openstack', node_checks=check_more_attrs)
    def test_openstack_node_lifecycle_more(self):
        # Test with more options than the normal test
        self._test_node_lifecycle('debian-normal')

    @simple_layout('layouts/openstack/nodepool.yaml', enable_nodepool=True)
    @driver_config('openstack', max_cores=4)
    def test_openstack_quota(self):
        self._test_quota('debian-normal')

    @simple_layout('layouts/openstack/resource-limits.yaml',
                   enable_nodepool=True)
    def test_openstack_resource_limits(self):
        self._test_quota('debian-normal')

    @simple_layout('layouts/openstack/nodepool-image.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        BaseOpenstackDriverTest.debian_return_data,
    )
    def test_openstack_diskimage(self):
        self._test_diskimage()

    # Openstack-driver specific tests

    @simple_layout('layouts/openstack/nodepool-multi.yaml',
                   enable_nodepool=True)
    def test_openstack_resource_cleanup(self):
        self.waitUntilSettled()
        self.launcher.cleanup_worker.INTERVAL = 1
        conn = self.fake_cloud[None]._getConnection()
        # This test class doesn't generally use fips, but since we're
        # testing *all* leaked resources, set it here.
        conn.cloud._fake_needs_floating_ip = True

        system_id = self.launcher.system.system_id
        tags = {
            'zuul_system_id': system_id,
            'zuul_node_uuid': '0000000042',
        }
        flavor = conn.list_flavors()[0]
        conn.create_server(
            name="test",
            meta=tags,
            flavor=flavor,
        )
        self.assertEqual(1, len(conn.list_servers()))

        fip = FakeOpenstackFloatingIp(
            id='42',
            floating_ip_address='fake',
            port_id=None,
            status='ACTIVE',
        )
        conn.cloud.floating_ips.append(fip)
        self.assertEqual(1, len(conn.list_floating_ips()))

        port = FakeOpenstackPort(
            id='43',
            status='DOWN',
            device_owner='compute:foo',
        )
        conn.cloud.ports.append(port)
        self.assertEqual(1, len(conn.list_ports()))

        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()

        for _ in iterate_timeout(30, 'instance deletion'):
            if not conn.list_servers():
                break
            time.sleep(1)
        for _ in iterate_timeout(30, 'fip deletion'):
            if not conn.list_floating_ips():
                break
            time.sleep(1)
        for _ in iterate_timeout(30, 'port deletion'):
            if not conn.list_ports():
                break
            time.sleep(1)

    @simple_layout('layouts/openstack/nodepool.yaml', enable_nodepool=True)
    def test_state_machines(self):
        label_name = "debian-normal"
        provider_name = "openstack-main"
        node_class = OpenstackProviderNode
        future_names = ['delete_future', 'create_future']
        self._test_state_machines(label_name, provider_name,
                                  node_class, future_names)


class TestOpenstackDriverFloatingIp(BaseOpenstackDriverTest,
                                    BaseCloudDriverTest):
    # This test is for nova-net clouds with floating ips that require
    # manual attachment.
    openstack_needs_floating_ip = True
    openstack_auto_attach_floating_ip = False

    @simple_layout('layouts/openstack/nodepool.yaml', enable_nodepool=True)
    def test_openstack_fip(self):
        self._test_node_lifecycle('debian-normal')

    @simple_layout('layouts/openstack/nodepool.yaml', enable_nodepool=True)
    def test_state_machines(self):
        label_name = "debian-normal"
        provider_name = "openstack-main"
        node_class = OpenstackProviderNode
        future_names = ['delete_future', 'create_future']
        self._test_state_machines(label_name, provider_name,
                                  node_class, future_names)


class TestOpenstackDriverAutoAttachFloatingIp(BaseOpenstackDriverTest,
                                              BaseCloudDriverTest):
    openstack_needs_floating_ip = True
    openstack_auto_attach_floating_ip = True

    @simple_layout('layouts/openstack/nodepool.yaml', enable_nodepool=True)
    def test_openstack_auto_attach_fip(self):
        self._test_node_lifecycle('debian-normal')

    @simple_layout('layouts/openstack/nodepool.yaml', enable_nodepool=True)
    def test_state_machines(self):
        label_name = "debian-normal"
        provider_name = "openstack-main"
        node_class = OpenstackProviderNode
        future_names = ['delete_future', 'create_future']
        self._test_state_machines(label_name, provider_name,
                                  node_class, future_names)


class TestOpenstackProviderHostname(unittest.TestCase):
    """Unit tests for OpenstackProvider.getCreateStateMachine hostname."""

    def _provider_for_label(self, label_based_hostname):
        p = OpenstackProvider()
        lbl = mock.Mock(
            flavor='flv', image='img',
            label_based_hostname=label_based_hostname)
        p._set(
            labels={'debian-normal': lbl},
            flavors={'flv': mock.Mock()},
            images={'img': mock.Mock()},
            _endpoint=mock.Mock(),
        )
        return p

    def test_hostname_legacy_when_unset(self):
        p = self._provider_for_label(None)
        node = mock.Mock(
            label='debian-normal',
            uuid='abcdef0123456789abcdef01234567')
        log = logging.getLogger('zuul.test')
        with mock.patch(
                'zuul.driver.openstack.openstackprovider.'
                'OpenstackCreateStateMachine') as ctor:
            p.getCreateStateMachine(node, None, log)
        ctor.assert_called_once()
        args = ctor.call_args[0]
        self.assertEqual(args[2], 'npabcdef0123456')

    def test_hostname_legacy_when_false(self):
        p = self._provider_for_label(False)
        node = mock.Mock(
            label='debian-normal',
            uuid='abcdef0123456789abcdef01234567')
        log = logging.getLogger('zuul.test')
        with mock.patch(
                'zuul.driver.openstack.openstackprovider.'
                'OpenstackCreateStateMachine') as ctor:
            p.getCreateStateMachine(node, None, log)
        self.assertEqual(ctor.call_args[0][2], 'npabcdef0123456')

    def test_hostname_label_based_when_true(self):
        p = self._provider_for_label(True)
        node = mock.Mock(
            label='debian-normal',
            uuid='abcdef0123456789abcdef01234567')
        log = logging.getLogger('zuul.test')
        with mock.patch(
                'zuul.driver.openstack.openstackprovider.'
                'OpenstackCreateStateMachine') as ctor:
            p.getCreateStateMachine(node, None, log)
        self.assertEqual(ctor.call_args[0][2], 'debian-normal-abcdef01')
