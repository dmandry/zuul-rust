# Copyright 2018 Red Hat, Inc.
# Copyright 2022 Acme Gating, LLC
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

import io
import os
import sys
import subprocess
import time
import configparser
import datetime
import dateutil.tz
import uuid
from unittest import mock

import fixtures
import jwt
import testtools
import sqlalchemy

from zuul.lib import encryption
from zuul.lib.keystorage import OIDCSigningKeys
from zuul.zk import ZooKeeperClient
from zuul.zk.locks import SessionAwareLock
from zuul.cmd.client import parse_cutoff

from tests.unit.test_launcher import LauncherBaseTestCase
from tests.base import BaseTestCase, ZuulTestCase
from tests.base import (
    FIXTURE_DIR,
    iterate_timeout,
    return_data,
    simple_layout,
)

from kazoo.exceptions import NoNodeError


class BaseClientTestCase(BaseTestCase):
    config_file = 'zuul.conf'
    config_with_zk = True

    def setUp(self):
        super(BaseClientTestCase, self).setUp()
        self.test_root = self.useFixture(fixtures.TempDir(
            rootdir=os.environ.get("ZUUL_TEST_ROOT"))).path
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(FIXTURE_DIR, self.config_file))
        if self.config_with_zk:
            self.config_add_zk()

    def config_add_zk(self):
        self.setupZK()
        self.config.add_section('zookeeper')
        self.config.set('zookeeper', 'hosts', self.zk_chroot_fixture.zk_hosts)
        self.config.set('zookeeper', 'session_timeout', '30')
        self.config.set('zookeeper', 'tls_cert',
                        self.zk_chroot_fixture.zookeeper_cert)
        self.config.set('zookeeper', 'tls_key',
                        self.zk_chroot_fixture.zookeeper_key)
        self.config.set('zookeeper', 'tls_ca',
                        self.zk_chroot_fixture.zookeeper_ca)


class TestTenantValidationClient(BaseClientTestCase):
    config_with_zk = True

    def test_client_tenant_conf_check(self):
        self.config.set(
            'scheduler', 'tenant_config',
            os.path.join(FIXTURE_DIR, 'config/tenant-parser/simple.yaml'))
        with open(os.path.join(self.test_root, 'tenant_ok.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'tenant_ok.conf'),
             'tenant-conf-check'], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 0, 'The command must exit 0')

        self.config.set(
            'scheduler', 'tenant_config',
            os.path.join(FIXTURE_DIR, 'config/tenant-parser/invalid.yaml'))
        with open(os.path.join(self.test_root, 'tenant_ko.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'tenant_ko.conf'),
             'tenant-conf-check'], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 1, "The command must exit 1")
        self.assertIn(
            b"expected a dictionary for dictionary", out,
            "Expected error message not found")

        self.config.set(
            'scheduler', 'tenant_config',
            os.path.join(FIXTURE_DIR, 'config/tenant-parser/semaphore.yaml'))
        with open(os.path.join(self.test_root, 'tenant_sem.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'tenant_sem.conf'),
             'tenant-conf-check'], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 1, "The command must exit 1")
        self.assertIn(
            b"global semaphore", out,
            "Expected error message not found")

    def test_client_rbac_conf_check(self):
        self.config.set(
            'scheduler', 'tenant_config',
            os.path.join(FIXTURE_DIR,
                         'config/tenant-parser/invalid_role_rbac.yaml'))
        with open(os.path.join(self.test_root, 't_role_rbac.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 't_role_rbac.conf'),
             'tenant-conf-check'], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 1, "The command must exit 1")
        self.assertIn(
            b'Authorization Role "not_a_valid_role" was not found', out,
            "Expected error message not found")

        self.config.set(
            'scheduler', 'tenant_config',
            os.path.join(FIXTURE_DIR,
                         'config/tenant-parser/invalid_rule_rbac.yaml'))
        with open(os.path.join(self.test_root, 't_rule_rbac.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 't_rule_rbac.conf'),
             'tenant-conf-check'], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 1, "The command must exit 1")
        self.assertIn(
            b'Authorization Rule "auth-rule-three" was not found', out,
            "Expected error message not found")

        self.config.set(
            'scheduler', 'tenant_config',
            os.path.join(FIXTURE_DIR,
                         'config/tenant-parser/invalid_root_role_rbac.yaml'))
        with open(os.path.join(self.test_root, 'r_role_rbac.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'r_role_rbac.conf'),
             'tenant-conf-check'], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 1, "The command must exit 1")
        self.assertIn(
            b'Authorization Role "not_a_valid_role" was not found', out,
            "Expected error message not found")

        self.config.set(
            'scheduler', 'tenant_config',
            os.path.join(FIXTURE_DIR,
                         'config/tenant-parser/invalid_root_rule_rbac.yaml'))
        with open(os.path.join(self.test_root, 'r_rule_rbac.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'r_rule_rbac.conf'),
             'tenant-conf-check'], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 1, "The command must exit 1")
        self.assertIn(
            b'Authorization Rule "auth-rule-three" was not found', out,
            "Expected error message not found")

        self.config.set(
            'scheduler', 'tenant_config',
            os.path.join(FIXTURE_DIR,
                         'config/tenant-parser/rbac.yaml'))
        with open(os.path.join(self.test_root, 'rbac.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'rbac.conf'),
             'tenant-conf-check'], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 0, "The command must exit 0")
        self.assertIn(
            b'Tenants config validated with success', out,
            "Expected error message not found")


class TestWebTokenClient(BaseClientTestCase):
    config_file = 'zuul-admin-web.conf'

    def test_no_authenticator(self):
        """Test that token generation is not possible without authenticator"""
        old_conf = io.StringIO()
        self.config.write(old_conf)
        self.config.remove_section('auth zuul_operator')
        with open(os.path.join(self.test_root,
                               'no_zuul_operator.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'no_zuul_operator.conf'),
             'create-auth-token',
             '--auth-config', 'zuul_operator',
             '--user', 'marshmallow_man',
             '--tenant', 'tenant_one', ],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        old_conf.seek(0)
        self.config = configparser.ConfigParser()
        self.config.read_file(old_conf)
        self.assertEqual(p.returncode, 1, 'The command must exit 1')

    def test_unsupported_driver(self):
        """Test that token generation is not possible with wrong driver"""
        old_conf = io.StringIO()
        self.config.write(old_conf)
        self.config.add_section('auth someauth')
        self.config.set('auth someauth', 'driver', 'RS256withJWKS')
        with open(os.path.join(self.test_root, 'JWKS.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'JWKS.conf'),
             'create-auth-token',
             '--auth-config', 'someauth',
             '--user', 'marshmallow_man',
             '--tenant', 'tenant_one', ],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        old_conf.seek(0)
        self.config = configparser.ConfigParser()
        self.config.read_file(old_conf)
        self.assertEqual(p.returncode, 1, 'The command must exit 1')

    def test_token_generation(self):
        """Test token generation"""
        with open(os.path.join(self.test_root, 'good.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'good.conf'),
             'create-auth-token',
             '--auth-conf', 'zuul_operator',
             '--user', 'marshmallow_man',
             '--tenant', 'tenant_one',
             '--claim', 'claim1:value1',
             '--claim', 'claim2:value2',
             '--print-meta-info', ],
            stdout=subprocess.PIPE)
        now = time.time()
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 0, 'The command must exit 0')
        self.assertTrue(out.startswith(b"Bearer "), out)
        # Get the token from the first line of the output
        token = jwt.decode(out.split(b'\n')[0][len("Bearer "):],
                           key=self.config.get(
                               'auth zuul_operator',
                               'secret'),
                           algorithms=[self.config.get(
                               'auth zuul_operator',
                               'driver')],
                           audience=self.config.get(
                               'auth zuul_operator',
                               'client_id'),)
        self.assertEqual('marshmallow_man', token.get('sub'))
        self.assertEqual('zuul_operator', token.get('iss'))
        self.assertEqual('zuul.example.com', token.get('aud'))
        self.assertEqual('value1', token.get('claim1'))
        self.assertEqual('value2', token.get('claim2'))
        admin_tenants = token.get('zuul', {}).get('admin', [])
        self.assertTrue('tenant_one' in admin_tenants, admin_tenants)
        # allow one minute for the process to run
        self.assertTrue(580 <= int(token['exp']) - now < 660,
                        (token['exp'], now))

        # Get the meta info from the second line of the output
        meta_info_header = out.split(b'\n')[1]
        self.assertIn("Meta Info", meta_info_header.decode('utf-8'))

    def test_token_generation_no_zuul_admin(self):
        """Test token generation without zuul.admin claim"""
        with open(os.path.join(self.test_root, 'good.conf'), 'w') as f:
            self.config.write(f)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', os.path.join(self.test_root, 'good.conf'),
             'create-auth-token',
             '--auth-conf', 'zuul_operator',
             '--user', 'marshmallow_man',],
            stdout=subprocess.PIPE,)
        now = time.time()
        out, _ = p.communicate()
        self.assertEqual(p.returncode, 0, 'The command must exit 0')
        self.assertTrue(out.startswith(b"Bearer "), out)
        # Get the token from the first line of the output
        token = jwt.decode(out.split(b'\n')[0][len("Bearer "):],
                           key=self.config.get(
                               'auth zuul_operator',
                               'secret'),
                           algorithms=[self.config.get(
                               'auth zuul_operator',
                               'driver')],
                           audience=self.config.get(
                               'auth zuul_operator',
                               'client_id'),)
        self.assertEqual('marshmallow_man', token.get('sub'))
        self.assertEqual('zuul_operator', token.get('iss'))
        self.assertEqual('zuul.example.com', token.get('aud'))
        self.assertNotIn('zuul', token)
        # allow one minute for the process to run
        self.assertTrue(580 <= int(token['exp']) - now < 660,
                        (token['exp'], now))


class TestKeyOperations(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_export_import(self):
        # Test a round trip export/import of keys
        export_root = os.path.join(self.test_root, 'export')
        config_file = os.path.join(self.test_root, 'zuul.conf')
        with open(config_file, 'w') as f:
            self.config.write(f)

        # Save a copy of the keys in ZK
        old_data = self.getZKTree('/keystorage')

        # Export keys
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'export-keys', export_root],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))
        self.assertEqual(p.returncode, 0)

        # Delete keys from ZK
        self.zk_client.client.delete('/keystorage', recursive=True)

        # Make sure it's really gone
        with testtools.ExpectedException(NoNodeError):
            self.getZKTree('/keystorage')

        # Import keys
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'import-keys', export_root],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))
        self.assertEqual(p.returncode, 0)

        # Make sure the new data matches the original
        new_data = self.getZKTree('/keystorage')
        self.assertEqual(new_data, old_data)

    def test_copy_delete(self):
        config_file = os.path.join(self.test_root, 'zuul.conf')
        with open(config_file, 'w') as f:
            self.config.write(f)

        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'copy-keys',
             'gerrit', 'org/project',
             'gerrit', 'neworg/newproject',
             ],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))
        self.assertEqual(p.returncode, 0)

        data = self.getZKTree('/keystorage')
        self.assertEqual(
            data['/keystorage/gerrit/org/org%2Fproject/secrets'],
            data['/keystorage/gerrit/neworg/neworg%2Fnewproject/secrets'])
        self.assertEqual(
            data['/keystorage/gerrit/org/org%2Fproject/ssh'],
            data['/keystorage/gerrit/neworg/neworg%2Fnewproject/ssh'])

        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'delete-keys',
             'gerrit', 'org/project',
             ],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))
        self.assertEqual(p.returncode, 0)

        data = self.getZKTree('/keystorage')
        self.assertIsNone(
            data.get('/keystorage/gerrit/org/org%2Fproject/secrets'))
        self.assertIsNone(
            data.get('/keystorage/gerrit/org/org%2Fproject/ssh'))
        self.assertIsNone(
            data.get('/keystorage/gerrit/org/org%2Fproject'))
        # Ensure that deleting one project in a tree doesn't remove other
        # projects in that tree.
        self.assertIsNotNone(
            data.get('/keystorage/gerrit/org/org%2Fproject1'))
        self.assertIsNotNone(
            data.get('/keystorage/gerrit/org/org%2Fproject2'))
        self.assertIsNotNone(
            data.get('/keystorage/gerrit/org'))

        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'delete-keys',
             'gerrit', 'org/project1',
             ],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))
        self.assertEqual(p.returncode, 0)

        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'delete-keys',
             'gerrit', 'org/project2',
             ],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))
        self.assertEqual(p.returncode, 0)

        data = self.getZKTree('/keystorage')
        # Ensure that the last project being removed also removes its
        # org prefix entry.
        self.assertIsNone(
            data.get('/keystorage/gerrit/org/org%2Fproject1'))
        self.assertIsNone(
            data.get('/keystorage/gerrit/org/org%2Fproject2'))
        self.assertIsNone(
            data.get('/keystorage/gerrit/org'))


class TestOIDCSigningKeyOperation(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_delete_oidc_signing_keys(self):
        algorithm = "RS256"
        keystore = self.scheds.first.sched.keystore

        # Makesure keys are created by calling getLatestOidcSigningKeys()
        private_key1, _, version1 = keystore.getLatestOidcSigningKeys(
            algorithm)

        with keystore.createZKContext() as context:
            # Check that the keys are there
            test_keys1 = OIDCSigningKeys.loadKeys(
                context, algorithm)
            self.assertEqual(len(test_keys1.keys), 1)
            self.assertEqual(version1, 0)

            # Config and exeute the delete command
            config_file = os.path.join(self.test_root, 'zuul.conf')
            with open(config_file, 'w') as f:
                self.config.write(f)

            p = subprocess.Popen(
                [os.path.join(sys.prefix, 'bin/zuul-admin'),
                 '-c', config_file,
                 'delete-oidc-signing-keys',
                 algorithm,
                 ],
                stdout=subprocess.PIPE)
            out, _ = p.communicate()
            self.log.debug(out.decode('utf8'))
            self.assertEqual(p.returncode, 0)

            # Keys should be gone
            test_keys2 = OIDCSigningKeys.loadKeys(
                context, algorithm)
            self.assertIsNone(test_keys2)

        # New keys are auto generated when calling getLatestOidcSigningKeys()
        private_key2, _, version2 = keystore.getLatestOidcSigningKeys(
            algorithm)
        self.assertEqual(4096, private_key2.key_size)
        self.assertNotEqual(
            encryption.serialize_rsa_private_key(private_key2),
            encryption.serialize_rsa_private_key(private_key1))
        self.assertEqual(version2, 0)


class TestOfflineZKOperations(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def shutdown(self):
        pass

    def assertFinalState(self):
        pass

    def assertCleanShutdown(self):
        pass

    def test_delete_state(self):
        # Shut everything down (as much as possible) to reduce
        # logspam and errors.
        ZuulTestCase.shutdown(self)

        # Re-start the client connection because we need one for the
        # test.
        self.zk_client = ZooKeeperClient.fromConfig(self.config)
        self.zk_client.connect()

        config_file = os.path.join(self.test_root, 'zuul.conf')
        with open(config_file, 'w') as f:
            self.config.write(f)

        # Save a copy of the keys in ZK
        old_data = self.getZKTree('/keystorage')

        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'delete-state',
             ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
        out, _ = p.communicate(b'yes\n')
        self.log.debug(out.decode('utf8'))

        # Make sure the keys are still around
        new_data = self.getZKTree('/keystorage')
        self.assertEqual(new_data, old_data)

        # Make sure we really deleted everything
        with testtools.ExpectedException(NoNodeError):
            self.getZKTree('/zuul')

        self.zk_client.disconnect()

    def test_delete_state_keep_config_cache(self):
        # Shut everything down (as much as possible) to reduce
        # logspam and errors.
        ZuulTestCase.shutdown(self)

        # Re-start the client connection because we need one for the
        # test.
        self.zk_client = ZooKeeperClient.fromConfig(self.config)
        self.zk_client.connect()

        config_file = os.path.join(self.test_root, 'zuul.conf')
        with open(config_file, 'w') as f:
            self.config.write(f)

        # Save a copy of the things we keep in ZK
        old_keys = self.getZKTree('/keystorage')
        old_config = self.getZKTree('/zuul/config')

        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'delete-state', '--keep-config-cache',
             ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
        out, _ = p.communicate(b'yes\n')
        self.log.debug(out.decode('utf8'))

        # Make sure the keys are still around
        new_keys = self.getZKTree('/keystorage')
        self.assertEqual(new_keys, old_keys)
        new_config = self.getZKTree('/zuul/config')
        self.assertEqual(new_config, old_config)

        # Make sure we really deleted everything
        children = self.zk_client.client.get_children('/zuul')
        self.assertEqual(['config'], children)

        self.zk_client.disconnect()


class TestOnlineZKOperations(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def assertSQLState(self):
        pass

    def _test_delete_pipeline(self, pipeline):
        sched = self.scheds.first.sched
        tenant = sched.abide.tenants['tenant-one']
        # Force a reconfiguration due to a config change (so that the
        # tenant trigger event queue gets a minimum timestamp set)
        file_dict = {'zuul.yaml': ''}
        M = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        M.setMerged()
        self.fake_gerrit.addEvent(M.getChangeMergedEvent())
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        if pipeline == 'check':
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        else:
            A.addApproval('Code-Review', 2)
            self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        # Lock the check pipeline so we don't process the event we're
        # about to submit (it should go into the pipeline trigger event
        # queue and stay there while we delete the pipeline state).
        # This way we verify that events arrived before the deletion
        # still work.
        plock = SessionAwareLock(
            self.zk_client.client,
            f"/zuul/locks/pipeline/{tenant.name}/{pipeline}")
        plock.acquire(blocking=True, timeout=None)
        try:
            self.log.debug('Got pipeline lock')
            # Add a new event while our old last reconfigure time is
            # in place.
            B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
            if pipeline == 'check':
                self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
            else:
                B.addApproval('Code-Review', 2)
                self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

            # Wait until it appears in the pipeline trigger event queue
            self.log.debug('Waiting for event')
            for x in iterate_timeout(30, 'trigger event queue has events'):
                if sched.pipeline_trigger_events[
                        tenant.name][pipeline].hasEvents():
                    break
            self.log.debug('Got event')
        except Exception:
            plock.release()
            raise
        # Grab the run handler lock so that we will continue to avoid
        # further processing of the event after we release the
        # pipeline lock (which the delete command needs to acquire).
        sched.run_handler_lock.acquire()
        try:
            plock.release()
            self.log.debug('Got run lock')
            config_file = os.path.join(self.test_root, 'zuul.conf')
            with open(config_file, 'w') as f:
                self.config.write(f)

            # Make sure the pipeline exists
            self.getZKTree(
                f'/zuul/tenant/{tenant.name}/pipeline/{pipeline}/item')
            # Save the old layout uuid
            tenant = sched.abide.tenants[tenant.name]
            old_layout_uuid = tenant.layout.uuid
            self.log.debug('Deleting pipeline state')
            p = subprocess.Popen(
                [os.path.join(sys.prefix, 'bin/zuul-admin'),
                 '-c', config_file,
                 'delete-pipeline-state',
                 tenant.name, pipeline,
                 ],
                stdout=subprocess.PIPE)
            # Delete the pipeline state
            out, _ = p.communicate()
            self.log.debug(out.decode('utf8'))
            self.assertEqual(p.returncode, 0, 'The command must exit 0')
            # Make sure it's deleted
            with testtools.ExpectedException(NoNodeError):
                self.getZKTree(
                    f'/zuul/tenant/{tenant.name}/pipeline/{pipeline}/item')
            # Make sure the change list is re-created
            self.getZKTree(
                f'/zuul/tenant/{tenant.name}/pipeline/{pipeline}/change_list')
        finally:
            sched.run_handler_lock.release()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='2,1'),
            dict(name='project-merge', result='SUCCESS', changes='3,1'),
            dict(name='project-test1', result='SUCCESS', changes='3,1'),
            dict(name='project-test2', result='SUCCESS', changes='3,1'),
        ], ordered=False)
        tenant = sched.abide.tenants[tenant.name]
        new_layout_uuid = tenant.layout.uuid
        self.assertEqual(old_layout_uuid, new_layout_uuid)
        self.assertEqual(
            tenant.layout.pipeline_managers[pipeline].state.layout_uuid,
            old_layout_uuid)

    def test_delete_pipeline_check(self):
        self._test_delete_pipeline('check')

    def test_delete_pipeline_gate(self):
        self._test_delete_pipeline('gate')


class TestDBPruneParse(BaseTestCase):
    def test_db_prune_parse(self):
        now = datetime.datetime(year=2023, month=5, day=28,
                                hour=22, minute=15, second=1,
                                tzinfo=dateutil.tz.tzutc())
        reference = datetime.datetime(year=2022, month=5, day=28,
                                      hour=22, minute=15, second=1,
                                      tzinfo=dateutil.tz.tzutc())
        # Test absolute times
        self.assertEqual(
            reference,
            parse_cutoff(now, '2022-05-28 22:15:01 UTC', None))
        self.assertEqual(
            reference,
            parse_cutoff(now, '2022-05-28 22:15:01', None))

        # Test relative times
        self.assertEqual(reference,
                         parse_cutoff(now, None, '8760h'))
        self.assertEqual(reference,
                         parse_cutoff(now, None, '365d'))
        with testtools.ExpectedException(RuntimeError):
            self.assertEqual(reference,
                             parse_cutoff(now, None, '1y'))


class DBPruneTestCase(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'
    # This should be larger than the limit size in sqlconnection
    num_buildsets = 55

    def _createBuildset(self, update_time):
        connection = self.scheds.first.sched.sql.connection
        buildset_uuid = uuid.uuid4().hex
        event_id = uuid.uuid4().hex
        tenant = 'tenant-one',
        event_type = "reconfiguration"
        description = "A reconfiguration happened"

        with connection.getSession() as db:
            start_time = update_time - datetime.timedelta(seconds=1)
            end_time = update_time
            db_buildset = db.createBuildSet(
                uuid=buildset_uuid,
                tenant=tenant,
                pipeline='check',
                event_id=event_id,
                event_timestamp=update_time,
                updated=update_time,
                first_build_start_time=start_time,
                last_build_end_time=end_time,
                result='SUCCESS',
            )
            db_ref = db.getOrCreateRef(
                project='org/project',
                ref='refs/changes/1',
                change=1,
                patchset='1',
                oldrev='',
                newrev='',
                branch='master',
                ref_url='http://gerrit.example.com/1',
            )
            db_buildset.refs.append(db_ref)

            for build_num in range(2):
                build_uuid = uuid.uuid4().hex
                db_build = db_buildset.createBuild(
                    ref=db_ref,
                    uuid=build_uuid,
                    job_name=f'job{build_num}',
                    start_time=start_time,
                    end_time=end_time,
                    result='SUCCESS',
                    voting=True,
                )
                for art_num in range(2):
                    db_build.createArtifact(
                        name=f'artifact{art_num}',
                        url='http://example.com',
                    )
                for provides_num in range(2):
                    db_build.createProvides(
                        name=f'item{provides_num}',
                    )
                for event_num in range(2):
                    db_build.createBuildEvent(
                        event_type=f'event{event_num}',
                        event_time=start_time,
                    )
            db.createSystemEvent(
                tenant=tenant,
                event_id=event_id,
                event_time=update_time,
                event_type=event_type,
                description=description,
            )

    def _query(self, db, model):
        table = model.__table__
        q = db.session().query(model).order_by(table.c.id.desc())
        try:
            return q.all()
        except sqlalchemy.orm.exc.NoResultFound:
            return []

    def _getBuildsets(self, db):
        return self._query(db, db.connection.buildSetModel)

    def _getBuilds(self, db):
        return self._query(db, db.connection.buildModel)

    def _getProvides(self, db):
        return self._query(db, db.connection.providesModel)

    def _getArtifacts(self, db):
        return self._query(db, db.connection.artifactModel)

    def _getBuildEvents(self, db):
        return self._query(db, db.connection.buildEventModel)

    def _getSystemEvents(self, db):
        return self._query(db, db.connection.systemEventModel)

    def _setup(self):
        config_file = os.path.join(self.test_root, 'zuul.conf')
        with open(config_file, 'w') as f:
            self.config.write(f)

        update_time = (datetime.datetime.utcnow() -
                       datetime.timedelta(minutes=self.num_buildsets))
        for x in range(self.num_buildsets):
            update_time = update_time + datetime.timedelta(minutes=1)
            self._createBuildset(update_time)

        connection = self.scheds.first.sched.sql.connection
        with connection.getSession() as db:
            buildsets = self._getBuildsets(db)
            builds = self._getBuilds(db)
            artifacts = self._getArtifacts(db)
            provides = self._getProvides(db)
            events = self._getBuildEvents(db)
            system_events = self._getSystemEvents(db)
        self.assertEqual(len(buildsets), self.num_buildsets)
        self.assertEqual(len(builds), 2 * self.num_buildsets)
        self.assertEqual(len(artifacts), 4 * self.num_buildsets)
        self.assertEqual(len(provides), 4 * self.num_buildsets)
        self.assertEqual(len(events), 4 * self.num_buildsets)
        self.assertEqual(len(system_events), self.num_buildsets)
        for build in builds:
            self.log.debug("Build %s %s %s",
                           build, build.start_time, build.end_time)
        return config_file

    def test_db_prune_before(self):
        # Test pruning buildsets before a specific date
        config_file = self._setup()
        connection = self.scheds.first.sched.sql.connection

        # Builds are reverse ordered; 0 is most recent
        buildsets = connection.getBuildsets()
        start_time = buildsets[0].first_build_start_time
        self.log.debug("Cutoff %s", start_time)

        # Use the default batch size (omit --batch-size arg)
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'prune-database',
             '--before', str(start_time),
             ],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))

        with connection.getSession() as db:
            buildsets = self._getBuildsets(db)
            builds = self._getBuilds(db)
            artifacts = self._getArtifacts(db)
            provides = self._getProvides(db)
            events = self._getBuildEvents(db)
            system_events = self._getSystemEvents(db)
        for build in builds:
            self.log.debug("Build %s %s %s",
                           build, build.start_time, build.end_time)
        self.assertEqual(len(buildsets), 1)
        self.assertEqual(len(builds), 2)
        self.assertEqual(len(artifacts), 4)
        self.assertEqual(len(provides), 4)
        self.assertEqual(len(events), 4)
        self.assertEqual(len(system_events), 1)

    def test_db_prune_older_than(self):
        # Test pruning buildsets older than a relative time
        config_file = self._setup()
        connection = self.scheds.first.sched.sql.connection

        # We use 0d as the relative time here since the earliest we
        # support is 1d and that's tricky in unit tests.  The
        # prune_before test handles verifying that we don't just
        # always delete everything.
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'prune-database',
             '--older-than', '0d',
             '--batch-size', '5',
             ],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))

        with connection.getSession() as db:
            buildsets = self._getBuildsets(db)
            builds = self._getBuilds(db)
            artifacts = self._getArtifacts(db)
            provides = self._getProvides(db)
            events = self._getBuildEvents(db)
            system_events = self._getSystemEvents(db)
        self.assertEqual(len(buildsets), 0)
        self.assertEqual(len(builds), 0)
        self.assertEqual(len(artifacts), 0)
        self.assertEqual(len(provides), 0)
        self.assertEqual(len(events), 0)
        self.assertEqual(len(system_events), 0)


class TestDBPruneMysql(DBPruneTestCase):
    config_file = 'zuul-sql-driver-mysql.conf'


class TestDBPrunePostgres(DBPruneTestCase):
    config_file = 'zuul-sql-driver-postgres.conf'


class TestImageOperations(LauncherBaseTestCase):
    config_file = 'zuul-connections-nodepool.conf'
    tenant_config_file = 'config/multi-tenant-provider/main.yaml'

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
    def test_image_export_import(self, mock_image_upload_run):
        # Test a round trip export/import of images
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        export_root = os.path.join(self.test_root, 'export')
        config_file = os.path.join(self.test_root, 'zuul.conf')
        with open(config_file, 'w') as f:
            self.config.write(f)

        # Save a copy of ZK (only the data, not the locks dirs)
        old_images = self.getZKTree('/zuul/images/artifacts')
        old_uploads = self.getZKTree('/zuul/image-uploads/uploads')

        # Stop the launcher so we don't touch ZK
        self.launcher.stop()
        self.launcher.join()

        # Export images
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'export-images', export_root],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))
        self.assertEqual(p.returncode, 0)

        # Delete images from ZK
        self.zk_client.client.delete('/zuul/images', recursive=True)
        self.zk_client.client.delete('/zuul/image-uploads', recursive=True)

        # Make sure it's really gone
        with testtools.ExpectedException(NoNodeError):
            self.getZKTree('/zuul/images')
        with testtools.ExpectedException(NoNodeError):
            self.getZKTree('/zuul/image-uploads')

        # Import keys
        p = subprocess.Popen(
            [os.path.join(sys.prefix, 'bin/zuul-admin'),
             '-c', config_file,
             'import-images', export_root],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()
        self.log.debug(out.decode('utf8'))
        self.assertEqual(p.returncode, 0)

        # Make sure the new data matches the original
        new_images = self.getZKTree('/zuul/images/artifacts')
        self.assertEqual(old_images, new_images)
        new_uploads = self.getZKTree('/zuul/image-uploads/uploads')
        self.assertEqual(old_uploads, new_uploads)

        # Restart the launcher for graceful test shutdown
        self.launcher = self.createLauncher()
        self.waitUntilSettled()
