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

import io
import time

import paramiko
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPublicKey,
    RSAPrivateKey,
)

from zuul.lib import encryption
from zuul.lib import keystorage3
from zuul.zk import ZooKeeperClient
from zuul.zk.components import (
    ComponentRegistry,
    COMPONENT_REGISTRY
)

from tests.base import (
    BaseTestCase,
    ZOOKEEPER_SESSION_TIMEOUT,
)


class TestKeyStorage3(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.setupZK()
        self.zk_client = ZooKeeperClient(
            self.zk_chroot_fixture.zk_hosts,
            tls_cert=self.zk_chroot_fixture.zookeeper_cert,
            tls_key=self.zk_chroot_fixture.zookeeper_key,
            tls_ca=self.zk_chroot_fixture.zookeeper_ca,
            timeout=ZOOKEEPER_SESSION_TIMEOUT,
        )
        self.addCleanup(self.zk_client.disconnect)
        self.zk_client.connect()
        self.setupModelPin()
        self.component_registry = ComponentRegistry(self.zk_client)
        # We don't have any other component to initialize the global
        # registry in these tests, so we do it ourselves.
        COMPONENT_REGISTRY.create(self.zk_client)
        self.password = 'keystorage_password'

    def test_project_ssh_keys(self):
        start_time = int(time.time())
        ks = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks.stop)
        # This will generate a new ssh key
        info = ks.getProjectSSHKeyInfo('gerrit', 'org/project')
        now = int(time.time())
        self.assertEqual(1, len(info.keys))
        key1 = info.keys[0]
        self.assertEqual(0, key1.version)
        self.assertTrue(start_time <= key1.created <= now)
        self.assertIn('ssh-rsa', key1.public_key)
        self.assertIn('RSA PRIVATE KEY', key1.private_key)
        pk = key1.toDict()['private_key']
        self.assertIn('ENCRYPTED', pk)
        # Make sure it's encrypted with the password
        with io.StringIO(pk) as o:
            paramiko.RSAKey.from_private_key(o, self.password)
        # Ensure the key info returns the right keys
        self.assertEqual(key1.public_key, info.public_key)
        self.assertEqual([key1.private_key], info.private_keys)

        # Make sure we get the same object if we look it up again
        info2 = ks.getProjectSSHKeyInfo('gerrit', 'org/project')
        self.assertTrue(info is info2)

        # Another client to read it from ZK
        ks2 = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks2.stop)
        ks2_info = ks2.getProjectSSHKeyInfo('gerrit', 'org/project')

        # This seems unlikely, but make sure we have different objects
        self.assertFalse(ks2_info is info)
        ks2_key1 = ks2_info.keys[0]
        self.assertFalse(ks2_key1 is key1)
        # And they are equivalent
        self.assertEqual(key1.toDict(), ks2_key1.toDict())
        self.assertEqual(key1.public_key, ks2_key1.public_key)
        self.assertEqual(key1.private_key, ks2_key1.private_key)

    def test_tenant_ssh_keys(self):
        start_time = int(time.time())
        ks = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks.stop)
        # This will generate a new ssh key
        info = ks.getTenantSSHKeyInfo('tenant1')
        now = int(time.time())
        self.assertEqual(1, len(info.keys))
        key1 = info.keys[0]
        self.assertEqual(0, key1.version)
        self.assertTrue(start_time <= key1.created <= now)
        self.assertIn('ssh-rsa', key1.public_key)
        self.assertIn('RSA PRIVATE KEY', key1.private_key)
        pk = key1.toDict()['private_key']
        self.assertIn('ENCRYPTED', pk)
        # Make sure it's encrypted with the password
        with io.StringIO(pk) as o:
            paramiko.RSAKey.from_private_key(o, self.password)
        # Ensure the key info returns the right keys
        self.assertEqual(key1.public_key, info.public_key)
        self.assertEqual([key1.private_key], info.private_keys)

        # Make sure we get the same object if we look it up again
        info2 = ks.getTenantSSHKeyInfo('tenant1')
        self.assertTrue(info is info2)

        # Another client to read it from ZK
        ks2 = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks2.stop)
        ks2_info = ks2.getTenantSSHKeyInfo('tenant1')

        # This seems unlikely, but make sure we have different objects
        self.assertFalse(ks2_info is info)
        ks2_key1 = ks2_info.keys[0]
        self.assertFalse(ks2_key1 is key1)
        # And they are equivalent
        self.assertEqual(key1.toDict(), ks2_key1.toDict())
        self.assertEqual(key1.public_key, ks2_key1.public_key)
        self.assertEqual(key1.private_key, ks2_key1.private_key)

    def test_project_secret_keys(self):
        start_time = int(time.time())
        ks = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks.stop)
        # This will generate a new secrets key
        info = ks.getProjectSecretsKeyInfo('gerrit', 'org/project')
        now = int(time.time())
        self.assertEqual(1, len(info.keys))
        key1 = info.keys[0]
        self.assertEqual(0, key1.version)
        self.assertTrue(start_time <= key1.created <= now)
        self.assertTrue(isinstance(key1.public_key, RSAPublicKey))
        self.assertTrue(isinstance(key1.private_key, RSAPrivateKey))
        pk = key1.toDict()['private_key']
        self.assertIn('ENCRYPTED', pk)
        # Make sure it's encrypted with the password
        encryption.deserialize_rsa_keypair(pk.encode('utf8'),
                                           self.password.encode('utf8'))
        # Ensure the key info returns the right keys
        self.assertEqual(key1.public_key, info.public_key)
        self.assertEqual([key1.private_key], info.private_keys)

        # Make sure we get the same object if we look it up again
        info2 = ks.getProjectSecretsKeyInfo('gerrit', 'org/project')
        self.assertTrue(info is info2)

        # Another client to read it from ZK
        ks2 = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks2.stop)
        ks2_info = ks2.getProjectSecretsKeyInfo('gerrit', 'org/project')

        # This seems unlikely, but make sure we have different objects
        self.assertFalse(ks2_info is info)
        ks2_key1 = ks2_info.keys[0]
        self.assertFalse(ks2_key1 is key1)
        # And they are equivalent
        self.assertEqual(key1.toDict(), ks2_key1.toDict())
        self.assertEqual(key1.public_key.public_numbers(),
                         ks2_key1.public_key.public_numbers())
        self.assertEqual(key1.private_key.private_numbers(),
                         ks2_key1.private_key.private_numbers())

    def test_rotate_project_ssh_keys(self):
        ks = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks.stop)
        # This will generate a new secrets key
        info = ks.getProjectSSHKeyInfo('gerrit', 'org/project')
        self.assertEqual(1, len(info.keys))
        key1 = info.keys[0]
        self.assertIn('ssh-rsa', key1.public_key)
        pk1 = key1.toDict()['private_key']

        ks.addProjectSSHKey('gerrit', 'org/project')
        self.assertEqual(2, len(info.keys))
        key2 = info.keys[1]
        self.assertIn('ssh-rsa', key2.public_key)
        pk2 = key2.toDict()['private_key']
        self.assertNotEqual(pk1, pk2)
        self.assertEqual(0, key1.version)
        self.assertEqual(1, key2.version)

        self.assertEqual(key2.public_key, info.public_key)
        self.assertEqual([key1.private_key, key2.private_key],
                         info.private_keys)

        ks.deleteProjectSSHKey('gerrit', 'org/project', 0)
        self.assertEqual(1, len(info.keys))
        key3 = info.keys[0]
        self.assertEqual(1, key3.version)

    def test_rotate_tenant_ssh_keys(self):
        ks = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks.stop)
        # This will generate a new secrets key
        info = ks.getTenantSSHKeyInfo('tenant-one')
        self.assertEqual(1, len(info.keys))
        key1 = info.keys[0]
        self.assertIn('ssh-rsa', key1.public_key)
        pk1 = key1.toDict()['private_key']

        ks.addTenantSSHKey('tenant-one')
        self.assertEqual(2, len(info.keys))
        key2 = info.keys[1]
        self.assertIn('ssh-rsa', key2.public_key)
        pk2 = key2.toDict()['private_key']
        self.assertNotEqual(pk1, pk2)
        self.assertEqual(0, key1.version)
        self.assertEqual(1, key2.version)

        self.assertEqual(key2.public_key, info.public_key)
        self.assertEqual([key1.private_key, key2.private_key],
                         info.private_keys)

        ks.deleteTenantSSHKey('tenant-one', 0)
        self.assertEqual(1, len(info.keys))
        key3 = info.keys[0]
        self.assertEqual(1, key3.version)

    def test_rotate_project_secret_keys(self):
        ks = keystorage3.KeyStorage3(self.zk_client, self.password)
        self.addCleanup(ks.stop)
        # This will generate a new secrets key
        info = ks.getProjectSecretsKeyInfo('gerrit', 'org/project')
        self.assertEqual(1, len(info.keys))
        key1 = info.keys[0]
        self.assertTrue(isinstance(key1.public_key, RSAPublicKey))
        pk1 = key1.toDict()['private_key']

        ks.addProjectSecretsKey('gerrit', 'org/project')
        self.assertEqual(2, len(info.keys))
        key2 = info.keys[1]
        self.assertTrue(isinstance(key2.public_key, RSAPublicKey))
        pk2 = key2.toDict()['private_key']
        self.assertNotEqual(pk1, pk2)
        self.assertEqual(0, key1.version)
        self.assertEqual(1, key2.version)

        self.assertEqual(key2.public_key, info.public_key)
        self.assertEqual([key1.private_key, key2.private_key],
                         info.private_keys)

        ks.deleteProjectSecretsKey('gerrit', 'org/project', 0)
        self.assertEqual(1, len(info.keys))
        key3 = info.keys[0]
        self.assertEqual(1, key3.version)
