# Copyright 2018 Red Hat, Inc.
# Copyright 2025 BMW Group
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

import logging

from zuul.lib import encryption
from zuul.lib.keystorage1 import KeyStorage1
from zuul.lib.keystorage3 import KeyStorage3, ZuulSecretKey, ZuulSSHKey
from zuul.lib.keystorage_oidc import OIDCSigningKeysCache, OIDCSigningKeys
from zuul.zk.components import COMPONENT_REGISTRY
from zuul.zk.zkobject import ZKContext

RSA_KEY_SIZE = 2048


class KeyStorage:
    log = logging.getLogger("zuul.KeyStorage")

    def __init__(self, zookeeper_client, password, start_cache=True):
        self.password = password
        self.password_bytes = password.encode('utf8')
        self.client = zookeeper_client
        self.keystorage1 = KeyStorage1(zookeeper_client, password)
        self.keystorage3 = KeyStorage3(zookeeper_client, password)
        if start_cache:
            self.oidc_signing_keys_cache = OIDCSigningKeysCache(self.client)
        else:
            self.oidc_signing_keys_cache = None

    @property
    def _keystorage(self):
        # TODO: When we remove this backwards compat code, we
        # should add a migration to rmtree the old cache.
        if COMPONENT_REGISTRY.model_api < 38:
            return self.keystorage1
        return self.keystorage3

    def createZKContext(self):
        return ZKContext(self.client, None, None, self.log)

    def stop(self):
        if self.oidc_signing_keys_cache:
            self.oidc_signing_keys_cache.stop()
        self.keystorage1.stop()
        self.keystorage3.stop()

    def exportKeys(self):
        data = self.keystorage1.exportKeys()
        data3 = self.keystorage3.exportKeys()
        data['keys'].update(data3['keys'])
        return data

    def importKeys(self, import_data, overwrite):
        self.keystorage1.importKeys(import_data, overwrite)
        self.keystorage3.importKeys(import_data, overwrite)

    def getProjectSSHKeysPath(self, connection_name, project_name):
        return self._keystorage.getProjectSSHKeysPath(
            connection_name, project_name)

    def getProjectSSHKeyInfo(self, connection_name, project_name,
                             create=True):
        if COMPONENT_REGISTRY.model_api < 38:
            return self._keystorage.getProjectSSHKeyInfo(
                connection_name, project_name)
        info = self.keystorage3.getProjectSSHKeyInfo(
            connection_name, project_name, create=False)
        if info is not None or create is False:
            return info
        old_data = self.keystorage1.loadProjectSSHKeys(
            connection_name, project_name)
        if old_data is None:
            return self.keystorage3.getProjectSSHKeyInfo(
                connection_name, project_name)
        keys = [ZuulSSHKey.fromDict(k, self.password)
                for k in old_data['keys']]
        return self.keystorage3.upgradeProjectSSHKeyInfo(
            connection_name, project_name, keys)

    def addProjectSSHKey(self, connection_name, project_name):
        if COMPONENT_REGISTRY.model_api < 38:
            raise Exception("Unsupported operation: "
                            "all Zuul components must be upgraded")
        self.keystorage3.addProjectSSHKey(connection_name, project_name)

    def copyProjectSSHKeys(self,
                           src_connection_name,
                           src_project_name,
                           dest_connection_name,
                           dest_project_name):
        return self._keystorage.copyProjectSSHKeys(
            src_connection_name,
            src_project_name,
            dest_connection_name,
            dest_project_name)

    def deleteProjectSSHKey(self, connection_name, project_name, version):
        if COMPONENT_REGISTRY.model_api < 38:
            raise Exception("Unsupported operation: "
                            "all Zuul components must be upgraded")
        self.keystorage3.deleteProjectSSHKey(
            connection_name, project_name, version)

    def deleteProjectSSHKeys(self, connection_name, project_name):
        return self._keystorage.deleteProjectSSHKeys(
            connection_name, project_name)

    def getTenantSSHKeysPath(self, tenant_name):
        return self._keystorage.getTenantSSHKeysPath(tenant_name)

    def getTenantSSHKeyInfo(self, tenant_name, create=True):
        if COMPONENT_REGISTRY.model_api < 38:
            return self._keystorage.getTenantSSHKeyInfo(tenant_name)
        info = self.keystorage3.getTenantSSHKeyInfo(tenant_name, create=False)
        if info is not None or create is False:
            return info
        old_data = self.keystorage1.loadTenantSSHKeys(tenant_name)
        if old_data is None:
            return self.keystorage3.getTenantSSHKeyInfo(tenant_name)
        keys = [ZuulSSHKey.fromDict(k, self.password)
                for k in old_data['keys']]
        return self.keystorage3.upgradeTenantSSHKeyInfo(tenant_name, keys)

    def addTenantSSHKey(self, tenant_name):
        if COMPONENT_REGISTRY.model_api < 38:
            raise Exception("Unsupported operation: "
                            "all Zuul components must be upgraded")
        self.keystorage3.addTenantSSHKey(tenant_name)

    def copyTenantSSHKeys(self,
                          src_tenant_name,
                          dest_tenant_name):
        return self._keystorage.copyTenantSSHKeys(
            src_tenant_name,
            dest_tenant_name)

    def deleteTenantSSHKey(self, tenant_name, version):
        if COMPONENT_REGISTRY.model_api < 38:
            raise Exception("Unsupported operation: "
                            "all Zuul components must be upgraded")
        self.keystorage3.deleteTenantSSHKey(tenant_name, version)

    def deleteTenantSSHKeys(self, tenant_name):
        return self._keystorage.deleteTenantSSHKeys(tenant_name)

    def getProjectSecretsKeysPath(self, connection_name, project_name):
        return self._keystorage.getProjectSecretsKeysPath(
            connection_name, project_name)

    def getProjectSecretsKeyInfo(self, connection_name, project_name,
                                 create=True):
        if COMPONENT_REGISTRY.model_api < 38:
            return self._keystorage.getProjectSecretsKeyInfo(
                connection_name, project_name)
        info = self.keystorage3.getProjectSecretsKeyInfo(
            connection_name, project_name, create=False)
        if info is not None or create is False:
            return info
        old_data = self.keystorage1.loadProjectSecretsKeys(
            connection_name, project_name)
        if old_data is None:
            return self.keystorage3.getProjectSecretsKeyInfo(
                connection_name, project_name)
        keys = [ZuulSecretKey.fromDict(k, self.password_bytes)
                for k in old_data['keys']]
        return self.keystorage3.upgradeProjectSecretsKeyInfo(
            connection_name, project_name, keys)

    def addProjectSecretsKey(self, connection_name, project_name):
        if COMPONENT_REGISTRY.model_api < 38:
            raise Exception("Unsupported operation: "
                            "all Zuul components must be upgraded")
        self.keystorage3.addProjectSecretsKey(connection_name, project_name)

    def copyProjectSecretsKeys(self,
                               src_connection_name,
                               src_project_name,
                               dest_connection_name,
                               dest_project_name):
        return self._keystorage.copyProjectSecretsKeys(
            src_connection_name,
            src_project_name,
            dest_connection_name,
            dest_project_name)

    def deleteProjectSecretsKey(self, connection_name, project_name, version):
        if COMPONENT_REGISTRY.model_api < 38:
            raise Exception("Unsupported operation: "
                            "all Zuul components must be upgraded")
        self.keystorage3.deleteProjectSecretsKey(
            connection_name, project_name, version)

    def deleteProjectSecretsKeys(self, connection_name, project_name):
        return self._keystorage.deleteProjectSecretsKeys(
            connection_name, project_name)

    def deleteProjectDir(self, connection_name, project_name):
        return self._keystorage.deleteProjectDir(
            connection_name, project_name)

    def rotateOidcSigningKeys(self, algorithm, rotation_interval, max_ttl):
        """
        Rotate the OIDC signing keys.
        This method is to be called periodically to rotate the OIDC
        signing keys. It creates a new key and/or remove the older
        keys when necessary.
        """
        with self.createZKContext() as context:
            OIDCSigningKeys.rotateKeys(
                context, algorithm, self.password_bytes,
                rotation_interval, max_ttl)

    def getOidcSigningKeyData(self, algorithm):
        """
        Return the key data of an algorithm of OIDC singing keys
        The data returned is from ZuulTreeCache, could be not in sync with
        the actual data in Zookeeper.
        """

        oidc_signing_keys = self.oidc_signing_keys_cache.getSigningKeys(
            algorithm)
        # If it is not found in cache, it could be the cache hasn't
        # been synced or the key has not been created yet. We need to
        # check both.
        if not oidc_signing_keys:
            with self.createZKContext() as context:
                oidc_signing_keys = OIDCSigningKeys.loadKeys(
                    context, algorithm)
                if not oidc_signing_keys:
                    OIDCSigningKeys.createAndStoreKeys(
                        context, algorithm, self.password_bytes)
                    oidc_signing_keys = OIDCSigningKeys.loadKeys(
                        context, algorithm)

        return oidc_signing_keys

    def getLatestOidcSigningKeys(self, algorithm):
        """
        Return the latest key pair of an algorithm of OIDC singing keys
        The data rerunted is from ZuulTreeCache, could be not in sync with
        the actual data in Zookeeper.
        """
        signing_key_data = self.getOidcSigningKeyData(algorithm=algorithm)
        latest_key = signing_key_data.keys[-1]
        pem_private_key = latest_key["private_key"].encode("utf-8")
        version = latest_key["version"]

        private_key, public_key = encryption.deserialize_rsa_keypair(
            pem_private_key, self.password_bytes)

        return private_key, public_key, version

    def deleteOidcSigningKeys(self, algorithm):
        """Delete the complete internal data structure"""
        with self.createZKContext() as context:
            OIDCSigningKeys.deleteKeysByAlgorithm(context, algorithm)
