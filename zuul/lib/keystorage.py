# Copyright 2018 Red Hat, Inc.
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
import itertools
import json
import logging
import time
from contextlib import suppress

import cachetools
import kazoo
import paramiko

from zuul.exceptions import AlgorithmNotSupportedException
from zuul.lib import encryption, strings
from zuul.zk import ZooKeeperBase
from zuul.zk.cache import ZuulTreeCache
from zuul.zk.zkobject import ZKContext, ZKObject

RSA_KEY_SIZE = 2048


class OIDCSigningKeysCache(ZuulTreeCache):

    def __init__(self, client):
        self.cache_updated_listeners = []
        super().__init__(
            client, OIDCSigningKeys.OIDC_ROOT_PATH, async_worker=False)

    def addCacheUpdatedListener(self, listener):
        self.cache_updated_listeners.append(listener)

    def postCacheHook(self, event, data, stat, key, obj):
        for listener in self.cache_updated_listeners:
            listener.onCacheUpdated()

    def objectFromRaw(self, key, data, shard_index, zstat):
        return OIDCSigningKeys._fromRaw(self._zk_context, data, zstat, None)

    def updateFromRaw(self, obj, key, data, shard_index, zstat):
        obj._updateFromRaw(self._zk_context, data, zstat, None)

    def parsePath(self, path):
        return (self._formatKey(path.split('/')[-1]), True, None)

    def getSigningKeys(self, algorithm):
        return self._cached_objects.get(self._formatKey(algorithm))

    def _formatKey(self, algorithm):
        # key format: ("oidc_keys", algorithm)
        return ('oidc_keys', algorithm)


class OIDCSigningKeys(ZKObject):

    OIDC_ROOT_PATH = "/keystorage-oidc"

    def __init__(self):
        super().__init__()

        self._set(
            schema=1,
            keys=[],
            algorithm=None,
        )

    def getPath(self):
        return self._getPathByAlgorithm(self.algorithm)

    def serialize(self, context):
        keydata = {
            'schema': self.schema,
            'keys': self.keys,
        }
        return json.dumps(keydata, sort_keys=True).encode("utf8")

    def appendKey(self, private_key, version):
        if not self._active_context:
            raise Exception("appendKey must be used with a context manager")
        key_dict = self._generateKeyDict(private_key, version)
        self.keys.append(key_dict)

    @classmethod
    def new(klass, context, algorithm, private_key, version=0):
        obj = klass()
        obj._set(algorithm=algorithm)
        obj._set(keys=[klass._generateKeyDict(private_key, version)])
        data = obj._trySerialize(context)
        obj._save(context, data, create=True)
        return obj

    @classmethod
    def loadKeys(klass, context, algorithm):
        path = klass._getPathByAlgorithm(algorithm)
        try:
            return klass.fromZK(context, path, algorithm=algorithm)
        except kazoo.exceptions.NoNodeError:
            return None

    @classmethod
    def createAndStoreKeys(
        klass, context, algorithm, password_bytes):
        context.log.debug(
            "Creating OIDC signing keys for algorithm: %s", algorithm)

        private_key = klass._generatePrivateKey(algorithm, password_bytes)
        klass.new(context, algorithm, private_key)

    @classmethod
    def rotateKeys(
        klass, context, algorithm, password_bytes, rotation_interval, max_ttl):
        context.log.debug("Rotating OIDC signing keys, algorithm: %s,"
                          "rotation_interval: %s, max_ttl: %s",
                          algorithm, rotation_interval, max_ttl)
        key_data = klass.loadKeys(context, algorithm)

        if not key_data:
            klass.createAndStoreKeys(context, algorithm, password_bytes)
        else:
            # Check if there are old keys needs to be deleted
            # Here also need to handle the corner case when max_ttl
            # is bigger than rotation_interval. In this case, multiple
            # valid keys can exist at the same time. In either case,
            # find the last key whose created time is smaller than the
            # current time - max_ttl, then all tokens signed by the
            # keys before it should be expired and we can remove them.
            older_than = int(time.time()) - max_ttl

            # Make sure keys are sorted by created time
            existing_keys = sorted(
                key_data.keys, key=lambda key: key['created'])

            with key_data.activeContext(context):
                for index in range(len(existing_keys) - 1, -1, -1):
                    key = existing_keys[index]
                    if key["created"] < older_than and index > 0:
                        context.log.info(
                            "Removing OIDC keys whose version is older"
                            "than %s", existing_keys[index]["version"])
                        key_data._set(keys=existing_keys[index:])
                        break

                # Check if latest key is outdated and create a new one
                latest_key = key_data.keys[-1]
                age_seconds = int(time.time()) - latest_key["created"]
                if age_seconds > rotation_interval:
                    new_version = latest_key["version"] + 1
                    context.log.info(
                        "Generating new OIDC key with version %s", new_version)
                    private_key = OIDCSigningKeys._generatePrivateKey(
                        algorithm, password_bytes)
                    key_data.appendKey(
                        private_key=private_key,
                        version=new_version)

    @classmethod
    def deleteKeysByAlgorithm(klass, context, algorithm):
        path = klass._getPathByAlgorithm(algorithm)
        klass._delete(context, path)

    @classmethod
    def _getPathByAlgorithm(klass, algorithm):
        return f"{klass.OIDC_ROOT_PATH}/{algorithm}"

    @staticmethod
    def _generateKeyDict(private_key, version):
        return {
            "version": version,
            "created": int(time.time()),
            "private_key": private_key,
        }

    @classmethod
    def _generatePrivateKey(klass, algorithm, password_bytes):
        if algorithm == "RS256":
            private_key, public_key = encryption.generate_rsa_keypair()
            pem_private_key = encryption.serialize_rsa_private_key(
                private_key, password_bytes)
            return pem_private_key.decode("utf-8")
        else:
            raise AlgorithmNotSupportedException(
                f"Algorithm {algorithm} is not supported")

    def __eq__(self, other):
        return (isinstance(other, OIDCSigningKeys) and
                self.schema == other.schema and
                self.keys == other.keys)


class KeyStorage(ZooKeeperBase):
    log = logging.getLogger("zuul.KeyStorage")
    # /keystorage/connection/orgname
    PREFIX_PATH = "/keystorage/{}/{}"
    # /keystorage/connection/orgname/projectuniqname
    PROJECT_PATH = PREFIX_PATH + "/{}"
    SECRETS_PATH = PROJECT_PATH + "/secrets"
    SSH_PATH = PROJECT_PATH + "/ssh"
    # TODO: migrate existing keystorage to keystorage2
    TENANT_SSH_PATH = "/keystorage2/tenant/{}/ssh"

    def __init__(self, zookeeper_client, password, backup=None,
                 start_cache=True):
        super().__init__(zookeeper_client)
        self.password = password
        self.password_bytes = password.encode("utf-8")
        if start_cache:
            self.oidc_signing_keys_cache = OIDCSigningKeysCache(self.client)
        else:
            self.oidc_signing_keys_cache = None

        self.getProjectSSHKeys = cachetools.func.lru_cache(maxsize=None)(
            self.getProjectSSHKeys)
        self.getProjectSecretsKeys = cachetools.func.lru_cache(maxsize=None)(
            self.getProjectSecretsKeys)

    def createZKContext(self):
        return ZKContext(self.client, None, None, self.log)

    def stop(self):
        if self.oidc_signing_keys_cache:
            self.oidc_signing_keys_cache.stop()
        super().stop()

    def _walk(self, root):
        ret = []
        children = self.kazoo_client.get_children(root)
        if children:
            for child in children:
                path = '/'.join([root, child])
                ret.extend(self._walk(path))
        else:
            data, _ = self.kazoo_client.get(root)
            try:
                ret.append((root, json.loads(data)))
            except Exception:
                self.log.error(f"Unable to load keys at {root}")
                # Keep processing exports
        return ret

    def exportKeys(self):
        keys = {}
        for (path, data) in itertools.chain(
                self._walk('/keystorage'),
                self._walk('/keystorage2'),
        ):
            self.log.info(f"Exported: {path}")
            keys[path] = data
        return {'keys': keys}

    def importKeys(self, import_data, overwrite):
        for path, data in import_data['keys'].items():
            # This includes /keystorage2
            if not path.startswith('/keystorage'):
                self.log.error(f"Invalid path: {path}")
                return
            data = json.dumps(data, sort_keys=True).encode('utf8')
            try:
                self.kazoo_client.create(path, value=data, makepath=True)
                self.log.info(f"Created key at {path}")
            except kazoo.exceptions.NodeExistsError:
                if overwrite:
                    self.kazoo_client.set(path, value=data)
                    self.log.info(f"Updated key at {path}")
                else:
                    self.log.warning(f"Not overwriting existing key at {path}")

    def getProjectSSHKeysPath(self, connection_name, project_name):
        prefix, name = strings.unique_project_name(project_name)
        key_path = self.SSH_PATH.format(connection_name, prefix, name)
        return key_path

    def getProjectSSHKeys(self, connection_name, project_name):
        """Return the public and private keys"""
        key = self._getProjectSSHKey(connection_name, project_name)
        if key is None:
            self.log.info("Generating a new SSH key for %s/%s",
                          connection_name, project_name)
            key = paramiko.RSAKey.generate(bits=RSA_KEY_SIZE)
            key_version = 0
            key_created = int(time.time())

            try:
                self._storeProjectSSHKey(connection_name, project_name, key,
                                         key_version, key_created)
            except kazoo.exceptions.NodeExistsError:
                # Handle race condition between multiple schedulers
                # creating the same SSH key.
                key = self._getProjectSSHKey(connection_name, project_name)

        with io.StringIO() as o:
            key.write_private_key(o)
            private_key = o.getvalue()
        public_key = "ssh-rsa {}".format(key.get_base64())

        return private_key, public_key

    def loadProjectSSHKeys(self, connection_name, project_name):
        """Return the complete internal data structure"""
        key_path = self.getProjectSSHKeysPath(connection_name, project_name)
        try:
            data, _ = self.kazoo_client.get(key_path)
            return json.loads(data)
        except kazoo.exceptions.NoNodeError:
            return None

    def saveProjectSSHKeys(self, connection_name, project_name, keydata):
        """Store the complete internal data structure"""
        key_path = self.getProjectSSHKeysPath(connection_name, project_name)
        data = json.dumps(keydata, sort_keys=True).encode("utf-8")
        self.kazoo_client.create(key_path, value=data, makepath=True)

    def deleteProjectSSHKeys(self, connection_name, project_name):
        """Delete the complete internal data structure"""
        key_path = self.getProjectSSHKeysPath(connection_name, project_name)
        with suppress(kazoo.exceptions.NoNodeError):
            self.kazoo_client.delete(key_path)

    def _getProjectSSHKey(self, connection_name, project_name):
        """Load and return the public and private keys"""
        keydata = self.loadProjectSSHKeys(connection_name, project_name)
        if keydata is None:
            return None
        encrypted_key = keydata['keys'][0]["private_key"]
        with io.StringIO(encrypted_key) as o:
            return paramiko.RSAKey.from_private_key(o, self.password)

    def _storeProjectSSHKey(self, connection_name, project_name, key,
                            version, created):
        """Create the internal data structure from the key and store it"""
        # key is an rsa key object
        with io.StringIO() as o:
            key.write_private_key(o, self.password)
            private_key = o.getvalue()
        keys = [{
            "version": version,
            "created": created,
            "private_key": private_key,
        }]
        keydata = {
            'schema': 1,
            'keys': keys
        }
        self.saveProjectSSHKeys(connection_name, project_name, keydata)

    def getTenantSSHKeysPath(self, tenant_name):
        name = strings.safe_tenant_name(tenant_name)
        key_path = self.TENANT_SSH_PATH.format(name)
        return key_path

    def getTenantSSHKeys(self, tenant_name):
        """Return the public and private keys"""
        key = self._getTenantSSHKey(tenant_name)
        if key is None:
            self.log.info("Generating a new SSH key for %s",
                          tenant_name)
            key = paramiko.RSAKey.generate(bits=RSA_KEY_SIZE)
            key_version = 0
            key_created = int(time.time())

            try:
                self._storeTenantSSHKey(tenant_name, key,
                                        key_version, key_created)
            except kazoo.exceptions.NodeExistsError:
                # Handle race condition between multiple schedulers
                # creating the same SSH key.
                key = self._getTenantSSHKey(tenant_name)

        with io.StringIO() as o:
            key.write_private_key(o)
            private_key = o.getvalue()
        public_key = "ssh-rsa {}".format(key.get_base64())

        return private_key, public_key

    def loadTenantSSHKeys(self, tenant_name):
        """Return the complete internal data structure"""
        key_path = self.getTenantSSHKeysPath(tenant_name)
        try:
            data, _ = self.kazoo_client.get(key_path)
            return json.loads(data)
        except kazoo.exceptions.NoNodeError:
            return None

    def saveTenantSSHKeys(self, tenant_name, keydata):
        """Store the complete internal data structure"""
        key_path = self.getTenantSSHKeysPath(tenant_name)
        data = json.dumps(keydata, sort_keys=True).encode("utf-8")
        self.kazoo_client.create(key_path, value=data, makepath=True)

    def deleteTenantSSHKeys(self, tenant_name):
        """Delete the complete internal data structure"""
        key_path = self.getTenantSSHKeysPath(tenant_name)
        with suppress(kazoo.exceptions.NoNodeError):
            self.kazoo_client.delete(key_path)

    def _getTenantSSHKey(self, tenant_name):
        """Load and return the public and private keys"""
        keydata = self.loadTenantSSHKeys(tenant_name)
        if keydata is None:
            return None
        encrypted_key = keydata['keys'][0]["private_key"]
        with io.StringIO(encrypted_key) as o:
            return paramiko.RSAKey.from_private_key(o, self.password)

    def _storeTenantSSHKey(self, tenant_name, key,
                           version, created):
        """Create the internal data structure from the key and store it"""
        # key is an rsa key object
        with io.StringIO() as o:
            key.write_private_key(o, self.password)
            private_key = o.getvalue()
        keys = [{
            "version": version,
            "created": created,
            "private_key": private_key,
        }]
        keydata = {
            'schema': 1,
            'keys': keys
        }
        self.saveTenantSSHKeys(tenant_name, keydata)

    def getProjectSecretsKeysPath(self, connection_name, project_name):
        prefix, name = strings.unique_project_name(project_name)
        key_path = self.SECRETS_PATH.format(connection_name, prefix, name)
        return key_path

    def getProjectSecretsKeys(self, connection_name, project_name):
        """Return the public and private keys"""
        pem_private_key = self._getSecretsKey(connection_name, project_name)
        if pem_private_key is None:
            self.log.info("Generating a new secrets key for %s/%s",
                          connection_name, project_name)
            private_key, public_key = encryption.generate_rsa_keypair()
            pem_private_key = encryption.serialize_rsa_private_key(
                private_key, self.password_bytes)
            key_version = 0
            key_created = int(time.time())

            try:
                self._storeSecretsKey(connection_name, project_name,
                                      pem_private_key, key_version,
                                      key_created)
            except kazoo.exceptions.NodeExistsError:
                # Handle race condition between multiple schedulers
                # creating the same secrets key.
                pem_private_key = self._getSecretsKey(
                    connection_name, project_name)

        private_key, public_key = encryption.deserialize_rsa_keypair(
            pem_private_key, self.password_bytes)

        return private_key, public_key

    def loadProjectsSecretsKeys(self, connection_name, project_name):
        """Return the complete internal data structure"""
        key_path = self.getProjectSecretsKeysPath(
            connection_name, project_name)
        try:
            data, _ = self.kazoo_client.get(key_path)
            return json.loads(data)
        except kazoo.exceptions.NoNodeError:
            return None

    def saveProjectsSecretsKeys(self, connection_name, project_name, keydata):
        """Store the complete internal data structure"""
        key_path = self.getProjectSecretsKeysPath(
            connection_name, project_name)
        data = json.dumps(keydata, sort_keys=True).encode("utf-8")
        self.kazoo_client.create(key_path, value=data, makepath=True)

    def deleteProjectsSecretsKeys(self, connection_name, project_name):
        """Delete the complete internal data structure"""
        key_path = self.getProjectSecretsKeysPath(
            connection_name, project_name)
        with suppress(kazoo.exceptions.NoNodeError):
            self.kazoo_client.delete(key_path)

    def _getSecretsKey(self, connection_name, project_name):
        """Load and return the private key"""
        keydata = self.loadProjectsSecretsKeys(
            connection_name, project_name)
        if keydata is None:
            return None
        return keydata['keys'][0]["private_key"].encode("utf-8")

    def _storeSecretsKey(self, connection_name, project_name, key,
                         version, created):
        """Create the internal data structure from the key and store it"""
        # key is a pem-encoded (base64) private key stored in bytes
        keys = [{
            "version": version,
            "created": created,
            "private_key": key.decode("utf-8"),
        }]
        keydata = {
            'schema': 1,
            'keys': keys
        }
        self.saveProjectsSecretsKeys(connection_name, project_name, keydata)

    def deleteProjectDir(self, connection_name, project_name):
        prefix, name = strings.unique_project_name(project_name)
        project_path = self.PROJECT_PATH.format(connection_name, prefix, name)
        prefix_path = self.PREFIX_PATH.format(connection_name, prefix)
        try:
            self.kazoo_client.delete(project_path)
        except kazoo.exceptions.NotEmptyError:
            # Rely on delete only deleting empty paths by default
            self.log.warning(f"Not deleting non empty path {project_path}")
        except kazoo.exceptions.NoNodeError:
            # Already deleted
            pass
        try:
            self.kazoo_client.delete(prefix_path)
        except kazoo.exceptions.NotEmptyError:
            # Normal for the org to remain due to other projects existing.
            pass
        except kazoo.exceptions.NoNodeError:
            # Already deleted
            pass

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
