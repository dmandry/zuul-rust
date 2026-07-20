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

import io
import json
import logging
import time

import kazoo
import paramiko

from zuul.lib import encryption, strings
from zuul.zk import ZooKeeperBase
from zuul.zk.cache import ZuulTreeCache
from zuul.zk.zkobject import ZKContext, ZKObject

RSA_KEY_SIZE = 2048


class ZuulSSHKey:
    def __init__(self):
        self._key_object = None
        self._encrypted_private_key = None
        self.public_key = None
        self.private_key = None
        self.created = None
        self.version = None

    @classmethod
    def new(cls, password, version):
        self = cls()
        self._key_object = paramiko.RSAKey.generate(bits=RSA_KEY_SIZE)
        with io.StringIO() as o:
            self._key_object.write_private_key(o, password)
            self._encrypted_private_key = o.getvalue()
        with io.StringIO() as o:
            self._key_object.write_private_key(o)
            self.private_key = o.getvalue()
        self.public_key = "ssh-rsa {}".format(
            self._key_object.get_base64())
        self.created = int(time.time())
        self.version = version
        return self

    @classmethod
    def fromDict(cls, data, password):
        self = cls()
        self._encrypted_private_key = data['private_key']
        with io.StringIO(self._encrypted_private_key) as o:
            self._key_object = paramiko.RSAKey.from_private_key(o, password)
        with io.StringIO() as o:
            self._key_object.write_private_key(o)
            self.private_key = o.getvalue()
        self.public_key = "ssh-rsa {}".format(
            self._key_object.get_base64())
        self.created = data['created']
        self.version = data['version']
        return self

    def toDict(self):
        return dict(
            private_key=self._encrypted_private_key,
            created=self.created,
            version=self.version,
        )


class ZuulSecretKey:
    def __init__(self):
        self._encrypted_private_key = None
        self.public_key = None
        self.private_key = None
        self.created = None
        self.version = None

    @classmethod
    def new(cls, password, version):
        self = cls()
        self.private_key, self.public_key = encryption.generate_rsa_keypair()
        self._encrypted_private_key = encryption.serialize_rsa_private_key(
            self.private_key, password).decode("utf-8")
        self.created = int(time.time())
        self.version = version
        return self

    @classmethod
    def fromDict(cls, data, password):
        self = cls()
        self._encrypted_private_key = data['private_key']
        self.private_key, self.public_key =\
            encryption.deserialize_rsa_keypair(
                self._encrypted_private_key.encode('utf8'), password)
        self.created = data['created']
        self.version = data['version']
        return self

    def toDict(self):
        return dict(
            private_key=self._encrypted_private_key,
            created=self.created,
            version=self.version,
        )


class ZuulKeyInfo(ZKObject):
    def __init__(self):
        super().__init__()
        self._set(
            schema=1,
            keys=[],
        )

    def getPath(self):
        return self._path

    def toDict(self):
        return {
            'schema': self.schema,
            'keys': [k.toDict() for k in self.keys],
        }

    def serialize(self, context):
        return json.dumps(self.toDict(), sort_keys=True).encode("utf8")


class ZuulSSHKeyInfo(ZuulKeyInfo):
    def deserialize(self, data, context, extra=None):
        data = super().deserialize(data, context, extra)
        data['keys'] = [ZuulSSHKey.fromDict(k, extra['password'])
                        for k in data['keys']]
        return data

    @classmethod
    def newFromDict(klass, context, extra=None, **data):
        if 'keys' in data:
            data['keys'] = [ZuulSSHKey.fromDict(k, extra['password'])
                            for k in data['keys']]
        return super().new(context, **data)


class ZuulSecretKeyInfo(ZuulKeyInfo):
    def deserialize(self, data, context, extra=None):
        data = super().deserialize(data, context, extra)
        data['keys'] = [ZuulSecretKey.fromDict(k, extra['password_bytes'])
                        for k in data['keys']]
        return data

    @classmethod
    def newFromDict(klass, context, extra=None, **data):
        if 'keys' in data:
            data['keys'] = [ZuulSecretKey.fromDict(k, extra['password_bytes'])
                            for k in data['keys']]
        return super().new(context, **data)


class KeystorageCache(ZuulTreeCache):
    def __init__(self, *args, **kw):
        self.password = kw.pop('password')
        self.password_bytes = self.password.encode('utf8')
        super().__init__(*args, **kw)

    def objectFromRaw(self, key, data, zstat):
        extra = dict(password=self.password,
                     password_bytes=self.password_bytes)
        if key.endswith('/ssh'):
            info = ZuulSSHKeyInfo._fromRaw(
                self._zk_context, data, zstat, extra)
            info._set(_path=key)
            return info
        elif key.endswith('/secrets'):
            info = ZuulSecretKeyInfo._fromRaw(
                self._zk_context, data, zstat, extra)
            info._set(_path=key)
            return info
        else:
            raise Exception("Unknown key class")

    def updateFromRaw(self, obj, key, data, zstat):
        obj._updateFromRaw(self._zk_context, data, zstat, None)

    def parsePath(self, path):
        key = None
        fetch = False
        shard_index = None

        object_path = path[len(self.root):].strip("/")
        parts = object_path.split('/')
        if not parts:
            return (key, fetch, shard_index)

        #     0             1              2        3            4
        # connection/{safe_connection}/project/{safe_project}/secrets
        # connection/{safe_connection}/project/{safe_project}/ssh
        # tenant/    {safe_tenant}/    ssh

        elif len(parts) == 5 and parts[0] == 'connection':
            key = path
            fetch = True
        elif len(parts) == 3 and parts[0] == 'tenant':
            key = path
            fetch = True
        return (key, fetch, shard_index)

    @classmethod
    def _makeProjectRootPath(cls, root, connection_name, project_name):
        safe_connection_name = strings.safe_name(connection_name)
        safe_project_name = strings.safe_name(project_name)
        return (f'{root}/connection/{safe_connection_name}'
                f'/project/{safe_project_name}')

    @classmethod
    def _makeProjectSecretsPath(cls, root, connection_name, project_name):
        safe_connection_name = strings.safe_name(connection_name)
        safe_project_name = strings.safe_name(project_name)
        return (f'{root}/connection/{safe_connection_name}'
                f'/project/{safe_project_name}/secrets')

    @classmethod
    def _makeProjectSSHPath(cls, root, connection_name, project_name):
        safe_connection_name = strings.safe_name(connection_name)
        safe_project_name = strings.safe_name(project_name)
        return (f'{root}/connection/{safe_connection_name}'
                f'/project/{safe_project_name}/ssh')

    @classmethod
    def _makeTenantSSHPath(cls, root, tenant_name):
        safe_tenant_name = strings.safe_name(tenant_name)
        return f'{root}/tenant/{safe_tenant_name}/ssh'

    def getProjectSSHKeyInfo(self, connection_name, project_name):
        key = self._makeProjectSSHPath(
            self.root, connection_name, project_name)
        return self._cached_objects.get(key)

    def getProjectSecretsKeyInfo(self, connection_name, project_name):
        key = self._makeProjectSecretsPath(
            self.root, connection_name, project_name)
        return self._cached_objects.get(key)

    def getTenantSSHKeyInfo(self, tenant_name):
        key = self._makeTenantSSHPath(self.root, tenant_name)
        return self._cached_objects.get(key)

    def createProjectSSHKeyInfo(
            self, context, connection_name, project_name, keys):
        path = self._makeProjectSSHPath(
            self.root, connection_name, project_name)
        obj = ZuulSSHKeyInfo.new(context, _path=path, keys=keys)
        return self.insert(obj)

    def createProjectSecretsKeyInfo(
            self, context, connection_name, project_name, keys):
        path = self._makeProjectSecretsPath(
            self.root, connection_name, project_name)
        obj = ZuulSecretKeyInfo.new(context, _path=path, keys=keys)
        return self.insert(obj)

    def createTenantSSHKeyInfo(self, context, tenant_name, keys):
        path = self._makeTenantSSHPath(self.root, tenant_name)
        obj = ZuulSSHKeyInfo.new(context, _path=path, keys=keys)
        return self.insert(obj)


class KeyStorage3(ZooKeeperBase):
    log = logging.getLogger("zuul.KeyStorage")
    ROOT_PATH = "/keystorage3"

    def __init__(self, zookeeper_client, password, start_cache=True):
        super().__init__(zookeeper_client)
        self.password = password
        self.password_bytes = password.encode("utf-8")
        if start_cache:
            self.keystorage_cache = KeystorageCache(
                self.client, self.ROOT_PATH, password=self.password)
        else:
            self.keystorage_cache = None

    def createZKContext(self):
        return ZKContext(self.client, None, None, self.log)

    def stop(self):
        if self.keystorage_cache:
            self.keystorage_cache.stop()
        super().stop()

    def exportKeys(self):
        keys = {}
        for path, info in self.keystorage_cache._cached_objects.items():
            self.log.info(f"Exported: {path}")
            keys[path] = info.toDict()
        return {'keys': keys}

    def importKeys(self, import_data, overwrite):
        self.keystorage_cache.ensureReady()
        extra = dict(password=self.password,
                     password_bytes=self.password_bytes)
        with self.createZKContext() as context:
            for path, data in import_data['keys'].items():
                if (path.startswith('/keystorage/') or
                    path.startswith('/keystorage2/')):
                    continue
                if not path.startswith(self.ROOT_PATH):
                    self.log.error(f"Invalid path: {path}")
                    return

                if path.endswith('ssh'):
                    cls = ZuulSSHKeyInfo
                elif path.endswith('secrets'):
                    cls = ZuulSecretKeyInfo
                else:
                    raise Exception(f"Unknown key type at {path}")

                if existing := self.keystorage_cache._cached_objects.get(path):
                    if overwrite:
                        existing.delete(context)
                        self.log.info("Deleted key at %s", path)
                    else:
                        self.log.warning(
                            "Not overwriting existing key at %s", path)
                        continue

                cls.newFromDict(context, extra=extra, _path=path, **data)
                self.log.info("Imported key at %s", path)

    def getProjectSSHKeysPath(self, connection_name, project_name):
        # This is only used by the unit tests
        return KeystorageCache._makeProjectSSHPath(
            self.ROOT_PATH, connection_name, project_name)

    def getProjectSSHKeyInfo(self, connection_name, project_name):
        self.keystorage_cache.ensureReady()
        key = self.keystorage_cache.getProjectSSHKeyInfo(
            connection_name, project_name)
        if key is None:
            self.log.info("Generating a new SSH key for %s/%s",
                          connection_name, project_name)
            zuul_key = ZuulSSHKey.new(self.password, version=0)
            keys = [zuul_key]
            try:
                with self.createZKContext() as context:
                    key = self.keystorage_cache.createProjectSSHKeyInfo(
                        context, connection_name, project_name, keys)
            except kazoo.exceptions.NodeExistsError:
                # Handle race condition between multiple schedulers
                # creating the same SSH key.
                key = self.keystorage_cache.getProjectSSHKeyInfo(
                    connection_name, project_name)
        return key

    def deleteProjectSSHKeys(self, connection_name, project_name):
        key = self.keystorage_cache.getProjectSSHKeyInfo(
            connection_name, project_name)
        if key is not None:
            with self.createZKContext() as context:
                key.delete(context)

    def getTenantSSHKeysPath(self, tenant_name):
        # This is only used by the unit tests
        return KeystorageCache._makeTenantSSHPath(
            self.ROOT_PATH, tenant_name)

    def getTenantSSHKeyInfo(self, tenant_name):
        self.keystorage_cache.ensureReady()
        key = self.keystorage_cache.getTenantSSHKeyInfo(tenant_name)
        if key is None:
            self.log.info("Generating a new SSH key for %s",
                          tenant_name)
            zuul_key = ZuulSSHKey.new(self.password, version=0)
            keys = [zuul_key]
            try:
                with self.createZKContext() as context:
                    key = self.keystorage_cache.createTenantSSHKeyInfo(
                        context, tenant_name, keys)
            except kazoo.exceptions.NodeExistsError:
                # Handle race condition between multiple schedulers
                # creating the same SSH key.
                key = self.keystorage_cache.getTenantSSHKeyInfo(
                    tenant_name)
        return key

    def deleteTenantSSHKeys(self, tenant_name):
        key = self.keystorage_cache.getTenantSSHKeyInfo(tenant_name)
        if key is not None:
            with self.createZKContext() as context:
                key.delete(context)

    def getProjectSecretsKeysPath(self, connection_name, project_name):
        # This is only used by the unit tests
        return KeystorageCache._makeProjectSecretsPath(
            self.ROOT_PATH, connection_name, project_name)

    def getProjectSecretsKeyInfo(self, connection_name, project_name):
        self.keystorage_cache.ensureReady()
        key = self.keystorage_cache.getProjectSecretsKeyInfo(
            connection_name, project_name)
        if key is None:
            self.log.info("Generating a new secrets key for %s/%s",
                          connection_name, project_name)
            zuul_key = ZuulSecretKey.new(self.password_bytes, version=0)
            keys = [zuul_key]
            try:
                with self.createZKContext() as context:
                    key = self.keystorage_cache.createProjectSecretsKeyInfo(
                        context, connection_name, project_name, keys)
            except kazoo.exceptions.NodeExistsError:
                # Handle race condition between multiple schedulers
                # creating the same secrets key.
                key = self.keystorage_cache.getProjectSecretsKeyInfo(
                    connection_name, project_name)
        return key

    def deleteProjectSecretsKeys(self, connection_name, project_name):
        key = self.keystorage_cache.getProjectSecretsKeyInfo(
            connection_name, project_name)
        if key is not None:
            with self.createZKContext() as context:
                key.delete(context)

    def deleteProjectDir(self, connection_name, project_name):
        path = KeystorageCache._makeProjectRootPath(
            self.ROOT_PATH, connection_name, project_name)
        try:
            self.kazoo_client.delete(path)
        except kazoo.exceptions.NotEmptyError:
            # Rely on delete only deleting empty paths by default
            self.log.warning(f"Not deleting non empty path {path}")
        except kazoo.exceptions.NoNodeError:
            # Already deleted
            pass
