# Copyright 2025 BMW Group
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

import json
import time

import kazoo

from zuul.lib import encryption
from zuul.exceptions import AlgorithmNotSupportedException
from zuul.zk.cache import ZuulTreeCache
from zuul.zk.zkobject import ZKObject


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

    def objectFromRaw(self, key, data, zstat):
        return OIDCSigningKeys._fromRaw(self._zk_context, data, zstat, None)

    def updateFromRaw(self, obj, key, data, zstat):
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
