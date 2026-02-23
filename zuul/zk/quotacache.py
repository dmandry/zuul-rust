# Copyright 2025 Acme Gating, LLC
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

from enum import StrEnum
import json
import logging
import math
import os
import urllib.parse

from kazoo.exceptions import (
    BadVersionError,
    NodeExistsError,
)

from zuul import model
from zuul.zk.cache import ZuulTreeCache
from zuul.zk.zkobject import ZKObject, ZKContext


class ZKQuotaInfo(ZKObject):
    def __init__(self):
        super().__init__()

    def getPath(self):
        path = f'zuul/endpoint/{self.endpoint}/quota/{self.kind}'
        if self.resource is not None:
            path += f'/{self.resource}'
        return path

    def serialize(self, context):
        data = {
            'quota': self.quota,
        }
        return json.dumps(data, sort_keys=True).encode("utf8")


class QuotaCache(ZuulTreeCache):
    """Stores endpoint quota information in ZK

    This stores two types of information: the overall quota limits of
    the endpoint, and resource usage for instance types, etc.

    The overall quota usage is expected to change very infrequently,
    and the resource usage even less (never).  To that end, any writes
    that fail due to concurrent modifications are simply ignored under
    the assumption that another launcher is refreshing the data at the
    same time.
    """

    log = logging.getLogger('zuul.QuotaCache')

    class Kind(StrEnum):
        LIMITS = "limits"
        UNMANAGED_USAGE = "unmanaged-usage"
        RESOURCE = "resource"

    def __init__(self, client, endpoint_name):
        self.endpoint = urllib.parse.quote_plus(endpoint_name)
        root = f'/zuul/endpoint/{self.endpoint}/quota'
        resource_path = os.path.join(root, QuotaCache.Kind.RESOURCE)
        super().__init__(client, root, async_worker=False)
        self.zk_client.client.ensure_path(resource_path)

    def objectFromRaw(self, key, data, shard_index, zstat):
        if len(key) == 2:
            resource = key[1]
        else:
            resource = None
        obj = ZKQuotaInfo._fromRaw(self._zk_context, data, zstat, None)
        obj._set(endpoint=self.endpoint,
                 kind=key[0],
                 resource=resource)
        return obj

    def updateFromRaw(self, obj, key, data, shard_index, zstat):
        obj._updateFromRaw(self._zk_context, data, zstat, None)

    def _makeKey(self, kind, resource=None):
        if kind == QuotaCache.Kind.RESOURCE:
            return (str(kind), resource)
        return (str(kind),)

    def parsePath(self, path):
        parts = path.split('/')
        parts = parts[5:]
        key = None
        if len(parts) == 1 and parts[0] == QuotaCache.Kind.LIMITS:
            key = self._makeKey(QuotaCache.Kind.LIMITS)
        elif len(parts) == 1 and parts[0] == QuotaCache.Kind.UNMANAGED_USAGE:
            key = self._makeKey(QuotaCache.Kind.UNMANAGED_USAGE)
        elif len(parts) == 2 and parts[0] == QuotaCache.Kind.RESOURCE:
            key = self._makeKey(QuotaCache.Kind.RESOURCE, parts[1])
        # We should fetch if we have a matching key
        return (key, bool(key), None)

    def hasLimits(self):
        key = self._makeKey(QuotaCache.Kind.LIMITS)
        obj = self._cached_objects.get(key)
        return bool(obj is not None)

    def hasUnmanagedUsage(self):
        key = self._makeKey(QuotaCache.Kind.UNMANAGED_USAGE)
        obj = self._cached_objects.get(key)
        return bool(obj is not None)

    def hasResource(self, resource):
        key = self._makeKey(QuotaCache.Kind.RESOURCE, resource)
        obj = self._cached_objects.get(key)
        return bool(obj is not None)

    def getLimits(self):
        key = self._makeKey(QuotaCache.Kind.LIMITS)
        obj = self._cached_objects.get(key)
        quota = obj and obj.quota or {}
        return model.QuotaInformation(default=math.inf, **quota)

    def getUnmanagedUsage(self):
        key = self._makeKey(QuotaCache.Kind.UNMANAGED_USAGE)
        obj = self._cached_objects.get(key)
        quota = obj and obj.quota or {}
        return model.QuotaInformation(**quota)

    def getResource(self, resource):
        key = self._makeKey(QuotaCache.Kind.RESOURCE, resource)
        obj = self._cached_objects.get(key)
        if obj is None:
            raise KeyError(f"Resource {resource} not in quota cache")
        return model.QuotaInformation(**obj.quota)

    def createZKContext(self):
        return ZKContext(self.zk_client, None, None, self.log)

    def _setValue(self, kind, resource, quota_info):
        path = os.path.join(self.root, kind)
        if kind == QuotaCache.Kind.RESOURCE:
            key = (kind, resource)
        else:
            key = (kind,)
        obj = self._cached_objects.get(key)
        context = self.createZKContext()
        try:
            if obj:
                obj.updateAttributes(context, quota=quota_info.quota)
            else:
                obj = ZKQuotaInfo.new(
                    context,
                    quota=quota_info.quota,
                    endpoint=self.endpoint,
                    kind=kind,
                    resource=resource,
                )
                # Put the item in the cache so that we can use it
                # immediately without hitting a cache error.  This
                # will race the addition triggered by the watch, but
                # we'll end up with the same data, and no consumers of
                # the quota cache would be affected by having the
                # object replaced.
                self._cached_objects.setdefault(key, obj)
        except (BadVersionError, NodeExistsError):
            self.log.debug("Skipping update of %s", path)

    def setLimits(self, quota_info):
        self._setValue(QuotaCache.Kind.LIMITS, None, quota_info)

    def setUnmanagedUsage(self, quota_info):
        self._setValue(QuotaCache.Kind.UNMANAGED_USAGE, None, quota_info)

    def setResource(self, resource, quota_info):
        self._setValue(QuotaCache.Kind.RESOURCE, resource, quota_info)
