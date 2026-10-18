#!/usr/bin/env python3
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

"""Generate zuul-launcher compatibility fixtures from this 14.3.0 tree.

The blobs are the bytes the 14.3.0 serializers write: json.dumps(...,
sort_keys=True) then zlib.compress (level 6). ProviderNode and provider
config gain a ``{driver_id}\\0`` prefix. Field lists are copied from:

- zuul/model.py NodesetRequest.serialize, ProviderNode.serialize,
  ImageBuildArtifact.serialize, ImageUpload.serialize,
  ProviderNodeAssignment.serialize, ProviderNodeLifecycle.serialize,
  ProviderNodeSnapshot.serialize, NodesetRequestRevision.serialize
- zuul/provider/__init__.py BaseProvider.serialize
- zuul/zk/zkobject.py PolymorphicZKObjectMixin._compressData
- zuul/launcher/server.py scores_for_label, getQuotaPercentage,
  _getUnassignedNodes
"""

import base64
import json
import os
import sys
import threading
import zlib

import mmh3
from kazoo.recipe.lock import Lock


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
OUT = os.path.join(ROOT, 'rust', 'zuul-launcher', 'tests', 'fixtures')


def dumps(obj):
    return json.dumps(obj, sort_keys=True).encode('utf-8')


def zblob(obj):
    return zlib.compress(dumps(obj), 6)


def prefixed(driver_id, obj):
    return driver_id.encode('utf-8') + b'\0' + zblob(obj)


def b64(data):
    return base64.b64encode(data).decode('ascii')


def nodeset_request():
    # zuul/model.py NodesetRequest.serialize
    return {
        'uuid': 'req-1',
        'state': 'requested',
        'event_state': 0,
        'tenant_name': 'tenant-a',
        'pipeline_name': 'check',
        'buildset_uuid': 'bs-1',
        'job_uuid': 'job-1',
        'job_name': 'test-job',
        'labels': ['debian-bookworm'],
        'priority': 200,
        'request_time': 1700000000.5,
        'zuul_event_id': 'evt-1',
        'span_info': None,
        'image_names': None,
        'image_upload_uuid': None,
        'preferred_provider': None,
        'provider_node_data': [],
        '_relative_priority': 0,
        'hash_key': None,
    }


def provider_node(driver_fields):
    # zuul/model.py ProviderNode.serialize + getNodeData(serialize_node=True)
    # + driver getDriverData()
    data = {
        'zuul_event_id': None,
        'span_info': None,
        'max_ready_age': 0,
        'max_age': 0,
        'min_retention_time': 0,
        'request_time': 1700000001.25,
        'state': 'requested',
        'state_time': 1700000001.25,
        'label': 'debian-bookworm',
        'label_config_hash': 'abc123',
        'label_aliases': [],
        'tags': {'zuul_node_uuid': 'node-1'},
        'connection_name': 'static-conn',
        'endpoint_name': 'static-conn',
        'comment': None,
        'main_node_id': None,
        'subnodes': [],
        'create_state': {},
        'delete_state': {},
        'quota': {'instances': 1},
        'image_upload_uuid': None,
        'uuid': 'node-1',
        'attributes': {},
        'az': None,
        'boot_timeout': 300,
        'snapshot_timeout': 3600,
        'snapshot_expiration': 604800,
        'executor_zone': None,
        'cloud': None,
        'connection_port': 22,
        'connection_type': 'ssh',
        'kubernetes_connection': {},
        'username': 'zuul',
        'python_path': 'auto',
        'shell_type': None,
        'hold_expiration': None,
        'host_id': None,
        'host_key_checking': True,
        'host_keys': [],
        'interface_ip': None,
        'private_ipv4': None,
        'private_ipv6': None,
        'provider': 'tenant-a-static',
        'public_ipv4': None,
        'public_ipv6': None,
        'region': None,
        'resources': None,
        'slot': None,
        'reuse': True,
        'node_properties': {},
    }
    data.update(driver_fields)
    return data


def image_artifact():
    # zuul/model.py ImageBuildArtifact.toDict / serialize
    return {
        'uuid': 'iba-1',
        'name': 'debian',
        'canonical_name': 'example%2Forg/debian',
        'project_canonical_name': 'example/org',
        'project_branch': 'master',
        'build_tenant_name': 'tenant-a',
        'build_uuid': 'build-1',
        'format': 'qcow2',
        'md5sum': None,
        'sha256': 'deadbeef',
        'url': 'https://example.invalid/image.qcow2',
        'timestamp': 1700000100.0,
        'validated': True,
        '_state': 'ready',
        'state_time': 1700000101.0,
    }


def image_upload():
    # zuul/model.py ImageUpload.toDict / serialize
    return {
        'uuid': 'up-1',
        'canonical_name': 'example%2Forg/debian',
        'artifact_uuid': 'iba-1',
        'build_uuid': None,
        'endpoint_name': 'aws/us-east-1',
        'providers': ['tenant-a-aws'],
        'config_hash': 'imghash',
        'external_id': None,
        'timestamp': 1700000200.0,
        'validated': False,
        'attempt': 0,
        '_state': 'pending',
        'state_time': 1700000200.0,
    }


def assignment():
    return {
        'request_id': 'req-1',
        'min_request_version': 3,
        'tenant_name': 'tenant-a',
    }


def lifecycle():
    return {
        'next_state': 'hold',
        'next_comment': 'paused',
    }


def snapshot():
    return {
        'snapshot_state': {},
        'complete': False,
        'external_id': None,
        'tags': {'zuul_expiration': '1700001000'},
    }


def revision():
    return {'relative_priority': 5}


def provider_config():
    # zuul/provider/__init__.py BaseProvider.serialize
    return {
        'tenant_name': 'tenant a/b',
        'canonical_name': 'static/main',
        'config': {
            'name': 'main',
            'connection': 'static-conn',
            'labels': [{'name': 'debian-bookworm'}],
        },
        'connection_name': 'static-conn',
    }


class _Handler:
    def event_object(self):
        return threading.Event()

    def lock_object(self):
        return threading.Lock()

    def sleep_func(self, _seconds):
        return None


class _FakeKazoo:
    """Just enough client for kazoo.recipe.lock.Lock.acquire."""

    def __init__(self):
        self.handler = _Handler()
        self.seq = 42
        self.children = {}
        self.payloads = {}

    def ensure_path(self, path):
        self.children.setdefault(path, [])

    def create(self, path, data, ephemeral=True, sequence=False):
        if not sequence:
            raise AssertionError('expected a sequential lock node')
        # ZooKeeper appends a 10-digit sequence to the create path.
        child_parent, child_prefix = path.rsplit('/', 1)
        seq = f'{self.seq:010d}'
        self.seq += 1
        child = child_prefix + seq
        full = child_parent + '/' + child
        self.children.setdefault(child_parent, []).append(child)
        self.payloads[full] = data
        return full

    def get_children(self, path):
        return list(self.children.get(path, []))

    def delete(self, path):
        parent, child = path.rsplit('/', 1)
        self.children.get(parent, []).remove(child)
        self.payloads.pop(path, None)


def kazoo_lock_child():
    client = _FakeKazoo()
    lock = Lock(client, '/zuul/nodes/locks/node-1', identifier='launcher-a.example.com')
    assert lock.acquire(blocking=False) is True
    child = lock.node
    payload = client.payloads['/zuul/nodes/locks/node-1/' + child]
    return {
        'path': '/zuul/nodes/locks/node-1/' + child,
        'child': child,
        'payload': payload.decode('utf-8'),
        'node_marker': Lock._NODE_NAME,
    }


def mmh3_vectors():
    samples = [
        'launcher-a.example.com-debian-bookworm',
        'launcher-b.example.com-debian-bookworm',
        'launcher-a.example.com-req-1',
        'launcher-a.example.com-node-1',
        'launcher-a.example.com',
        'launcher-a.example.com-aws%2Fus-east-1',
        'plain',
        'tenant a/b',
        'unicode-é',
    ]
    return [{'input': s, 'hash': mmh3.hash(s, signed=False)} for s in samples]


def quota_rounding():
    # zuul/launcher/server.py Launcher.getQuotaPercentage
    samples = [0.0, 0.04, 0.05, 0.14, 0.15, 0.25, 0.949, 0.95, 0.99,
               1.0, 1.04, 1.25, 2.0]
    out = []
    for pct in samples:
        rounded = round(pct, 1) if pct < 1.0 else pct
        out.append({'input': pct, 'output': rounded})
    return out


def main():
    os.makedirs(OUT, exist_ok=True)
    models = {
        'nodeset_request': nodeset_request(),
        'provider_node_static': provider_node({}),
        'provider_node_aws': provider_node({
            'aws_instance_id': None,
            'aws_dedicated_host_id': None,
            'aws_launch_template_name': None,
        }),
        'provider_node_azure': provider_node({
            'azure_vm_id': None,
        }),
        'provider_node_openstack': provider_node({
            'openstack_server_id': None,
            'openstack_floating_ip_id': None,
        }),
        'provider_node_kubernetes': provider_node({
            'kubernetes_namespace_id': None,
        }),
        'provider_node_subnode': provider_node({}),
        'image_build_artifact': image_artifact(),
        'image_upload': image_upload(),
        'provider_node_assignment': assignment(),
        'provider_node_lifecycle': lifecycle(),
        'provider_node_snapshot': snapshot(),
        'nodeset_request_revision': revision(),
        'provider_config': provider_config(),
    }
    prefixed_ids = {
        'provider_node_static': 'static',
        'provider_node_aws': 'aws',
        'provider_node_azure': 'azure',
        'provider_node_openstack': 'openstack',
        'provider_node_kubernetes': 'kubernetes',
        'provider_node_subnode': 'subnode',
        'provider_config': 'static',
    }
    blobs = {}
    for name, obj in models.items():
        raw = dumps(obj)
        compressed = zlib.compress(raw, 6)
        entry = {
            'json': raw.decode('utf-8'),
            'zlib_b64': b64(compressed),
        }
        if name in prefixed_ids:
            driver = prefixed_ids[name]
            entry['driver_id'] = driver
            entry['prefixed_b64'] = b64(driver.encode('utf-8') + b'\0' + compressed)
        # A reader must also accept the uncompressed JSON.
        entry['uncompressed_b64'] = b64(raw)
        blobs[name] = entry

    # Unknown key is retained by the decoder and omitted by the serializer.
    extra = dict(nodeset_request())
    extra['future_field'] = 'keep-me'
    blobs['nodeset_request_unknown_key'] = {
        'json': dumps(extra).decode('utf-8'),
        'zlib_b64': b64(zblob(extra)),
        'serializer_omits': ['future_field'],
    }

    # Sharding: 1_000_000-byte slices of the already-compressed blob.
    big = b'x' * 1_000_050
    shards = [big[i:i + 1_000_000] for i in range(0, len(big), 1_000_000)]
    shard_meta = {
        'total': len(big),
        'limit': 1_000_000,
        'lengths': [len(s) for s in shards],
        'shards_b64': [b64(s) for s in shards],
    }

    doc = {
        'model_api': 38,
        'version': '14.3.0',
        'mmh3': mmh3_vectors(),
        'quota_rounding': quota_rounding(),
        'assignable_order': ['ready', 'building', 'requested'],
        'kazoo_lock': kazoo_lock_child(),
        'blobs': blobs,
        'shard_example': shard_meta,
        'component_template': {
            'connection_filter': ['aws-conn'],
            'hostname': 'launcher-a.example.com',
            'kind': 'launcher',
            'model_api': 38,
            'state': 'stopped',
            'version': '14.3.0',
        },
    }
    doc['component_json'] = dumps(doc['component_template']).decode('utf-8')
    path = os.path.join(OUT, 'fixtures.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(doc, f, indent=2, sort_keys=True)
        f.write('\n')
    print('wrote', path)


if __name__ == '__main__':
    sys.path.insert(0, ROOT)
    main()
