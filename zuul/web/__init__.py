# Copyright (c) 2017 Red Hat
# Copyright 2021-2026 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import cherrypy
import socket
from collections import defaultdict

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from opentelemetry import trace
from ws4py.server.cherrypyserver import WebSocketPlugin, WebSocketTool
from ws4py.websocket import WebSocket
import codecs
import copy
from datetime import datetime, timezone
import json
import jwt
import logging
import os
import pathlib
import time
import re
import select
import ssl
import threading
import uuid
import prometheus_client
import urllib.parse
import types

import zuul.executor.common
from zuul import exceptions
from zuul.configloader import ConfigLoader
from zuul.connection import BaseConnection, ReadOnlyBranchCacheError
import zuul.lib.repl
from zuul.launcher.client import LauncherClient
from zuul.lib import commandsocket, encryption, streamer_utils, tracing
from zuul.lib.ansible import AnsibleManager
from zuul.lib.jsonutil import ZuulJSONEncoder
from zuul.lib.keystorage import KeyStorage
from zuul.lib.monitoring import MonitoringServer
from zuul.lib.re2util import filter_allowed_disallowed
from zuul import model
from zuul.model import (
    Abide,
    Branch,
    BuildSet,
    ChangeQueue,
    DequeueEvent,
    EnqueueEvent,
    HoldRequest,
    NodesetRequest,
    PromoteEvent,
    ProviderNode,
    QueueItem,
    Ref,
    SystemAttributes,
    Tag,
    UnparsedAbideConfig,
    WebInfo,
)
from zuul.version import get_version_string
from zuul.zk import ZooKeeperClient
from zuul.zk.components import COMPONENT_REGISTRY, WebComponent
from zuul.zk.config_cache import SystemConfigCache, UnparsedConfigCache
from zuul.zk.event_queues import (
    EventWatcher,
    TenantManagementEventQueue,
    TenantTriggerEventQueue,
    PipelineManagementEventQueue,
    PipelineResultEventQueue,
    PipelineTriggerEventQueue,
    TENANT_EVENT_STATE,
)
from zuul.zk.exceptions import LockException
from zuul.zk.executor import ExecutorApi
from zuul.zk.image_registry import (
    ImageBuildRegistry,
    ImageUploadRegistry,
)
from zuul.zk.launcher import RequestCache, NodeCache
from zuul.zk.layout import (
    LayoutProvidersStore,
    LayoutStateStore,
)
from zuul.zk.locks import tenant_read_lock
from zuul.zk.nodepool import ZooKeeperNodepool
from zuul.zk.system import ZuulSystem
from zuul.zk.zkobject import LocalZKContext, ZKContext
from zuul.lib.auth import AuthenticatorRegistry
from zuul.lib.config import get_default
from zuul.lib.logutil import get_annotated_logger
from zuul.lib.statsd import get_statsd, normalize_statsd_name
from zuul.web.logutil import ZuulCherrypyLogManager

STATIC_DIR = os.path.join(os.path.dirname(__file__), 'static')
cherrypy.tools.websocket = WebSocketTool()

COMMANDS = [
    commandsocket.StopCommand,
    commandsocket.ReplCommand,
    commandsocket.NoReplCommand,
]


def get_zuul_request_id():
    request = cherrypy.serving.request
    if not hasattr(request, 'zuul_request_id'):
        request.zuul_request_id = uuid.uuid4().hex
    return request.zuul_request_id


def get_request_logger(logger=None):
    if logger is None:
        logger = logging.getLogger("zuul.web")
    zuul_request_id = get_zuul_request_id()
    return get_annotated_logger(logger, None, request=zuul_request_id)


def _datetimeToString(my_datetime):
    if my_datetime:
        return my_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    return None


class Prop:
    def __init__(self, description, value):
        """A property of an OpenAPI schema.

        :param str description: The description of this property
        :param type value: The type of the property; either a native
            Python scalar type, Prop instance, or list or dictionary of
            the preceding.
        """
        self.description = description
        self.value = value

    @staticmethod
    def _toOpenAPI(value):
        ret = {}
        if isinstance(value, Prop):
            ret['description'] = value.description
            value = value.value
        if isinstance(value, (list, tuple)):
            ret['type'] = 'array'
            ret['items'] = Prop._toOpenAPI(value[0])
        elif isinstance(value, (types.MappingProxyType, dict)):
            ret['type'] = 'object'
            ret['properties'] = {
                k: Prop._toOpenAPI(v) for (k, v) in value.items()
            }
        elif value is str:
            ret['type'] = 'string'
        elif value is int:
            ret['type'] = 'integer'
        elif value is float:
            ret['type'] = 'number'
        elif value is bool:
            ret['type'] = 'boolean'
        elif value is dict:
            ret['type'] = 'object'
        elif value is object:
            ret['type'] = 'object'
        return ret

    def toOpenAPI(self):
        "Convert this Prop to an OpenAPI schema."
        return Prop._toOpenAPI(self.value)


class RefConverter:
    # A class to encapsulate the conversion of database Ref objects to
    # API output.
    @staticmethod
    def toDict(ref):
        return {
            'project': ref.project,
            'branch': ref.branch,
            'change': ref.change,
            'patchset': ref.patchset,
            'ref': ref.ref,
            'oldrev': ref.oldrev,
            'newrev': ref.newrev,
            'ref_url': ref.ref_url,
        }

    @staticmethod
    def schema():
        return Prop('The ref', {
            'project': str,
            'branch': str,
            'change': str,
            'patchset': str,
            'ref': str,
            'oldrev': str,
            'newrev': str,
            'ref_url': str,
        })


class BuildConverter:
    # A class to encapsulate the conversion of database Build objects to
    # API output.
    def toDict(build, buildset=None, skip_refs=False):
        execute_time = _datetimeToString(build.execute_time)
        start_time = _datetimeToString(build.start_time)
        end_time = _datetimeToString(build.end_time)
        if build.start_time and build.end_time:
            duration = (build.end_time -
                        build.start_time).total_seconds()
        else:
            duration = None

        ret = {
            '_id': build.id,
            'uuid': build.uuid,
            'job_name': build.job_name,
            'result': build.result,
            'held': build.held,
            'execute_time': execute_time,
            'start_time': start_time,
            'end_time': end_time,
            'duration': duration,
            'voting': build.voting,
            'log_url': build.log_url,
            'nodeset': build.nodeset,
            'error_detail': build.error_detail,
            'final': build.final,
            'artifacts': [],
            'provides': [],
            'ref': RefConverter.toDict(build.ref),
        }
        if buildset:
            # We enter this branch if we're returning top-level build
            # objects (ie, not builds under a buildset).
            event_timestamp = _datetimeToString(buildset.event_timestamp)
            ret.update({
                'pipeline': buildset.pipeline,
                'event_id': buildset.event_id,
                'event_timestamp': event_timestamp,
                'queue_item_uuid': buildset.queue_item_uuid,
                'buildset': {
                    'uuid': buildset.uuid,
                },
            })
            if not skip_refs:
                ret['buildset']['refs'] = [
                    RefConverter.toDict(ref)
                    for ref in buildset.refs
                ]

        for artifact in build.artifacts:
            art = {
                'name': artifact.name,
                'url': artifact.url,
            }
            if artifact.meta:
                art['metadata'] = json.loads(artifact.meta)
            ret['artifacts'].append(art)
        for provides in build.provides:
            ret['provides'].append({
                'name': provides.name,
            })
        return ret

    @staticmethod
    def schema(buildset=False, skip_refs=False):
        ret = {
            '_id': str,
            'uuid': str,
            'job_name': str,
            'result': str,
            'held': str,
            'execute_time': str,
            'start_time': str,
            'end_time': str,
            'duration': str,
            'voting': str,
            'log_url': str,
            'nodeset': str,
            'error_detail': str,
            'final': str,
            'artifacts': [{
                'name': str,
                'url': str,
                'metadata': dict,
            }],
            'provides': [{
                'name': str,
            }],
            'ref': RefConverter.schema(),
        }
        if buildset:
            # We enter this branch if we're returning top-level build
            # objects (ie, not builds under a buildset).
            ret.update({
                'pipeline': str,
                'event_id': str,
                'event_timestamp': str,
                'buildset': {
                    'uuid': str,
                },
            })
            if not skip_refs:
                ret['buildset']['refs'] = [RefConverter.schema()]

        ret = Prop('The build', ret)
        return ret


class BuildsetEventConverter:
    # A class to encapsulate the conversion of database BuildsetEvent
    # objects to API output.
    def toDict(event):
        event_time = _datetimeToString(event.event_time)
        ret = {
            'event_time': event_time,
            'event_type': event.event_type,
            'description': event.description,
        }
        return ret

    def schema(builds=False):
        ret = {
            'event_time': str,
            'event_type': str,
            'description': str,
        }
        return Prop('The buildset event', ret)


class BuildsetConverter:
    # A class to encapsulate the conversion of database Buildset
    # objects to API output.
    def toDict(buildset, builds=None, events=None):
        event_timestamp = _datetimeToString(buildset.event_timestamp)
        start = _datetimeToString(buildset.first_build_start_time)
        end = _datetimeToString(buildset.last_build_end_time)
        ret = {
            '_id': buildset.id,
            'uuid': buildset.uuid,
            'result': buildset.result,
            'message': buildset.message,
            'pipeline': buildset.pipeline,
            'event_id': buildset.event_id,
            'event_timestamp': event_timestamp,
            'queue_item_uuid': buildset.queue_item_uuid,
            'first_build_start_time': start,
            'last_build_end_time': end,
            'user_id': buildset.user_id,
            'parameters': buildset.parameters,
            'refs': [
                RefConverter.toDict(ref)
                for ref in buildset.refs
            ],
        }
        if builds:
            ret['builds'] = [BuildConverter.toDict(b) for b in builds]
        if events:
            ret['events'] = [BuildsetEventConverter.toDict(e) for e in events]
        return ret

    def schema(builds=False, events=False):
        ret = {
            '_id': str,
            'uuid': str,
            'result': str,
            'message': str,
            'pipeline': str,
            'event_id': str,
            'event_timestamp': str,
            'first_build_start_time': str,
            'last_build_end_time': str,
            'user_id': str,
            'parameters': dict,
            'refs': [
                RefConverter.schema()
            ],
        }
        if builds:
            ret['builds'] = [BuildConverter.schema()]
        if events:
            ret['events'] = [BuildsetEventConverter.schema()]
        return Prop('The buildset', ret)


class ProviderConverter:
    # A class to encapsulate the conversion of Provider objects to
    # API output.
    @staticmethod
    def toDict(tenant, provider, build_artifacts, uploads):
        # These are the flattened versions of these objects for this
        # provider.
        images = []
        for image in provider.images.values():
            ret_image = {
                'name': image.name,
                'canonical_name': image.canonical_name,
                'type': image.type,
            }
            images.append(ret_image)
            image_build_artifacts = [
                iba for iba in build_artifacts
                if iba.canonical_name == image.canonical_name
            ]
            if image_build_artifacts:
                ret_image['build_artifacts'] = [
                    ImageBuildArtifactConverter.toDict(
                        tenant, b,
                        [u for u in uploads if u.artifact_uuid == b.uuid]
                    )
                    for b in image_build_artifacts
                ]
        labels = [
            {'name': x.name,
             'canonical_name': x.canonical_name,
             }
            for x in provider.labels.values()
        ]
        flavors = [
            {'name': x.name,
             'canonical_name': x.canonical_name,
             }
            for x in provider.flavors.values()
        ]
        return {
            'name': provider.name,
            'canonical_name': provider.canonical_name,
            'images': images,
            'labels': labels,
            'flavors': flavors,
        }

    @staticmethod
    def schema():
        return Prop('The provider', {
            'name': str,
            'canonical_name': str,
            'images': [
                {'name': str,
                 'canonical_name': str,
                 'type': str,
                 'build_artifacts': [ImageBuildArtifactConverter.schema()],
                 }
            ],
            'labels': [
                {'name': str,
                 'canonical_name': str,
                 }
            ],
            'flavors': [
                {'name': str,
                 'canonical_name': str,
                 }
            ]
        })


class ImageUploadConverter:
    # A class to encapsulate the conversion of image upload objects to
    # API output.
    @staticmethod
    def toDict(upload):
        timestamp = _datetimeToString(
            datetime.utcfromtimestamp(upload.timestamp))
        state_time = _datetimeToString(
            datetime.utcfromtimestamp(upload.state_time))
        ret = {
            'uuid': upload.uuid,
            'canonical_name': upload.canonical_name,
            'artifact_uuid': upload.artifact_uuid,
            'endpoint_name': upload.endpoint_name,
            'external_id': upload.external_id,
            'timestamp': timestamp,
            'validated': upload.validated,
            'build_uuid': upload.build_uuid,
            'state': upload.state,
            'state_time': state_time,
            'is_locked': upload.is_locked,
            'lock_holder': upload.lock_holder,
        }
        return ret

    @staticmethod
    def schema():
        return Prop('The image upload', {
            'uuid': str,
            'canonical_name': str,
            'artifact_uuid': str,
            'endpoint_name': str,
            'external_id': str,
            'timestamp': str,
            'validated': str,
            'build_uuid': str,
            'state': str,
            'state_time': str,
            'is_locked': bool,
            'lock_holder': str,
        })


class ImageBuildArtifactConverter:
    # A class to encapsulate the conversion of image build objects to
    # API output.
    @staticmethod
    def toDict(tenant, build, uploads):
        timestamp = _datetimeToString(
            datetime.utcfromtimestamp(build.timestamp))
        state_time = _datetimeToString(
            datetime.utcfromtimestamp(build.state_time))
        ret = {
            'uuid': build.uuid,
            'canonical_name': build.canonical_name,
            'build_tenant': tenant.name == build.build_tenant_name,
            'build_uuid': build.build_uuid,
            'format': build.format,
            'md5sum': build.md5sum,
            'sha256': build.sha256,
            'url': build.url,
            'timestamp': timestamp,
            'validated': build.validated,
            'state': build.state,
            'state_time': state_time,
            'is_locked': build.is_locked,
            'lock_holder': build.lock_holder,
        }
        if uploads:
            ret['uploads'] = [ImageUploadConverter.toDict(u)
                              for u in uploads]
        return ret

    @staticmethod
    def schema():
        return Prop('The image build artifact', {
            'uuid': str,
            'canonical_name': str,
            'build_tenant': bool,
            'build_uuid': str,
            'format': str,
            'md5sum': str,
            'sha256': str,
            'url': str,
            'timestamp': str,
            'validated': str,
            'state': str,
            'state_time': str,
            'is_locked': bool,
            'lock_holder': str,
            'uploads': [ImageUploadConverter.schema()],
        })


class ImageConverter:
    # A class to encapsulate the conversion of Image objects to
    # API output.
    @staticmethod
    def toDict(tenant, image, build_artifacts, uploads):
        ret = {
            'name': image.name,
            'canonical_name': image.canonical_name,
            'project_canonical_name': image.project_canonical_name,
            'branch': image.branch,
            'type': image.type,
        }
        if build_artifacts:
            ret['build_artifacts'] = [
                ImageBuildArtifactConverter.toDict(
                    tenant, b,
                    [u for u in uploads if u.artifact_uuid == b.uuid]
                )
                for b in build_artifacts
            ]
        return ret

    @staticmethod
    def schema():
        return Prop('The image', {
            'name': str,
            'canonical_name': str,
            'project_canonical_name': str,
            'branch': str,
            'type': str,
            'build_artifacts': [ImageBuildArtifactConverter.schema()],
        })


class FlavorConverter:
    # A class to encapsulate the conversion of Flavor objects to
    # API output.
    @staticmethod
    def toDict(tenant, flavor):
        ret = {
            'name': flavor.name,
            'canonical_name': flavor.canonical_name,
            'description': flavor.description,
        }
        return ret

    @staticmethod
    def schema():
        return Prop('The image', {
            'name': str,
            'canonical_name': str,
            'description': str,
        })


class LabelConverter:
    # A class to encapsulate the conversion of Label objects to
    # API output.
    @staticmethod
    def toDict(tenant, label):
        ret = {
            'name': label.name,
            'canonical_name': label.canonical_name,
            'description': label.description,
        }
        return ret

    @staticmethod
    def schema():
        return Prop('The image', {
            'name': str,
            'canonical_name': str,
            'description': str,
        })


class ProviderNodeConverter:
    # A class to encapsulate the conversion of ProviderNode objects to
    # API output.
    @staticmethod
    def toDict(node):
        state_time = _datetimeToString(
            datetime.utcfromtimestamp(node.state_time))
        request_time = _datetimeToString(
            datetime.utcfromtimestamp(node.request_time)
            if node.request_time is not None else None
        )
        ret = {
            'id': node.uuid,
            'uuid': node.uuid,
            # TODO: remove Nodepool backwards-compat type
            'type': [node.label],
            'label': node.label,
            'connection_type': node.connection_type,
            'external_id': None,
            'state': node.state,
            'state_time': state_time,
            'next_state': node.next_state,
            'comment': getattr(node, 'comment', None),
            'max_ready_age': node.max_ready_age,
            'max_age': node.max_age,
            'interface_ip': node.interface_ip,
            'public_ipv4': node.public_ipv4,
            'private_ipv4': node.private_ipv4,
            'public_ipv6': node.public_ipv6,
            'private_ipv6': node.private_ipv6,
            'slot': node.slot,
            'az': node.az,
            'cloud': node.cloud,
            'provider': node.provider,
            'region': node.region,
            'username': node.username,
            'hold_expiration': node.hold_expiration,
            'quota': node.quota.getResources(),
            'is_locked': node.is_locked,
            'lock_holder': node.lock_holder,
            'request_id': node.request_id,
            'request_time': request_time,
        }
        return ret

    @staticmethod
    def schema():
        return Prop('The node', {
            'id': str,
            'uuid': str,
            # TODO: remove Nodepool backwards-compat type
            'type': [str],
            'label': str,
            'connection_type': str,
            'external_id': str,
            'state': str,
            'state_time': str,
            'next_state': str,
            'comment': str,
            'max_ready_age': int,
            'max_age': int,
            'interface_ip': str,
            'public_ipv4': str,
            'private_ipv4': str,
            'public_ipv6': str,
            'private_ipv6': str,
            'slot': int,
            'az': str,
            'cloud': str,
            'provider': str,
            'region': str,
            'username': str,
            'hold_expiration': int,
            'quota': dict,
            'is_locked': bool,
            'lock_holder': str,
            'request_id': str,
            'request_time': str,
        })


class NodesetRequestConverter:
    # A class to encapsulate the conversion of NodesetRequest objects
    # to API output.
    @staticmethod
    def toDict(request):
        request_time = _datetimeToString(
            datetime.utcfromtimestamp(request.request_time))

        provider_node_data = [
            {
                'uuid': data['uuid'],
                'executor_zone': data['executor_zone'],
                'failed_providers': data['failed_providers'],
            }
            for data in request.provider_node_data
        ]

        ret = {
            'uuid': request.uuid,
            'state': request.state,
            'pipeline_name': request.pipeline_name,
            'buildset_uuid': request.buildset_uuid,
            # Omitting job_uuid because it is not externally
            # meaningful (it is not a build uuid, and it may be
            # confused as one if we don't otherwise expose the frozen
            # job uuid).
            'job_name': request.job_name,
            'labels': request.labels,
            'priority': request.priority,
            'request_time': request_time,
            'zuul_event_id': request.zuul_event_id,
            'image_names': request.image_names,
            'image_upload_uuid': request.image_upload_uuid,
            'relative_priority': request.relative_priority,
            'provider_node_data': provider_node_data,
            'is_locked': request.is_locked,
            'lock_holder': request.lock_holder,
        }
        return ret

    @staticmethod
    def schema():
        return Prop('The nodeset request', {
            'uuid': str,
            'state': str,
            'pipeline_name': str,
            'buildset_uuid': str,
            # Omitting job_uuid because it is not externally
            # meaningful (it is not a build uuid, and it may be
            # confused as one if we don't otherwise expose the frozen
            # job uuid).
            'job_name': str,
            'labels': [str],
            'priority': int,
            'request_time': str,
            'zuul_event_id': str,
            'image_names': [str],
            'image_upload_uuid': str,
            'relative_priority': int,
            'provider_node_data': [
                {
                    'uuid': str,
                    'executor_zone': str,
                    'failed_providers': [str],
                }
            ],
            'is_locked': bool,
            'lock_holder': str,
        })


class SystemEventConverter:
    # A class to encapsulate the conversion of database SystemEvent
    # objects to API output.
    def toDict(event):
        event_time = _datetimeToString(event.event_time)
        ret = {
            'event_id': event.event_id,
            'event_time': event_time,
            'event_type': event.event_type,
            'description': event.description,
        }
        return ret

    def schema(builds=False):
        ret = {
            'event_id': str,
            'event_time': str,
            'event_type': str,
            'description': str,
        }
        return Prop('The system event', ret)


class ProjectPipelineConverter:
    # A class to encapsulate the conversion of project pipeline
    # config objects to API output.
    def toDict(ppc):
        parameters = []
        if ppc:
            for p in ppc.parameters.values():
                parameters.append({
                    'name': p.name,
                    'type': p.type,
                    'description': p.description,
                    'values': p.values,
                    'default': p.default,
                    'required': p.required,
                })
        ret = {
            'parameters': parameters,
        }
        return ret

    def schema():
        parameter = {
            'name': str,
            'type': str,
            'description': str,
            'values': [str],
            'default': str,
            'required': bool,
        }

        ret = {
            'parameters': [parameter],
        }
        return Prop('The project pipeline config', ret)


class APIError(cherrypy.HTTPError):
    def __init__(self, code, json_doc=None, headers=None):
        self._headers = headers or {}
        self._json_doc = json_doc
        super().__init__(code)

    def set_response(self):
        super().set_response()
        resp = cherrypy.response
        resp.headers.update(self._headers)
        if self._json_doc:
            ret = json.dumps(self._json_doc).encode('utf8')
            resp.body = ret
            resp.headers['Content-Type'] = 'application/json'
            resp.headers["Content-Length"] = len(ret)
        else:
            resp.body = b''
            resp.headers["Content-Length"] = '0'


class SaveParamsTool(cherrypy.Tool):
    """
    Save the URL parameters to allow them to take precedence over query
    string parameters.
    """
    def __init__(self):
        cherrypy.Tool.__init__(self, 'on_start_resource',
                               self.saveParams, priority=10)

    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach('before_handler',
                                      self.restoreParams)

    def saveParams(self, restore=True):
        cherrypy.request.url_params = cherrypy.request.params.copy()
        cherrypy.request.url_params_restore = restore

    def restoreParams(self):
        if cherrypy.request.url_params_restore:
            cherrypy.request.params.update(cherrypy.request.url_params)


cherrypy.tools.save_params = SaveParamsTool()


def handle_options(allowed_methods=None):
    if cherrypy.request.method == 'OPTIONS':
        methods = allowed_methods or ['GET', 'OPTIONS']
        if allowed_methods and 'OPTIONS' not in allowed_methods:
            methods = methods + ['OPTIONS']
        # discard decorated handler
        request = cherrypy.serving.request
        request.handler = None
        # Set CORS response headers
        resp = cherrypy.response
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Headers'] =\
            ', '.join(['Authorization', 'Content-Type'])
        resp.headers['Access-Control-Allow-Methods'] =\
            ', '.join(methods)
        # Allow caching of the preflight response
        resp.headers['Access-Control-Max-Age'] = 86400
        resp.status = 204


cherrypy.tools.handle_options = cherrypy.Tool('on_start_resource',
                                              handle_options,
                                              priority=50)


def openapi_response(
        code,
        description=None,
        content_type=None,
        example=None,
        schema=None,
):
    """Describe an OpenAPI response

    :param int code: The HTTP response code
    :param str description: A description for the response
    :param str content_type: The HTTP content type
    :param str example: An example of the response output
    :param Prop schema: A Prop describing the returned schema
    """
    response_spec = {}
    if description:
        response_spec['description'] = description
    if content_type:
        content_spec = {}
        response_spec['content'] = {content_type: content_spec}
        if example:
            content_spec['example'] = example
        if schema:
            content_spec['schema'] = schema.toOpenAPI()

    def decorator(func):
        if not hasattr(func, '__openapi__'):
            func.__openapi__ = {}
        func.__openapi__.setdefault('responses', {})
        func.__openapi__['responses'][code] = response_spec
        return func

    return decorator


class AuthInfo:
    def __init__(self, uid, admin, claims):
        self.uid = uid
        self.admin = admin
        self.claims = claims


class AuthContext:
    """This stores common information about the authorization context for
    a resource so that the request for the resource can be authorized
    either in a Cherrpy tool or a Websocket handler.

    :param Tenant tenant: The Zuul tenant object; supply if it is relevant
        for authorization to the protected resource
    :param bool require_admin: Whether admin access is required for
        this resource
    :param bool require_auth: Whether authenticated access is required for
        this resource
    :param str permission: Which role permissions is required to access
        this resource
    """

    def __init__(self, tenant=None, require_admin=None, require_auth=None,
                 permission=None):
        request = cherrypy.serving.request
        zuulweb = request.app.root.zuulweb
        if require_admin:
            require_auth = True

        if tenant:
            if not require_auth and not tenant.anonymous_read_access:
                # This tenant requires auth for read-only access
                require_auth = True
            elif not require_auth and tenant.access_rules:
                # This tenant requires auth for read-only access
                require_auth = True
        else:
            anon_read_access = zuulweb.abide.api_root.anonymous_read_access
            if not require_auth and not anon_read_access:
                # The API root requires auth for read-only access
                require_auth = True
            elif not require_auth and zuulweb.abide.api_root.access_rules:
                # The API root requires auth for read-only access
                require_auth = True
        self.require_admin = require_admin
        self.require_auth = require_auth
        self.permission = permission
        self.headers = request.headers
        self.tenant = tenant
        self.zuulweb = zuulweb

    def validate(self, token=None):
        """Validate access to the resource

        :param str token: The bearer token; if not supplied, it will be
            retreieved from the request header

        Raises an exception if authorization failed and was required.

        :returns: an AuthInfo instance if authorization succeeded;
            None if it failed but was not required.

        """
        if token is None:
            try:
                token = self._getTokenFromHeader()
            except APIError:
                if self.require_auth:
                    raise
                return None
        try:
            claims = self._getClaims(token)
        except APIError:
            if self.require_auth:
                raise
            return None

        access, admin = self.zuulweb.api._isAuthorized(self.tenant,
                                                       claims,
                                                       self.permission)
        if ((self.require_auth and not access) or
            (self.require_admin and not admin)):
            raise APIError(403)

        return AuthInfo(claims['__zuul_uid_claim'], admin, claims)

    def _getTokenFromHeader(self):
        """Make sure protected endpoints have a Authorization header with the
        bearer token."""
        token_header = self.headers.get('Authorization', None)
        # Add basic checks here
        if token_header is None:
            e = 'MissingAuthError'
            e_desc = 'Missing "Authorization" header'
        elif not token_header.lower().startswith('bearer '):
            e = 'InvalidAuthFormat'
            e_desc = '"Authorization" header must start with "Bearer"'
        else:
            token = token_header[len('Bearer '):]
            return token
        error_header = '''Bearer realm="%s"
       error="%s"
       error_description="%s"''' % (self.zuulweb.authenticators.default_realm,
                                    e,
                                    e_desc)
        error_data = {'description': e_desc,
                      'error': e,
                      'realm': self.zuulweb.authenticators.default_realm}
        raise APIError(401, json_doc=error_data, headers={
            "WWW-Authenticate": error_header
        })

    def _getClaims(self, token):
        try:
            claims = self.zuulweb.authenticators.authenticate(token)
        except exceptions.AuthTokenException as e:
            error_data = {'description': str(e.error_description),
                          'error': str(e.error),
                          'realm': str(e.realm)}
            raise APIError(e.HTTPError, json_doc=error_data,
                           headers=e.getAdditionalHeaders())
        return claims


def check_root_auth(**kw):
    """Use this for root-level (non-tenant) methods"""
    cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
    request = cherrypy.serving.request
    if request.handler is None:
        # handle_options has already aborted the request.
        return
    auth_context = AuthContext(**kw)
    request.params['auth'] = auth_context.validate()


def check_tenant_auth(**kw):
    """Use this for tenant-scoped methods"""
    cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
    request = cherrypy.serving.request
    zuulweb = request.app.root
    if request.handler is None:
        # handle_options has already aborted the request.
        return

    tenant_name = request.params.get('tenant_name')
    # Always set the tenant variable
    tenant = zuulweb._getTenantOrRaise(tenant_name)
    request.params['tenant'] = tenant
    auth_context = AuthContext(tenant=tenant, **kw)
    request.params['auth'] = auth_context.validate()


cherrypy.tools.check_root_auth = cherrypy.Tool('before_handler',
                                               check_root_auth,
                                               priority=90)
cherrypy.tools.check_tenant_auth = cherrypy.Tool('before_handler',
                                                 check_tenant_auth,
                                                 priority=90)


class StatsTool(cherrypy.Tool):
    def __init__(self, statsd, metrics):
        self.statsd = statsd
        self.metrics = metrics
        self.hostname = normalize_statsd_name(socket.getfqdn())
        cherrypy.Tool.__init__(self, 'on_start_resource',
                               self.emitStats)

    def emitStats(self):
        if not cherrypy.server.httpserver:
            # Server may not have started yet (if we're called from apsched)
            return
        idle = cherrypy.server.httpserver.requests.idle
        qsize = cherrypy.server.httpserver.requests.qsize
        self.metrics.threadpool_idle.set(idle)
        self.metrics.threadpool_queue.set(qsize)
        if self.statsd:
            self.statsd.gauge(
                f'zuul.web.server.{self.hostname}.threadpool.idle',
                idle)
            self.statsd.gauge(
                f'zuul.web.server.{self.hostname}.threadpool.queue',
                qsize)


class WebMetrics:
    def __init__(self):
        self.threadpool_idle = prometheus_client.Gauge(
            'web_threadpool_idle', 'The number of idle worker threads')
        self.threadpool_queue = prometheus_client.Gauge(
            'web_threadpool_queue', 'The number of queued requests')
        self.streamers = prometheus_client.Gauge(
            'web_streamers', 'The number of log streamers currently operating')


# Custom JSONEncoder that combines the ZuulJSONEncoder with cherrypy's
# JSON functionality.
class ZuulWebJSONEncoder(ZuulJSONEncoder):

    def iterencode(self, value):
        # Adapted from cherrypy/_json.py
        for chunk in super().iterencode(value):
            yield chunk.encode("utf-8")


json_encoder = ZuulWebJSONEncoder()


def json_handler(*args, **kwargs):
    # Adapted from cherrypy/lib/jsontools.py
    value = cherrypy.serving.request._json_inner_handler(*args, **kwargs)
    return json_encoder.iterencode(value)


class RefFilter(object):
    def __init__(self, desired):
        self.desired = desired

    def filterPayload(self, payload):
        status = []
        for pipeline in payload['pipelines']:
            for change_queue in pipeline.get('change_queues', []):
                for head in change_queue['heads']:
                    for item in head:
                        want_item = False
                        for ref in item['refs']:
                            if self.wantRef(ref):
                                want_item = True
                                break
                        if want_item:
                            status.append(copy.deepcopy(item))
        return status

    def wantRef(self, ref):
        return ref['id'] == self.desired


class LogStreamHandler(WebSocket):
    def __init__(self, *args, **kw):
        kw['heartbeat_freq'] = 20
        self.log = get_request_logger()
        super(LogStreamHandler, self).__init__(*args, **kw)
        self.streamer = None

        # Because we lose our request context by the time we get the
        # authorization token over the websocket protocol, we create
        # an AuthContext here, and then perform delayed validation on
        # messages we recieve.
        request = cherrypy.serving.request
        self.zuulweb = request.app.root.zuulweb
        self.tenant_name = request.params.get('tenant_name')
        tenant = self.zuulweb.api._getTenantOrRaise(self.tenant_name)
        self.auth_context = AuthContext(tenant=tenant)

    def received_message(self, message):
        if message.is_text:
            req = json.loads(message.data.decode('utf-8'))

            token = req.pop('token', None)

            self.log.debug("Websocket request: %s", req)
            if self.streamer:
                self.log.debug("Ignoring request due to existing streamer")
                return

            try:
                self.auth_context.validate(token=token)
            except APIError as e:
                if e._json_doc and e._json_doc.get('error'):
                    msg = e._json_doc.get('error').encode('utf8')[:123]
                else:
                    msg = b'Authorization error'
                return self.logClose(4000, msg)

            try:
                self._streamLog(req)
            except Exception:
                self.log.exception("Error processing websocket message:")
                raise

    def closed(self, code, reason=None):
        self.log.debug("Websocket closed: %s %s", code, reason)
        if self.streamer:
            try:
                self.streamer.zuulweb.stream_manager.unregisterStreamer(
                    self.streamer)
            except Exception:
                self.log.exception("Error on remote websocket close:")

    def logClose(self, code, msg):
        self.log.debug("Websocket close: %s %s", code, msg)
        try:
            self.close(code, msg)
        except Exception:
            self.log.exception("Error closing websocket:")

    def _streamLog(self, request):
        """
        Stream the log for the requested job back to the client.

        :param dict request: The client request parameters.
        """
        for key in ('uuid', 'logfile'):
            if key not in request:
                return self.logClose(
                    4000,
                    "'{key}' missing from request payload".format(
                        key=key))

        try:
            port_location = streamer_utils.getJobLogStreamAddress(
                self.zuulweb.executor_api,
                request['uuid'], source_zone=self.zuulweb.zone,
                tenant_name=self.tenant_name)
        except exceptions.StreamingError as e:
            return self.logClose(4011, str(e))

        if not port_location:
            return self.logClose(4011, "Error with log streaming")

        self.streamer = LogStreamer(
            self.zuulweb, self,
            port_location['server'], port_location['port'],
            request['uuid'], port_location.get('use_ssl'))


class LogStreamer(object):
    def __init__(self, zuulweb, websocket, server, port, build_uuid, use_ssl):
        """
        Create a client to connect to the finger streamer and pull results.

        :param str server: The executor server running the job.
        :param str port: The executor server port.
        :param str build_uuid: The build UUID to stream.
        """
        self.fileno = None
        self.log = websocket.log
        self.log.debug("Connecting to finger server %s:%s", server, port)
        Decoder = codecs.getincrementaldecoder('utf8')
        self.decoder = Decoder()
        self.zuulweb = zuulweb
        self.finger_socket = socket.create_connection(
            (server, port), timeout=10)
        if use_ssl:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = self.zuulweb.finger_tls_verify_hostnames
            context.load_cert_chain(
                self.zuulweb.finger_tls_cert, self.zuulweb.finger_tls_key)
            context.load_verify_locations(self.zuulweb.finger_tls_ca)
            self.finger_socket = context.wrap_socket(
                self.finger_socket, server_hostname=server)

        self.finger_socket.settimeout(None)
        self.websocket = websocket
        self.uuid = build_uuid
        msg = "%s\n" % build_uuid    # Must have a trailing newline!
        self.finger_socket.sendall(msg.encode('utf-8'))
        self.fileno = self.finger_socket.fileno()
        self.zuulweb.stream_manager.registerStreamer(self)

    def __repr__(self):
        return '<LogStreamer %s uuid:%s fd:%s>' % (
            self.websocket, self.uuid, self.fileno)

    def errorClose(self):
        try:
            self.websocket.logClose(4011, "Unknown error")
        except Exception:
            self.log.exception("Error closing web:")

    def closeSocket(self):
        try:
            self.finger_socket.close()
        except Exception:
            self.log.exception("Error closing streamer socket:")

    def handle(self, event):
        if event & select.POLLIN:
            data = self.finger_socket.recv(1024)
            if data:
                data = self.decoder.decode(data)
                if data:
                    self.websocket.send(data, False)
            else:
                # Make sure we flush anything left in the decoder
                data = self.decoder.decode(b'', final=True)
                if data:
                    self.websocket.send(data, False)
                self.zuulweb.stream_manager.unregisterStreamer(self)
                return self.websocket.logClose(1000, "No more data")
        else:
            self.zuulweb.stream_manager.unregisterStreamer(self)
            return self.websocket.logClose(1000, "Remote error")


class ZuulWebOIDC(object):
    def __init__(self, zuulweb):
        self.zuulweb = zuulweb
        self.keystore = zuulweb.keystore
        # We listen to the keystore cache for changes to the OIDC signing keys
        # instead of listening to the zk event to make sure the zuul-web
        # cache is updated only after the keystore cache is updated.
        if self.keystore.oidc_signing_keys_cache:
            self.keystore.oidc_signing_keys_cache.addCacheUpdatedListener(self)

        self.jwks_cache = None

    # This method is called when the keystore cache is updated
    def onCacheUpdated(self):
        self.jwks_cache = self._getJwks()

    @cherrypy.expose
    @cherrypy.tools.handle_options()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    def global_openid_configuration(self):
        return self.tenant_openid_configuration()

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    def tenant_openid_configuration(self, tenant_name=None):
        # If tenant_name is None or does not exist,
        # fallback to the global web_root
        web_root = self.zuulweb.abide.tenants.get(
            tenant_name, self.zuulweb.globals
        ).web_root
        # If web_root is not configured in zuul and tenant config,
        # just keep it empty, but it is not really useful for OIDC
        web_root = web_root.split("/t/")[0].rstrip("/") if web_root else ""

        return {
            "issuer": web_root,
            "jwks_uri": f"{web_root}/oidc/jwks",
            "claims_supported": [
                "aud", "iat", "iss", "name", "sub", "custom"
            ],
            "response_types_supported": ["id_token"],
            "id_token_signing_alg_values_supported": (
                self.zuulweb.globals.oidc_supported_signing_algorithms),
            "subject_types_supported": ["public"]
        }

    @cherrypy.expose
    @cherrypy.tools.handle_options()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    def global_jwks(self):
        return self.tenant_jwks()

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    def tenant_jwks(self, tenant_name=None):
        if not self.jwks_cache:
            self.jwks_cache = self._getJwks()

        return self.jwks_cache

    def _getJwks(self):
        jwks_keys = []
        for alg in self.zuulweb.globals.oidc_supported_signing_algorithms:
            signing_key_data = self.keystore.getOidcSigningKeyData(
                algorithm=alg)
            for key in signing_key_data.keys:
                pem_private_key = key["private_key"].encode("utf-8")
                _, public_key = encryption.deserialize_rsa_keypair(
                    pem_private_key, self.keystore.password_bytes)
                jwks_keys.append(
                    self._generateJwkDict(alg, key['version'], public_key))

        return {
            "keys": jwks_keys
        }

    def _generateJwkDict(self, alg, version, public_key):
        Algorithm = jwt.algorithms.get_default_algorithms()[alg]
        jwk_dict = Algorithm.to_jwk(public_key, as_dict=True)
        jwk_dict.update({
            "alg": alg,
            "use": "sig",
            "kid": f"{alg}-{version}"
        })
        # We remove the key_ops for a few reasons:
        # 1) The RFC frowns on having both "use" and "key_ops":
        #    https://datatracker.ietf.org/doc/html/rfc7517#section-4.3
        #    (However, it's not explicitly disallowed, and it says
        #    that if both are present, they should be conisstent, and
        #    "signature" (use) "verification" (key_ops) sounds
        #    consistent).
        # 2) Both AWS and Azure have undocumented prohibitions against
        #    supplying both for use with their federated identity
        #    services (STS, Service principal authentication).
        # 3) AWS will accept either but not both.
        # 4) Azure will only accept "use".
        # 5) Github's JWKS only supplies "use".
        # Therefore, it seems like the best option is to add "use" and
        # drop "key_ops" (which, as of this writing, is included
        # automatically by pyjwt, but "use" is not).
        jwk_dict.pop('key_ops', None)

        return jwk_dict


class ZuulWebAPI(object):
    _rbac_param_map = {
        "tenant_name": "tenant",
        "project_name": "project",
        "branch_name": "branch",
        "job_name": "job",
        "pipeline_name": "pipeline",
        "image_name": "image",
    }

    def __init__(self, zuulweb):
        self.zuulweb = zuulweb
        self.zk_client = zuulweb.zk_client
        self.zk_nodepool = ZooKeeperNodepool(self.zk_client,
                                             enable_node_cache=True)
        self.status_caches = {}
        self.status_cache_times = {}
        self.status_cache_locks = defaultdict(threading.Lock)
        self.tenants_cache = []
        self.tenants_cache_time = 0
        self.tenants_cache_lock = threading.Lock()

        self.cache_expiry = 1
        self.static_cache_expiry = zuulweb.static_cache_expiry
        # SQL build query timeout, in milliseconds:
        self.query_timeout = 30000

    @property
    def log(self):
        return get_request_logger()

    def _get_mapped_rbac_params(self):
        params = {}
        if hasattr(cherrypy.request, 'json'):
            params = cherrypy.request.json.copy()
        params.update(cherrypy.request.params)
        for p, v in self._rbac_param_map.items():
            # Replace internal API names with human friendly names when
            # checking rbac permissions
            if p in params:
                params[v] = params[p]
                del params[p]
        return params

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['POST', ])
    @cherrypy.tools.check_tenant_auth(require_admin=True, permission='dequeue')
    def dequeue(self, tenant_name, tenant, auth, project_name):
        if cherrypy.request.method != 'POST':
            raise cherrypy.HTTPError(405)
        self.log.info(f'User {auth.uid} requesting dequeue on '
                      f'{tenant_name}/{project_name}')

        project = self._getProjectOrRaise(tenant, project_name)

        body = cherrypy.request.json
        if 'pipeline' in body and (
                ('change' in body and 'ref' not in body) or
                ('change' not in body and 'ref' in body)):
            # Validate the pipeline so we can enqueue the event directly
            # in the pipeline management event queue and don't need to
            # take the detour via the tenant management event queue.
            pipeline_name = body['pipeline']
            manager = tenant.layout.pipeline_managers.get(pipeline_name)
            if manager is None:
                raise cherrypy.HTTPError(400, 'Unknown pipeline')

            event = DequeueEvent(
                tenant_name, pipeline_name, project.canonical_hostname,
                project.name, body.get('change', None), body.get('ref', None))
            event.zuul_event_id = get_zuul_request_id()
            self.zuulweb.pipeline_management_events[tenant_name][
                pipeline_name].put(event)
        else:
            raise cherrypy.HTTPError(400, 'Invalid request body')
        return True

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['POST', ])
    @cherrypy.tools.check_tenant_auth(require_admin=True, permission='enqueue')
    def enqueue(self, tenant_name, tenant, auth, project_name):
        body = cherrypy.request.json
        if cherrypy.request.method != 'POST':
            raise cherrypy.HTTPError(405)
        self.log.info(f'User {auth.uid} requesting enqueue on '
                      f'{tenant_name}/{project_name}')

        project = self._getProjectOrRaise(tenant, project_name)

        if 'pipeline' not in body:
            raise cherrypy.HTTPError(400, 'Invalid request body')

        # Validate the pipeline so we can enqueue the event directly
        # in the pipeline management event queue and don't need to
        # take the detour via the tenant management event queue.
        pipeline_name = body['pipeline']
        manager = tenant.layout.pipeline_managers.get(pipeline_name)
        if manager is None:
            raise cherrypy.HTTPError(400, 'Unknown pipeline')

        if body.get('parameters'):
            ref = body.get('ref', '')
            if not (ref.startswith('refs/heads') or
                    ref.startswith('refs/tags')):
                raise cherrypy.HTTPError(
                    400, 'Parameters are supported only with branches or tags')

        if 'change' in body:
            return self._enqueue(tenant, project, manager.pipeline,
                                 body['change'])
        elif 'ref' in body:
            return self._enqueue_ref(tenant, project,
                                     manager.pipeline, body['ref'],
                                     body.get('oldrev'), body.get('newrev'),
                                     auth.uid, body.get('parameters'))
        else:
            raise cherrypy.HTTPError(400, 'Invalid request body')

    def _enqueue(self, tenant, project, pipeline, change):
        event = EnqueueEvent(tenant.name, pipeline.name,
                             project.canonical_hostname, project.name,
                             change, ref=None, oldrev=None, newrev=None)
        event.zuul_event_id = get_zuul_request_id()
        self.zuulweb.pipeline_management_events[tenant.name][
            pipeline.name].put(event)

        return True

    def _enqueue_ref(self, tenant, project, pipeline, ref, oldrev,
                     newrev, user_id, parameters):
        event = EnqueueEvent(tenant.name, pipeline.name,
                             project.canonical_hostname, project.name,
                             change=None, ref=ref, oldrev=oldrev,
                             newrev=newrev, user_id=user_id,
                             parameters=parameters)
        event.zuul_event_id = get_zuul_request_id()
        self.zuulweb.pipeline_management_events[tenant.name][
            pipeline.name].put(event)

        return True

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['POST', ])
    @cherrypy.tools.check_tenant_auth(require_admin=True, permission='promote')
    def promote(self, tenant_name, tenant, auth):
        if cherrypy.request.method != 'POST':
            raise cherrypy.HTTPError(405)

        body = cherrypy.request.json
        pipeline_name = body.get('pipeline')
        changes = body.get('changes')

        self.log.info(f'User {auth.uid} requesting promote on '
                      f'{tenant_name}/{pipeline_name}')

        # Validate the pipeline so we can enqueue the event directly
        # in the pipeline management event queue and don't need to
        # take the detour via the tenant management event queue.
        manager = tenant.layout.pipeline_managers.get(pipeline_name)
        if manager is None:
            raise cherrypy.HTTPError(400, 'Unknown pipeline')

        event = PromoteEvent(tenant_name, pipeline_name, changes)
        event.zuul_event_id = get_zuul_request_id()
        self.zuulweb.pipeline_management_events[tenant_name][
            pipeline_name].put(event)

        return True

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['POST', ])
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='set-tenant-state')
    def state_post(self, tenant_name, tenant, auth):
        body = cherrypy.request.json
        current_state = self.zuulweb.event_watcher.tenant_state[tenant.name]
        with self.zuulweb.createZKContext(None, self.log) as ctx:
            path = TENANT_EVENT_STATE.format(tenant=tenant_name)
            tqs = model.TenantEventState(path)
            self.log.info('User %s setting tenant %s state to %s',
                          auth.uid, tenant_name, body)

            reason = body.get('reason')
            if reason:
                # We limit the reason to 4096 chars to limit the
                # zk node size.
                reason = str(reason[:4096])
            tqs._set(
                trigger_queue_discarding=bool(
                    body.get('trigger_queue_discarding',
                             current_state.trigger_queue_discarding)),
                trigger_queue_paused=bool(
                    body.get('trigger_queue_paused',
                             current_state.trigger_queue_paused)),
                result_queue_paused=bool(
                    body.get('result_queue_paused',
                             current_state.result_queue_paused)),
                reason=reason,
            )
            tqs.internalCreate(ctx)

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def autohold_list(self, tenant_name, tenant, auth, *args, **kwargs):
        # filter by project if passed as a query string
        project_name = cherrypy.request.params.get('project', None)
        return self._autohold_list(tenant_name, project_name)

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['GET', 'POST'])
    @cherrypy.tools.check_tenant_auth()
    def autohold_project_get(self, tenant_name, tenant, auth, project_name):
        # Note: GET handling is redundant with autohold_list
        # and could be removed.
        return self._autohold_list(tenant_name, project_name)

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    # Options handled by _get method
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='autohold')
    def autohold_project_post(self, tenant_name, tenant, auth, project_name):
        project = self._getProjectOrRaise(tenant, project_name)
        self.log.info(f'User {auth.uid} requesting autohold on '
                      f'{tenant_name}/{project_name}')

        jbody = cherrypy.request.json

        # Validate the payload
        jbody['change'] = jbody.get('change', None)
        jbody['ref'] = jbody.get('ref', None)
        count = jbody.get('count')
        if jbody['change'] and jbody['ref']:
            raise cherrypy.HTTPError(
                400, 'change and ref are mutually exclusive')
        if not all(p in jbody for p in [
                'job', 'count', 'change', 'ref', 'reason',
                'node_hold_expiration']):
            raise cherrypy.HTTPError(400, 'Invalid request body')
        if count < 0:
            raise cherrypy.HTTPError(400, "Count must be greater than 0")

        project_name = project.canonical_name

        if jbody['change']:
            ref_filter = project.source.getRefForChange(jbody['change'])
        elif jbody['ref']:
            ref_filter = str(jbody['ref'])
        else:
            ref_filter = ".*"

        try:
            _ = re.compile(ref_filter)
        except re.error as e:
            raise cherrypy.HTTPError(
                400,
                'argument is not a valid regular expression: "%s"' % e
            )
        self._autohold(tenant_name, project_name, jbody['job'], ref_filter,
                       jbody['reason'], jbody['count'],
                       jbody['node_hold_expiration'])
        return True

    def _autohold(self, tenant_name, project_name, job_name, ref_filter,
                  reason, count, node_hold_expiration):
        key = (tenant_name, project_name, job_name, ref_filter)
        self.log.debug("Autohold requested for %s", key)

        request = HoldRequest()
        request.tenant = tenant_name
        request.project = project_name
        request.job = job_name
        request.ref_filter = ref_filter
        request.reason = reason
        request.max_count = count

        zuul_globals = self.zuulweb.globals
        # Set node_hold_expiration to default if no value is supplied
        if node_hold_expiration is None:
            node_hold_expiration = zuul_globals.default_hold_expiration

        # Reset node_hold_expiration to max if it exceeds the max
        elif zuul_globals.max_hold_expiration and (
                node_hold_expiration == 0 or
                node_hold_expiration > zuul_globals.max_hold_expiration):
            node_hold_expiration = zuul_globals.max_hold_expiration

        request.node_expiration = node_hold_expiration

        # No need to lock it since we are creating a new one.
        self.zk_nodepool.storeHoldRequest(request)

    def _autohold_list(self, tenant_name, project_name=None):
        result = []
        for request_id in self.zk_nodepool.getHoldRequests():
            request = self.zk_nodepool.getHoldRequest(request_id)
            if not request:
                continue

            if tenant_name != request.tenant:
                continue

            if project_name is None or request.project.endswith(project_name):
                result.append({
                    'id': request.id,
                    'tenant': request.tenant,
                    'project': request.project,
                    'job': request.job,
                    'ref_filter': request.ref_filter,
                    'max_count': request.max_count,
                    'current_count': request.current_count,
                    'reason': request.reason,
                    'node_expiration': request.node_expiration,
                    'expired': request.expired,
                    'nodes': request.nodes,
                })

        return result

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['GET', 'DELETE', ])
    @cherrypy.tools.check_tenant_auth()
    def autohold_get(self, tenant_name, tenant, auth, request_id):
        request = self._getAutoholdRequest(tenant_name, request_id)
        return {
            'id': request.id,
            'tenant': request.tenant,
            'project': request.project,
            'job': request.job,
            'ref_filter': request.ref_filter,
            'max_count': request.max_count,
            'current_count': request.current_count,
            'reason': request.reason,
            'node_expiration': request.node_expiration,
            'expired': request.expired,
            'nodes': request.nodes,
        }

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    # Options handled by get method
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='autohold')
    def autohold_delete(self, tenant_name, tenant, auth, request_id):
        request = self._getAutoholdRequest(tenant_name, request_id)
        self.log.info(f'User {auth.uid} requesting autohold-delete on '
                      f'{request.tenant}/{request.project}')

        # User is authorized, so remove the autohold request
        # TODO Currently autohold deletions are not scoped to project, ref,
        # etc. They probably should be with an RBAC system?
        self.log.debug("Removing autohold %s", request)
        try:
            self.zk_nodepool.deleteHoldRequest(request, self.zuulweb.launcher)
        except Exception:
            self.log.exception(
                "Error removing autohold request %s:", request)

        cherrypy.response.status = 204

    def _getAutoholdRequest(self, tenant_name, request_id):
        hold_request = None
        try:
            hold_request = self.zk_nodepool.getHoldRequest(request_id)
        except Exception:
            self.log.exception("Error retrieving autohold ID %s", request_id)

        if hold_request is None:
            raise cherrypy.HTTPError(
                404, f'Hold request {request_id} not found.')

        if tenant_name != hold_request.tenant:
            # return 404 rather than 403 to avoid leaking tenant info
            raise cherrypy.HTTPError(
                404, 'Hold request {request_id} not found.')

        return hold_request

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_root_auth()
    def index(self, auth):
        return {
            'info': '/api/info',
            'connections': '/api/connections',
            'components': '/api/components',
            'authorizations': '/api/authorizations',
            'tenants': '/api/tenants',
            'tenant_info': '/api/tenant/{tenant}/info',
            'status': '/api/tenant/{tenant}/status',
            'status_change': '/api/tenant/{tenant}/status/change/{change}',
            'jobs': '/api/tenant/{tenant}/jobs',
            'job': '/api/tenant/{tenant}/job/{job_name}',
            'projects': '/api/tenant/{tenant}/projects',
            'project': '/api/tenant/{tenant}/project/{project:.*}',
            'project_freeze_pipeline': '/api/tenant/{tenant}/project/'
                                       '{project:.*}/freeze-pipeline',
            'project_freeze_jobs': '/api/tenant/{tenant}/pipeline/{pipeline}/'
                                   'project/{project:.*}/branch/{branch:.*}/'
                                   'freeze-jobs',
            'pipelines': '/api/tenant/{tenant}/pipelines',
            'semaphores': '/api/tenant/{tenant}/semaphores',
            'labels': '/api/tenant/{tenant}/labels',
            'nodes': '/api/tenant/{tenant}/nodes',
            'nodeset_requests': '/api/tenant/{tenant}/nodeset-requests',
            'key': '/api/tenant/{tenant}/key/{project:.*}.pub',
            'project_ssh_key': '/api/tenant/{tenant}/project-ssh-key/'
                               '{project:.*}.pub',
            'tenant_ssh_key': '/api/tenant/{tenant}/tenant-ssh-key/'
                               'ssh.pub',
            'console_stream': '/api/tenant/{tenant}/console-stream',
            'badge': '/api/tenant/{tenant}/badge',
            'builds': '/api/tenant/{tenant}/builds',
            'build': '/api/tenant/{tenant}/build/{uuid}',
            'buildsets': '/api/tenant/{tenant}/buildsets',
            'buildset': '/api/tenant/{tenant}/buildset/{uuid}',
            'config_errors': '/api/tenant/{tenant}/config-errors',
            'tenant_authorizations': ('/api/tenant/{tenant}'
                                      '/authorizations'),
            'tenant_status': '/api/tenant/{tenant}/tenant-status',
            'autohold': '/api/tenant/{tenant}/project/{project:.*}/autohold',
            'autohold_list': '/api/tenant/{tenant}/autohold',
            'autohold_by_request_id': ('/api/tenant/{tenant}'
                                       '/autohold/{request_id}'),
            'autohold_delete': ('/api/tenant/{tenant}'
                                '/autohold/{request_id}'),
            'enqueue': '/api/tenant/{tenant}/project/{project:.*}/enqueue',
            'dequeue': '/api/tenant/{tenant}/project/{project:.*}/dequeue',
            'promote': '/api/tenant/{tenant}/promote',
        }

    @cherrypy.expose
    @cherrypy.tools.handle_options()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    # Info endpoints never require authentication because they supply
    # authentication information.
    def info(self):
        info = self.zuulweb.info.copy()
        auth_info = info.capabilities.capabilities['auth']

        root_realm = self.zuulweb.abide.api_root.default_auth_realm
        if root_realm:
            auth_info['default_realm'] = root_realm
        read_protected = (
            (not self.zuulweb.abide.api_root.anonymous_read_access) or
            bool(self.zuulweb.abide.api_root.access_rules)
        )
        auth_info['read_protected'] = read_protected
        auth_info['auth_log_file_requests'] =\
            self.zuulweb.auth_log_file_requests
        return self._handleInfo(info.toDict())

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    # Info endpoints never require authentication because they supply
    # authentication information.
    def tenant_info(self, tenant_name):
        info = self.zuulweb.info.copy()
        auth_info = info.capabilities.capabilities['auth']
        info.tenant = tenant_name
        tenant = self.zuulweb.abide.tenants.get(tenant_name)
        info = info.toDict()
        if tenant is not None:
            # TODO: should we return 404 if tenant not found?
            if tenant.default_auth_realm is not None:
                auth_info['default_realm'] = tenant.default_auth_realm
            read_protected = (
                (not tenant.anonymous_read_access) or
                bool(tenant.access_rules)
            )
            auth_info['read_protected'] = read_protected
            # TODO: remove this after NIZ transition is complete
            auth_info['niz'] = bool(self.zuulweb.tenant_providers[tenant_name])
            auth_info['auth_log_file_requests'] =\
                self.zuulweb.auth_log_file_requests
        return self._handleInfo(info)

    def _handleInfo(self, info):
        ret = {'info': info}
        resp = cherrypy.response
        if self.static_cache_expiry:
            resp.headers['Cache-Control'] = "public, max-age=%d" % \
                self.static_cache_expiry
        resp.headers['Last-Modified'] =\
            self.zuulweb.start_time.strftime('%a, %d %b %Y %X GMT')
        # We don't wrap info methods with check_auth
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return ret

    def _isAuthorized(self, tenant, claims, permission):
        abide = self.zuulweb.abide
        # First, check for zuul.admin override
        if tenant:
            tenant_name = tenant.name
            admin_rules = tenant.admin_rules
            # These are the old simple access rules
            access_rules = tenant.access_rules
            # These are the modern rbac role based role mappings
            role_mappings = tenant.role_mappings
            anonymous_read_access = tenant.anonymous_read_access
        else:
            tenant_name = '*'
            admin_rules = []
            access_rules = abide.api_root.access_rules
            role_mappings = abide.api_root.role_mappings
            anonymous_read_access = abide.api_root.anonymous_read_access
        override = claims.get('zuul', {}).get('admin', [])
        if (override == '*' or
            (isinstance(override, list) and tenant_name in override)):
            return (True, True)

        if not tenant:
            tenant_name = '<root>'

        # Determine basic read access
        access = False
        if not access_rules and not role_mappings and anonymous_read_access:
            access = True
        elif role_mappings and anonymous_read_access:
            access = True
        if role_mappings:
            admin = False
            if access and permission is None:
                # Only requesting read access; we can short circuit
                return (access, admin)

            check_params = self._get_mapped_rbac_params()

            # With RBAC controls 'access' indicates RO access and 'admin'
            # indicates RW access to the specified permission. If the old
            # system below is removed we can rename things for clarity.
            for rule_name, roles in role_mappings.items():
                rule = abide.authz_rules.get(rule_name)
                if not rule:
                    self.log.error('Undefined rule "%s"', rule_name)
                    continue
                self.log.debug('Applying access rule "%s" from '
                               'tenant "%s" to claims %s',
                               rule_name, tenant_name, json.dumps(claims))
                matched = rule(claims, tenant)
                if matched:
                    # We have matched the user claim to an authz rule. Check
                    # if any assigned roles have sufficient permissions.
                    for role_name in roles:
                        role = abide.authz_roles.get(role_name)
                        authorized = role(permission, check_params)
                        if authorized:
                            if '__zuul_uid_claim' in claims:
                                uid = claims['__zuul_uid_claim']
                            else:
                                uid = json.dumps(claims)
                            access = access or True
                            # It is important that we only set the admin value
                            # if we set access to True due to defaults in the
                            # Role classes. We check the isAuthorized return
                            # values with an OR against access and then admin
                            # which means if we set admin to True when access
                            # is False we will have problems.
                            admin = admin or role.admin
                            if (permission is None and access) or admin:
                                # If we only need read only access and we have
                                # that or we have RW access to the request we
                                # are done.
                                self.log.info('%s authorized access on '
                                              'tenant "%s" by rule "%s '
                                              'and role %s"',
                                              uid, tenant_name,
                                              rule_name, role_name)
                                break
        else:
            # Backward compatible behavior handling. Once role_mappings are
            # defined they are the only rules that matter.
            for rule_name in access_rules:
                rule = abide.authz_rules.get(rule_name)
                if not rule:
                    self.log.error('Undefined rule "%s"', rule_name)
                    continue
                self.log.debug('Applying access rule "%s" from '
                               'tenant "%s" to claims %s',
                               rule_name, tenant_name, json.dumps(claims))
                authorized = rule(claims, tenant)
                if authorized:
                    if '__zuul_uid_claim' in claims:
                        uid = claims['__zuul_uid_claim']
                    else:
                        uid = json.dumps(claims)
                    self.log.info('%s authorized access on '
                                  'tenant "%s" by rule "%s"',
                                  uid, tenant_name, rule_name)
                    access = True
                    break

            admin = False
            for rule_name in admin_rules:
                rule = abide.authz_rules.get(rule_name)
                if not rule:
                    self.log.error('Undefined rule "%s"', rule_name)
                    continue
                self.log.debug('Applying admin rule "%s" from '
                               'tenant "%s" to claims %s',
                               rule_name, tenant_name, json.dumps(claims))
                authorized = rule(claims, tenant)
                if authorized:
                    if '__zuul_uid_claim' in claims:
                        uid = claims['__zuul_uid_claim']
                    else:
                        uid = json.dumps(claims)
                    self.log.info('%s authorized admin on '
                                  'tenant "%s" by rule "%s"',
                                  uid, tenant_name, rule_name)
                    access = admin = True
                    break
        return (access, admin)

    def _findAuthorizations(self, tenant, auth):
        # This method is a bit redundant with isAuthorized but
        # instead of strictly checking whether a specific action is authorized
        # we just want to find the list of potentially authorized actions
        # and return them here.
        if not auth:
            # We need to be authenticated to provide the list of authorizations
            raise APIError(403)

        abide = self.zuulweb.abide
        # First, check for zuul.admin override
        if tenant:
            tenant_name = tenant.name
            role_mappings = tenant.role_mappings
            anonymous_read_access = tenant.anonymous_read_access
        else:
            tenant_name = '*'
            role_mappings = abide.api_root.role_mappings
            anonymous_read_access = abide.api_root.anonymous_read_access
        scope = tenant_name
        override = auth.claims.get('zuul', {}).get('admin', [])
        permissions = {}
        if anonymous_read_access:
            permissions['read'] = True
        # This should only be set to true if not using the new rbac system
        # Any admin level perms in the rbac setup will be configured via an
        # explicit role.
        if auth.admin:
            permissions['admin'] = True
        # TODO: remove zuul.admin value once everything is only
        # checking permissions.
        if override == '*':
            return {'zuul': {'admin': auth.admin,
                             'permissions': permissions,
                             'scope': [scope]}, }
        elif isinstance(override, list) and tenant_name in override:
            return {'zuul': {'admin': auth.admin,
                             'permissions': permissions,
                             'scope': [scope, ]}, }

        # Modern RBAC based authorizations
        if role_mappings:
            if not tenant:
                scope = '<root>'
            ret = {'zuul': {'permissions': permissions,
                            'scope': [scope, ]}}
            for rule_name, roles in role_mappings.items():
                rule = abide.authz_rules.get(rule_name)
                if not rule:
                    self.log.error('Undefined rule "%s"', rule_name)
                    continue
                matched = rule(auth.claims, tenant)
                if matched:
                    for role_name in roles:
                        role = abide.authz_roles.get(role_name)
                        for perm in role.permissions.keys():
                            if perm:
                                permissions[perm] = True
            return ret
        # Backward compatibility for the old authorization rule based
        # auth system
        else:
            return {'zuul': {'admin': auth.admin,
                             'permissions': permissions,
                             'scope': [scope, ]}, }

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_root_auth(require_auth=True)
    def root_authorizations(self, auth):
        return self._findAuthorizations(None, auth)

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth(require_auth=True)
    def tenant_authorizations(self, tenant_name, tenant, auth):
        return self._findAuthorizations(tenant, auth)

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_root_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the list of tenants',
        schema=Prop('The list of tenants', [{
            'name': Prop('Tenant name', str),
        }]),
    )
    @openapi_response(404, description='Tenant not found')
    def tenants(self, auth):
        resp = cherrypy.response
        resp.headers["Cache-Control"] = f"public, max-age={self.cache_expiry}"
        tenants = sorted(self.zuulweb.abide.tenants.keys())
        return [{"name": t} for t in tenants]

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_root_auth()
    def connections(self, auth):
        ret = [s.connection.toDict()
               for s in self.zuulweb.connections.getSources()]
        return ret

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type="application/json; charset=utf-8")
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_root_auth()
    def components(self, auth):
        ret = {}
        for kind, components in self.zuulweb.component_registry.all():
            for comp in components:
                comp_json = {
                    "hostname": comp.hostname,
                    "state": comp.state,
                    "version": comp.version,
                }
                ret.setdefault(kind, []).append(comp_json)
        return ret

    def _getStatus(self, tenant):
        cache_time = self.status_cache_times.get(tenant.name, 0)
        if tenant.name not in self.status_cache_locks or \
           (time.time() - cache_time) > self.cache_expiry:
            if self.status_cache_locks[tenant.name].acquire(
                blocking=False
            ):
                try:
                    self.status_caches[tenant.name] =\
                        self.formatStatus(tenant)
                    self.status_cache_times[tenant.name] =\
                        time.time()
                finally:
                    self.status_cache_locks[tenant.name].release()
            if not self.status_caches.get(tenant.name):
                # If the cache is empty at this point it means that we didn't
                # get the lock but another thread is initializing the cache
                # for the first time. In this case we just wait for the lock
                # to wait for it to finish.
                with self.status_cache_locks[tenant.name]:
                    pass
        payload = self.status_caches[tenant.name]
        resp = cherrypy.response
        resp.headers["Cache-Control"] = f"public, max-age={self.cache_expiry}"
        last_modified = datetime.utcfromtimestamp(
            self.status_cache_times[tenant.name]
        )
        last_modified_header = last_modified.strftime('%a, %d %b %Y %X GMT')
        resp.headers["Last-Modified"] = last_modified_header
        resp.headers['Content-Type'] = 'application/json; charset=utf-8'
        return payload

    def formatStatus(self, tenant):
        data = {}
        data['zuul_version'] = self.zuulweb.component_info.version

        data['trigger_event_queue'] = {}
        data['trigger_event_queue']['length'] = len(
            self.zuulweb.trigger_events[tenant.name])

        data['state'] =\
            self.zuulweb.event_watcher.tenant_state[tenant.name].toDict()

        data['management_event_queue'] = {}
        data['management_event_queue']['length'] = len(
            self.zuulweb.management_events[tenant.name]
        )
        data['connection_event_queues'] = {}
        for connection in self.zuulweb.connections.connections.values():
            queue = connection.getEventQueue()
            if queue is not None:
                data['connection_event_queues'][connection.connection_name] = {
                    'length': len(queue),
                }

        layout_state = self.zuulweb.tenant_layout_state[tenant.name]
        data['last_reconfigured'] = layout_state.last_reconfigured * 1000

        pipelines = []
        data['pipelines'] = pipelines

        trigger_event_queues = self.zuulweb.pipeline_trigger_events[
            tenant.name]
        result_event_queues = self.zuulweb.pipeline_result_events[tenant.name]
        management_event_queues = self.zuulweb.pipeline_management_events[
            tenant.name]
        with self.zuulweb.zk_context as ctx:
            for manager in tenant.layout.pipeline_managers.values():
                status = manager.summary.refresh(ctx)
                status['trigger_events'] = len(
                    trigger_event_queues[manager.pipeline.name])
                status['result_events'] = len(
                    result_event_queues[manager.pipeline.name])
                status['management_events'] = len(
                    management_event_queues[manager.pipeline.name])
                pipelines.append(status)
        return data, json.dumps(data).encode('utf-8')

    def _getTenantOrRaise(self, tenant_name):
        tenant = self.zuulweb.abide.tenants.get(tenant_name)
        if tenant:
            return tenant
        if tenant_name not in self.zuulweb.unparsed_abide.tenants:
            raise cherrypy.HTTPError(404, "Unknown tenant")
        self.log.warning("Tenant %s isn't loaded", tenant_name)
        raise cherrypy.HTTPError(422, f"Tenant {tenant_name} isn't ready")

    def _getProjectOrRaise(self, tenant, project_name):
        _, project = tenant.getProject(project_name)
        if not project:
            raise cherrypy.HTTPError(404, "Unknown project")
        return project

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def status(self, tenant_name, tenant, auth):
        """Return the tenant status.

        Note: the output format is not currently documented and
        subject to change without notice.

        """
        return self._getStatus(tenant)[1]

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def status_change(self, tenant_name, tenant, auth, change):
        """Return the status for a single change.

        Note: the output format is not currently documented and
        subject to change without notice.

        """
        payload = self._getStatus(tenant)[0]
        result_filter = RefFilter(change)
        return result_filter.filterPayload(payload)

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(
        content_type='application/json; charset=utf-8', handler=json_handler,
    )
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the list of jobs',
        schema=Prop('The list of jobs', [{
            'name': str,
            'description': str,
            'tags': [str],
            'variants': [{
                'parent': str,
                'branches': [str],
            }],
        }]),
    )
    @openapi_response(404, description='Tenant not found')
    def jobs(self, tenant_name, tenant, auth):
        result = []
        for job_name in sorted(tenant.layout.jobs):
            desc = None
            tags = set()
            variants = []
            for variant in tenant.layout.jobs[job_name]:
                if not desc and variant.description:
                    desc = variant.description.split('\n')[0]
                if variant.tags:
                    tags.update(list(variant.tags))
                job_variant = {}
                if not variant.isBase():
                    if variant.parent:
                        job_variant['parent'] = str(variant.parent)
                    else:
                        job_variant['parent'] = tenant.default_base_job
                branches = variant.getBranches(tenant)
                if branches:
                    job_variant['branches'] = branches
                if job_variant:
                    variants.append(job_variant)

            job_output = {"name": job_name}
            if desc:
                job_output["description"] = desc
            if variants:
                job_output["variants"] = variants
            if tags:
                job_output["tags"] = list(tags)
            result.append(job_output)

        return result

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def config_errors(self, tenant_name, tenant, auth,
                      project=None, branch=None, severity=None, name=None,
                      limit=50, skip=0):
        skip = int(skip)
        limit = int(limit)
        count = 0
        ret = []
        for e in tenant.layout.loading_errors.errors:
            if not (
                (project is None or e.key.context.project_name == project) and
                (branch is None or e.key.context.branch == branch) and
                (severity is None or e.severity == severity) and
                (name is None or e.name == name)):
                continue
            count += 1
            if count <= skip:
                continue
            ret.append({
                'source_context': e.key.context.toDict(),
                'error': e.error,
                'short_error': e.short_error,
                'severity': e.severity,
                'name': e.name,
            })
            if len(ret) >= limit:
                break
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the list of system events',
        schema=Prop('The list of system events',
                    [SystemEventConverter.schema()]),
    )
    @openapi_response(404, 'Tenant not found')
    def system_events(self, tenant_name, tenant, auth, event_type=None,
                      idx_min=None, idx_max=None,
                      limit=50, skip=0):
        connection = self._get_connection()

        if tenant_name not in self.zuulweb.abide.tenants.keys():
            raise cherrypy.HTTPError(
                404,
                f'Tenant {tenant_name} does not exist.')

        try:
            _idx_max = idx_max is not None and int(idx_max) or idx_max
            _idx_min = idx_min is not None and int(idx_min) or idx_min
        except ValueError:
            raise cherrypy.HTTPError(400, 'idx_min, idx_max must be integers')

        events = connection.getSystemEvents(
            tenant=tenant_name, event_type=event_type,
            limit=limit, offset=skip, idx_min=_idx_min,
            idx_max=_idx_max,
            query_timeout=self.query_timeout)

        return [SystemEventConverter.toDict(e) for e in events]

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def tenant_status(self, tenant_name, tenant, auth):
        ret = {
            'config_error_count': len(tenant.layout.loading_errors.errors),
        }
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(
        content_type='application/json; charset=utf-8', handler=json_handler)
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def job(self, tenant_name, tenant, auth, job_name):
        job_name = urllib.parse.unquote_plus(job_name)
        job_variants = tenant.layout.jobs.get(job_name)
        if job_variants is None:
            raise cherrypy.HTTPError(404, "Job not found")
        result = []
        for job in job_variants:
            result.append(job.toDict(tenant))

        return result

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def projects(self, tenant_name, tenant, auth):
        result = []
        for tpc in tenant.all_tpcs:
            project = tpc.project
            pobj = project.toDict()
            pobj['type'] = "config" if tpc.trusted else "untrusted"
            result.append(pobj)
        return sorted(result, key=lambda project: project["name"])

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(
        content_type='application/json; charset=utf-8', handler=json_handler)
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def project(self, tenant_name, tenant, auth, project_name):
        project = self._getProjectOrRaise(tenant, project_name)

        result = project.toDict()
        result['configs'] = []
        result['metadata'] = {}
        md = tenant.layout.getProjectMetadata(project.canonical_name)
        if md is None:
            # No actual configuration of the project in this tenant
            return result
        md = md.toDict()
        md['merge_mode'] = model.get_merge_mode_name(md['merge_mode'])
        result['metadata'] = md
        configs = tenant.layout.getAllProjectConfigs(project.canonical_name)
        for config_obj in configs:
            config = config_obj.toDict()
            config['pipelines'] = []
            for pipeline_name, pipeline_config in sorted(
                    config_obj.pipelines.items()):
                pipeline = pipeline_config.toDict()
                pipeline['name'] = pipeline_name
                pipeline['jobs'] = []
                for jobs in pipeline_config.job_list.jobs.values():
                    job_list = []
                    for job in jobs:
                        job_list.append(job.toDict(tenant))
                    pipeline['jobs'].append(job_list)
                config['pipelines'].append(pipeline)
            result['configs'].append(config)

        return result

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the list of providers',
        schema=Prop('The list of providers', [ProviderConverter.schema()]),
    )
    @openapi_response(404, 'Tenant not found')
    def providers(self, tenant_name, tenant, auth):
        providers = self.zuulweb.tenant_providers[tenant_name]
        ret = []
        ibr = self.zuulweb.image_build_registry
        iur = self.zuulweb.image_upload_registry
        for provider in providers:
            build_artifacts = []
            uploads = []
            for image in provider.images.values():
                if image.type == 'zuul':
                    uploads.extend([
                        u for u in iur.getUploadsForImage(image.canonical_name)
                        if u.isPermittedForProvider(image, provider)
                    ])
                    artifact_uuids = set([u.artifact_uuid for u in uploads])
                    build_artifacts.extend([
                        b for b in ibr.getArtifactsForImage(
                            image.canonical_name)
                        if b.uuid in artifact_uuids
                    ])
            ret.append(ProviderConverter.toDict(
                tenant, provider, build_artifacts, uploads))
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def pipelines(self, tenant_name, tenant, auth):
        ret = []
        for pipeline_name, manager in tenant.layout.pipeline_managers.items():
            triggers = []
            for trigger in manager.pipeline.triggers:
                if isinstance(trigger.connection, BaseConnection):
                    name = trigger.connection.connection_name
                else:
                    # Trigger not based on a connection doesn't use this attr
                    name = trigger.name
                triggers.append({
                    "name": name,
                    "driver": trigger.driver.name,
                })
            ret.append({"name": pipeline_name, "triggers": triggers})

        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the list of images',
        schema=Prop('The list of images', [ImageConverter.schema()]),
    )
    @openapi_response(404, 'Tenant not found')
    def images(self, tenant_name, tenant, auth):
        ret = []
        ibr = self.zuulweb.image_build_registry
        iur = self.zuulweb.image_upload_registry
        providers = self.zuulweb.tenant_providers[tenant_name]

        permitted_upload_uuids = set()
        for provider in providers:
            for image in provider.images.values():
                if image.type == 'zuul':
                    for upload in iur.getUploadsForImage(image.canonical_name):
                        if upload.isPermittedForProvider(image, provider):
                            permitted_upload_uuids.add(upload.uuid)

        for image in tenant.layout.images.values():
            if image.type == 'zuul':
                # Include uploads used by providers in the tenant
                uploads = [
                    u for u in iur.getUploadsForImage(image.canonical_name)
                    if u.uuid in permitted_upload_uuids
                ]
                artifact_uuids = set(u.artifact_uuid for u in uploads)
                # Include build artifacts used by relevant uploads
                build_artifacts = [
                    b for b in ibr.getArtifactsForImage(image.canonical_name)
                    if b.uuid in artifact_uuids
                ]
            else:
                build_artifacts = []
                uploads = []
            ret.append(ImageConverter.toDict(tenant, image,
                                             build_artifacts, uploads))
        return ret

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['POST'])
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='build-image')
    def image_build(self, tenant_name, tenant, auth, image_name):
        providers = self.zuulweb.tenant_providers.get(tenant.name)
        if not providers:
            raise cherrypy.HTTPError(404, "Image not found in tenant")

        image = None
        for provider in providers:
            image = provider.images.get(image_name)
            if image:
                break
        if not image or image.type != "zuul":
            raise cherrypy.HTTPError(404, "Image not found in tenant")

        project_hostname, project_name = \
            image.project_canonical_name.split('/', 1)
        driver = self.zuulweb.connections.drivers['zuul']
        event = driver.getImageBuildEvent(
            [image.name], project_hostname, project_name, image.branch)

        self.log.info('User %s requesting image-build for %s %s',
                      auth.uid, tenant.name, image.name)
        self.zuulweb.trigger_events[tenant.name].put(
            event.trigger_name, event)

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['DELETE'])
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='delete-image-build-artifact')
    def image_build_artifact_delete(self, tenant_name, tenant, auth,
                                    artifact_id):
        iba = self.zuulweb.image_build_registry.getItem(artifact_id)
        iur = self.zuulweb.image_upload_registry

        if not iba or tenant.name != iba.build_tenant_name:
            raise cherrypy.HTTPError(
                404, "Image build artifact not found in tenant")

        self.log.info(f'User {auth.uid} requesting '
                      'image-build-artifact-delete on '
                      f'{artifact_id}')

        # We just let the LockException propagate up if we can't lock
        # it.
        with self.zuulweb.createZKContext(None, self.log) as ctx:
            with iba.locked(ctx, blocking=False):
                with iba.activeContext(ctx):
                    iba.state = model.STATE_DELETING

            for upload in iur.getUploadsForImage(iba.canonical_name):
                # This is best effort to change all the uploads
                try:
                    with upload.locked(ctx, blocking=False):
                        with upload.activeContext(ctx):
                            upload.state = model.STATE_DELETING
                except LockException:
                    self.log.info("Unable to lock %s for deletion", upload)

        cherrypy.response.status = 204

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['DELETE'])
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='delete-image-upload')
    def image_upload_delete(self, tenant_name, tenant, auth, upload_id):
        upload = self.zuulweb.image_upload_registry.getItem(upload_id)
        if upload:
            iba = self.zuulweb.image_build_registry.getItem(
                upload.artifact_uuid)
        else:
            iba = None

        if not iba or tenant.name != iba.build_tenant_name:
            raise cherrypy.HTTPError(
                404, "Image upload not found in tenant")

        self.log.info(f'User {auth.uid} requesting image-upload-delete on '
                      f'{upload_id}')

        # We just let the LockException propagate up if we can't lock
        # it.
        with self.zuulweb.createZKContext(None, self.log) as ctx:
            with upload.locked(ctx, blocking=False):
                with upload.activeContext(ctx):
                    upload.state = model.STATE_DELETING
        cherrypy.response.status = 204

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['POST'])
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='validate-image-upload')
    def image_upload_validate(self, tenant_name, tenant, auth, upload_id):
        upload = self.zuulweb.image_upload_registry.getItem(upload_id)
        if upload:
            iba = self.zuulweb.image_build_registry.getItem(
                upload.artifact_uuid)
        else:
            iba = None

        if not iba or tenant.name != iba.build_tenant_name:
            raise cherrypy.HTTPError(
                404, "Image upload not found in tenant")

        self.log.info(f'User {auth.uid} requesting image-upload-validate on '
                      f'{upload_id}')

        project_hostname, project_name = \
            iba.project_canonical_name.split('/', 1)
        driver = self.zuulweb.connections.drivers['zuul']
        event = driver.getImageValidateEvent(
            [iba.name], project_hostname, project_name, iba.project_branch,
            upload.uuid)
        self.zuulweb.trigger_events[tenant_name].put(
            event.trigger_name, event)

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['POST'])
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='upload-image')
    def image_upload_upload(self, tenant_name, tenant, auth, upload_id):
        upload = self.zuulweb.image_upload_registry.getItem(upload_id)
        if upload:
            iba = self.zuulweb.image_build_registry.getItem(
                upload.artifact_uuid)
        else:
            iba = None

        if not iba or tenant.name != iba.build_tenant_name:
            raise cherrypy.HTTPError(
                404, "Image upload not found in tenant")

        with self.zuulweb.createZKContext(None, self.log) as ctx:
            # We just let the LockException propagate up if we can't lock
            # it.
            with upload.locked(ctx, blocking=False):
                upload.refresh(ctx)
                if upload.state != upload.State.FAILED:
                    self.log.info(
                        'User %s denied image re-upload on %s',
                        auth.uid, upload)
                    raise cherrypy.HTTPError(400, 'Invalid upload state')
                self.log.info(
                    'User %s requesting image re-upload on %s',
                    auth.uid, upload)
                with upload.activeContext(ctx):
                    new_upload = upload.copy(ctx, attempt=0)
                    self.log.debug("Replacing upload %s with %s",
                                   upload, new_upload)
                    self.log.debug("Marking upload %s deleting", upload)
                    upload.state = upload.State.DELETING

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the list of flavors',
        schema=Prop('The list of flavors', [FlavorConverter.schema()]),
    )
    @openapi_response(404, 'Tenant not found')
    def flavors(self, tenant_name, tenant, auth):
        ret = []
        for flavor in tenant.layout.flavors.values():
            ret.append(FlavorConverter.toDict(tenant, flavor))
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def labels(self, tenant_name, tenant, auth):
        allowed_labels = tenant.allowed_labels or []
        disallowed_labels = tenant.disallowed_labels or []
        labels = set()
        for launcher in self.zk_nodepool.getRegisteredLaunchers():
            labels.update(filter_allowed_disallowed(
                launcher.supported_labels,
                allowed_labels, disallowed_labels))
        ret = [{'name': label} for label in sorted(labels)]
        for label in tenant.layout.labels.values():
            ret.append(LabelConverter.toDict(tenant, label))
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def nodes(self, tenant_name, tenant, auth):
        ret = []
        for node_id in self.zk_nodepool.getNodes(cached=True):
            node = self.zk_nodepool.getNode(node_id)
            # This returns all nodes; some of which may not be
            # intended for use by Zuul, so be extra careful checking
            # user_data.
            if not (node.user_data and
                    isinstance(node.user_data, dict) and
                    node.user_data.get('zuul_system') ==
                    self.zuulweb.system.system_id and
                    node.tenant_name == tenant_name):
                continue
            node_data = {}
            for key in ("id", "type", "connection_type", "external_id",
                        "provider", "state", "state_time", "comment"):
                node_data[key] = getattr(node, key, None)
            ret.append(node_data)

        providers = self.zuulweb.tenant_providers.get(tenant.name)
        if providers:
            provider_cnames = [p.canonical_name for p in providers]
            for node in sorted(self.zuulweb.nodes_cache.getItems()):
                if node.provider in provider_cnames:
                    if (node.tenant_name == tenant.name or
                        node.tenant_name is None):
                        ret.append(ProviderNodeConverter.toDict(node))
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['PUT', ])
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='modify-node')
    def nodes_put(self, tenant_name, tenant, auth, node_id):
        node = self.zuulweb.nodes_cache.getItem(node_id)

        if not node:
            raise cherrypy.HTTPError(404, "Node not found")

        if node.tenant_name is None:
            # In this case, we need to see if the node is attached to
            # one of the tenant providers
            providers = self.zuulweb.tenant_providers.get(tenant.name)
            provider_cnames = [p.canonical_name for p in providers]
            if node.provider not in provider_cnames:
                raise cherrypy.HTTPError(404, "Node not found")
        elif node.tenant_name != tenant.name:
            raise cherrypy.HTTPError(404, "Node not found")

        body = cherrypy.request.json
        state = body.get('state')
        if state not in {
                node.State.HOLD,
                node.State.USED,
                node.State.OUTDATED,
        }:
            raise cherrypy.HTTPError(400, 'Invalid request body')

        # Make a request to the launcher to change the node state at
        # its earliest convenience.  No locking is necessary.
        self.log.info('User %s setting node %s to %s',
                      auth.uid, node_id, state)
        nodes = self.zuulweb.launcher.setNextNodeState(
            node, state,
            cache=self.zuulweb.nodes_cache)

        # If this is a slot host, the actual nodes changed will be
        # returned.
        for n in nodes:
            self.log.info('User %s set node %s to %s',
                          auth.uid, n.uuid, state)
        cherrypy.response.status = 201

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def nodeset_requests(self, tenant_name, tenant, auth):
        ret = []
        # TODO: remove this providers check once nodepool is gone
        providers = self.zuulweb.tenant_providers.get(tenant.name)
        if providers:
            for request in self.zuulweb.requests_cache.getItems():
                if request.tenant_name == tenant.name:
                    ret.append(NodesetRequestConverter.toDict(request))
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options(allowed_methods=['DELETE', ])
    @cherrypy.tools.check_tenant_auth(require_admin=True,
                                      permission='modify-nodeset-request')
    def nodeset_requests_delete(self, tenant_name, tenant, auth, request_id):
        request = self.zuulweb.requests_cache.getItem(request_id)
        if not request or request.tenant_name != tenant.name:
            raise cherrypy.HTTPError(404, "Request not found")

        # We just let the LockException propagate up if we can't lock
        # it.
        with self.zuulweb.createZKContext(None, self.log) as ctx:
            with request.locked(ctx, blocking=False):
                self.log.info(f'User {auth.uid} deleting nodeset request '
                              f'{request_id}')
                with request.activeContext(ctx):
                    request.delete(ctx)
        cherrypy.response.status = 204

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='text/plain',
        description=('Returns the project public key that is used '
                     'to encrypt secrets'),
        example=('-----BEGIN PUBLIC KEY-----\n'
                 'MIICI...\n'
                 '-----END PUBLIC KEY-----\n'),
        schema=Prop('The project secrets public key '
                    'in PKCS8 format', str)
    )
    @openapi_response(404, 'Tenant or Project not found')
    def key(self, tenant_name, tenant, auth, project_name):
        project = self._getProjectOrRaise(tenant, project_name)

        key = encryption.serialize_rsa_public_key(project.public_secrets_key)
        resp = cherrypy.response
        resp.headers['Content-Type'] = 'text/plain'
        return key

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='text/plain',
        description=('Returns the project public key that executor '
                     'adds to SSH agent'),
        example='ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACA',
        schema=Prop('The project secrets public key '
                    'in SSH2 format', str),
    )
    @openapi_response(404, 'Tenant or Project not found')
    def project_ssh_key(self, tenant_name, tenant, auth, project_name):
        project = self._getProjectOrRaise(tenant, project_name)

        key = f"{project.public_ssh_key}\n"
        resp = cherrypy.response
        resp.headers['Content-Type'] = 'text/plain'
        return key

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='text/plain',
        description=('Returns the tenant public key that executor '
                     'adds to SSH agent'),
        example='ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACA',
        schema=Prop('The tenant public key '
                    'in SSH2 format', str),
    )
    @openapi_response(404, 'Tenant or Project not found')
    def tenant_ssh_key(self, tenant_name, tenant, auth):
        key = f"{tenant.public_ssh_key}\n"
        resp = cherrypy.response
        resp.headers['Content-Type'] = 'text/plain'
        return key

    def _get_connection(self):
        return self.zuulweb.connections.connections['database']

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the list of builds',
        schema=Prop('The list of builds', [BuildConverter.schema()]),
    )
    @openapi_response(404, 'Tenant not found')
    def builds(self, tenant_name, tenant, auth, project=None,
               pipeline=None, change=None, branch=None, patchset=None,
               ref=None, newrev=None, uuid=None, job_name=None,
               queue_item_uuid=None, voting=None, nodeset=None, result=None,
               final=None, held=None, complete=None, limit=50, skip=0,
               idx_min=None, idx_max=None, exclude_result=None):
        """
        List the executed builds
        """

        connection = self._get_connection()

        if tenant_name not in self.zuulweb.abide.tenants.keys():
            raise cherrypy.HTTPError(
                404,
                f'Tenant {tenant_name} does not exist.')

        # If final is None, we return all builds, both final and non-final
        if final is not None:
            final = final.lower() == "true"

        if complete is not None:
            complete = complete.lower() == 'true'

        try:
            _idx_max = idx_max is not None and int(idx_max) or idx_max
            _idx_min = idx_min is not None and int(idx_min) or idx_min
        except ValueError:
            raise cherrypy.HTTPError(400, 'idx_min, idx_max must be integers')

        builds = connection.getBuilds(
            tenant=tenant_name, project=project, pipeline=pipeline,
            change=change, branch=branch, patchset=patchset, ref=ref,
            newrev=newrev, uuid=uuid, job_name=job_name,
            queue_item_uuid=queue_item_uuid, voting=voting,
            nodeset=nodeset, result=result, final=final, held=held,
            complete=complete, limit=limit, offset=skip, idx_min=_idx_min,
            idx_max=_idx_max, exclude_result=exclude_result,
            query_timeout=self.query_timeout)

        return [BuildConverter.toDict(b, b.buildset, skip_refs=True)
                for b in builds]

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def build(self, tenant_name, tenant, auth, uuid):
        connection = self._get_connection()

        data = connection.getBuild(tenant_name, uuid)
        if not data:
            raise cherrypy.HTTPError(404, "Build not found")
        data = BuildConverter.toDict(data, data.buildset)
        return data

    def buildTimeToDict(self, build):
        start_time = _datetimeToString(build.start_time)
        end_time = _datetimeToString(build.end_time)
        if build.start_time and build.end_time:
            duration = (build.end_time -
                        build.start_time).total_seconds()
        else:
            duration = None

        ret = {
            '_id': build.id,
            'uuid': build.uuid,
            'job_name': build.job_name,
            'result': build.result,
            'start_time': start_time,
            'end_time': end_time,
            'duration': duration,
            'final': build.final,
            'project': build.ref.project,
            'branch': build.ref.branch,
            'pipeline': build.buildset.pipeline,
            'ref': build.ref.ref,
        }
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def build_times(self, tenant_name, tenant, auth, project=None,
                    pipeline=None, branch=None,
                    ref=None, job_name=None,
                    final=None,
                    start_time=None, end_time=None,
                    limit=50, skip=0,
                    exclude_result=None):
        connection = self._get_connection()

        if tenant_name not in self.zuulweb.abide.tenants.keys():
            raise cherrypy.HTTPError(
                404,
                f'Tenant {tenant_name} does not exist.')

        # If final is None, we return all builds, both final and non-final
        if final is not None:
            final = final.lower() == "true"

        build_times = connection.getBuildTimes(
            tenant=tenant_name, project=project, pipeline=pipeline,
            branch=branch, ref=ref,
            job_name=job_name,
            final=final,
            limit=limit, offset=skip,
            exclude_result=exclude_result,
            start_time=start_time,
            end_time=end_time,
            query_timeout=self.query_timeout)

        return [self.buildTimeToDict(b) for b in build_times]

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='image/svg+xml',
        description=('Badge describing the result of '
                     'the latest buildset found.'),
        schema=Prop('SVG image', object),
    )
    @openapi_response(404, 'No buildset found')
    def badge(self, tenant_name, tenant, auth, project=None,
              pipeline=None, branch=None):
        """
        Get a badge describing the result of the latest buildset found.

        :param str tenant_name: The tenant name
        :param Tenant tenant: The tenant object
        :param AuthInfo auth: The auth object
        :param str project: A project name
        :param str pipeline: A pipeline name
        :param str branch: A branch name
        """

        connection = self._get_connection()

        buildsets = connection.getBuildsets(
            tenant=tenant_name, project=project, pipeline=pipeline,
            branch=branch, complete=True, limit=1,
            query_timeout=self.query_timeout)
        if not buildsets:
            raise cherrypy.HTTPError(404, 'No buildset found')

        if buildsets[0].result == 'SUCCESS':
            file = 'passing.svg'
        else:
            file = 'failing.svg'
        path = os.path.join(self.zuulweb.static_path, file)

        # Ensure the badge are not cached
        cherrypy.response.headers['Cache-Control'] = "no-cache"

        return cherrypy.lib.static.serve_file(
            path=path, content_type="image/svg+xml")

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the list of buildsets',
        schema=Prop('The list of buildsets', [BuildsetConverter.schema()]),
    )
    @openapi_response(404, 'Tenant not found')
    def buildsets(self, tenant_name, tenant, auth, project=None,
                  pipeline=None, change=None, branch=None,
                  patchset=None, ref=None, newrev=None, uuid=None,
                  queue_item_uuid=None, result=None,
                  complete=None, limit=50, skip=0,
                  idx_min=None, idx_max=None, exclude_result=None):
        connection = self._get_connection()

        if complete:
            complete = complete.lower() == 'true'

        try:
            _idx_max = idx_max is not None and int(idx_max) or idx_max
            _idx_min = idx_min is not None and int(idx_min) or idx_min
        except ValueError:
            raise cherrypy.HTTPError(400, 'idx_min, idx_max must be integers')

        buildsets = connection.getBuildsets(
            tenant=tenant_name, project=project, pipeline=pipeline,
            change=change, branch=branch, patchset=patchset, ref=ref,
            newrev=newrev, uuid=uuid, queue_item_uuid=queue_item_uuid,
            result=result, complete=complete,
            limit=limit, offset=skip, idx_min=_idx_min, idx_max=_idx_max,
            exclude_result=exclude_result, query_timeout=self.query_timeout)

        return [BuildsetConverter.toDict(b) for b in buildsets]

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns a buildset',
        schema=BuildsetConverter.schema(builds=True, events=True),
    )
    @openapi_response(404, 'Tenant not found')
    def buildset(self, tenant_name, tenant, auth, uuid):
        connection = self._get_connection()

        data = connection.getBuildset(tenant_name, uuid)
        if not data:
            raise cherrypy.HTTPError(404, "Buildset not found")
        data = BuildsetConverter.toDict(data, data.builds,
                                        data.buildset_events)
        return data

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(
        content_type='application/json; charset=utf-8', handler=json_handler,
    )
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        description='Returns the list of semaphores',
        schema=Prop('The list of semaphores', [{
            'name': str,
            'global': bool,
            'max': int,
            'holders': {
                'count': int,
                'other_tenants': int,
                'this_tenant': [{
                    'buildset_uuid': str,
                    'job_name': str,
                }],
            },
        }]),
    )
    @openapi_response(404, 'Tenant not found')
    def semaphores(self, tenant_name, tenant, auth):
        result = []
        names = set(tenant.layout.semaphores.keys())
        names = names.union(tenant.global_semaphores)
        for semaphore_name in sorted(names):
            semaphore = tenant.layout.getSemaphore(
                self.zuulweb.abide, semaphore_name)
            holders = tenant.semaphore_handler.semaphoreHolders(semaphore_name)
            this_tenant = []
            other_tenants = 0
            for holder in holders:
                (holder_tenant, holder_pipeline,
                 holder_item_uuid, holder_buildset_uuid
                 ) = BuildSet.parsePath(holder['buildset_path'])
                if holder_tenant != tenant_name:
                    other_tenants += 1
                    continue
                this_tenant.append({'buildset_uuid': holder_buildset_uuid,
                                    'job_name': holder['job_name']})
            sem_out = {'name': semaphore.name,
                       'global': semaphore.global_scope,
                       'max': semaphore.max,
                       'holders': {
                           'count': len(this_tenant) + other_tenants,
                           'this_tenant': this_tenant,
                           'other_tenants': other_tenants},
                       }
            result.append(sem_out)
        return result

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.handle_options()
    # We don't check auth here since we would never fall through to it
    def console_stream_options(self, tenant_name):
        pass

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.websocket(handler_cls=LogStreamHandler)
    # Options handling in _options method
    # The Authorization header is not included when upgrading to
    # websocket, so the websocket handler itself will validate auth
    # using the websocket protocol.
    def console_stream_get(self, tenant_name):
        pass

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def project_freeze_jobs(self, tenant_name, tenant, auth,
                            pipeline_name, project_name, branch_name):
        item, change = self._freeze_jobs(
            tenant, pipeline_name, project_name, branch_name)

        output = []
        for job in item.current_build_set.job_graph.getJobs():
            output.append({
                'name': job.name,
                'dependencies':
                    list(map(lambda x: x.toDict(), job.dependencies)),
            })

        ret = output
        return ret

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    def project_freeze_job(self, tenant_name, tenant, auth,
                           pipeline_name, project_name, branch_name,
                           job_name):
        # TODO(jhesketh): Allow a canonical change/item to be passed in which
        # would return the job with any in-change modifications.
        item, change = self._freeze_jobs(
            tenant, pipeline_name, project_name, branch_name)
        job = item.current_build_set.job_graph.getJob(
            job_name, change.cache_key)
        if not job:
            raise cherrypy.HTTPError(404)

        uuid = "0" * 32
        params = zuul.executor.common.construct_build_params(
            uuid, self.zuulweb.connections, job, item, item.manager.pipeline)
        params['zuul'].update(zuul.executor.common.zuul_params_from_job(job))
        del params['job_ref']
        del params['parent_data']
        del params['secret_parent_data']
        params['job'] = job.name
        params['zuul']['buildset'] = None
        params['pre_timeout'] = job.pre_timeout
        params['timeout'] = job.timeout
        params['post_timeout'] = job.post_timeout
        params['override_branch'] = job.override_branch
        params['override_checkout'] = job.override_checkout
        params['ansible_version'] = job.ansible_version
        params['ansible_split_streams'] = job.ansible_split_streams
        params['workspace_scheme'] = job.workspace_scheme
        params['workspace_checkout'] = job.workspace_checkout
        if job.name != 'noop':
            params['playbooks'] = job.run
            params['pre_playbooks'] = job.pre_run
            params['post_playbooks'] = job.post_run
            params['cleanup_playbooks'] = job.cleanup_run
        params["nodeset"] = job.nodeset.toDict()
        params['vars'] = job.variables
        params['extra_vars'] = job.extra_variables
        params['host_vars'] = job.host_variables
        params['group_vars'] = job.group_variables
        params['secret_vars'] = job.secret_parent_data
        params['failure_output'] = job.failure_output

        ret = params
        return ret

    def _freeze_jobs(self, tenant, pipeline_name, project_name,
                     branch_name):

        project = self._getProjectOrRaise(tenant, project_name)
        manager = tenant.layout.pipeline_managers.get(pipeline_name)
        if not manager:
            raise cherrypy.HTTPError(404, 'Unknown pipeline')

        change = Branch(project)
        change.branch = branch_name or "master"
        change.cache_stat = FakeCacheKey()
        with LocalZKContext(self.log) as context:
            queue = ChangeQueue.new(context, manager=manager)
            item = QueueItem.new(context, queue=queue, changes=[change])
            item.freezeJobGraph(tenant.layout, context,
                                skip_file_matcher=True,
                                redact_secrets_and_keys=True)

        return item, change

    @cherrypy.expose
    @cherrypy.tools.save_params()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @cherrypy.tools.handle_options()
    @cherrypy.tools.check_tenant_auth()
    @openapi_response(
        code=200,
        content_type='application/json',
        description='Returns the project pipeline config',
        schema=ProjectPipelineConverter.schema(),
    )
    def project_freeze_pipeline(self, tenant_name, tenant, auth,
                                project_name):
        body = cherrypy.request.json
        if 'ref' not in body:
            raise cherrypy.HTTPError(400, 'ref is required')
        if 'pipeline' not in body:
            raise cherrypy.HTTPError(400, 'pipeline is required')

        project = self._getProjectOrRaise(tenant, project_name)
        manager = tenant.layout.pipeline_managers.get(body['pipeline'])
        if not manager:
            raise cherrypy.HTTPError(404, 'Unknown pipeline')

        # refs/heads/...
        ref = body['ref'].split('/', 2)
        if ref[1] == 'heads':
            change = Branch(project)
            change.branch = ref[2]
        elif ref[1] == 'tags':
            change = Tag(project)
            change.tag = ref[2]
        else:
            change = Ref(project)
            change.ref = body['ref']

        change.cache_stat = FakeCacheKey()
        with LocalZKContext(self.log) as context:
            queue = ChangeQueue.new(context, manager=manager)
            item = QueueItem.new(context, queue=queue, changes=[change])
            ppc = tenant.layout.getProjectPipelineConfig(item, change)
        return ProjectPipelineConverter.toDict(ppc)


class StaticHandler(object):
    def __init__(self, root):
        self.root = pathlib.Path(root).resolve()

    def default(self, path, **kwargs):
        # We don't expect path to be provided in the query string,
        # this is only supposed to handle static urls
        if cherrypy.request.query_string:
            params = cherrypy.lib.httputil.parse_query_string(
                cherrypy.request.query_string)
            if params and 'path' in params:
                raise cherrypy.HTTPError(403)
        # Make sure the resulting path is under our root
        plain_path = self.root / pathlib.Path(path)
        if not plain_path.is_relative_to(self.root):
            raise cherrypy.HTTPError(403)
        # Follow symlinks and double check
        plain_path = plain_path.resolve()
        if not plain_path.is_relative_to(self.root):
            raise cherrypy.HTTPError(403)
        # Construct a hypothetical index.html path under the resulting
        # path to use as a fallback in case the path provided is a
        # directory.
        index_path = (plain_path / "index.html").resolve()
        # The ultimate fallback is the root index; start with that.
        new_path = (self.root / "index.html").resolve()
        # If the directory index exists, use that.
        if os.path.exists(index_path):
            new_path = index_path
        # If the supplied path exists, use that.
        elif os.path.exists(plain_path):
            new_path = plain_path
        return cherrypy.lib.static.serve_file(path=str(new_path))


class StreamManager(object):
    log = logging.getLogger("zuul.web")

    def __init__(self, statsd, metrics):
        self.thread = None
        self.statsd = statsd
        self.metrics = metrics
        self.hostname = normalize_statsd_name(socket.getfqdn())
        self.streamers = {}
        self.poll = select.poll()
        self.bitmask = (select.POLLIN | select.POLLERR |
                        select.POLLHUP | select.POLLNVAL)
        # Remember to close all pipes on __del__ to prevent leaks in
        # tests.
        self.wake_read, self.wake_write = os.pipe()
        self.poll.register(self.wake_read, self.bitmask)
        self.poll_lock = threading.Lock()

    def __del__(self):
        os.close(self.wake_read)
        os.close(self.wake_write)

    def start(self):
        self._stopped = False
        self.thread = threading.Thread(
            target=self.run,
            name='StreamManager')
        self.thread.start()

    def stop(self):
        if self.thread:
            self._stopped = True
            os.write(self.wake_write, b'\n')
            self.thread.join()

    def run(self):
        while not self._stopped:
            try:
                self._run()
            except Exception:
                self.log.exception("Error in StreamManager run method")

    def _run(self):
        for fd, event in self.poll.poll():
            if self._stopped:
                return
            if fd == self.wake_read:
                os.read(self.wake_read, 1024)
                continue
            streamer = self.streamers.get(fd)
            if streamer:
                try:
                    streamer.handle(event)
                except Exception:
                    self.log.exception("Error in streamer:")
                    streamer.errorClose()
                    self.unregisterStreamer(streamer)
            else:
                with self.poll_lock:
                    # Double check this now that we have the lock
                    streamer = self.streamers.get(fd)
                    if not streamer:
                        self.log.error(
                            "Unregistering missing streamer fd: %s", fd)
                        try:
                            self.poll.unregister(fd)
                        except KeyError:
                            pass

    def emitStats(self):
        streamers = len(self.streamers)
        self.metrics.streamers.set(streamers)
        if self.statsd:
            self.statsd.gauge(f'zuul.web.server.{self.hostname}.streamers',
                              streamers)

    def registerStreamer(self, streamer):
        with self.poll_lock:
            self.log.debug("Registering streamer %s", streamer)
            self.streamers[streamer.fileno] = streamer
            self.poll.register(streamer.fileno, self.bitmask)
            os.write(self.wake_write, b'\n')
        self.emitStats()

    def unregisterStreamer(self, streamer):
        with self.poll_lock:
            self.log.debug("Unregistering streamer %s", streamer)
            old_streamer = self.streamers.get(streamer.fileno)
            if old_streamer and old_streamer is streamer:
                # Otherwise, we may have a new streamer which reused
                # the fileno, so leave the poll registration in place.
                del self.streamers[streamer.fileno]
                try:
                    self.poll.unregister(streamer.fileno)
                except KeyError:
                    pass
                except Exception:
                    self.log.exception("Error unregistering streamer:")
            streamer.closeSocket()
        self.emitStats()


class FakeCacheKey:
    class Dummy():
        pass

    def __init__(self):
        self.key = self.Dummy()
        self.key.reference = uuid.uuid4().hex


class ZuulWeb(object):
    log = logging.getLogger("zuul.web")
    tracer = trace.get_tracer("zuul")
    _stats_interval = IntervalTrigger(seconds=60)

    @staticmethod
    def generateRouteMap(api, oidc, include_auth):
        route_map = cherrypy.dispatch.RoutesDispatcher()
        route_map.connect('api', '/api',
                          controller=api, action='index')
        route_map.connect('api', '/api/info',
                          controller=api, action='info')
        route_map.connect('api', '/api/connections',
                          controller=api, action='connections')
        route_map.connect('api', '/api/components',
                          controller=api, action='components')
        route_map.connect('api', '/api/tenants',
                          controller=api, action='tenants')
        route_map.connect('api', '/api/tenant/{tenant_name}/info',
                          controller=api, action='tenant_info')
        route_map.connect('api', '/api/tenant/{tenant_name}/status',
                          controller=api, action='status')
        route_map.connect('api', '/api/tenant/{tenant_name}/status/change'
                          '/{change}',
                          controller=api, action='status_change')
        route_map.connect('api', '/api/tenant/{tenant_name}/semaphores',
                          controller=api, action='semaphores')
        route_map.connect('api', '/api/tenant/{tenant_name}/jobs',
                          controller=api, action='jobs')
        route_map.connect('api', '/api/tenant/{tenant_name}/job/{job_name}',
                          controller=api, action='job')
        # if no auth configured, deactivate admin routes
        if include_auth:
            # route order is important, put project actions before the more
            # generic tenant/{tenant_name}/project/{project} route
            route_map.connect('api',
                              '/api/tenant/{tenant_name}/authorizations',
                              controller=api,
                              action='tenant_authorizations')
            route_map.connect('api',
                              '/api/authorizations',
                              controller=api,
                              action='root_authorizations')
            route_map.connect('api', '/api/tenant/{tenant_name}/promote',
                              controller=api, action='promote')
            route_map.connect(
                'api',
                '/api/tenant/{tenant_name}/project/{project_name:.*}/autohold',
                controller=api,
                conditions=dict(method=['GET', 'OPTIONS']),
                action='autohold_project_get')
            route_map.connect(
                'api',
                '/api/tenant/{tenant_name}/project/{project_name:.*}/autohold',
                controller=api,
                conditions=dict(method=['POST']),
                action='autohold_project_post')
            route_map.connect(
                'api',
                '/api/tenant/{tenant_name}/state',
                controller=api,
                action='state_post')
            route_map.connect(
                'api',
                '/api/tenant/{tenant_name}/project/{project_name:.*}/enqueue',
                controller=api, action='enqueue')
            route_map.connect(
                'api',
                '/api/tenant/{tenant_name}/project/{project_name:.*}/dequeue',
                controller=api, action='dequeue')
        route_map.connect(
            'api',
            '/api/tenant/{tenant_name}/project/{project_name:.*}'
            '/freeze-pipeline',
            controller=api,
            action='project_freeze_pipeline',
        )
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/autohold/{request_id}',
                          controller=api,
                          conditions=dict(method=['GET', 'OPTIONS']),
                          action='autohold_get')
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/autohold/{request_id}',
                          controller=api,
                          conditions=dict(method=['DELETE']),
                          action='autohold_delete')
        route_map.connect('api', '/api/tenant/{tenant_name}/autohold',
                          controller=api, action='autohold_list')
        route_map.connect('api', '/api/tenant/{tenant_name}/projects',
                          controller=api, action='projects')
        route_map.connect('api', '/api/tenant/{tenant_name}/project/'
                          '{project_name:.*}',
                          controller=api, action='project')
        route_map.connect(
            'api',
            '/api/tenant/{tenant_name}/pipeline/{pipeline_name}'
            '/project/{project_name:.*}/branch/{branch_name:.*}/freeze-jobs',
            controller=api, action='project_freeze_jobs'
        )
        route_map.connect(
            'api',
            '/api/tenant/{tenant_name}/pipeline/{pipeline_name}'
            '/project/{project_name:.*}/branch/{branch_name:.*}'
            '/freeze-job/{job_name}',
            controller=api, action='project_freeze_job'
        )
        route_map.connect('api', '/api/tenant/{tenant_name}/providers',
                          controller=api, action='providers')
        route_map.connect('api', '/api/tenant/{tenant_name}/pipelines',
                          controller=api, action='pipelines')
        route_map.connect('api', '/api/tenant/{tenant_name}/images',
                          controller=api, action='images')
        route_map.connect('api', '/api/tenant/{tenant_name}/flavors',
                          controller=api, action='flavors')
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/'
                          'image/{image_name}/build',
                          controller=api,
                          conditions=dict(method=['POST', 'OPTIONS']),
                          action='image_build')
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/'
                          'image-build-artifact/{artifact_id}',
                          controller=api,
                          conditions=dict(method=['DELETE', 'OPTIONS']),
                          action='image_build_artifact_delete')
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/'
                          'image-upload/{upload_id}',
                          controller=api,
                          conditions=dict(method=['DELETE', 'OPTIONS']),
                          action='image_upload_delete')
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/'
                          'image-upload/{upload_id}/validate',
                          controller=api,
                          conditions=dict(method=['POST', 'OPTIONS']),
                          action='image_upload_validate')
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/'
                          'image-upload/{upload_id}/upload',
                          controller=api,
                          conditions=dict(method=['POST', 'OPTIONS']),
                          action='image_upload_upload')
        route_map.connect('api', '/api/tenant/{tenant_name}/labels',
                          controller=api, action='labels')
        route_map.connect('api', '/api/tenant/{tenant_name}/nodes',
                          controller=api, action='nodes')
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/'
                          'nodes/{node_id}',
                          controller=api,
                          conditions=dict(method=['PUT', 'OPTIONS']),
                          action='nodes_put')
        route_map.connect('api', '/api/tenant/{tenant_name}/nodeset-requests',
                          controller=api, action='nodeset_requests')
        route_map.connect('api',
                          '/api/tenant/{tenant_name}/'
                          'nodeset-requests/{request_id}',
                          controller=api,
                          conditions=dict(method=['DELETE', 'OPTIONS']),
                          action='nodeset_requests_delete')
        route_map.connect('api', '/api/tenant/{tenant_name}/key/'
                          '{project_name:.*}.pub',
                          controller=api, action='key')
        route_map.connect('api', '/api/tenant/{tenant_name}/'
                          'project-ssh-key/{project_name:.*}.pub',
                          controller=api, action='project_ssh_key')
        route_map.connect('api', '/api/tenant/{tenant_name}/'
                          'tenant-ssh-key/ssh.pub',
                          controller=api, action='tenant_ssh_key')
        route_map.connect('api', '/api/tenant/{tenant_name}/console-stream',
                          controller=api, action='console_stream_get',
                          conditions=dict(method=['GET']))
        route_map.connect('api', '/api/tenant/{tenant_name}/console-stream',
                          controller=api, action='console_stream_options',
                          conditions=dict(method=['OPTIONS']))
        route_map.connect('api', '/api/tenant/{tenant_name}/builds',
                          controller=api, action='builds')
        route_map.connect('api', '/api/tenant/{tenant_name}/badge',
                          controller=api, action='badge')
        route_map.connect('api', '/api/tenant/{tenant_name}/build/{uuid}',
                          controller=api, action='build')
        route_map.connect('api', '/api/tenant/{tenant_name}/buildsets',
                          controller=api, action='buildsets')
        route_map.connect('api', '/api/tenant/{tenant_name}/buildset/{uuid}',
                          controller=api, action='buildset')
        route_map.connect('api', '/api/tenant/{tenant_name}/build-times',
                          controller=api, action='build_times')
        route_map.connect('api', '/api/tenant/{tenant_name}/config-errors',
                          controller=api, action='config_errors')
        route_map.connect('api', '/api/tenant/{tenant_name}/tenant-status',
                          controller=api, action='tenant_status')
        route_map.connect('api', '/api/tenant/{tenant_name}/system-events',
                          controller=api, action='system_events')
        # global webroot access
        route_map.connect('oidc', '/oidc/jwks',
                          controller=oidc, action='global_jwks')
        route_map.connect(
            'oidc', '/.well-known/openid-configuration',
            controller=oidc, action='global_openid_configuration')
        # whitelabel webroot access
        route_map.connect('oidc', '/oidc/tenant/{tenant_name}/jwks',
                          controller=oidc, action='tenant_jwks')
        route_map.connect(
            'oidc',
            '/oidc/tenant/{tenant_name}/.well-known/openid-configuration',
            controller=oidc, action='tenant_openid_configuration')
        # Backwards compat, remove after zuul 15
        route_map.connect('oidc', '/oidc/.well-known/jwks',
                          controller=oidc, action='global_jwks')
        route_map.connect('oidc', '/.well-known/jwks',
                          controller=oidc, action='global_jwks')
        route_map.connect('oidc', '/oidc/{tenant_name}/jwks',
                          controller=oidc, action='tenant_jwks')
        route_map.connect('oidc', '/oidc/{tenant_name}/.well-known/jwks',
                          controller=oidc, action='tenant_jwks')
        return route_map

    def __init__(self,
                 config,
                 connections,
                 authenticators: AuthenticatorRegistry,
                 info: WebInfo = None):
        self._running = False
        self.start_time = datetime.now(timezone.utc)
        self.config = config
        self.tracing = tracing.Tracing(self.config)
        self.metrics = WebMetrics()
        self.statsd = get_statsd(config)
        self.wsplugin = None

        self.listen_address = get_default(self.config,
                                          'web', 'listen_address',
                                          '127.0.0.1')
        self.listen_port = get_default(self.config, 'web', 'port', 9000)
        self.server = None
        self.static_cache_expiry = get_default(self.config, 'web',
                                               'static_cache_expiry',
                                               3600)
        self.info = info
        self.static_path = os.path.abspath(
            get_default(self.config, 'web', 'static_path', STATIC_DIR)
        )
        self.hostname = socket.getfqdn()

        self.zk_client = ZooKeeperClient.fromConfig(self.config)
        self.zk_client.connect()

        self.executor_api = ExecutorApi(self.zk_client, use_cache=False)

        self.component_info = WebComponent(
            self.zk_client, self.hostname, version=get_version_string())
        self.component_info.register()

        self.event_watcher = EventWatcher(self.zk_client, None)

        self.monitoring_server = MonitoringServer(self.config, 'web',
                                                  self.component_info)
        self.monitoring_server.start()

        self.component_registry = COMPONENT_REGISTRY.create(self.zk_client)
        self.system = ZuulSystem(self.zk_client)

        self.system_config_thread = None
        self.system_config_cache_wake_event = threading.Event()
        self.system_config_cache = SystemConfigCache(
            self.zk_client,
            self.system_config_cache_wake_event.set)

        self.unparsed_config_cache = UnparsedConfigCache(self.zk_client)
        self.keystore = KeyStorage(
            self.zk_client, password=self._get_key_store_password())
        self.globals = SystemAttributes.fromConfig(self.config)
        self.ansible_manager = AnsibleManager(
            default_version=self.globals.default_ansible_version)
        self.abide = Abide()
        self.unparsed_abide = UnparsedAbideConfig()
        self.tenant_layout_state = LayoutStateStore(
            self.zk_client, self.system_config_cache_wake_event.set)
        self.local_layout_state = {}

        self.connections = connections
        self.authenticators = authenticators
        self.stream_manager = StreamManager(self.statsd, self.metrics)
        self.zone = get_default(self.config, 'web', 'zone')

        self.tenant_providers = {}
        self.layout_providers_store = LayoutProvidersStore(
            self.zk_client, self.connections, self.system.system_id)
        self.image_build_registry = ImageBuildRegistry(self.zk_client)
        self.image_upload_registry = ImageUploadRegistry(self.zk_client)
        self.requests_cache = RequestCache(
            self.zk_client,
            None,
            root=NodesetRequest.ROOT,
            items_path=NodesetRequest.REQUESTS_PATH,
            locks_path=NodesetRequest.LOCKS_PATH,
            zkobject_class=NodesetRequest)
        self.nodes_cache = NodeCache(
            self.zk_client,
            None,
            root=ProviderNode.ROOT,
            items_path=ProviderNode.NODES_PATH,
            locks_path=ProviderNode.LOCKS_PATH,
            zkobject_class=ProviderNode)
        self.launcher = LauncherClient(self.zk_client, None)

        self.management_events = TenantManagementEventQueue.createRegistry(
            self.zk_client)
        self.pipeline_management_events = (
            PipelineManagementEventQueue.createRegistry(self.zk_client)
        )
        self.trigger_events = TenantTriggerEventQueue.createRegistry(
            self.zk_client, self.connections
        )
        self.pipeline_trigger_events = (
            PipelineTriggerEventQueue.createRegistry(
                self.zk_client, self.connections
            )
        )
        self.pipeline_result_events = PipelineResultEventQueue.createRegistry(
            self.zk_client
        )

        self.zk_context = ZKContext(self.zk_client, None, None, self.log)

        command_socket = get_default(
            self.config, 'web', 'command_socket',
            '/var/lib/zuul/web.socket'
        )

        self.command_socket = commandsocket.CommandSocket(command_socket)

        self.repl = None

        self.command_map = {
            commandsocket.StopCommand.name: self.stop,
            commandsocket.ReplCommand.name: self.startRepl,
            commandsocket.NoReplCommand.name: self.stopRepl,
        }

        self.finger_tls_key = get_default(
            self.config, 'fingergw', 'tls_key')
        self.finger_tls_cert = get_default(
            self.config, 'fingergw', 'tls_cert')
        self.finger_tls_ca = get_default(
            self.config, 'fingergw', 'tls_ca')
        self.finger_tls_verify_hostnames = get_default(
            self.config, 'fingergw', 'tls_verify_hostnames', default=True)

        # NOTE: taking this from the zuul.conf for now, but we might want
        # to make this configurable per-tenant in the future
        self.auth_log_file_requests = get_default(
            self.config, "web", "auth_log_file_requests", default=False
        )

        api = ZuulWebAPI(self)
        oidc = ZuulWebOIDC(self)
        self.api = api
        route_map = self.generateRouteMap(
            api, oidc, bool(self.authenticators.authenticators))
        # Add fallthrough routes at the end for the static html/js files
        route_map.connect(
            'root_static', '/{path:.*}',
            controller=StaticHandler(self.static_path),
            action='default')

        for connection in connections.connections.values():
            controller = connection.getWebController(self)
            if controller:
                cherrypy.tree.mount(
                    controller,
                    '/api/connection/%s' % connection.connection_name)

        self.apsched = BackgroundScheduler()
        cherrypy.tools.stats = StatsTool(self.statsd, self.metrics)

        conf = {
            '/': {
                'request.dispatch': route_map,
                'tools.stats.on': True,
            }
        }
        cherrypy.config.update({
            'global': {
                'environment': 'production',
                'server.socket_host': self.listen_address,
                'server.socket_port': int(self.listen_port),
            },
        })

        api_app = cherrypy.tree.mount(api, '/', config=conf)
        api_app.log = ZuulCherrypyLogManager(appid=api_app.log.appid)

    @property
    def port(self):
        return cherrypy.server.bound_addr[1]

    def start(self):
        self.log.info("ZuulWeb starting")

        self._running = True
        self.component_info.state = self.component_info.INITIALIZING

        self.log.info("Starting command processor")
        self._command_running = True
        self.command_socket.start()
        self.command_thread = threading.Thread(target=self.runCommand,
                                               name='command')
        self.command_thread.daemon = True
        self.command_thread.start()

        # Wait for system config and layouts to be loaded
        self.log.info("Waiting for system config from scheduler")
        while not self.system_config_cache.is_valid:
            self.system_config_cache_wake_event.wait(1)
            if not self._running:
                return

        # Initialize the system config
        self.updateSystemConfig()

        # Wait until all layouts/tenants are loaded
        self.log.info("Waiting for all tenants to load")
        while True:
            self.system_config_cache_wake_event.clear()
            self.updateLayout()
            if (set(self.unparsed_abide.tenants.keys())
                != set(self.abide.tenants.keys())):
                while True:
                    self.system_config_cache_wake_event.wait(1)
                    if not self._running:
                        return
                    if self.system_config_cache_wake_event.is_set():
                        break
            else:
                break

        self.apsched.start()
        self.apsched.add_job(cherrypy.tools.stats.emitStats,
                             name="Regular cherrypy stats reporting",
                             trigger=self._stats_interval)

        self.log.info("Starting HTTP listeners")
        self.stream_manager.start()
        self.wsplugin = WebSocketPlugin(cherrypy.engine)
        self.wsplugin.subscribe()
        cherrypy.engine.start()

        self.component_info.state = self.component_info.RUNNING

        self.system_config_thread = threading.Thread(
            target=self.updateConfig,
            name='system_config')
        self._system_config_running = True
        self.system_config_thread.daemon = True
        self.system_config_thread.start()

    def stop(self):
        self.log.info("ZuulWeb stopping")
        self.keystore.stop()
        self._running = False
        self.component_info.state = self.component_info.STOPPED

        self.apsched.shutdown()

        cherrypy.engine.exit()
        # Not strictly necessary, but without this, if the server is
        # started again (e.g., in the unit tests) it will reuse the
        # same host/port settings.
        cherrypy.server.httpserver = None
        if self.wsplugin:
            self.wsplugin.unsubscribe()
        self.stream_manager.stop()
        self._system_config_running = False
        self.system_config_cache_wake_event.set()
        if self.system_config_thread:
            self.system_config_thread.join()
        self.stopRepl()
        self._command_running = False
        self.command_socket.stop()
        self.monitoring_server.stop()
        self.tracing.stop()
        self.requests_cache.stop()
        self.nodes_cache.stop()
        self.zk_client.disconnect()

    def join(self):
        self.command_thread.join()
        self.monitoring_server.join()

    def runCommand(self):
        while self._command_running:
            try:
                command, args = self.command_socket.get()
                if command != '_stop':
                    self.command_map[command]()
            except Exception:
                self.log.exception("Exception while processing command")

    def startRepl(self):
        if self.repl:
            return
        self.repl = zuul.lib.repl.REPLServer(self)
        self.repl.start()

    def stopRepl(self):
        if not self.repl:
            return
        self.repl.stop()
        self.repl = None

    def _get_key_store_password(self):
        try:
            return self.config["keystore"]["password"]
        except KeyError:
            raise RuntimeError("No key store password configured!")

    def updateConfig(self):
        while self._system_config_running:
            try:
                self.system_config_cache_wake_event.wait()
                self.system_config_cache_wake_event.clear()
                if not self._system_config_running:
                    return
                self.updateSystemConfig()
                if not self.updateLayout():
                    # Branch cache errors with at least one tenant,
                    # try again.
                    time.sleep(10)
                    self.system_config_cache_wake_event.set()
            except Exception:
                self.log.exception("Exception while updating system config")

    def updateSystemConfig(self):
        self.log.debug("Updating system config")
        self.unparsed_abide, self.globals = self.system_config_cache.get()
        self.ansible_manager = AnsibleManager(
            default_version=self.globals.default_ansible_version)

        loader = ConfigLoader(
            self.connections, self.system, self.zk_client, self.globals,
            self.unparsed_config_cache, keystorage=self.keystore)

        tenant_names = set(self.abide.tenants)
        deleted_tenants = tenant_names.difference(
            self.unparsed_abide.tenants.keys())

        # Remove TPCs of deleted tenants
        for tenant_name in deleted_tenants:
            self.abide.clearTPCRegistry(tenant_name)

        loader.loadAuthzRules(self.abide, self.unparsed_abide)
        loader.loadSemaphores(self.abide, self.unparsed_abide)
        loader.loadTPCs(self.abide, self.unparsed_abide)

    def updateLayout(self):
        self.log.debug("Updating layout state")
        loader = ConfigLoader(
            self.connections, self.system, self.zk_client, self.globals,
            self.unparsed_config_cache, keystorage=self.keystore)

        # We need to handle new and deleted tenants, so we need to process all
        # tenants currently known and the new ones.
        tenant_names = set(self.abide.tenants)
        tenant_names.update(self.unparsed_abide.tenants.keys())

        success = True
        for tenant_name in tenant_names:
            # Reload the tenant if the layout changed.
            try:
                self._updateTenantLayout(loader, tenant_name)
            except ReadOnlyBranchCacheError:
                self.log.info(
                    "Unable to update layout due to incomplete branch "
                    "cache, possibly due to in-progress tenant "
                    "reconfiguration; will retry")
                success = False
        self.log.debug("Done updating layout state")
        return success

    def _updateTenantLayout(self, loader, tenant_name):
        # Reload the tenant if the layout changed.
        if (self.local_layout_state.get(tenant_name)
                == self.tenant_layout_state.get(tenant_name)):
            return
        self.log.debug("Reloading tenant %s", tenant_name)
        with tenant_read_lock(self.zk_client, tenant_name, self.log) as tlock:
            layout_state = self.tenant_layout_state.get(tenant_name)
            layout_uuid = layout_state and layout_state.uuid

            if layout_state:
                min_ltimes = self.tenant_layout_state.getMinLtimes(
                    layout_state)
                branch_cache_min_ltimes = (
                    layout_state.branch_cache_min_ltimes)
                with self.createZKContext(tlock, self.log) as context:
                    providers = list(self.layout_providers_store.get(
                        context, self.zk_client, tenant_name))
            else:
                # Consider all project branch caches valid if
                # we don't have a layout state.
                min_ltimes = defaultdict(
                    lambda: defaultdict(lambda: -1))
                branch_cache_min_ltimes = defaultdict(lambda: -1)

            # The tenant will be stored in self.abide.tenants after
            # it was loaded.
            tenant = loader.loadTenant(
                self.abide, tenant_name, self.ansible_manager,
                self.unparsed_abide, min_ltimes=min_ltimes,
                layout_uuid=layout_uuid,
                branch_cache_min_ltimes=branch_cache_min_ltimes)
            if tenant is not None:
                self.local_layout_state[tenant_name] = layout_state
                self.tenant_providers[tenant_name] = providers
            else:
                self.tenant_providers.pop(tenant_name, None)
                self.local_layout_state.pop(tenant_name, None)

    def createZKContext(self, lock, log):
        # TODO: consider adding a stop event to zuul-web
        return ZKContext(self.zk_client, lock, None, log)
