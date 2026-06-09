# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2021-2025 Acme Gating, LLC
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

import abc
import copy
import hashlib
import itertools
import json
import logging
import math
import threading
import time
import textwrap
import types
import urllib.parse
import uuid
from collections import OrderedDict, defaultdict, namedtuple, UserDict
from enum import StrEnum, IntEnum
from functools import partial, total_ordering
from uuid import uuid4
import voluptuous as vs

import re2
import jsonpath_rw
from cachetools.func import lru_cache
from kazoo.exceptions import NodeExistsError, NoNodeError
from opentelemetry import trace

from zuul import change_matcher
from zuul.exceptions import (
    SEVERITY_ERROR,
    SEVERITY_WARNING,
    OIDCIssuerNotAllowedError,
    LabelForbiddenError,
    MaxOIDCTTLError,
    MaxTimeoutError,
    LabelNotFoundError,
    NodesetNotFoundError,
    PreTimeoutExceedsTimeoutError,
    ProjectNotFoundError,
    ProjectNotPermittedError,
    ReporterJobNotPermittedError,
    UnknownConnection,
)
from zuul.lib.re2util import filter_allowed_disallowed
from zuul.lib import tracing
from zuul.lib import yamlutil as yaml
from zuul.lib.capabilities import capabilities_registry
from zuul.lib.config import get_default
from zuul.lib.jsonutil import json_dumps
from zuul.lib.logutil import get_annotated_logger
from zuul.lib.result_data import get_artifacts_from_result_data
from zuul.lib.varnames import check_varnames
from zuul.zk import zkobject
from zuul.zk.blob_store import BlobStore
from zuul.zk.change_cache import ChangeKey
from zuul.zk.components import COMPONENT_REGISTRY


MERGER_MERGE = 1            # "git merge"
MERGER_MERGE_RESOLVE = 2    # "git merge -s resolve"
MERGER_CHERRY_PICK = 3      # "git cherry-pick"
MERGER_SQUASH_MERGE = 4     # "git merge --squash"
MERGER_REBASE = 5           # "git rebase"
MERGER_MERGE_RECURSIVE = 6  # "git merge -s recursive"
MERGER_MERGE_ORT = 7        # "git merge -s ort"

MERGER_MAP = {
    'merge': MERGER_MERGE,
    'merge-resolve': MERGER_MERGE_RESOLVE,
    'merge-recursive': MERGER_MERGE_RECURSIVE,
    'merge-ort': MERGER_MERGE_ORT,
    'cherry-pick': MERGER_CHERRY_PICK,
    'squash-merge': MERGER_SQUASH_MERGE,
    'rebase': MERGER_REBASE,
}
ALL_MERGE_MODES = list(MERGER_MAP.values())

PRECEDENCE_NORMAL = 0
PRECEDENCE_LOW = 1
PRECEDENCE_HIGH = 2

PRECEDENCE_MAP = {
    None: PRECEDENCE_NORMAL,
    'low': PRECEDENCE_LOW,
    'normal': PRECEDENCE_NORMAL,
    'high': PRECEDENCE_HIGH,
}

PRIORITY_MAP = {
    PRECEDENCE_NORMAL: 200,
    PRECEDENCE_LOW: 300,
    PRECEDENCE_HIGH: 100,
}

# Request states
STATE_REQUESTED = 'requested'
STATE_FULFILLED = 'fulfilled'
STATE_FAILED = 'failed'
REQUEST_STATES = set([STATE_REQUESTED,
                      STATE_FULFILLED,
                      STATE_FAILED])

# Node states
STATE_BUILDING = 'building'
STATE_TESTING = 'testing'
STATE_READY = 'ready'
STATE_IN_USE = 'in-use'
STATE_USED = 'used'
STATE_HOLD = 'hold'
STATE_DELETING = 'deleting'
NODE_STATES = set([STATE_BUILDING,
                   STATE_TESTING,
                   STATE_READY,
                   STATE_IN_USE,
                   STATE_USED,
                   STATE_HOLD,
                   STATE_DELETING])

# Workspace scheme
SCHEME_GOLANG = 'golang'
SCHEME_FLAT = 'flat'
SCHEME_UNIQUE = 'unique'


def add_debug_line(debug_messages, msg, indent=0):
    if debug_messages is None:
        return
    if indent:
        indent = '  ' * indent
    else:
        indent = ''
    debug_messages.append(indent + msg)


def get_merge_mode_name(merge_mode):
    "Look up the merge mode name given the constant"
    for k, v in MERGER_MAP.items():
        if v == merge_mode:
            return k


def filter_severity(error_list, errors=True, warnings=True):
    return [e for e in error_list
            if (
                (errors and e.severity == SEVERITY_ERROR) or
                (warnings and e.severity == SEVERITY_WARNING)
            )]


class QuotaInformation:
    def __init__(self, default=0, **kw):
        '''
        Initializes the quota information with some values. None values will
        be initialized with default which will be typically 0 or math.inf
        indicating an infinite limit.

        :param default: The default value to use for any attribute not supplied
                        (usually 0 or math.inf).
        '''
        self.quota = {}
        for k, v in kw.items():
            self.quota[k] = v
        self.default = default

    def __repr__(self):
        return f'<QuotaInformation default={self.default} quota={self.quota}>'

    def __eq__(self, other):
        return (isinstance(other, QuotaInformation) and
                self.default == other.default and
                self.quota == other.quota)

    def _get_default(self, value, default):
        return value if value is not None else default

    def _add_subtract(self, other, add=True):
        for resource in other.quota.keys():
            self.quota.setdefault(resource, self.default)
        for resource in self.quota.keys():
            other_value = other.quota.get(resource, other.default)
            if add:
                self.quota[resource] += other_value
            else:
                self.quota[resource] -= other_value

    def copy(self):
        return QuotaInformation(self.default, **self.quota)

    def subtract(self, other):
        self._add_subtract(other, add=False)

    def add(self, other):
        self._add_subtract(other, True)

    def min(self, other):
        for resource, theirs in other.quota.items():
            ours = self.quota.get(resource, self.default)
            self.quota[resource] = min(ours, theirs)

    def nonNegative(self):
        for resource, value in self.quota.items():
            if value < 0:
                return False
        return True

    def getResources(self):
        '''Return resources value to register in ZK node'''
        return self.quota

    def __str__(self):
        return str(self.quota)


class QueryCacheEntry:
    def __init__(self, ltime, results):
        self.ltime = ltime
        self.results = results


class QueryCache:
    """Cache query information while processing dependencies"""

    def __init__(self, zk_client):
        self.zk_client = zk_client
        self.ltime = 0
        self.clear(0)

    def clear(self, ltime):
        self.ltime = ltime
        self.topic_queries = {}

    def clearIfOlderThan(self, event):
        if not hasattr(event, "zuul_event_ltime"):
            return
        ltime = event.zuul_event_ltime
        if ltime > self.ltime:
            ltime = self.zk_client.getCurrentLtime()
            self.clear(ltime)


class MergeOp:
    def __init__(self, cmd=None, timestamp=None, comment=None, path=None):
        """A class representing a merge operation, returned by the merger to
        tell the user what was done."""
        self.cmd = cmd
        self.timestamp = timestamp
        self.comment = comment
        self.path = path

    def toDict(self):
        ret = {}
        for k in ['cmd', 'timestamp', 'comment', 'path']:
            v = getattr(self, k)
            if v is not None:
                ret[k] = v
        return ret


class ZuulMark:
    # The yaml mark class differs between the C and python versions.
    # The C version does not provide a snippet, and also appears to
    # lose data under some circumstances.
    def __init__(self, start_mark, end_mark, stream):
        self.name = start_mark.name
        self.index = start_mark.index
        self.line = start_mark.line
        self.end_line = end_mark.line
        self.end_index = end_mark.index
        self.column = start_mark.column
        self.end_column = end_mark.column
        self.snippet = stream[start_mark.index:end_mark.index]

    def __str__(self):
        return '  in "{name}", line {line}, column {column}'.format(
            name=self.name,
            line=self.line + 1,
            column=self.column + 1,
        )

    def __eq__(self, other):
        if not isinstance(other, ZuulMark):
            return False
        return (self.line == other.line and
                self.snippet == other.snippet)

    line_snippet_context = 4

    def getLineSnippet(self, line):
        start = max(line - self.line - self.line_snippet_context, 0)
        end = start + (self.line_snippet_context * 2) + 1
        all_lines = self.snippet.splitlines()
        lines = all_lines[start:end]
        if start > 0:
            lines.insert(0, '...')
        if end < len(all_lines):
            lines.append('...')
        return '\n'.join(lines)

    def getLineLocation(self, line):
        return '  in "{name}", line {line}'.format(
            name=self.name,
            line=line + 1,
        )

    def serialize(self):
        return {
            "name": self.name,
            "index": self.index,
            "line": self.line,
            "end_line": self.end_line,
            "end_index": self.end_index,
            "column": self.column,
            "end_column": self.end_column,
            "snippet": self.snippet,
        }

    @classmethod
    def deserialize(cls, data):
        o = cls.__new__(cls)
        o.__dict__.update(data)
        return o


class ConfigurationErrorKey(object):
    """A class which attempts to uniquely identify configuration errors
    based on their file location.  It's not perfect, but it's usually
    sufficient to determine whether we should show an error to a user.
    """

    # Note: this class is serialized to ZK via ConfigurationErrorList,
    # ensure that it serializes and deserializes appropriately.

    def __init__(self, context, mark, error_text):
        self.context = context
        self.mark = mark
        self.error_text = error_text
        elements = []
        if context:
            elements.extend([
                context.project_canonical_name,
                context.branch,
                context.path,
            ])
        else:
            elements.extend([None, None, None])
        if mark:
            elements.extend([
                mark.line,
                mark.snippet,
            ])
        else:
            elements.extend([None, None])
        elements.append(error_text)

        hasher = hashlib.sha256()
        hasher.update(json.dumps(elements, sort_keys=True).encode('utf8'))
        self._hash = hasher.hexdigest()

    def serialize(self):
        return {
            "context": self.context and self.context.serialize(),
            "mark": self.mark and self.mark.serialize(),
            "error_text": self.error_text,
            "_hash": self._hash,
        }

    @classmethod
    def deserialize(cls, data):
        data.update({
            "context": data["context"] and SourceContext.deserialize(
                data["context"]),
            "mark": data["mark"] and ZuulMark.deserialize(data["mark"]),
        })
        o = cls.__new__(cls)
        o.__dict__.update(data)
        return o

    def __hash__(self):
        return hash(self._hash)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, ConfigurationErrorKey):
            return False
        return (self.context == other.context and
                self.mark == other.mark and
                self.error_text == other.error_text)


class ConfigurationError(object):
    """A configuration error"""

    # Note: this class is serialized to ZK via ConfigurationErrorList,
    # ensure that it serializes and deserializes appropriately.

    def __init__(self, context, mark, error, short_error=None,
                 severity=None, name=None):
        self.error = error
        self.short_error = short_error
        self.severity = severity or SEVERITY_ERROR
        self.name = name or 'Unknown'
        self.key = ConfigurationErrorKey(context, mark, self.error)

    def serialize(self):
        return {
            "error": self.error,
            "short_error": self.short_error,
            "key": self.key.serialize(),
            "severity": self.severity,
            "name": self.name,
        }

    @classmethod
    def deserialize(cls, data):
        data["key"] = ConfigurationErrorKey.deserialize(data["key"])
        data['severity'] = data['severity']
        data['name'] = data['name']
        o = cls.__new__(cls)
        o.__dict__.update(data)
        return o

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, ConfigurationError):
            return False
        return (self.error == other.error and
                self.short_error == other.short_error and
                self.key == other.key and
                self.severity == other.severity and
                self.name == other.name)


class ConfigurationErrorList(zkobject.ShardedZKObject):
    """A list of configuration errors.

    BuildSets may have zero or one of these.
    """

    def __repr__(self):
        return '<ConfigurationErrorList>'

    def getPath(self):
        return self._path

    def serialize(self, context):
        data = {
            "errors": [e.serialize() for e in self.errors],
        }
        return json.dumps(data, sort_keys=True).encode("utf8")

    def deserialize(self, raw, context, extra=None):
        data = super().deserialize(raw, context)
        data.update({
            "errors": [ConfigurationError.deserialize(d)
                       for d in data["errors"]],
        })
        return data


class LoadingErrors(object):
    """A configuration errors accumalator attached to a layout object
    """
    def __init__(self):
        self.errors = []
        self.error_keys = set()

    def makeError(self, context, mark, error, short_error=None,
                  severity=None, name=None):
        e = ConfigurationError(context, mark, error,
                               short_error=short_error,
                               severity=severity,
                               name=name)
        self.addError(e)

    def addError(self, error):
        self.errors.append(error)
        self.error_keys.add(error.key)

    def __getitem__(self, index):
        return self.errors[index]

    def __len__(self):
        return len(self.errors)


class RequirementsError(Exception):
    """A job's requirements were not met."""
    pass


class JobConfigurationError(Exception):
    """A job has an invalid configuration.

    These are expected user errors when freezing a job graph.
    """
    pass


class TemplateNotFoundError(JobConfigurationError):
    """A project referenced a template that does not exist."""
    pass


class NoMatchingParentError(JobConfigurationError):
    """A job referenced a parent, but that parent had no variants which
    matched the current change."""
    pass


class JobNotDefinedError(JobConfigurationError):
    """A job was not defined."""
    pass


class SecretNotFoundError(JobConfigurationError):
    """A job referenced a semaphore that does not exist."""
    pass


class Attributes(object):
    """A class to hold attributes for string formatting."""

    def __init__(self, **kw):
        setattr(self, '__dict__', kw)

    def toDict(self):
        return self.__dict__


class ConfigObject:
    def __init__(self):
        super().__init__()
        self.source_context = None
        self.start_mark = None


class Pipeline(object):
    """A configuration that ties together triggers, reporters and managers

    Trigger
        A description of which events should be processed

    Manager
        Responsible for enqueing and dequeing Changes

    Reporter
        Communicates success and failure results somewhere
    """
    STATE_NORMAL = 'normal'
    STATE_ERROR = 'error'

    def __init__(self, name):
        self.name = name
        self.allow_other_connections = True
        self.allow_parameters = False
        self.connections = []
        self.source_context = None
        self.start_mark = None
        self.description = None
        self.failure_message = None
        self.merge_conflict_message = None
        self.success_message = None
        self.footer_message = None
        self.enqueue_message = None
        self.start_message = None
        self.dequeue_message = None
        self.post_review = False
        self.dequeue_on_new_patchset = True
        self.ignore_dependencies = False
        self.manager_name = None
        self.precedence = PRECEDENCE_NORMAL
        self.supercedes = []
        self.triggers = []
        self.enqueue_actions = []
        self.start_actions = []
        self.success_actions = []
        self.failure_actions = []
        self.merge_conflict_actions = []
        self.no_jobs_actions = []
        self.disabled_actions = []
        self.dequeue_actions = []
        self.disable_at = None
        self.window = None
        self.window_floor = None
        self.window_ceiling = None
        self.window_increase_type = None
        self.window_increase_factor = None
        self.window_decrease_type = None
        self.window_decrease_factor = None
        self.ref_filters = []
        self.event_filters = []
        self.builds_images = False

    @property
    def actions(self):
        return (
            self.enqueue_actions +
            self.start_actions +
            self.success_actions +
            self.failure_actions +
            self.merge_conflict_actions +
            self.no_jobs_actions +
            self.disabled_actions +
            self.dequeue_actions
        )

    def __repr__(self):
        return '<Pipeline %s>' % self.name

    def getSafeAttributes(self):
        return Attributes(name=self.name)

    def validateReferences(self, layout):
        # Verify that references to other objects in the layout are
        # valid.

        for pipeline in self.supercedes:
            if not layout.pipeline_managers.get(pipeline):
                raise Exception(
                    'The pipeline "{this}" supercedes an unknown pipeline '
                    '{other}.'.format(
                        this=self.name,
                        other=pipeline))


class PipelineState(zkobject.ZKObject):

    def __init__(self):
        super().__init__()
        self._set(
            state=Pipeline.STATE_NORMAL,
            queues=[],
            old_queues=[],
            consecutive_failures=0,
            disabled=False,
            layout_uuid=None,
            # Local pipeline manager reference (not persisted in Zookeeper)
            manager=None,
            _read_only=False,
        )

    def _lateInitData(self):
        # If we're initializing the object on our initial refresh,
        # reset the data to this.
        return dict(
            state=Pipeline.STATE_NORMAL,
            queues=[],
            old_queues=[],
            consecutive_failures=0,
            disabled=False,
            layout_uuid=self.manager.tenant.layout.uuid,
        )

    @classmethod
    def fromZK(klass, context, path, manager, **kw):
        obj = klass()
        obj._set(manager=manager, **kw)
        # Bind the state to the manager, so child objects can access
        # the full pipeline state.
        manager.state = obj
        obj._load(context, path=path)
        return obj

    @classmethod
    def create(cls, manager, old_state=None):
        # If we are resetting an existing pipeline, we will have an
        # old_state, so just clean up the object references there and
        # let the next refresh handle updating any data.
        # TODO: This apparently hasn't been called in some time; fix.
        if old_state:
            old_state._resetObjectRefs()
            return old_state

        # Otherwise, we are initializing a pipeline that we haven't
        # seen before.  It still might exist in ZK, but since we
        # haven't seen it, we don't have any object references to
        # clean up.  We can just start with a clean object, set the
        # manager reference, and let the next refresh deal with
        # whether there might be any data in ZK.
        state = cls()
        state._set(manager=manager)
        return state

    def _resetObjectRefs(self):
        # Update the pipeline references on the queue objects.
        for queue in self.queues + self.old_queues:
            queue.manager = self.manager

    def getPath(self):
        if hasattr(self, '_path'):
            return self._path
        return self.pipelinePath(self.manager)

    @classmethod
    def pipelinePath(cls, manager):
        safe_tenant = urllib.parse.quote_plus(manager.tenant.name)
        safe_pipeline = urllib.parse.quote_plus(manager.pipeline.name)
        return f"/zuul/tenant/{safe_tenant}/pipeline/{safe_pipeline}"

    @classmethod
    def parsePath(self, path):
        """Return path components for use by the REST API"""
        root, safe_tenant, pipeline, safe_pipeline = path.rsplit('/', 3)
        return (urllib.parse.unquote_plus(safe_tenant),
                urllib.parse.unquote_plus(safe_pipeline))

    def _dirtyPath(self):
        return f'{self.getPath()}/dirty'

    def isDirty(self, client):
        return bool(client.exists(self._dirtyPath()))

    def setDirty(self, client):
        try:
            client.create(self._dirtyPath())
        except NodeExistsError:
            pass

    def clearDirty(self, client):
        try:
            client.delete(self._dirtyPath())
        except NoNodeError:
            pass

    def removeOldQueue(self, context, queue):
        if queue in self.old_queues:
            with self.activeContext(context):
                self.old_queues.remove(queue)

    def addQueue(self, queue):
        with self.activeContext(self.manager.current_context):
            self.queues.append(queue)

    def getQueue(self, project_cname, branch):
        # Queues might be branch specific so match with branch
        for queue in self.queues:
            if queue.matches(project_cname, branch):
                return queue
        return None

    def removeQueue(self, queue):
        if queue in self.queues:
            with self.activeContext(self.manager.current_context):
                self.queues.remove(queue)
            queue.delete(self.manager.current_context)

    def promoteQueue(self, queue):
        if queue not in self.queues:
            return
        with self.activeContext(self.manager.current_context):
            self.queues.remove(queue)
            self.queues.insert(0, queue)

    def getAllItems(self, include_old=False):
        items = []
        for shared_queue in self.queues:
            items.extend(shared_queue.queue)
        if include_old:
            for shared_queue in self.old_queues:
                items.extend(shared_queue.queue)
        return items

    def serialize(self, context):
        if self._read_only:
            raise RuntimeError("Attempt to serialize read-only pipeline state")
        data = {
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "disabled": self.disabled,
            "queues": [q.getPath() for q in self.queues],
            "old_queues": [q.getPath() for q in self.old_queues],
            "layout_uuid": self.layout_uuid,
        }
        return json.dumps(data, sort_keys=True).encode("utf8")

    def refresh(self, context, read_only=False):
        # Set read_only to True to indicate that we should avoid
        # "resetting" the pipeline state if the layout has changed.
        # This is so that we can refresh the object in circumstances
        # where we haven't verified that our local layout matches
        # what's in ZK.

        # Notably, this need not prevent us from performing the
        # initialization below if necessary.  The case of the object
        # being brand new in ZK supercedes our worry that our old copy
        # might be out of date since our old copy is, itself, brand
        # new.
        self._set(_read_only=read_only)
        try:
            return super().refresh(context)
        except NoNodeError:
            # If the object doesn't exist we will receive a
            # NoNodeError.  This happens because the postConfig call
            # creates this object without holding the pipeline lock,
            # so it can't determine whether or not it exists in ZK.
            # We do hold the pipeline lock here, so if we get this
            # error, we know we're initializing the object, and we
            # should write it to ZK.

            # Note that typically this code is not used since
            # currently other objects end up creating the pipeline
            # path in ZK first.  It is included in case that ever
            # changes.  Currently the empty byte-string code path in
            # deserialize() is used instead.
            context.log.warning("Initializing pipeline state for %s; "
                                "this is expected only for new pipelines",
                                self.manager.pipeline.name)
            self._set(**self._lateInitData())
            self.internalCreate(context)

    def deserialize(self, raw, context, extra=None):
        # We may have old change objects in the pipeline cache, so
        # make sure they are the same objects we would get from the
        # source change cache.
        self.manager.clearCache()

        # If the object doesn't exist we will get back an empty byte
        # string.  This happens because the postConfig call creates
        # this object without holding the pipeline lock, so it can't
        # determine whether or not it exists in ZK.  We do hold the
        # pipeline lock here, so if we get the empty byte string, we
        # know we're initializing the object.  In that case, we should
        # initialize the layout id to the current layout.  Nothing
        # else needs to be set.
        if raw == b'':
            context.log.warning("Initializing pipeline state for %s; "
                                "this is expected only for new pipelines",
                                self.manager.pipeline.name)
            return self._lateInitData()

        data = super().deserialize(raw, context)

        if not self._read_only:
            # Skip this check if we're in a context where we want to
            # read the state without updating it (in case we're not
            # certain that the layout is up to date).
            if data['layout_uuid'] != self.manager.tenant.layout.uuid:
                # The tenant layout has updated since our last state; we
                # need to reset the state.
                data = dict(
                    state=Pipeline.STATE_NORMAL,
                    queues=[],
                    old_queues=data["old_queues"] + data["queues"],
                    consecutive_failures=0,
                    disabled=False,
                    layout_uuid=self.manager.tenant.layout.uuid,
                )

        existing_queues = {
            q.getPath(): q for q in self.queues + self.old_queues
        }

        # Restore the old queues first, so that in case an item is
        # already in one of the new queues the item(s) ahead/behind
        # pointers are corrected when restoring the new queues.
        old_queues = []
        for queue_path in data["old_queues"]:
            queue = existing_queues.get(queue_path)
            if queue:
                queue.refresh(context)
            else:
                queue = ChangeQueue.fromZK(context, queue_path,
                                           manager=self.manager)
            old_queues.append(queue)

        queues = []
        for queue_path in data["queues"]:
            queue = existing_queues.get(queue_path)
            if queue:
                queue.refresh(context)
            else:
                queue = ChangeQueue.fromZK(context, queue_path,
                                           manager=self.manager)
            queues.append(queue)

        if hasattr(self.manager, "change_queue_managers"):
            # Perform a quick check to see if the queue objects in ZK
            # are different than the ones we have in memory.
            managed_queues = set()
            for cq_manager in self.manager.change_queue_managers:
                managed_queues |= set(cq_manager.created_for_branches.values())

            # Only if they are different, perform the more expensive
            # calculation below to realign them.
            if managed_queues != set(queues):
                # Clear out references to old queues
                for cq_manager in self.manager.change_queue_managers:
                    cq_manager.created_for_branches.clear()

                # Add queues to matching change queue managers
                for queue in queues:
                    cq_manager = None
                    if queue.name:
                        cq_manager = self.manager.named_queue_managers.get(
                            queue.name)
                    if cq_manager is None:
                        # If the queue has no name, then it is an
                        # implied per-project all-branches queue.
                        # There will be only one project, so use that
                        # project to find the queue manager.
                        queue_project_name = queue.project_branches[0][0]
                        cq_manager = self.manager.default_queue_managers.get(
                            queue_project_name)
                    if cq_manager is not None:
                        for project_name, branch in queue.project_branches:
                            cq_manager.created_for_branches[branch] = queue

        data.update({
            "queues": queues,
            "old_queues": old_queues,
        })
        return data

    def cleanup(self, context):
        pipeline_path = self.getPath()
        try:
            all_items = set(context.client.get_children(
                f"{pipeline_path}/item"))
        except NoNodeError:
            all_items = set()

        known_item_objs = self.getAllItems(include_old=True)
        known_items = {i.uuid for i in known_item_objs}
        items_referenced_by_builds = set()
        for i in known_item_objs:
            build_set = i.current_build_set
            # Drop some attributes from local objects to save memory
            build_set._set(_files=None,
                           _repo_state=RepoState())
            job_graph = build_set.job_graph
            if not job_graph:
                continue
            for job in job_graph.getJobs():
                build = build_set.getBuild(job)
                if build:
                    items_referenced_by_builds.add(build.build_set.item.uuid)
        stale_items = all_items - known_items - items_referenced_by_builds
        for item_uuid in stale_items:
            self.manager.log.debug("Cleaning up stale item %s",
                                   item_uuid)
            context.client.delete(QueueItem.itemPath(pipeline_path, item_uuid),
                                  recursive=True)

        try:
            all_queues = set(context.client.get_children(
                f"{pipeline_path}/queue"))
        except NoNodeError:
            all_queues = set()

        known_queues = {q.uuid for q in (*self.old_queues, *self.queues)}
        stale_queues = all_queues - known_queues
        for queue_uuid in stale_queues:
            self.manager.log.debug("Cleaning up stale queue %s",
                                   queue_uuid)
            context.client.delete(
                ChangeQueue.queuePath(pipeline_path, queue_uuid),
                recursive=True)


class PipelineChangeList(zkobject.ShardedZKObject):
    """A list of change references within a pipeline

       This is used by the scheduler to quickly decide if events which
       otherwise don't match the pipeline triggers should be
       nevertheless forwarded to the pipeline.

       It is also used to maintain the connection cache.
    """
    # We can read from this object without locking, and since it's
    # sharded, that may produce an error.  If that happens, don't
    # delete the object, just retry.
    delete_on_error = False
    log_error_missing = False

    def __init__(self):
        super().__init__()
        self._set(
            changes=[],
            _change_keys=[],
        )

    def refresh(self, context, allow_init=True):
        # Set allow_init to false to indicate that we don't hold the
        # lock and we should not try to initialize the object in ZK if
        # it does not exist.
        try:
            self._retry(context, super().refresh,
                        context, max_tries=5)
        except NoNodeError:
            # If the object doesn't exist we will receive a
            # NoNodeError.  This happens because the postConfig call
            # creates this object without holding the pipeline lock,
            # so it can't determine whether or not it exists in ZK.
            # We do hold the pipeline lock here, so if we get this
            # error, we know we're initializing the object, and
            # we should write it to ZK.
            if allow_init:
                context.log.warning(
                    "Initializing pipeline change list for %s; "
                    "this is expected only for new pipelines",
                    self.manager.pipeline.name)
                self.internalCreate(context)
            else:
                # If we're called from a context where we can't
                # initialize the change list, re-raise the exception.
                raise

    def getPath(self):
        return self.getChangeListPath(self.manager)

    @classmethod
    def getChangeListPath(cls, manager):
        pipeline_path = manager.state.getPath()
        return pipeline_path + '/change_list'

    @classmethod
    def create(cls, manager):
        # This object may or may not exist in ZK, but we using any of
        # that data here.  We can just start with a clean object, set
        # the manager reference, and let the next refresh deal with
        # whether there might be any data in ZK.
        change_list = cls()
        change_list._set(manager=manager)
        return change_list

    def serialize(self, context):
        data = {
            "changes": self.changes,
        }
        return json.dumps(data, sort_keys=True).encode("utf8")

    def deserialize(self, raw, context, extra=None):
        data = super().deserialize(raw, context)
        change_keys = []
        # We must have a dictionary with a 'changes' key; otherwise we
        # may be reading immediately after truncating.  Allow the
        # KeyError exception to propogate in that case.
        for ref in data['changes']:
            change_keys.append(ChangeKey.fromReference(ref))
        data['_change_keys'] = change_keys
        return data

    def setChangeKeys(self, context, change_keys):
        change_refs = [key.reference for key in change_keys]
        if change_refs == self.changes:
            return
        self.updateAttributes(context, changes=change_refs)
        self._set(_change_keys=change_keys)

    def getChangeKeys(self):
        return self._change_keys


class PipelineSummary(zkobject.ShardedZKObject):

    log = logging.getLogger("zuul.PipelineSummary")
    truncate_on_create = True
    delete_on_error = False
    log_error_missing = False

    def __init__(self):
        super().__init__()
        self._set(
            status={},
        )

    def getPath(self):
        return f"{PipelineState.pipelinePath(self.manager)}/status"

    def update(self, context, zuul_globals):
        status = self.manager.formatStatusJSON(
            zuul_globals.websocket_url)
        self.updateAttributes(context, status=status)

    def serialize(self, context):
        data = {
            "status": self.status,
        }
        return json.dumps(data, sort_keys=True).encode("utf8")

    def refresh(self, context):
        # Ignore exceptions and just re-use the previous state. This
        # might happen in case the sharded status data is truncated
        # while zuul-web tries to read it.
        try:
            super().refresh(context)
        except NoNodeError:
            self.log.warning("No pipeline summary found "
                             "(may not be created yet)")
        except Exception:
            self.log.exception("Failed to refresh data")
        return self.status


class ChangeQueue(zkobject.ZKObject):

    """A ChangeQueue contains Changes to be processed for related projects.

    A Pipeline with a DependentPipelineManager has multiple parallel
    ChangeQueues shared by different projects. For instance, there may a
    ChangeQueue shared by interrelated projects foo and bar, and a second queue
    for independent project baz.

    A Pipeline with an IndependentPipelineManager puts every Change into its
    own ChangeQueue.

    The ChangeQueue Window is inspired by TCP windows and controlls how many
    Changes in a given ChangeQueue will be considered active and ready to
    be processed. If a Change succeeds, the Window is increased by
    `window_increase_factor`. If a Change fails, the Window is decreased by
    `window_decrease_factor`.

    A ChangeQueue may be a dynamically created queue, which may be removed
    from a DependentPipelineManager once empty.
    """
    def __init__(self):
        super().__init__()
        self._set(
            uuid=uuid4().hex,
            manager=None,
            name="",
            project_branches=[],
            _jobs=set(),
            queue=[],
            window=0,
            window_floor=1,
            window_ceiling=math.inf,
            window_increase_type="linear",
            window_increase_factor=1,
            window_decrease_type="exponential",
            window_decrease_factor=2,
            dynamic=False,
        )

    def serialize(self, context):
        data = {
            "uuid": self.uuid,
            "name": self.name,
            "project_branches": self.project_branches,
            "_jobs": list(self._jobs),
            "queue": [i.getPath() for i in self.queue],
            "window": self.window,
            "window_floor": self.window_floor,
            "window_ceiling": self.window_ceiling,
            "window_increase_type": self.window_increase_type,
            "window_increase_factor": self.window_increase_factor,
            "window_decrease_type": self.window_decrease_type,
            "window_decrease_factor": self.window_decrease_factor,
            "dynamic": self.dynamic,
        }
        return json.dumps(data, sort_keys=True).encode("utf8")

    def deserialize(self, raw, context, extra=None):
        data = super().deserialize(raw, context)

        existing_items = {}
        for item in self.queue:
            existing_items[item.getPath()] = item

        items_by_path = OrderedDict()
        # This is a tuple of (x, Future), where x is None if no action
        # needs to be taken, or a string to indicate which kind of job
        # it was.  This structure allows us to execute async ZK reads
        # and perform local data updates in order.
        tpe_jobs = []
        tpe = context.executor[ChangeQueue]
        for item_path in data["queue"]:
            item = existing_items.get(item_path)
            items_by_path[item_path] = item
            if item:
                tpe_jobs.append((None, tpe.submit(item.refresh, context)))
            else:
                tpe_jobs.append(('item', tpe.submit(
                    QueueItem.fromZK, context, item_path,
                    queue=self)))

        for (kind, future) in tpe_jobs:
            result = future.result()
            if kind == 'item':
                items_by_path[result.getPath()] = result

        # Resolve ahead/behind references between queue items
        for item in items_by_path.values():
            # After a re-enqueue we might have references to items
            # outside the current queue. We will resolve those
            # references to None for the item ahead or simply exclude
            # it in the list of items behind.
            # The pipeline manager will take care of correcting the
            # references on the next queue iteration.
            item._set(
                item_ahead=items_by_path.get(item._item_ahead),
                items_behind=[items_by_path[p] for p in item._items_behind
                              if p in items_by_path])

        data.update({
            "_jobs": set(data["_jobs"]),
            "queue": list(items_by_path.values()),
            "project_branches": [tuple(pb) for pb in data["project_branches"]],
        })
        return data

    def getPath(self):
        pipeline_path = self.manager.state.getPath()
        return self.queuePath(pipeline_path, self.uuid)

    @classmethod
    def queuePath(cls, pipeline_path, queue_uuid):
        return f"{pipeline_path}/queue/{queue_uuid}"

    @property
    def zk_context(self):
        return self.manager.current_context

    def __repr__(self):
        return '<ChangeQueue %s: %s>' % (self.manager.pipeline.name, self.name)

    def getJobs(self):
        return self._jobs

    def addProject(self, project, branch):
        """
        Adds a project branch combination to the queue.

        The queue will match exactly this combination. If the caller doesn't
        care about branches it can supply None (but must supply None as well
        when matching)
        """
        project_branch = (project.canonical_name, branch)
        if project_branch not in self.project_branches:
            with self.activeContext(self.zk_context):
                self.project_branches.append(project_branch)

    def matches(self, project_cname, branch):
        return (project_cname, branch) in self.project_branches

    def enqueueChanges(self, changes, event, span_info=None,
                       enqueue_time=None, debug=False):
        if enqueue_time is None:
            enqueue_time = time.time()

        if event:
            event_ref_cache_key = None
            if isinstance(event, EventInfo):
                event_ref_cache_key = event.ref
            elif getattr(event, 'orig_ref', None):
                event_ref_cache_key = event.orig_ref
            elif hasattr(event, 'canonical_project_name'):
                trusted, project = self.manager.tenant.getProject(
                    event.canonical_project_name)
                if project:
                    change_key = project.source.getChangeKey(event)
                    event_ref_cache_key = change_key.reference
            else:
                # We handle promote, enqueue, and trigger events
                # above; it's unclear what other unhandled event would
                # cause an enqueue, but if it happens, log and
                # continue.
                self.manager.log.warning(
                    "Unable to identify triggering ref from event %s",
                    event)
            event_info = EventInfo.fromEvent(event, event_ref_cache_key)
            parameters = getattr(event, 'parameters', None)
        else:
            event_info = None
            parameters = None
        item = QueueItem.new(self.zk_context,
                             queue=self,
                             changes=changes,
                             event=event_info,
                             span_info=span_info,
                             enqueue_time=enqueue_time,
                             parameters=parameters,
                             debug=debug)
        self.enqueueItem(item)
        return item

    def enqueueItem(self, item):
        item._set(queue=self)
        if self.queue:
            item.updateAttributes(self.zk_context, item_ahead=self.queue[-1])
            with item.item_ahead.activeContext(self.zk_context):
                item.item_ahead.items_behind.append(item)
        with self.activeContext(self.zk_context):
            self.queue.append(item)

    def dequeueItem(self, item):
        if item in self.queue:
            with self.activeContext(self.zk_context):
                self.queue.remove(item)
        if item.item_ahead:
            with item.item_ahead.activeContext(self.zk_context):
                item.item_ahead.items_behind.remove(item)
                item.item_ahead.items_behind.extend(item.items_behind)
        for item_behind in item.items_behind:
            item_behind.updateAttributes(self.zk_context,
                                         item_ahead=item.item_ahead)

        item.delete(self.zk_context)
        # We use the dequeue time for stats reporting, but the queue
        # item will no longer be in Zookeeper at this point.
        item._set(dequeue_time=time.time())

    def moveItem(self, item, item_ahead):
        if item.item_ahead == item_ahead:
            return False
        # Remove from current location
        if item.item_ahead:
            with item.item_ahead.activeContext(self.zk_context):
                item.item_ahead.items_behind.remove(item)
                item.item_ahead.items_behind.extend(item.items_behind)
        for item_behind in item.items_behind:
            item_behind.updateAttributes(
                self.zk_context,
                item_ahead=item.item_ahead)
        # Add to new location
        item.updateAttributes(
            self.zk_context,
            item_ahead=item_ahead,
            items_behind=[])
        if item.item_ahead:
            with item.item_ahead.activeContext(self.zk_context):
                item.item_ahead.items_behind.append(item)
        return True

    def isActionable(self, item):
        if not self.window:
            return True
        return item in self.queue[:self.window]

    def increaseWindowSize(self):
        if not self.window:
            return
        with self.activeContext(self.zk_context):
            if self.window_increase_type == 'linear':
                self.window = min(
                    self.window_ceiling,
                    self.window + self.window_increase_factor)
            elif self.window_increase_type == 'exponential':
                self.window = min(
                    self.window_ceiling,
                    self.window * self.window_increase_factor)

    def decreaseWindowSize(self):
        if not self.window:
            return
        with self.activeContext(self.zk_context):
            if self.window_decrease_type == 'linear':
                self.window = max(
                    self.window_floor,
                    self.window - self.window_decrease_factor)
            elif self.window_decrease_type == 'exponential':
                self.window = max(
                    self.window_floor,
                    int(self.window / self.window_decrease_factor))


class Project(object):
    """A Project represents a git repository such as openstack/nova."""

    # NOTE: Projects should only be instantiated via a Source object
    # so that they are associated with and cached by their Connection.
    # This makes a Project instance a unique identifier for a given
    # project from a given source.

    def __init__(self, name, source, foreign=False):
        self.name = name
        self.source = source
        self.connection_name = source.connection.connection_name
        self.canonical_hostname = source.canonical_hostname
        self.canonical_name = source.canonical_hostname + '/' + name
        self.private_secrets_key = None
        self.public_secrets_key = None
        self.private_ssh_key = None
        self.public_ssh_key = None
        # foreign projects are those referenced in dependencies
        # of layout projects, this should matter
        # when deciding whether to enqueue their changes
        # TODOv3 (jeblair): re-add support for foreign projects if needed
        self.foreign = foreign

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Project %s>' % (self.name)

    def getSafeAttributes(self):
        return Attributes(name=self.name)

    def toDict(self):
        d = {}
        d['name'] = self.name
        d['connection_name'] = self.connection_name
        d['canonical_name'] = self.canonical_name
        return d


class ApiRoot(ConfigObject):
    def __init__(self, default_auth_realm=None):
        super().__init__()
        self.default_auth_realm = default_auth_realm
        self.access_rules = []

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, ApiRoot):
            return False
        return (self.default_auth_realm == other.default_auth_realm,
                self.access_rules == other.access_rules)

    def __repr__(self):
        return f'<ApiRoot realm={self.default_auth_realm}>'


class ImageBuildArtifact(zkobject.LockableZKObject):
    ROOT = "/zuul/images"
    IMAGES_PATH = "artifacts"
    LOCKS_PATH = "locks"

    class State(StrEnum):
        READY = "ready"
        DELETING = "deleting"

    STATES = set([
        State.READY,
        State.DELETING,
    ])

    def __init__(self):
        super().__init__()
        self._set(
            uuid=None,  # A random UUID for the image build artifact
            canonical_name=None,
            name=None,  # For validation builds
            project_canonical_name=None,  # For validation builds
            project_branch=None,  # For validation builds
            build_tenant_name=None,  # For validation builds
            build_uuid=None,  # The UUID of the build job
            format=None,
            md5sum=None,
            sha256=None,
            url=None,
            timestamp=None,
            validated=None,
            _state=None,
            state_time=None,
            # Attributes that are not serialized
            lock=None,
            is_locked=False,
            lock_holder=None,
        )

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value not in self.STATES:
            raise TypeError("'%s' is not a valid state" % value)
        self._state = value
        self.state_time = time.time()

    def __repr__(self):
        return (f"<ImageBuildArtifact {self.uuid} "
                f"state: {self.state} "
                f"canonical_name: {self.canonical_name} "
                f"build_uuid: {self.build_uuid} "
                f"validated: {self.validated}>")

    def getPath(self):
        return f"{self.ROOT}/{self.IMAGES_PATH}/{self.uuid}"

    def getLockPath(self):
        return f"{self.ROOT}/{self.LOCKS_PATH}/{self.uuid}"

    def toDict(self):
        # This is used by the export command
        return dict(
            uuid=self.uuid,
            name=self.name,
            canonical_name=self.canonical_name,
            project_canonical_name=self.project_canonical_name,
            project_branch=self.project_branch,
            build_tenant_name=self.build_tenant_name,
            build_uuid=self.build_uuid,
            format=self.format,
            md5sum=self.md5sum,
            sha256=self.sha256,
            url=self.url,
            timestamp=self.timestamp,
            validated=self.validated,
            _state=self._state,
            state_time=self.state_time,
        )

    def serialize(self, context):
        data = self.toDict()
        return json.dumps(data, sort_keys=True).encode("utf-8")


class ImageUpload(zkobject.LockableZKObject):
    ROOT = "/zuul/image-uploads"
    UPLOADS_PATH = "uploads"
    LOCKS_PATH = "locks"

    class State(StrEnum):
        READY = "ready"
        DELETING = "deleting"
        PENDING = "pending"
        UPLOADING = "uploading"
        FAILED = "failed"

    STATES = set([
        State.READY,
        State.DELETING,
        State.PENDING,
        State.UPLOADING,
        State.FAILED,
    ])

    def __init__(self):
        super().__init__()
        self._set(
            uuid=None,  # A random UUID for the image upload
            canonical_name=None,
            artifact_uuid=None,  # The UUID of the ImageBuildArtifact
            build_uuid=None,  # The UUID of the validate job
            endpoint_name=None,
            providers=None,
            config_hash=None,
            external_id=None,
            timestamp=None,
            validated=None,
            _state=None,
            state_time=None,
            attempt=0,
            # Attributes that are not serialized
            lock=None,
            is_locked=False,
            lock_holder=None,
        )

    def copy(self, context, attempt=None):
        upload_uuid = uuid.uuid4().hex
        if attempt is None:
            attempt = self.attempt + 1
        else:
            attempt = attempt
        return ImageUpload.new(
            context,
            uuid=upload_uuid,
            canonical_name=self.canonical_name,
            artifact_uuid=self.artifact_uuid,
            endpoint_name=self.endpoint_name,
            providers=self.providers,
            config_hash=self.config_hash,
            timestamp=self.timestamp,
            validated=self.validated,
            attempt=attempt,
            _state=ImageUpload.State.PENDING,
            state_time=time.time(),
        )

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value not in self.STATES:
            raise TypeError("'%s' is not a valid state" % value)
        self._state = value
        self.state_time = time.time()

    def __repr__(self):
        return (f"<ImageUpload {self.uuid} "
                f"state: {self.state} "
                f"attempt: {self.attempt} "
                f"endpoint: {self.endpoint_name} "
                f"artifact: {self.artifact_uuid} "
                f"validated: {self.validated} "
                f"external_id: {self.external_id}>")

    def getPath(self):
        return f"{self.ROOT}/{self.UPLOADS_PATH}/{self.uuid}"

    def getLockPath(self):
        return f"{self.ROOT}/{self.LOCKS_PATH}/{self.uuid}"

    def toDict(self):
        # This is used by the export command
        return dict(
            uuid=self.uuid,
            canonical_name=self.canonical_name,
            artifact_uuid=self.artifact_uuid,
            build_uuid=self.build_uuid,
            endpoint_name=self.endpoint_name,
            providers=self.providers,
            config_hash=self.config_hash,
            external_id=self.external_id,
            timestamp=self.timestamp,
            validated=self.validated,
            attempt=self.attempt,
            _state=self._state,
            state_time=self.state_time,
        )

    def serialize(self, context):
        data = self.toDict()
        return json.dumps(data, sort_keys=True).encode("utf-8")

    def isPermittedForProvider(self, image, provider):
        if provider.canonical_name in self.providers:
            return True

        endpoint = provider.getEndpoint()
        # MODEL_API: remove image.config_hash check after zuul 15
        return ((self.config_hash == image.config_hash or
                 self.config_hash == image.zuul_config_hash) and
                endpoint.canonical_name == self.endpoint_name)


class Image(ConfigObject):
    """A zuul or cloud image.

    Images are associated with labels and providers.
    """

    def __init__(self, name, image_type, description):
        super().__init__()
        self.name = name
        self.type = image_type
        self.description = description

    @property
    def project_canonical_name(self):
        return self.source_context.project_canonical_name

    @property
    def branch(self):
        return self.source_context.branch

    @property
    def canonical_name(self):
        return '/'.join([
            urllib.parse.quote_plus(self.project_canonical_name),
            urllib.parse.quote_plus(self.name),
        ])

    def __repr__(self):
        return '<Image %s>' % (self.name,)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Image):
            return False
        return (self.name == other.name and
                self.type == other.type and
                self.description == other.description)

    def toDict(self):
        return {
            'project_canonical_name': self.project_canonical_name,
            'name': self.name,
            'branch': self.branch,
            'type': self.type,
            'description': self.description,
        }

    def toConfig(self):
        return {
            'name': self.name,
            'type': self.type,
            'description': self.description,
        }


class Flavor(ConfigObject):
    """A node flavor.

    Flavors are associated with provider-specific instance types.
    """

    def __init__(self, name, description):
        super().__init__()
        self.name = name
        self.description = description

    @property
    def project_canonical_name(self):
        return self.source_context.project_canonical_name

    @property
    def canonical_name(self):
        return '/'.join([
            urllib.parse.quote_plus(self.project_canonical_name),
            urllib.parse.quote_plus(self.name),
        ])

    def __repr__(self):
        return '<Flavor %s>' % (self.name,)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Flavor):
            return False
        return (self.name == other.name and
                self.description == other.description)

    def toDict(self):
        return {
            'project_canonical_name': self.project_canonical_name,
            'name': self.name,
            'description': self.description,
        }

    def toConfig(self):
        return {
            'name': self.name,
            'description': self.description,
        }


class Label(ConfigObject):
    """A node label.

    Labels are associated with provider-specific instance types.
    """

    def __init__(self, name, image, flavor, description, min_ready,
                 max_ready_age, max_age, max_nodes, min_retention_time):
        super().__init__()
        self.name = name
        self.image = image
        self.flavor = flavor
        self.description = description
        self.min_ready = min_ready
        self.max_ready_age = max_ready_age
        self.max_age = max_age
        self.max_nodes = max_nodes
        self.min_retention_time = min_retention_time

    @property
    def project_canonical_name(self):
        return self.source_context.project_canonical_name

    @property
    def canonical_name(self):
        return '/'.join([
            urllib.parse.quote_plus(self.project_canonical_name),
            urllib.parse.quote_plus(self.name),
        ])

    def __repr__(self):
        return '<Label %s>' % (self.name,)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Label):
            return False
        return (self.name == other.name and
                self.image == other.image and
                self.flavor == other.flavor and
                self.description == other.description and
                self.min_ready == other.min_ready and
                self.max_ready_age == other.max_ready_age and
                self.max_age == other.max_age and
                self.max_nodes == other.max_nodes and
                self.min_retention_time == other.min_retention_time)

    def toDict(self):
        return {
            'project_canonical_name': self.project_canonical_name,
            'name': self.name,
            'image': self.image,
            'flavor': self.flavor,
            'description': self.description,
            'min_ready': self.min_ready,
            'max_ready_age': self.max_ready_age,
            'max_age': self.max_age,
            'max_nodes': self.max_nodes,
            'min_retention_time': self.min_retention_time,
        }

    def toConfig(self):
        return {
            'name': self.name,
            'image': self.image,
            'flavor': self.flavor,
            'description': self.description,
            # min-ready is only permitted at the top level
            'max-ready-age': self.max_ready_age,
            'max-age': self.max_age,
            # max-nodes is only permitted at the top level
            'min-retention-time': self.min_retention_time,
        }

    def validateReferences(self, layout):
        if not layout.images.get(self.image):
            raise Exception(
                f'The label "{self.name}" references an unknown image '
                f'"{self.image}"')
        if not layout.flavors.get(self.flavor):
            raise Exception(
                f'The label "{self.name}" references an unknown flavor '
                f'"{self.flavor}"')


class Section(ConfigObject):
    """A provider section.

    A section is part (or all) of a cloud provider.  It might be a
    region, or availability zone.
    """

    def __init__(self, name):
        super().__init__()
        self.name = name
        # Required for basic functionality
        self.parent = None
        self.abstract = None
        self.connection = None
        self.description = None
        # Unlike most config objects, the section is minimally parsed
        # and only fully realized when creating a provider.
        self.config = {}

    @property
    def canonical_name(self):
        return '/'.join([
            urllib.parse.quote_plus(
                self.source_context.project_canonical_name),
            urllib.parse.quote_plus(self.name),
        ])

    def __repr__(self):
        return '<Section %s>' % (self.name,)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Section):
            return False
        return (self.name == other.name and
                self.parent == other.parent and
                self.abstract == other.abstract and
                self.description == other.description and
                self.connection == other.connection and
                self.config == other.config)


class ProviderConfig(ConfigObject):
    """A provider configuration.

    This represents the provider config object.  It is combined with
    referenced sections in order to produce an instance of a
    zuul.provider.Provider which is the actual object used for
    managing images and nodes.
    """

    def __init__(self, name, section):
        super().__init__()
        self.name = name
        self.section = section
        self.description = None
        # Unlike most config objects, the provider config is minimally
        # parsed and only fully realized when creating a final
        # provider.
        self.config = {}

    @property
    def canonical_name(self):
        return '/'.join([
            urllib.parse.quote_plus(
                self.source_context.project_canonical_name),
            urllib.parse.quote_plus(self.name),
        ])

    def __repr__(self):
        return '<ProviderConfig %s>' % (self.name,)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, ProviderConfig):
            return False
        return (self.name == other.name and
                self.section == other.section and
                self.description == other.description and
                self.config == other.config)

    @staticmethod
    def _mergeDict(a, b):
        # Merge nested dictionaries and append lists if possible,
        # otherwise, overwrite the value in 'a' with the value in 'b'.
        ret = {}
        for k, av in a.items():
            if k not in b:
                ret[k] = av
        for k, bv in b.items():
            av = a.get(k)
            if (isinstance(av, (dict, types.MappingProxyType)) and
                isinstance(bv, (dict, types.MappingProxyType))):
                ret[k] = ProviderConfig._mergeDict(av, bv)
            elif (isinstance(av, (list, tuple)) and
                  isinstance(bv, (list, tuple))):
                ret[k] = list(av) + list(bv)
            else:
                ret[k] = bv
        return ret

    @staticmethod
    def _dropNone(d):
        ret = {}
        for k, v in list(d.items()):
            if v is not None:
                ret[k] = v
        return ret

    @staticmethod
    def updateFromDefaults(obj, defaults, layout_objects, schema,
                           schema_class=None):
        # Provide defaults from the layout objects
        layout_object = ProviderConfig._dropNone(
            layout_objects[obj['name']].toConfig())
        obj.update(ProviderConfig._mergeDict(layout_object, obj))

        # Special case for images since we don't know the type until
        # after we apply the defaults.
        if schema_class:
            if obj['type'] == 'zuul':
                schema = schema_class.getInheritableZuulImageSchema()
            else:
                schema = schema_class.getInheritableCloudImageSchema()

        while isinstance(schema.schema, vs.Schema):
            schema = schema.schema
        # If schema.schema.keys ever becomes problematic due to
        # schema complexity, we could convert the above methods
        # into methods that just return a list of keys.
        # Convert the key to string since it may be an instance of Optional
        for k in (str(k) for k in schema.schema.keys()):
            if k not in obj and k in defaults:
                obj[k] = defaults[k]
            # For tags we don't want the label config to override the
            # defaults. Instead they should be deep merged.
            elif k == "tags" and k in obj and k in defaults:
                obj[k].update(
                    ProviderConfig._mergeDict(defaults[k], obj[k]))

    @staticmethod
    def applyConfig(config, section, layout, schema_class):
        # Return an updated config by applying the config from the
        # specified section/provider
        other_config = copy.deepcopy(section.config)
        old_images = {i['name']: i for i in config.get('images', [])}
        old_flavors = {i['name']: i for i in config.get('flavors', [])}
        old_labels = {i['name']: i for i in config.get('labels', [])}
        # Save the images, flavors, labels before we merge
        new_images = {i['name']: copy.deepcopy(i)
                      for i in other_config.pop('images', [])}
        new_flavors = {i['name']: copy.deepcopy(i)
                       for i in other_config.pop('flavors', [])}
        new_labels = {i['name']: copy.deepcopy(i)
                      for i in other_config.pop('labels', [])}

        # Special handling for max-retain-count: make sure it never
        # increases.
        old_retain_count_max = config.get(
            'image-defaults', {}).get('max-retain-count')
        new_retain_count_max = other_config.get(
            'image-defaults', {}).get('max-retain-count')
        if (old_retain_count_max is not None and
            new_retain_count_max is not None and
            new_retain_count_max > old_retain_count_max):
            raise Exception(
                "Unable to increase image max-retain-count")

        # Merge the config dicts, except for images, flavors, labels
        config = ProviderConfig._mergeDict(config, other_config)
        # Apply the defaults to our saved images, flavors, labels
        image_defaults = config.get('image-defaults', {})
        image_defaults_schema = schema_class.getInheritableImageSchema()
        image_defaults_schema(image_defaults)

        flavor_defaults = config.get('flavor-defaults', {})
        flavor_defaults_schema = schema_class.getInheritableFlavorSchema()
        flavor_defaults_schema(flavor_defaults)

        label_defaults = config.get('label-defaults', {})
        label_defaults_schema = schema_class.getInheritableLabelSchema()
        label_defaults_schema(label_defaults)

        # If the new objects are marked final, then go ahead and assign
        # their defaults so that we "freeze" them at this level.
        for image in new_images.values():
            if image.get('final') or image_defaults.get('final'):
                ProviderConfig.updateFromDefaults(
                    image, image_defaults, layout.images, None, schema_class)
        for flavor in new_flavors.values():
            if flavor.get('final') or flavor_defaults.get('final'):
                ProviderConfig.updateFromDefaults(
                    flavor, flavor_defaults, layout.flavors,
                    flavor_defaults_schema)
        for label in new_labels.values():
            if label.get('final') or label_defaults.get('final'):
                ProviderConfig.updateFromDefaults(
                    label, label_defaults, layout.labels,
                    label_defaults_schema)

        # Redefinition:
        # If final=allow-override then a new definition can override
        # Otherwise raise an error
        for obj_type, new_objects, old_objects in [
                ('image', new_images, old_images),
                ('flavor', new_flavors, old_flavors),
                ('label', new_labels, old_labels),
        ]:
            for obj_name, obj in new_objects.items():
                if obj_name in old_objects:
                    old_obj = old_objects[obj_name]
                    old_final = old_obj.get('final', False)
                    if old_final and old_final != 'allow-override':
                        raise Exception(
                            f"Unable to redefine final {obj_type} {obj_name}")
                    # Special handling for max-retain-count: make sure
                    # it never increases.
                    if obj_type == 'image':
                        if (old_obj.get('max-retain-count') is not None and
                            obj.get('max-retain-count') is not None):
                            if (obj['max-retain-count'] >
                                old_obj['max-retain-count']):
                                raise Exception(
                                    "Unable to increase image "
                                    "max-retain-count")
                old_objects[obj_name] = obj

        # Add the images, flavors, labels back into the new config
        config['images'] = list(old_images.values())
        config['flavors'] = list(old_flavors.values())
        config['labels'] = list(old_labels.values())
        return config

    def flattenConfig(self, layout, connections):
        inheritance_path = [self]
        parent_name = self.section
        # We will take the first available connection name as we walk
        # up the hierarchy, but we won't allow it to be redefined (so
        # from the user's perspective, it doesn't matter which order
        # we use internally).
        connection_name = self.config.get('connection')
        while parent_name:
            parent_section = layout.sections[parent_name]
            inheritance_path.append(parent_section)
            new_connection_name = parent_section.connection
            if new_connection_name:
                if connection_name:
                    raise Exception(
                        'Unable to change connections in provider inheritance')
                connection_name = new_connection_name
            parent_name = parent_section.parent
        inheritance_path.reverse()

        config = {}
        connection = connections.get(connection_name)
        if connection is None:
            raise UnknownConnection(connection_name)

        # Apply inheritance
        schema_class = connection.driver.getProviderSchemaClass()()
        for section in inheritance_path:
            config = ProviderConfig.applyConfig(
                config, section, layout, schema_class)

        for image in config.get('images', []):
            # Handle default inheritance for any non-final images.
            if not image.get('final'):
                ProviderConfig.updateFromDefaults(
                    image, config.get('image-defaults', {}),
                    layout.images, None, schema_class)
        flavor_defaults_schema = schema_class.getInheritableFlavorSchema()
        for flavor in config.get('flavors', []):
            if not flavor.get('final'):
                ProviderConfig.updateFromDefaults(
                    flavor, config.get('flavor-defaults', {}),
                    layout.flavors, flavor_defaults_schema)
        label_defaults_schema = schema_class.getInheritableLabelSchema()
        for label in config.get('labels', []):
            if not label.get('final'):
                ProviderConfig.updateFromDefaults(
                    label, config.get('label-defaults', {}),
                    layout.labels, label_defaults_schema)

        # Validate the overall schema
        schema = connection.driver.getProviderSchema()
        schema(config)

        # Complex validations
        for image in config.get('images', []):
            if ((rc := image.get('retain-count')) is not None and
                (rcmax := image.get('max-retain-count')) is not None):
                if rc > rcmax:
                    raise Exception(
                        f"Unable to retain {rc} images since "
                        "that is more than the max-retain-count "
                        f"of {rcmax}")

        # Set internal attributes
        self._setInternalAttributes(layout, config, schema_class)

        # Make sure the launcher will be able to parse this
        internal_schema = connection.driver.getProviderSchema(internal=True)
        internal_schema(config)

        return config

    def _setInternalAttributes(self, layout, config, schema_class):
        # Set config hashes
        image_hashes = {}
        zuul_image_hash_keys = set(schema_class.getImageConfigKeys())
        for image in config.get('images', []):
            # This is used for identifying unique image configurations
            # across multiple providers.
            image_object = layout.images[image['name']]
            zuul_image_hash_dict = {
                k: v for k, v in image.items() if k in zuul_image_hash_keys
            }
            # This is used for detecting identical uploaded content
            image['zuul_config_hash'] = hashlib.sha256(
                json.dumps(zuul_image_hash_dict, sort_keys=True).encode(
                    "utf8")).hexdigest()
            # This is used for detecting identical label configuration
            image['config_hash'] = hashlib.sha256(
                json.dumps(image, sort_keys=True).encode(
                    "utf8")).hexdigest()
            image['project_canonical_name'] =\
                image_object.project_canonical_name
            image['branch'] = image_object.branch
            image_hashes[image['name']] = image['config_hash']
        flavor_hashes = {}
        for flavor in config.get('flavors', []):
            flavor_object = layout.flavors[flavor['name']]
            flavor['config_hash'] = hashlib.sha256(
                json.dumps(flavor, sort_keys=True).encode("utf8")).hexdigest()
            flavor['project_canonical_name'] =\
                flavor_object.project_canonical_name
            flavor_hashes[flavor['name']] = flavor['config_hash']
        for label in config.get('labels', []):
            label_object = layout.labels[label['name']]
            try:
                label['config_hash'] = self._getLabelConfigHash(
                    label, image_hashes, flavor_hashes)
            except Exception:
                # We might miss some flavor or label, but this will be
                # caught later during config validation.
                label['config_hash'] = None
            label['project_canonical_name'] =\
                label_object.project_canonical_name
            label['min-ready'] = label_object.min_ready
            label['max-nodes'] = label_object.max_nodes

    def _getLabelConfigHash(self, label, image_hashes, flavor_hashes):
        label_hash = hashlib.sha256(
            json.dumps(label, sort_keys=True).encode("utf8")
        ).hexdigest()
        image_hash = image_hashes[label["image"]]
        flavor_hash = flavor_hashes[label["flavor"]]
        # XOR relevant config hashes
        _hash = (
            int(label_hash, 16)
            ^ int(image_hash, 16)
            ^ int(flavor_hash, 16)
        )
        return f"{_hash:x}"


class Node(ConfigObject):
    """A single node for use by a job.

    This may represent a request for a node, or an actual node
    provided by Nodepool.
    """

    def __init__(self, name, label):
        super(Node, self).__init__()
        self.name = name
        self.label = label
        self.id = None
        self.lock = None
        self.hold_job = None
        self.comment = None
        self.user_data = None
        # Attributes from Nodepool
        self._state = 'unknown'
        self.state_time = time.time()
        self.host_id = None
        self.interface_ip = None
        self.public_ipv4 = None
        self.private_ipv4 = None
        self.public_ipv6 = None
        self.private_ipv6 = None
        self.connection_port = 22
        self.connection_type = None
        self.kubernetes_connection = {}
        self.username = None
        self.python_path = None
        self.shell_type = None
        self.slot = None
        self._keys = []
        self.az = None
        self.cloud = None
        self.provider = None
        self.region = None
        self.hold_expiration = None
        self.resources = None
        self.allocated_to = None
        self.attributes = {}
        self.tenant_name = None
        self.requestor = None
        self.reuse = False
        self.node_properties = {}

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value not in NODE_STATES:
            raise TypeError("'%s' is not a valid state" % value)
        self._state = value
        self.state_time = time.time()

    def __repr__(self):
        return '<Node %s %s:%s>' % (self.id, self.name, self.label)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        return (self.name == other.name and
                self.label == other.label and
                self.id == other.id)

    def toDict(self, internal_attributes=False):
        d = {}
        d["id"] = self.id
        d['state'] = self.state
        d['hold_job'] = self.hold_job
        d['comment'] = self.comment
        d['user_data'] = self.user_data
        d['tenant_name'] = self.tenant_name
        d['requestor'] = self.requestor
        for k in self._keys:
            d[k] = getattr(self, k)
        if internal_attributes:
            # These attributes are only useful for the rpc serialization
            d['name'] = self.name[0]
            d['aliases'] = list(self.name[1:])
            d['label'] = self.label
        return d

    def updateFromDict(self, data):
        self._state = data['state']
        keys = []
        for k, v in data.items():
            if k in ['state', 'name', 'aliases']:
                continue
            keys.append(k)
            setattr(self, k, v)
        self._keys = keys

    @classmethod
    def fromDict(cls, data):
        aliases = data.get('aliases', [])
        node = cls([data["name"]] + aliases, data["label"])
        node.updateFromDict(data)
        return node


class Group(ConfigObject):
    """A logical group of nodes for use by a job.

    A Group is a named set of node names that will be provided to
    jobs in the inventory to describe logical units where some subset of tasks
    run.
    """

    def __init__(self, name, nodes):
        super(Group, self).__init__()
        self.name = name
        self.nodes = nodes

    def __repr__(self):
        return '<Group %s %s>' % (self.name, str(self.nodes))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Group):
            return False
        return (self.name == other.name and
                self.nodes == other.nodes)

    def toDict(self):
        return {
            'name': self.name,
            'nodes': self.nodes
        }

    @classmethod
    def fromDict(cls, data):
        return cls(data["name"], data["nodes"])


class NodeSet(ConfigObject):
    """A set of nodes.

    In configuration, NodeSets are attributes of Jobs indicating that
    a Job requires nodes matching this description.

    They may appear as top-level configuration objects and be named,
    or they may appears anonymously in in-line job definitions.
    """

    def __init__(self, name=None):
        super(NodeSet, self).__init__()
        self.name = name or ''
        self.nodes = OrderedDict()
        self.groups = OrderedDict()
        self.alternatives = []

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, NodeSet):
            return False
        return (self.name == other.name and
                self.nodes == other.nodes and
                self.groups == other.groups and
                self.alternatives == other.alternatives)

    def toDict(self):
        d = {}
        d['name'] = self.name
        d['nodes'] = []
        for node in self.nodes.values():
            d['nodes'].append(node.toDict(internal_attributes=True))
        d['groups'] = []
        for group in self.groups.values():
            d['groups'].append(group.toDict())
        d['alternatives'] = []
        for alt in self.alternatives:
            if isinstance(alt, NodeSet):
                d['alternatives'].append(alt.toDict())
            else:
                d['alternatives'].append(alt)
        return d

    @classmethod
    def fromDict(cls, data):
        nodeset = cls(data["name"])
        for node in data["nodes"]:
            nodeset.addNode(Node.fromDict(node))
        for group in data["groups"]:
            nodeset.addGroup(Group.fromDict(group))
        for alt in data.get('alternatives', []):
            if isinstance(alt, str):
                if isinstance(alt, str):
                    nodeset.addAlternative(alt)
                else:
                    nodeset.addAlternative(NodeSet.fromDict(alt))
        return nodeset

    def copy(self):
        n = NodeSet(self.name)
        for name, node in self.nodes.items():
            n.addNode(Node(node.name, node.label))
        for name, group in self.groups.items():
            n.addGroup(Group(group.name, group.nodes[:]))
        for alt in self.alternatives:
            if isinstance(alt, str):
                n.addAlternative(alt)
            else:
                n.addAlternative(alt.copy())
        return n

    def addNode(self, node):
        for name in node.name:
            if name in self.nodes:
                raise Exception("Duplicate node in %s" % (self,))
        self.nodes[tuple(node.name)] = node

    def getNodes(self):
        return list(self.nodes.values())

    def addGroup(self, group):
        if group.name in self.groups:
            raise Exception("Duplicate group in %s" % (self,))
        self.groups[group.name] = group

    def getGroups(self):
        return list(self.groups.values())

    def addAlternative(self, alt):
        self.alternatives.append(alt)

    def flattenAlternativeLabels(self, results=None):
        # Get the complete list of labels referenced by this nodeset
        # and any alternatives for validation purposes.  This does not
        # dereference other nodesets by name (it only looks at this
        # nodeset).
        if results is None:
            results = set()
        for n in self.nodes.values():
            results.add(n.label)
        for alt in self.alternatives:
            if isinstance(alt, NodeSet):
                alt.flattenAlternativeLabels(results)
        return results

    def flattenAlternatives(self, layout):
        alts = []
        history = []
        self._flattenAlternatives(layout, self, alts, history)
        return alts

    def _flattenAlternatives(self, layout, nodeset,
                             alternatives, history):
        if isinstance(nodeset, str):
            # This references an existing named nodeset in the layout.
            ns = layout.nodesets.get(nodeset)
            if ns is None:
                raise NodesetNotFoundError(nodeset)
        else:
            ns = nodeset
        if ns in history:
            raise Exception(f'Nodeset cycle detected on "{nodeset}"')
        history.append(ns)
        if ns.alternatives:
            for alt in ns.alternatives:
                self._flattenAlternatives(layout, alt, alternatives, history)
        else:
            alternatives.append(ns)

    def validateReferences(self, layout):
        self.flattenAlternatives(layout)

        if not layout.tenant.use_nodepool:
            requested_labels = self.flattenAlternativeLabels()
            for name in requested_labels:
                if name not in layout.labels:
                    raise LabelNotFoundError(name)

    def __repr__(self):
        if self.name:
            name = self.name + ' '
        else:
            name = ''
        return '<NodeSet %s%s>' % (name, list(self.nodes.values()))

    def __len__(self):
        return len(self.nodes)


class NodeRequest(object):
    """A request for a set of nodes."""

    def __init__(self, requestor, build_set_uuid, tenant_name, pipeline_name,
                 job_uuid, job_name, labels, provider, relative_priority,
                 event_id=None, span_info=None):
        self.requestor = requestor
        self.build_set_uuid = build_set_uuid
        self.tenant_name = tenant_name
        self.pipeline_name = pipeline_name
        self.job_uuid = job_uuid
        # The requestor doesn't need the job name anymore after moving
        # to job UUIDs, but we should keep it in the requestor data,
        # since it can be used in Nodepool for dynamic label tags.
        self.job_name = job_name
        self.labels = labels
        self.nodes = []
        self._state = STATE_REQUESTED
        self.requested_time = time.time()
        self.state_time = time.time()
        self.created_time = None
        self.stat = None
        self.relative_priority = relative_priority
        self.provider = provider
        self.id = None
        self._zk_data = {}  # Data that we read back from ZK
        self.event_id = event_id
        self.span_info = span_info
        # Zuul internal flags (not stored in ZK so they are not
        # overwritten).
        self.failed = False
        self.canceled = False

    def reset(self):
        # Reset the node request for re-submission
        self._zk_data = {}
        # Remove any real node information
        self.nodes = []
        self.id = None
        self.state = STATE_REQUESTED
        self.stat = None
        self.failed = False
        self.canceled = False

    @property
    def zuul_event_id(self):
        return self.event_id

    @property
    def fulfilled(self):
        return (self._state == STATE_FULFILLED) and not self.failed

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value not in REQUEST_STATES:
            raise TypeError("'%s' is not a valid state" % value)
        self._state = value
        self.state_time = time.time()

    def __repr__(self):
        return '<NodeRequest %s %s>' % (self.id, self.labels)

    def toDict(self):
        """
        Serialize a NodeRequest so it can be stored in ZooKeeper.

        Any additional information must be stored in the requestor_data field,
        so Nodepool doesn't strip the information when it fulfills the request.
        """
        # Start with any previously read data
        d = self._zk_data.copy()
        # The requestor_data is opaque to nodepool and won't be touched by
        # nodepool when it fulfills the request.
        d["requestor_data"] = {
            "build_set_uuid": self.build_set_uuid,
            "tenant_name": self.tenant_name,
            "pipeline_name": self.pipeline_name,
            "job_uuid": self.job_uuid,
            "job_name": self.job_name,
            "span_info": self.span_info,
        }
        d.setdefault('node_types', self.labels)
        d.setdefault('requestor', self.requestor)
        d.setdefault('created_time', self.created_time)
        d.setdefault('provider', self.provider)
        # We might change these
        d['state'] = self.state
        d['state_time'] = self.state_time
        d['relative_priority'] = self.relative_priority
        d['event_id'] = self.event_id
        d['tenant_name'] = self.tenant_name
        return d

    def updateFromDict(self, data):
        self._zk_data = data
        self._state = data['state']
        self.state_time = data['state_time']
        self.relative_priority = data.get('relative_priority', 0)
        self.event_id = data['event_id']
        # Make sure we don't update tenant_name to 'None'.
        # This can happen if nodepool does not report one back and leads
        # to errors at other places where we rely on that info.
        if 'tenant_name' in data:
            self.tenant_name = data['tenant_name']
        self.nodes = data.get('nodes', [])
        self.created_time = data.get('created_time')

    @classmethod
    def fromDict(cls, data):
        """Deserialize a NodeRequest from the data in ZooKeeper.

        Any additional information must be stored in the requestor_data field,
        so Nodepool doesn't strip the information when it fulfills the request.
        """
        # The requestor_data contains zuul-specific information which is opaque
        # to nodepool and returned as-is when the NodeRequest is fulfilled.
        requestor_data = data["requestor_data"]
        if requestor_data is None:
            requestor_data = {}
        request = cls(
            requestor=data["requestor"],
            build_set_uuid=requestor_data.get("build_set_uuid"),
            tenant_name=requestor_data.get("tenant_name"),
            pipeline_name=requestor_data.get("pipeline_name"),
            job_uuid=requestor_data.get("job_uuid"),
            job_name=requestor_data.get("job_name"),
            labels=data["node_types"],
            provider=data["provider"],
            relative_priority=data.get("relative_priority", 0),
            span_info=requestor_data.get("span_info"),
        )

        request.updateFromDict(data)

        return request


class NodesetInfo:

    def __init__(self, nodes=None, provider=None, zone=None):
        self.nodes = nodes or []
        self.provider = provider
        self.zone = zone

    @classmethod
    def fromDict(cls, data):
        return cls(**data)

    def toDict(self):
        return dict(
            nodes=self.nodes,
            provider=self.provider,
            zone=self.zone,
        )


@total_ordering
class NodesetRequest(zkobject.LockableZKObject):

    class State(StrEnum):
        REQUESTED = "requested"
        ACCEPTED = "accepted"
        FULFILLED = "fulfilled"
        FAILED = "failed"
        # Only used for testing
        TEST_HOLD = "test-hold"

    FINAL_STATES = (
        State.FULFILLED,
        State.FAILED,
    )

    REVISE_STATES = (
        State.REQUESTED,
        State.ACCEPTED,
        State.TEST_HOLD,
    )

    ROOT = "/zuul/nodeset"
    REQUESTS_PATH = "requests"
    LOCKS_PATH = "locks"

    # Tracks whether the request has emitted the completion event to
    # notify the scheduler
    class EventState(IntEnum):
        PENDING = 0
        COMPLETE = 1

    def __init__(self):
        super().__init__()
        revision = NodesetRequestRevision()
        revision._set(request=self)
        self._set(
            uuid=uuid4().hex,
            state=self.State.REQUESTED,
            event_state=self.EventState.PENDING,
            tenant_name="",
            pipeline_name="",
            buildset_uuid="",
            job_uuid="",
            job_name="",
            labels=[],
            priority=0,
            request_time=time.time(),
            zuul_event_id="",
            span_info=None,
            image_names=None,
            image_upload_uuid=None,
            preferred_provider=None,
            # Revisable attributes
            _relative_priority=0,
            # A dict of info about the node we have assigned to each label
            provider_node_data=[],
            # Attributes that are not serialized
            lock=None,
            is_locked=False,
            lock_holder=None,
            _revision=revision,
            # Attributes set by the launcher
            _lscores=None,
        )

    @property
    def relative_priority(self):
        if self._revision.getZKVersion() is not None:
            return self._revision.relative_priority
        return self._relative_priority

    def __lt__(self, other):
        return (
            (self.priority, self.relative_priority, self.request_time)
            < (other.priority, other.relative_priority, other.request_time)
        )

    def revise(self, ctx, relative_priority):
        self._revision.force(ctx, relative_priority=relative_priority)

    def addProviderNode(self, provider_node):
        self.provider_node_data.append(dict(
            uuid=provider_node.uuid,
            executor_zone=provider_node.executor_zone,
            failed_providers=[],
        ))

    def updateProviderNode(self, index, provider_node,
                           add_failed_provider=None):
        data = self.provider_node_data[index]
        data.update(dict(
            uuid=provider_node.uuid,
            executor_zone=provider_node.executor_zone,
        ))
        if add_failed_provider is not None:
            data['failed_providers'].append(add_failed_provider)

    def getProviderNodeData(self, index):
        if index < len(self.provider_node_data):
            return self.provider_node_data[index]
        return None

    def getSafeAttributes(self):
        attrs = dict(
            request_id=self.uuid,
        )
        request_attrs = (
            "pipeline_name",
            "buildset_uuid",
            "job_uuid",
            "job_name",
            "labels",
            "zuul_event_id",
            "tenant_name",
        )
        attrs.update({
            a: getattr(self, a, None) for a in request_attrs
        })
        return Attributes(**attrs)

    @property
    def fulfilled(self):
        return self.state == self.State.FULFILLED

    @property
    def nodes(self):
        return [n['uuid'] for n in self.provider_node_data]

    @property
    def executor_zones(self):
        return [n.get('executor_zone') for n in self.provider_node_data]

    @property
    def created_time(self):
        return self.request_time

    @property
    def id(self):
        return self.uuid

    def getPath(self):
        return self._getPath(self.uuid)

    @classmethod
    def _getPath(cls, request_id):
        return f"{cls.ROOT}/{cls.REQUESTS_PATH}/{request_id}"

    def getLockPath(self):
        return f"{self.ROOT}/{self.LOCKS_PATH}/{self.uuid}"

    def serialize(self, context):
        data = dict(
            uuid=self.uuid,
            state=self.state,
            event_state=self.event_state,
            tenant_name=self.tenant_name,
            pipeline_name=self.pipeline_name,
            buildset_uuid=self.buildset_uuid,
            job_uuid=self.job_uuid,
            job_name=self.job_name,
            labels=self.labels,
            priority=self.priority,
            request_time=self.request_time,
            zuul_event_id=self.zuul_event_id,
            span_info=self.span_info,
            image_names=self.image_names,
            image_upload_uuid=self.image_upload_uuid,
            preferred_provider=self.preferred_provider,
            provider_node_data=self.provider_node_data,
            _relative_priority=self.relative_priority,
        )
        return json.dumps(data, sort_keys=True).encode("utf-8")

    def deserialize(self, raw, context, extra=None):
        if context is not None:
            try:
                self._revision.refresh(context)
            except NoNodeError:
                pass
        return super().deserialize(raw, context, extra)

    def __repr__(self):
        return (f"<NodesetRequest uuid={self.uuid}, state={self.state},"
                f" labels={self.labels}, path={self.getPath()}>")

    def getSpanAttributes(self):
        return dict(
            uuid=self.uuid,
            state=self.state,
            job=self.job_name,
            zuul_event_id=self.zuul_event_id,
        )


class NodesetRequestRevision(zkobject.ZKObject):
    # We don't want to re-create the request in case it was deleted
    makepath = False
    log_error_missing = False

    def __init__(self):
        super().__init__()
        self._set(
            relative_priority=0,
            # Not serialized
            request=None,
        )

    def force(self, context, **kw):
        self._set(**kw)
        if getattr(self, '_zstat', None) is None:
            try:
                return self.internalCreate(context)
            except NodeExistsError:
                pass
        data = self._trySerialize(context)
        self._save(context, data)

    def getPath(self):
        return f"{self.request.getPath()}/revision"

    def serialize(self, context):
        data = dict(
            relative_priority=self.relative_priority,
        )
        return json.dumps(data, sort_keys=True).encode("utf-8")


@total_ordering
class ProviderNode(zkobject.PolymorphicZKObjectMixin,
                   zkobject.LockableZKObject):

    class State(StrEnum):
        REQUESTED = "requested"
        BUILDING = "building"
        READY = "ready"
        FAILED = "failed"
        TEMPFAILED = "tempfailed"
        IN_USE = "in-use"
        USED = "used"
        OUTDATED = "outdated"
        DEREGISTER = "deregister"
        HOLD = "hold"
        SNAPSHOT = "snapshot"
        SLOT_HOST = "slot-host"

    # States where quota counts
    ALLOCATED_STATES = (
        State.BUILDING,
        State.READY,
        State.IN_USE,
        State.USED,
        State.OUTDATED,
        State.DEREGISTER,
        State.HOLD,
        State.SNAPSHOT,
        State.SLOT_HOST,
    )

    FINAL_STATES = (
        State.READY,
        State.FAILED,
        State.SLOT_HOST,
    )

    FAILED_STATES = (
        State.TEMPFAILED,
        State.FAILED,
    )

    CREATE_STATES = (
        State.REQUESTED,
        State.BUILDING,
    )

    LAUNCHER_STATES = (
        State.REQUESTED,
        State.BUILDING,
        State.FAILED,
        State.USED,
        State.OUTDATED,
        State.DEREGISTER,
    )

    # The order of preference for unassigned node states (we want
    # ready nodes first, then building).
    ASSIGNABLE_STATES = {
        State.READY: 0,
        State.BUILDING: 1,
    }

    ROOT = "/zuul/nodes"
    NODES_PATH = "nodes"
    LOCKS_PATH = "locks"

    def __init__(self):
        super().__init__()
        snapshot = ProviderNodeSnapshot()
        snapshot._set(node=self)
        assignment = ProviderNodeAssignment()
        assignment._set(node=self)
        lifecycle = ProviderNodeLifecycle()
        lifecycle._set(node=self)
        self._set(
            uuid=uuid4().hex,
            zuul_event_id=None,
            span_info=None,
            max_ready_age=None,
            max_age=None,
            min_retention_time=None,
            state=self.State.REQUESTED,
            request_time=time.time(),
            state_time=time.time(),
            label="",
            label_config_hash=None,
            label_aliases=[],
            tags={},
            connection_name="",
            endpoint_name=None,
            create_state={},
            delete_state={},
            host_key_checking=None,
            comment=None,
            main_node_id=None,
            subnodes=[],
            # Node data
            boot_timeout=None,
            snapshot_timeout=None,
            snapshot_expiration=None,
            executor_zone=None,
            host_id=None,
            interface_ip=None,
            public_ipv4=None,
            private_ipv4=None,
            public_ipv6=None,
            private_ipv6=None,
            connection_port=22,
            connection_type=None,
            kubernetes_connection={},
            username=None,
            python_path=None,
            shell_type=None,
            slot=None,
            az=None,
            cloud=None,
            provider=None,
            region=None,
            hold_expiration=None,
            attributes={},
            host_keys=[],
            quota=QuotaInformation(),
            image_upload_uuid=None,
            reuse=False,
            # This is provided to the job verbatim
            node_properties={},
            # Attributes that are not serialized
            is_locked=False,
            lock_holder=None,
            create_state_machine=None,
            delete_state_machine=None,
            nodescan_request=None,
            snapshot=snapshot,
            assignment=assignment,
            lifecycle=lifecycle,
            # Attributes set by the launcher
            _lscores=None,
        )

    def __repr__(self):
        return (f"<{self.__class__.__name__} uuid={self.uuid},"
                f" label={self.label}, state={self.state},"
                f" provider={self.provider}>")

    def __lt__(self, other):
        return (
            (self.request_time or 0) < (other.request_time or 0)
        )

    def getPath(self):
        return self._getPath(self.uuid)

    @classmethod
    def _getPath(cls, node_id):
        return f"{cls.ROOT}/{cls.NODES_PATH}/{node_id}"

    def getLockPath(self):
        return f"{self.ROOT}/{self.LOCKS_PATH}/{self.uuid}"

    @property
    def all_labels(self):
        return [self.label] + self.label_aliases

    @property
    def request_id(self):
        if self.assignment.getZKVersion() is not None:
            return self.assignment.request_id
        return None

    @property
    def min_request_version(self):
        if self.assignment.getZKVersion() is not None:
            return self.assignment.min_request_version
        return None

    @property
    def min_request_zxid(self):
        # The request is always updated after assignment, therefore,
        # the request mzxid will be greater than the assigment czxid.
        # Therefore, it is sufficient to wait for the assignment czxid
        # to know that the request request will exist; it may have
        # been updated, but min_request_version can be used for that.
        if (r := self.assignment.getZKczxid()) is not None:
            return r
        return None

    @property
    def tenant_name(self):
        if self.assignment.getZKVersion() is not None:
            return self.assignment.tenant_name
        return None

    def assign(self, context, request_id, tenant_name,
               min_request_version=None):
        self.assignment._set(
            request_id=request_id,
            min_request_version=min_request_version,
            tenant_name=tenant_name,
        )
        self.assignment.internalCreate(context)

    def unassign(self, context):
        try:
            self.assignment.delete(context)
        except NoNodeError:
            pass
        self.assignment._clear()

    @property
    def next_state(self):
        if self.lifecycle.getZKVersion() is not None:
            return self.lifecycle.next_state
        return None

    def setNextState(self, context, next_state):
        if self.lifecycle.getZKVersion() is not None:
            self.lifecycle.updateAttributes(
                context,
                next_state=next_state)
        else:
            self.lifecycle._set(next_state=next_state)
            self.lifecycle.internalCreate(context)

    def clearNextState(self, context):
        try:
            self.lifecycle.delete(context)
        except NoNodeError:
            pass
        self.lifecycle._clear()

    def deserialize(self, raw, context, extra=None):
        # Update our UUID first so that our subnode paths are accurate
        data = super().deserialize(raw, context)
        self._set(uuid=data.get('uuid'))
        try:
            self.assignment.refresh(context)
        except NoNodeError:
            self.assignment._clear()
        try:
            self.lifecycle.refresh(context)
        except NoNodeError:
            self.lifecycle._clear()
        resources = data.get('quota') or {}
        data['quota'] = QuotaInformation(**resources)
        # TODO: remove this backwards compat code at any time
        min_request_version = data.pop('min_request_version', None)
        request_id = data.pop('request_id', None)
        tenant_name = data.pop('tenant_name', None)
        if min_request_version or request_id or tenant_name:
            try:
                self.assign(
                    context, request_id, tenant_name, min_request_version)
            except NodeExistsError:
                pass
        # End TODO
        data.setdefault('label_aliases', [])
        return data

    def serialize(self, context):
        if self.create_state_machine:
            self._set(create_state=self.create_state_machine.toDict())
        if self.delete_state_machine:
            self._set(delete_state=self.delete_state_machine.toDict())

        data = dict(
            zuul_event_id=self.zuul_event_id,
            span_info=self.span_info,
            max_ready_age=self.max_ready_age,
            max_age=self.max_age,
            min_retention_time=self.min_retention_time,
            request_time=self.request_time,
            state=self.state,
            state_time=self.state_time,
            label=self.label,
            label_config_hash=self.label_config_hash,
            label_aliases=self.label_aliases,
            tags=self.tags,
            connection_name=self.connection_name,
            endpoint_name=self.endpoint_name,
            comment=self.comment,
            main_node_id=self.main_node_id,
            subnodes=self.subnodes,
            create_state=self.create_state,
            delete_state=self.delete_state,
            quota=self.quota.getResources(),
            image_upload_uuid=self.image_upload_uuid,
            **self.getNodeData(serialize_node=True),
            **self.getDriverData(),
        )
        return json.dumps(data, sort_keys=True).encode("utf-8")

    def setState(self, new_state):
        if self.state == new_state:
            return
        self.state = new_state
        self.state_time = time.time()

    def hasExpired(self):
        if self.min_retention_time:
            # Min. retention time takes priority over max-age
            if (self.request_time + self.min_retention_time) > time.time():
                return False
        if self.max_age:
            if (self.request_time + self.max_age) < time.time():
                return True
        if self.max_ready_age and self.state == self.State.READY:
            if (self.state_time + self.max_ready_age) < time.time():
                return True
        return False

    def hasHoldExpired(self):
        if not self.hold_expiration:
            return False
        if self.state != self.State.HOLD:
            return False
        return (self.state_time + self.hold_expiration) < time.time()

    def getDriverData(self):
        return dict()

    def getNodeData(self, serialize_node=False):
        # This gets stored on the ProviderNode object, but also copied
        # to the Node object for use by the executor.
        ret = dict(
            uuid=self.uuid,
            attributes=self.attributes,
            az=self.az,
            boot_timeout=self.boot_timeout,
            snapshot_timeout=self.snapshot_timeout,
            snapshot_expiration=self.snapshot_expiration,
            executor_zone=self.executor_zone,
            cloud=self.cloud,
            connection_port=self.connection_port,
            connection_type=self.connection_type,
            kubernetes_connection=self.kubernetes_connection,
            username=self.username,
            python_path=self.python_path,
            shell_type=self.shell_type,
            hold_expiration=self.hold_expiration,
            host_id=self.host_id,
            host_key_checking=self.host_key_checking,
            host_keys=self.host_keys,
            interface_ip=self.interface_ip,
            private_ipv4=self.private_ipv4,
            private_ipv6=self.private_ipv6,
            provider=self.provider,
            public_ipv4=self.public_ipv4,
            public_ipv6=self.public_ipv6,
            region=self.region,
            resources=None,
            slot=self.slot,
            reuse=self.reuse,
            node_properties=self.node_properties,
        )
        if not serialize_node:
            ret['tenant_name'] = self.tenant_name
        return ret

    def getRequiredResources(self):
        """Return a list of resource unique_ids that are required by
        this node.  Used by the launcher to avoid deleting in-use
        infrastructure resources."""
        return []

    def getSpanAttributes(self):
        return dict(
            uuid=self.uuid,
            request_uuid=self.request_id,
            state=self.state,
            provider=self.provider,
            label=self.label,
            zuul_event_id=self.zuul_event_id,
        )

    def createSnapshot(self, context):
        self.snapshot.internalCreate(context)

    def updateFromInstance(self, instance):
        if instance is None:
            return

        # TODO:
        # if (pool.use_internal_ip and
        #     (instance.private_ipv4 or instance.private_ipv6)):
        #     server_ip = instance.private_ipv4 or instance.private_ipv6
        # else:
        server_ip = instance.interface_ip

        self.interface_ip = server_ip
        self.public_ipv4 = instance.public_ipv4
        self.private_ipv4 = instance.private_ipv4
        self.public_ipv6 = instance.public_ipv6
        self.private_ipv6 = instance.private_ipv6
        self.host_id = instance.host_id
        self.cloud = instance.cloud
        self.region = instance.region
        self.az = instance.az
        self.driver_data = instance.driver_data

        # If we did not know the resource information before
        # launching, update it now.
        self.quota = instance.getQuotaInformation()

        # Optionally, if the node has updated values that we set from
        # the image attributes earlier, set those.
        for attr in ('username', 'python_path', 'shell_type',
                     'connection_port', 'connection_type',
                     'host_keys'):
            if hasattr(instance, attr):
                setattr(self, attr, getattr(instance, attr))

        # As a special case for metastatic, if we got node_attributes
        # from the backing driver, use them as default values and let
        # the values from the pool override.
        instance_node_attrs = getattr(instance, 'node_attributes', None)
        if instance_node_attrs is not None:
            attrs = instance_node_attrs.copy()
            if self.attributes:
                attrs.update(self.attributes)
            self.attributes = attrs


class SnapshotStatus:
    class Type(StrEnum):
        MESSAGE = "message"
        STARTED = "started"
        COMPLETED = "completed"

    def __init__(self, kind, message=None):
        self.type = kind
        self.message = message


class ProviderNodeSnapshot(zkobject.LockableZKObject):
    # We don't want to re-create the node in case it was deleted
    makepath = False
    SNAPSHOT_PATH = 'snapshot'
    SNAPSHOT_LOCK_PATH = 'snapshot-lock'

    def __init__(self):
        super().__init__()
        self._set(
            complete=None,
            external_id=None,
            tags=None,
            snapshot_state={},
            # Not serialized
            node=None,
            state_machine=None,
        )

    def getPath(self):
        return f"{self.node.getPath()}/{self.SNAPSHOT_PATH}"

    def getLockPath(self):
        return f"{self.node.getPath()}/{self.SNAPSHOT_LOCK_PATH}"

    def serialize(self, context):
        if self.state_machine:
            snapshot_state = self.state_machine.toDict()
        else:
            snapshot_state = {}
        data = dict(
            snapshot_state=snapshot_state,
            complete=self.complete,
            external_id=self.external_id,
            tags=self.tags,
        )
        return json.dumps(data, sort_keys=True).encode("utf-8")


class ProviderNodeAssignment(zkobject.ZKObject):
    # We don't want to re-create the node in case it was deleted
    makepath = False
    log_error_missing = False
    ASSIGNMENT_PATH = 'assignment'

    # This object allows us to change the request to which a node is
    # assigned without locking it.  That allows us to reassign a node
    # that is in the building state while the launcher still holds the
    # node lock.  The conflict resolution algorithm is simply that we
    # always create or delete the assignment subnode, we never update
    # it.  Any launcher that wants to assign the node to a request
    # should simply create the node.  If another launcher races, the
    # loser will receive a NodeExistsError.  To unassign, delete it.
    #
    # If the node is in the READY state, the launcher should still
    # acquire the node lock before creating the child znode. That's
    # because the launcher responsible for the node may race it when
    # performing a state change to OUTDATED. Or an API user may change
    # the state from READY to HOLD. Only in the BUILDING state should
    # the child znode be created without holding the node lock.

    def __init__(self):
        super().__init__()
        self._set(
            request_id=None,
            min_request_version=None,
            tenant_name=None,
            # Not serialized
            node=None,
        )

    def getPath(self):
        return f"{self.node.getPath()}/{self.ASSIGNMENT_PATH}"

    def serialize(self, context):
        data = dict(
            request_id=self.request_id,
            min_request_version=self.min_request_version,
            tenant_name=self.tenant_name,
        )
        return json.dumps(data, sort_keys=True).encode("utf-8")

    def _clear(self):
        # Internal method used to reset values on deletion.
        self._set(
            request_id=None,
            min_request_version=None,
            tenant_name=None,
        )


class ProviderNodeLifecycle(zkobject.ZKObject):
    # We don't want to re-create the node in case it was deleted
    makepath = False
    log_error_missing = False
    LIFECYCLE_PATH = 'lifecycle'

    # This object allows us to request that a node be deleted or
    # otherwise removed from service without locking it.  That allows
    # us to request that an in-use reusable node be deleted at the
    # completion of its service, main nodes to be deleted when all
    # subnodes are finished, and static nodes to be kept out of
    # service.

    # There is no locking for this since it is only ever set by user
    # interaction, and any conflict resolution (either first or last
    # wins) is acceptable.

    def __init__(self):
        super().__init__()
        self._set(
            next_state=None,
            # Not serialized
            node=None,
        )

    def getPath(self):
        return f"{self.node.getPath()}/{self.LIFECYCLE_PATH}"

    def serialize(self, context):
        data = dict(
            next_state=self.next_state,
        )
        return json.dumps(data, sort_keys=True).encode("utf-8")

    def _clear(self):
        # Internal method used to reset values on deletion.
        self._set(
            next_state=None,
            _zstat=None,
        )


class Secret(ConfigObject):
    """A collection of private data.

    In configuration, Secrets are collections of private data in
    key-value pair format.  They are defined as top-level
    configuration objects and then referenced by Jobs.

    """

    def __init__(self, name, source_context):
        super(Secret, self).__init__()
        self.name = name
        self.source_context = source_context
        # The secret data may or may not be encrypted.  This attribute
        # is named 'secret_data' to make it easy to search for and
        # spot where it is directly used.
        self.secret_data = {}
        # This attribute stores the oidc token configuration for the
        # oidc secrets. Mutually exclusive with secret_data.
        self.secret_oidc = {}

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Secret):
            return False
        return (self.name == other.name and
                self.secret_data == other.secret_data and
                self.secret_oidc == other.secret_oidc)

    def __repr__(self):
        return '<Secret %s>' % (self.name,)

    def _decrypt(self, private_key, secret_data):
        # recursive function to decrypt data
        if hasattr(secret_data, 'decrypt'):
            return secret_data.decrypt(private_key)

        if isinstance(secret_data, (dict, types.MappingProxyType)):
            decrypted_secret_data = {}
            for k, v in secret_data.items():
                decrypted_secret_data[k] = self._decrypt(private_key, v)
            return decrypted_secret_data

        if isinstance(secret_data, (list, tuple)):
            decrypted_secret_data = []
            for v in secret_data:
                decrypted_secret_data.append(self._decrypt(private_key, v))
            return decrypted_secret_data

        return secret_data

    def decrypt(self, private_key):
        """Return a copy of this secret with any encrypted data decrypted.
        Note that the original remains encrypted."""

        r = Secret(self.name, self.source_context)
        r.secret_data = self._decrypt(private_key, self.secret_data)
        return r

    def serialize(self, layout):
        # The output of this method is used by the executor
        # Set the ttl for this tenant
        if self.secret_oidc:
            secret_oidc = self.secret_oidc.copy()
            # We previously checked that it's not greater than the tenant
            # max.
            secret_oidc['ttl'] = secret_oidc.get(
                'ttl', layout.tenant.default_oidc_ttl)
            # The original name needs to be passed for generating
            # the value of the 'sub' claim
            secret_oidc['name'] = self.name
            secret_oidc['_source_project_canonical_name'] =\
                self.source_context.project_canonical_name
        else:
            secret_oidc = {}

        if COMPONENT_REGISTRY.model_api >= 35:
            data = {
                "secret_data": self.secret_data,
                "secret_oidc": secret_oidc
            }
        else:
            data = self.secret_data
        return yaml.encrypted_dump(data, default_flow_style=False)


class SecretUse(ConfigObject):
    """A use of a secret in a Job"""

    def __init__(self, name, alias):
        super(SecretUse, self).__init__()
        self.name = name
        self.alias = alias
        self.pass_to_parent = False


class FrozenSecret(ConfigObject):
    """A frozen secret for use by the executor"""

    def __init__(self, connection_name, project_name, name, encrypted_data):
        super(FrozenSecret, self).__init__()
        self.connection_name = connection_name
        self.project_name = project_name
        self.name = name
        self.encrypted_data = encrypted_data

    @staticmethod
    @lru_cache(maxsize=1024)
    def construct_cached(connection_name, project_name, name, encrypted_data):
        """
        A caching constructor that enables re-use already existing
        FrozenSecret objects.
        """
        return FrozenSecret(connection_name, project_name, name,
                            encrypted_data)

    def toDict(self):
        # Name is omitted since this is used in a dictionary
        return dict(
            connection_name=self.connection_name,
            project_name=self.project_name,
            encrypted_data=self.encrypted_data,
        )


class SourceContext(ConfigObject):
    """A reference to the branch of a project in configuration.

    Jobs and playbooks reference this to keep track of where they
    originate."""

    def __init__(self, project_canonical_name, project_name,
                 project_connection_name, branch, path):
        super(SourceContext, self).__init__()
        self.project_canonical_name = project_canonical_name
        self.project_name = project_name
        self.project_connection_name = project_connection_name
        self.branch = branch
        self.path = path
        # These attributes are not copied in copy()
        self.implied_branch_matchers = None
        self.implied_branches = None
        self.implied_branch_matcher = None

    def __str__(self):
        return '%s/%s@%s' % (
            self.project_name, self.path, self.branch)

    def __repr__(self):
        return '<SourceContext %s>' % (str(self),)

    def __deepcopy__(self, memo):
        return self.copy()

    def copy(self):
        return self.__class__(
            self.project_canonical_name, self.project_name,
            self.project_connection_name, self.branch, self.path)

    def isSameProject(self, other):
        if not isinstance(other, SourceContext):
            return False
        return self.project_canonical_name == other.project_canonical_name

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, SourceContext):
            return False
        return (self.project_canonical_name == other.project_canonical_name and
                self.branch == other.branch and
                self.path == other.path)

    def serialize(self):
        ibs = None
        if self.implied_branches:
            ibs = [ibm.serialize() for ibm in self.implied_branches]
        return {
            "project_canonical_name": self.project_canonical_name,
            "project_name": self.project_name,
            "project_connection_name": self.project_connection_name,
            "branch": self.branch,
            "path": self.path,
            "implied_branch_matchers": self.implied_branch_matchers,
            "implied_branches": ibs,
        }

    @classmethod
    def deserialize(cls, data):
        o = cls.__new__(cls)
        ibs = data.get('implied_branches')
        if ibs:
            data['implied_branches'] = []
            for matcher_data in ibs:
                if matcher_data['implied']:
                    cls = change_matcher.ImpliedBranchMatcher
                else:
                    cls = change_matcher.BranchMatcher
                data['implied_branches'].append(
                    cls.deserialize(matcher_data))
        o.__dict__.update(data)
        return o

    def toDict(self):
        return dict(
            project=self.project_name,
            branch=self.branch,
            path=self.path,
        )


class PlaybookContext(ConfigObject):
    """A reference to a playbook in the context of a project.

    Jobs refer to objects of this class for their main, pre, and post
    playbooks so that we can keep track of which repos and security
    contexts are needed in order to run them.

    We also keep a list of roles so that playbooks only run with the
    roles which were defined at the point the playbook was defined.

    """

    def __init__(self, source_context, path, roles, secrets,
                 semaphores, cleanup=False):
        super(PlaybookContext, self).__init__()
        self.source_context = source_context
        self.path = path
        self.roles = roles
        # The original SecretUse objects describing how the secret
        # should be used
        self.secrets = secrets
        # FrozenSecret objects which contain only the info the
        # executor needs
        self.frozen_secrets = ()
        # The original JobSemaphore objects
        self.semaphores = semaphores
        # the result of getSemaphoreInfo from semaphore handler
        self.frozen_semaphores = ()
        self.cleanup = cleanup
        # Set internally after inheritance
        self.nesting_level = None
        self.frozen_trusted = None

    def __repr__(self):
        return '<PlaybookContext %s %s>' % (self.source_context,
                                            self.path)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, PlaybookContext):
            return False
        return (self.source_context == other.source_context and
                self.path == other.path and
                self.roles == other.roles and
                self.secrets == other.secrets and
                self.semaphores == other.semaphores and
                self.cleanup == other.cleanup)

    def copy(self):
        r = PlaybookContext(self.source_context,
                            self.path,
                            self.roles,
                            self.secrets,
                            self.semaphores,
                            self.cleanup)
        return r

    def validateReferences(self, layout):
        # Verify that references to other objects in the layout are
        # valid.
        for secret_use in self.secrets:
            secret = layout.secrets.get(secret_use.name)
            if secret is None:
                raise Exception(
                    'The secret "{name}" was not found.'.format(
                        name=secret_use.name))
            check_varnames({secret_use.alias: ''})
            if not secret.source_context.isSameProject(self.source_context):
                raise Exception(
                    "Unable to use secret {name}.  Secrets must be "
                    "defined in the same project in which they "
                    "are used".format(
                        name=secret_use.name))
            project = layout.tenant.getProject(
                self.source_context.project_canonical_name)[1]
            # Decrypt a copy of the secret to verify it can be done
            secret.decrypt(project.private_secrets_key)
        # TODO: if we remove the implicit max=1 semaphore, validate
        # references here.

    def freezeSemaphores(self, layout, semaphore_handler):
        semaphores = []
        for job_semaphore in self.semaphores:
            info = semaphore_handler.getSemaphoreInfo(job_semaphore)
            semaphores.append(info)
        self.frozen_semaphores = tuple(semaphores)

    def freezeSecrets(self, layout):
        secrets = []
        for secret_use in self.secrets:
            secret = layout.secrets.get(secret_use.name)
            secret_name = secret_use.alias
            encrypted_secret_data = secret.serialize(layout)
            # Use *our* project, not the secret's, because we want to decrypt
            # with *our* key.
            project = layout.tenant.getProject(
                self.source_context.project_canonical_name)[1]
            secrets.append(FrozenSecret.construct_cached(
                project.connection_name, project.name, secret_name,
                encrypted_secret_data))
        self.frozen_secrets = tuple(secrets)

    def addSecrets(self, frozen_secrets):
        current_names = set([s.name for s in self.frozen_secrets])
        new_secrets = [s for s in frozen_secrets
                       if s.name not in current_names]
        self.frozen_secrets = self.frozen_secrets + tuple(new_secrets)

    def freezeTrusted(self, layout):
        # noop job does not have source_context
        if self.source_context:
            self.frozen_trusted = layout.tenant.isTrusted(
                self.source_context.project_canonical_name)
        else:
            self.frozen_trusted = False

    def toDict(self, redact_secrets=True):
        # Render to a dict to use in passing json to the executor
        secrets = {}
        for secret in self.frozen_secrets:
            if redact_secrets:
                secrets[secret.name] = 'REDACTED'
            else:
                secrets[secret.name] = secret.toDict()
        return dict(
            connection=self.source_context.project_connection_name,
            project=self.source_context.project_name,
            branch=self.source_context.branch,
            trusted=self.frozen_trusted,
            roles=[r.toDict() for r in self.roles],
            secrets=secrets,
            semaphores=self.frozen_semaphores,
            path=self.path,
            cleanup=self.cleanup,
            nesting_level=self.nesting_level,
        )

    def toSchemaDict(self):
        # Render to a dict to use in REST api
        d = {
            'path': self.path,
            'roles': list(map(lambda x: x.toDict(), self.roles)),
            'secrets': [{'name': secret.name, 'alias': secret.alias}
                        for secret in self.secrets],
            'semaphores': [{'name': sem.name} for sem in self.semaphores],
            'cleanup': self.cleanup,
        }
        if self.source_context:
            d['source_context'] = self.source_context.toDict()
        else:
            d['source_context'] = None
        return d


class Role(ConfigObject, metaclass=abc.ABCMeta):
    """A reference to an ansible role."""

    def __init__(self, target_name):
        super(Role, self).__init__()
        self.target_name = target_name

    @abc.abstractmethod
    def __repr__(self):
        pass

    def __ne__(self, other):
        return not self.__eq__(other)

    @abc.abstractmethod
    def __eq__(self, other):
        if not isinstance(other, Role):
            return False
        return (self.target_name == other.target_name)

    @abc.abstractmethod
    def toDict(self):
        # Render to a dict to use in passing json to the executor
        return dict(target_name=self.target_name)


class ZuulRole(Role):
    """A reference to an ansible role in a Zuul project."""

    def __init__(self, target_name, project_name, implicit=False):
        super(ZuulRole, self).__init__(target_name)
        self.project_name = project_name
        self.implicit = implicit

    def __repr__(self):
        return '<ZuulRole %s %s>' % (self.project_name,
                                     self.target_name)

    def __hash__(self):
        return hash(json.dumps(self.toDict(), sort_keys=True))

    def __eq__(self, other):
        if not isinstance(other, ZuulRole):
            return False
        # Implicit is not consulted for equality so that we can handle
        # implicit to explicit conversions.
        return (super(ZuulRole, self).__eq__(other) and
                self.project_name == other.project_name)

    def copy(self):
        return ZuulRole(self.target_name, self.project_name, self.implicit)

    def toDict(self):
        # Render to a dict to use in passing json to the executor
        d = super(ZuulRole, self).toDict()
        d['type'] = 'zuul'
        d['project_canonical_name'] = self.project_name
        d['implicit'] = self.implicit
        return d

    @classmethod
    def fromDict(cls, data):
        self = cls(data['target_name'],
                   data['project_canonical_name'],
                   data['implicit'])
        return self


class JobData(zkobject.ShardedZKObject):
    """Data or variables for a job.

    These can be arbitrarily large, so they are stored as sharded ZK objects.

    A hash attribute can be stored on the job object itself to detect
    whether the data need to be refreshed.

    """

    # We can always recreate data if necessary, so go ahead and
    # truncate when we update so we avoid corrupted data.
    truncate_on_create = True

    def __repr__(self):
        return '<JobData>'

    def getPath(self):
        return self._path

    @classmethod
    def new(klass, context, create=True, **kw):
        """Create a new instance and save it in ZooKeeper"""
        obj = klass()
        kw['hash'] = JobData.getHash(kw['data'])
        obj._set(**kw)
        if create:
            data = obj._trySerialize(context)
            obj._save(context, data, create=True)
        return obj

    @staticmethod
    def getHash(data):
        hasher = hashlib.sha256()
        # Use json_dumps to strip any ZuulMark entries
        hasher.update(json_dumps(data, sort_keys=True).encode('utf8'))
        return hasher.hexdigest()

    def serialize(self, context):
        data = {
            "data": self.data,
            "hash": self.hash,
            "_path": self._path,
        }
        return json_dumps(data, sort_keys=True).encode("utf8")

    def __hash__(self):
        return hash(self.hash)

    def __eq__(self, other):
        if not isinstance(other, JobData):
            return False
        return self.hash == other.hash


class FrozenJob(zkobject.ZKObject):
    """A rendered job definition that will actually be run.

    This is the combination of one or more Job variants to produce a
    rendered job definition that can be serialized and run by the
    executor.

    Most variables should not be updated once created, except some
    variables which deal with the current state of the job in the
    pipeline.
    """
    # If data/variables are more than 10k, we offload them to another
    # object, otherwise we store them on this one.
    MAX_DATA_LEN = 10 * 1024

    attributes = ('ansible_version',
                  'ansible_split_streams',
                  'dependencies',
                  'image_build_name',
                  'inheritance_path',
                  'name',
                  'nodeset_alternatives',
                  'nodeset_index',
                  'override_branch',
                  'override_checkout',
                  'pre_timeout',
                  'post_timeout',
                  'required_projects',
                  'include_projects',
                  'exclude_projects',
                  'semaphores',
                  'tags',
                  'timeout',
                  'voting',
                  'queued',
                  'hold_following_changes',
                  'waiting_status',
                  'pre_run',
                  'run',
                  'post_run',
                  # MODEL_API < 30; all references to cleanup_run may
                  # be removed.
                  'cleanup_run',
                  'attempts',
                  'success_message',
                  'failure_message',
                  'provides',
                  'requires',
                  'workspace_scheme',
                  'workspace_checkout',
                  'config_hash',
                  'deduplicate',
                  'failure_output',
                  'image_build_name',
                  'include_vars',
                  'type',
                  )

    job_data_attributes = ('artifact_data',
                           'extra_variables',
                           'group_variables',
                           'host_variables',
                           'secret_parent_data',
                           'variables',
                           'parent_data',
                           'secrets',
                           'affected_projects',
                           )

    def __init__(self):
        super().__init__()
        self._set(
            ref=None,
            other_refs=[],
            image_build_name=None,
            workspace_checkout=True,  # MODEL_API <= 32
            type='regular',  # MODEL_API <= 36
            # Not serialized
            matches_change=True,
        )

    def __repr__(self):
        name = getattr(self, 'name', '<UNKNOWN>')
        return f'<FrozenJob {name}>'

    def isEqual(self, other):
        # Compare two frozen jobs to determine whether they are
        # effectively equal.  The inheritance path will always be
        # different, so it is ignored.  But if otherwise they have the
        # same attributes, they will probably produce the same
        # results.
        if not isinstance(other, FrozenJob):
            return False
        if self.name != other.name:
            return False
        for k in self.attributes:
            if k in ['config_hash', 'inheritance_path', 'waiting_status',
                     'queued']:
                continue
            if getattr(self, k) != getattr(other, k):
                return False
        for k in self.job_data_attributes:
            if getattr(self, k) != getattr(other, k):
                return False
        return True

    @classmethod
    def new(klass, context, **kw):
        raise NotImplementedError()

    @classmethod
    def createInMemory(klass, **kw):
        obj = klass()
        obj._set(uuid=uuid4().hex)

        for k in klass.job_data_attributes:
            v = kw.pop(k, None)
            kw['_' + k] = v
        obj._set(**kw)
        return obj

    def internalCreate(self, context):
        # Convert these to JobData after creation.
        job_data_vars = []
        for k in self.job_data_attributes:
            v = getattr(self, '_' + k)
            if v:
                # If the value is long, we need to make this a
                # JobData; otherwise we can use the value as-is.
                # TODO(jeblair): if we apply the same createInMemory
                # approach to JobData creation, we can avoid this
                # serialization test as well as rewriting the
                # frozenjob object below.
                v = self._makeJobData(context, k, v, create=False)
                self._set(**{'_' + k: v})
                if isinstance(v, JobData):
                    job_data_vars.append(v)
        super().internalCreate(context)

        # If we need to make any JobData entries, do that now.
        for v in job_data_vars:
            v.internalCreate(context)

    def isBase(self):
        return self.parent is None

    @classmethod
    def jobPath(cls, job_id, parent_path):
        return f"{parent_path}/job/{job_id}"

    def getPath(self):
        return self.jobPath(self.uuid, self.buildset.getPath())

    @property
    def all_refs(self):
        return [self.ref, *self.other_refs]

    def serialize(self, context):
        # Ensure that any special handling in this method is matched
        # in Job.freezeJob so that FrozenJobs are identical regardless
        # of whether they have been deserialized.
        data = {
            "uuid": self.uuid,
        }
        for k in self.attributes:
            # TODO: Backwards compat handling, remove after 5.0
            if k == 'config_hash':
                if not hasattr(self, k):
                    continue
            v = getattr(self, k)
            if k == 'nodeset_alternatives':
                v = [alt.toDict() for alt in v]
            elif k == 'dependencies':
                # frozenset of JobDependency
                v = [dep.toDict() for dep in v]
            elif k == 'semaphores':
                # list of JobSemaphores
                v = [sem.toDict() for sem in v]
            elif k in ('provides', 'requires', 'tags'):
                v = list(v)
            elif k == 'required_projects':
                # dict of name->JobProject
                v = {project_name: job_project.toDict()
                     for (project_name, job_project) in v.items()}
            data[k] = v

        for k in self.job_data_attributes:
            v = getattr(self, '_' + k)
            if isinstance(v, JobData):
                v = {'storage': 'offload', 'path': v.getPath(), 'hash': v.hash}
            else:
                v = {'storage': 'local', 'data': v}
            data[k] = v

        data['ref'] = self.ref
        data['other_refs'] = self.other_refs

        # Use json_dumps to strip any ZuulMark entries
        return json_dumps(data, sort_keys=True).encode("utf8")

    def deserialize(self, raw, context, extra=None):
        # Ensure that any special handling in this method is matched
        # in Job.freezeJob so that FrozenJobs are identical regardless
        # of whether they have been deserialized.
        data = super().deserialize(raw, context)

        if hasattr(self, 'nodeset_alternatives'):
            alts = self.nodeset_alternatives
        else:
            alts = data.get('nodeset_alternatives', [])
            alts = [NodeSet.fromDict(alt) for alt in alts]
        data['nodeset_alternatives'] = alts

        if hasattr(self, 'dependencies'):
            data['dependencies'] = self.dependencies
        else:
            data['dependencies'] = frozenset(JobDependency.fromDict(dep)
                                             for dep in data['dependencies'])

        if hasattr(self, 'semaphores'):
            data['semaphores'] = self.semaphores
        else:
            data['semaphores'] = [JobSemaphore.fromDict(sem)
                                  for sem in data['semaphores']]

        if hasattr(self, 'required_projects'):
            data['required_projects'] = self.required_projects
        else:
            data['required_projects'] = {
                project_name: JobProject.fromDict(job_project)
                for (project_name, job_project)
                in data['required_projects'].items()}

        # MODEL_API <= 31
        data.setdefault('include_vars', [])
        data['provides'] = frozenset(data['provides'])
        data['requires'] = frozenset(data['requires'])
        data['tags'] = frozenset(data['tags'])

        # MODEL_API <= 33
        data.setdefault('pre_timeout', None)

        # MODEL_API <= 36
        data.setdefault('include_projects', None)
        data.setdefault('exclude_projects', None)

        for job_data_key in self.job_data_attributes:
            job_data = data.pop(job_data_key, None)
            if job_data:
                # This is a dict which tells us where the actual data is.
                if job_data['storage'] == 'local':
                    # The data are stored locally in this dict
                    data['_' + job_data_key] = job_data['data']
                elif job_data['storage'] == 'offload':
                    existing_job_data = getattr(self, f"_{job_data_key}", None)
                    if (getattr(existing_job_data, 'hash', None) ==
                        job_data['hash']
                        and job_data['hash'] is not None):
                        # Re-use the existing object since it's the same
                        data['_' + job_data_key] = existing_job_data
                    else:
                        if job_data['hash'] is None:
                            context.log.error("JobData hash is None on %s",
                                              self)
                        # Load the object from ZK
                        data['_' + job_data_key] = JobData.fromZK(
                            context, job_data['path'])
            else:
                data['_' + job_data_key] = None
        return data

    def _save(self, context, *args, **kw):
        # Before saving, update the buildset with the new job version
        # so that future readers know to refresh it.
        self.buildset.updateJobVersion(context, self)
        return super()._save(context, *args, **kw)

    def setWaitingStatus(self, status):
        if self.waiting_status == status:
            return
        self.updateAttributes(
            self.buildset.item.manager.current_context,
            waiting_status=status)

    def _getJobData(self, name):
        val = getattr(self, name, None)
        if isinstance(val, JobData):
            return val.data
        return val

    @property
    def nodeset(self):
        if self.nodeset_alternatives:
            return self.nodeset_alternatives[self.nodeset_index]
        return None

    @property
    def parent_data(self):
        return self._getJobData('_parent_data')

    @property
    def secret_parent_data(self):
        return self._getJobData('_secret_parent_data')

    @property
    def artifact_data(self):
        return self._getJobData('_artifact_data')

    @property
    def extra_variables(self):
        return self._getJobData('_extra_variables')

    @property
    def group_variables(self):
        return self._getJobData('_group_variables')

    @property
    def host_variables(self):
        return self._getJobData('_host_variables')

    @property
    def variables(self):
        return self._getJobData('_variables')

    @property
    def secrets(self):
        return self._getJobData('_secrets')

    @property
    def affected_projects(self):
        return self._getJobData('_affected_projects')

    def getSafeAttributes(self):
        return Attributes(name=self.name)

    @staticmethod
    def updateParentData(parent_data, secret_parent_data, artifact_data,
                         other_build):
        # Update variables, but give the new values priority. If more than one
        # parent job returns the same variable, the value from the later job
        # in the job graph will take precedence.
        other_vars = other_build.result_data
        v = parent_data
        v = Job._deepUpdate(v, other_vars)
        # To avoid running afoul of checks that jobs don't set zuul
        # variables, remove them from parent data here.
        v.pop('zuul', None)
        # For safety, also drop nodepool and unsafe_vars
        v.pop('nodepool', None)
        v.pop('unsafe_vars', None)
        parent_data = v

        secret_other_vars = other_build.secret_result_data
        v = secret_parent_data
        v = Job._deepUpdate(secret_other_vars, v)
        if 'zuul' in v:
            del v['zuul']
        secret_parent_data = v

        artifacts = get_artifacts_from_result_data(other_vars)
        artifact_data = artifact_data[:]
        for a in artifacts:
            # Change here may be any ref type (tag, change, etc)
            ref = other_build.build_set.item.getChangeForJob(other_build.job)
            a.update({'project': ref.project.name,
                      'job': other_build.job.name})
            # Change is a Branch
            if hasattr(ref, 'branch'):
                a.update({'branch': ref.branch})
                if hasattr(ref, 'number') and hasattr(ref, 'patchset'):
                    a.update({'change': str(ref.number),
                              'patchset': ref.patchset})
            # Otherwise we are ref type
            else:
                a.update({'ref': ref.ref,
                          'oldrev': ref.oldrev,
                          'newrev': ref.newrev})
                if hasattr(ref, 'tag'):
                    a.update({'tag': ref.tag})
            if a not in artifact_data:
                artifact_data.append(a)
        return parent_data, secret_parent_data, artifact_data

    def _makeJobData(self, context, name, data, create=True):
        # If the data is large, store it in another object
        if (len(json_dumps(data, sort_keys=True).encode('utf8')) >
            self.MAX_DATA_LEN):
            return JobData.new(
                context, create=create, _path=self.getPath() + '/' + name,
                data=data)
        # Otherwise we can store it as a local dict
        return data

    def setParentData(self, parent_data, secret_parent_data, artifact_data):
        context = self.buildset.item.manager.current_context
        kw = {}
        if self.parent_data != parent_data:
            kw['_parent_data'] = self._makeJobData(
                context, 'parent_data', parent_data)
        if self.secret_parent_data != secret_parent_data:
            kw['_secret_parent_data'] = self._makeJobData(
                context, 'secret_parent_data', secret_parent_data)
        if self.artifact_data != artifact_data:
            kw['_artifact_data'] = self._makeJobData(
                context, 'artifact_data', artifact_data)
        if kw:
            self.updateAttributes(
                self.buildset.item.manager.current_context,
                **kw)

    def setArtifactData(self, artifact_data):
        context = self.buildset.item.manager.current_context
        if self.artifact_data != artifact_data:
            self.updateAttributes(
                context,
                _artifact_data=self._makeJobData(
                    context, 'artifact_data', artifact_data))

    @property
    def all_playbooks(self):
        for k in ('pre_run', 'run', 'post_run', 'cleanup_run'):
            playbooks = getattr(self, k)
            yield from playbooks

    def incrementNodesetIndex(self):
        if len(self.nodeset_alternatives) <= self.nodeset_index + 1:
            # No alternatives to fall back upon
            return False

        context = self.buildset.item.manager.current_context
        # Increment the nodeset index
        with self.activeContext(context):
            self.nodeset_index = self.nodeset_index + 1

        return True

    def includesProject(self, project, change, item):
        # project is a Project object
        include = False
        if self.include_projects is None:
            include = True
        else:
            for (ptype, pname) in self.include_projects:
                include = False
                if ptype == 'name':
                    if pname == project.canonical_name:
                        include = True
                elif ptype == 'change':
                    if change.project.canonical_name == project.canonical_name:
                        include = True
                elif ptype == 'item':
                    for item_change in item.changes:
                        if (item_change.project.canonical_name ==
                            project.canonical_name):
                            include = True
                            break
                if include:
                    break
        if not include:
            return False
        if self.exclude_projects is None:
            return True
        for (ptype, pname) in self.exclude_projects:
            if ptype == 'name':
                if pname == project.canonical_name:
                    return False
            elif ptype == 'change':
                if change.project.canonical_name == project.canonical_name:
                    return False
            elif ptype == 'item':
                for item_change in item.changes:
                    if (item_change.project.canonical_name ==
                        project.canonical_name):
                        return False
        return True


class Job(ConfigObject):
    """A Job represents the defintion of actions to perform.

    A Job is an abstract configuration concept.  It describes what,
    where, and under what circumstances something should be run
    (contrast this with Build which is a concrete single execution of
    a Job).

    NB: Do not modify attributes of this class, set them directly
    (e.g., "job.run = ..." rather than "job.run.append(...)").
    """

    # Pre-allocated empty nodeset so we don't have to allocate a new one
    # with every job variant.
    empty_nodeset = NodeSet()

    BASE_JOB_MARKER = object()
    # Secrets larger than this size will be put in the blob store
    SECRET_BLOB_SIZE = 10 * 1024

    def isBase(self):
        return self.parent is self.BASE_JOB_MARKER

    def toDict(self, tenant):
        '''
        Convert a Job object's attributes to a dictionary.
        '''
        d = {}
        d['name'] = self.name
        d['branches'] = self.getBranches(tenant)
        d['override_checkout'] = self.override_checkout
        d['files'] = self._files
        d['irrelevant_files'] = self._irrelevant_files
        d['variant_description'] = self.variant_description
        if self.source_context:
            d['source_context'] = self.source_context.toDict()
        else:
            d['source_context'] = None
        d['description'] = self.description
        d['required_projects'] = []
        for project in self.required_projects.values():
            d['required_projects'].append(project.toDict())
        d['include_projects'] = self._makeIncludeExcludeProjectsList(
            self.include_projects)
        d['exclude_projects'] = self._makeIncludeExcludeProjectsList(
            self.exclude_projects)
        d['semaphores'] = [s.toDict() for s in self.semaphores]
        d['variables'] = self.variables
        d['extra_variables'] = self.extra_variables
        d['host_variables'] = self.host_variables
        d['group_variables'] = self.group_variables
        d['final'] = self.final
        d['abstract'] = self.abstract
        d['intermediate'] = self.intermediate
        d['protected'] = self.protected
        d['voting'] = self.voting
        d['pre_timeout'] = self.pre_timeout
        d['timeout'] = self.timeout
        d['post_timeout'] = self.post_timeout
        d['tags'] = list(self.tags)
        d['provides'] = list(self.provides)
        d['requires'] = list(self.requires)
        d['dependencies'] = list(map(lambda x: x.toDict(), self.dependencies))
        d['attempts'] = self.attempts
        d['roles'] = list(map(lambda x: x.toDict(), self.roles))
        d['run'] = list(map(lambda x: x.toSchemaDict(), self.run))
        d['pre_run'] = list(map(lambda x: x.toSchemaDict(), self.pre_run))
        d['post_run'] = list(map(lambda x: x.toSchemaDict(), self.post_run))
        d['cleanup_run'] = list(map(lambda x: x.toSchemaDict(),
                                    self.cleanup_run))
        d['post_review'] = self.post_review
        d['match_on_config_updates'] = self.match_on_config_updates
        d['deduplicate'] = self.deduplicate
        d['failure_output'] = self.failure_output
        d['image_build_name'] = self.image_build_name
        d['type'] = self.type
        d['include_vars'] = list(map(lambda x: x.toDict(), self.include_vars))
        if self.isBase():
            d['parent'] = None
        elif self.parent:
            d['parent'] = self.parent
        else:
            d['parent'] = tenant.default_base_job
        alts = self.flattenNodesetAlternatives(tenant.layout)
        if len(alts) == 1 and len(alts[0]):
            d['nodeset'] = alts[0].toDict()
        elif len(alts) > 1:
            d['nodeset_alternatives'] = [x.toDict() for x in alts]
        d['ansible_split_streams'] = self.ansible_split_streams
        if self.ansible_version:
            d['ansible_version'] = self.ansible_version
        else:
            d['ansible_version'] = None
        d['workspace_scheme'] = self.workspace_scheme
        d['workspace_checkout'] = self.workspace_checkout
        return d

    def __init__(self, name):
        super().__init__()
        # These attributes may override even the final form of a job
        # in the context of a project-pipeline.  They can not affect
        # the execution of the job, but only whether the job is run
        # and how it is reported.
        self.context_attributes = dict(
            voting=True,
            hold_following_changes=False,
            failure_message=None,
            success_message=None,
            explicit_branch_matcher=None,
            implied_branch_matcher=None,
            file_matcher=None,
            _files=(),
            irrelevant_file_matcher=None,  # skip-if
            _irrelevant_files=(),
            match_on_config_updates=True,
            deduplicate='auto',
            tags=frozenset(),
            provides=frozenset(),
            requires=frozenset(),
            dependencies=frozenset(),
            ignore_allowed_projects=None,  # internal, but inherited
                                           # in the usual manner
        )

        # These attributes affect how the job is actually run and more
        # care must be taken when overriding them.  If a job is
        # declared "final", these may not be overridden in a
        # project-pipeline.
        self.execution_attributes = dict(
            parent=None,
            pre_timeout=None,
            timeout=None,
            post_timeout=None,
            variables={},
            extra_variables={},
            host_variables={},
            group_variables={},
            include_vars=[],
            nodeset=Job.empty_nodeset,
            workspace=None,
            pre_run=(),
            post_run=(),
            cleanup_run=(),
            run=(),
            ansible_split_streams=None,
            ansible_version=None,
            semaphores=(),
            attempts=3,
            final=False,
            abstract=False,
            intermediate=False,
            protected=None,
            roles=(),
            required_projects={},
            include_projects=None,
            exclude_projects=None,
            allowed_projects=None,
            override_branch=None,
            override_checkout=None,
            post_review=None,
            workspace_scheme=SCHEME_GOLANG,
            workspace_checkout=True,
            failure_output=(),
            image_build_name=None,
            type='regular',
        )

        override_control = defaultdict(lambda: True)
        override_control['file_matcher'] = True
        override_control['irrelevant_file_matcher'] = True
        override_control['tags'] = False
        override_control['provides'] = False
        override_control['requires'] = False
        override_control['dependencies'] = True
        override_control['variables'] = False
        override_control['extra_variables'] = False
        override_control['host_variables'] = False
        override_control['group_variables'] = False
        override_control['include_vars'] = False
        override_control['required_projects'] = False
        override_control['include_projects'] = False
        override_control['exclude_projects'] = False
        override_control['failure_output'] = False

        final_control = defaultdict(lambda: False)

        # These are generally internal attributes which are not
        # accessible via configuration.
        self.other_attributes = dict(
            name=None,
            source_context=None,
            start_mark=None,
            inheritance_path=(),
            parent_data=None,
            secret_parent_data=None,
            artifact_data=None,
            description=None,
            variant_description=None,
            protected_origin=None,
            secrets=(),  # secrets aren't inheritable
            queued=False,
            waiting_status=None,  # Text description of why its waiting
            # Override settings for context attributes:
            override_control=override_control,
            # Finalize individual attributes (context or execution):
            final_control=final_control,
            project_pipeline=False,
        )

        self.attributes = {}
        self.attributes.update(self.context_attributes)
        self.attributes.update(self.execution_attributes)
        self.attributes.update(self.other_attributes)

        self.name = name

    def _makeIncludeExcludeProjectsList(self, projects):
        # Used by toDict
        if projects is None:
            return None
        ret = []
        for ptype, pname in projects:
            p = {'type': ptype}
            if ptype == 'name':
                p['name'] = pname
            ret.append(p)
        return ret

    def _getAffectedProjects(self, tenant):
        """
        Gets all projects that are required to run this job. This includes
        required_projects, referenced playbooks, roles and dependent changes.
        """
        project_canonical_names = set()
        project_canonical_names.update(self.required_projects.keys())
        if self.run:
            run = [self.run[0]]
        else:
            run = []
        project_canonical_names.update(self._projectsFromPlaybooks(
            itertools.chain(self.pre_run, run, self.post_run,
                            self.cleanup_run), with_implicit=True))
        project_canonical_names.update({
            iv.project_name
            for iv in self.include_vars
            if iv.project_name
        })
        # Return a sorted list so the order is deterministic for
        # comparison.
        return sorted(project_canonical_names)

    def _projectsFromPlaybooks(self, playbooks, with_implicit=False):
        for playbook in playbooks:
            # noop job does not have source_context
            if playbook.source_context:
                yield playbook.source_context.project_canonical_name
            for role in playbook.roles:
                if role.implicit and not with_implicit:
                    continue
                yield role.project_name

    def _freezePlaybook(self, layout, item, playbook, redact_secrets_and_keys):
        d = playbook.toDict(redact_secrets=redact_secrets_and_keys)
        for role in d['roles']:
            if role['type'] != 'zuul':
                continue
            project_metadata = layout.getProjectMetadata(
                role['project_canonical_name'])
            if project_metadata:
                role['project_default_branch'] = \
                    project_metadata.default_branch
            else:
                role['project_default_branch'] = 'master'
            role_trusted, role_project = item.manager.tenant.getProject(
                role['project_canonical_name'])
            role_connection = role_project.source.connection
            role['connection'] = role_connection.connection_name
            role['project'] = role_project.name
        return d

    def _freezeIncludeVars(self, tenant, layout, change, include_vars):
        project_cname = (include_vars.project_name or
                         change.project.canonical_name)
        (trusted, project) = tenant.getProject(project_cname)
        project_metadata = layout.getProjectMetadata(project_cname)
        if project_metadata:
            default_branch = project_metadata.default_branch
        else:
            default_branch = 'master'
        connection = project.source.connection
        return dict(
            name=include_vars.name,
            connection=connection.connection_name,
            project=project.name,
            project_default_branch=default_branch,
            trusted=trusted,
            required=include_vars.required,
            use_ref=include_vars.use_ref,
        )

    def _deduplicateSecrets(self, context, frozen_playbooks):
        # At the end of this method, the values in the playbooks'
        # secrets dictionary will be mutated to either be an integer
        # (which is an index into the job's secret list) or a dict
        # (which contains a pointer to a key in the global blob
        # store).

        blobstore = BlobStore(context)

        def _resolve_index(secret_value, secrets):
            return secrets.index(secret_value)

        # Collect secrets that can be deduplicated
        secrets_list = []
        for playbook in frozen_playbooks:
            # Cast to list so we can modify in place
            for secret_key, secret_value in list(playbook['secrets'].items()):
                secret_serialized = json_dumps(
                    secret_value, sort_keys=True).encode("utf8")
                if (len(secret_serialized) > self.SECRET_BLOB_SIZE):
                    # If the secret is large, store it in the blob store
                    # and store the key in the playbook secrets dict.
                    blob_key = blobstore.put(secret_serialized)
                    playbook['secrets'][secret_key] = {'blob': blob_key}
                else:
                    if secret_value not in secrets_list:
                        secrets_list.append(secret_value)
                    playbook['secrets'][secret_key] = partial(
                        _resolve_index, secret_value)

        # The list of secrets needs to be sorted so that the same job
        # compares equal accross scheduler instances.
        secrets = sorted(secrets_list, key=lambda item: item['encrypted_data'])

        # Resolve secret indexes
        for playbook in frozen_playbooks:
            # Cast to list so we can modify in place
            for secret_key in list(playbook['secrets'].keys()):
                resolver = playbook['secrets'][secret_key]
                if callable(resolver):
                    playbook['secrets'][secret_key] = resolver(secrets)
        return secrets

    def flattenNodesetAlternatives(self, layout):
        nodeset = self.nodeset
        if isinstance(nodeset, str):
            # This references an existing named nodeset in the layout.
            ns = layout.nodesets.get(nodeset)
            if ns is None:
                raise NodesetNotFoundError(nodeset)
        else:
            ns = nodeset
        return ns.flattenAlternatives(layout)

    def freezeJob(self, context, tenant, layout, item, change,
                  redact_secrets_and_keys):
        buildset = item.current_build_set
        kw = {}

        # It is an error to have a pre-timeout that is larger than
        # timeout, because pre-timeout is counted as part of timeout.
        # We detect this situation and report the error if it occurs
        # on a single job definition, but if it happens due to
        # inheritance, we let the timeout value take priority and
        # simply cap the pre-timeout as timeout.
        if (self.pre_timeout and self.timeout and
            self.pre_timeout > self.timeout):
            self.pre_timeout = self.timeout

        attributes = (set(FrozenJob.attributes) |
                      set(FrozenJob.job_data_attributes))
        # De-duplicate the secrets across all playbooks, store them in
        # this array, and then refer to them by index.
        attributes.discard('secrets')
        attributes.discard('affected_projects')
        attributes.discard('config_hash')
        # Nodeset alternatives are flattened at this point
        attributes.discard('nodeset_alternatives')
        attributes.discard('nodeset_index')
        frozen_playbooks = []
        for k in attributes:
            # If this is a config object, it's frozen, so it's
            # safe to shallow copy.
            v = getattr(self, k)
            # On a frozen job, parent=None means a base job
            if v is self.BASE_JOB_MARKER:
                v = None
            # Playbooks have a lot of objects that can be flattened at
            # this point to simplify serialization.
            if k in ('pre_run', 'run', 'post_run', 'cleanup_run'):
                v = [self._freezePlaybook(layout, item, pb,
                                          redact_secrets_and_keys)
                     for pb in v if pb.source_context]
                frozen_playbooks.extend(v)

            kw[k] = v

        kw['nodeset_index'] = 0
        kw['secrets'] = []
        if not redact_secrets_and_keys:
            # If we're redacting, don't de-duplicate so that
            # it's clear that the value ("REDACTED") is
            # redacted.
            kw['secrets'] = self._deduplicateSecrets(context, frozen_playbooks)
        kw['affected_projects'] = self._getAffectedProjects(tenant)
        # Fill in the zuul project for any include-vars that don't specify it
        kw['include_vars'] = [self._freezeIncludeVars(
            tenant, layout, change, iv) for iv in kw['include_vars']]
        kw['config_hash'] = self.getConfigHash(tenant)
        # Ensure that the these attributes are exactly equal to what
        # would be deserialized on another scheduler.
        kw['nodeset_alternatives'] = [
            NodeSet.fromDict(alt.toDict()) for alt in
            self.flattenNodesetAlternatives(layout)
        ]
        kw['dependencies'] = frozenset(kw['dependencies'])
        kw['semaphores'] = list(kw['semaphores'])
        kw['failure_output'] = list(kw['failure_output'])
        kw['ref'] = change.cache_key
        # Don't add buildset to attributes since it's not serialized
        kw['buildset'] = buildset
        # This creates the frozen job in memory but does not write it
        # to ZK yet.  We may end up combining the job with other jobs
        # before we finalize the job graph.  We will write all
        # remaining jobs to zk at that point.
        return FrozenJob.createInMemory(**kw)

    def getConfigHash(self, tenant):
        # Make a hash of the job configuration for determining whether
        # it has been updated.
        hasher = hashlib.sha256()
        job_dict = self.toDict(tenant)
        # Ignore changes to file matchers since they don't affect
        # the content of the job.
        for attr in ['files', 'irrelevant_files',
                     'source_context', 'description']:
            job_dict.pop(attr, None)
        # Use json_dumps to strip any ZuulMark entries
        hasher.update(json_dumps(job_dict, sort_keys=True).encode('utf8'))
        return hasher.hexdigest()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        # Compare the name and all inheritable attributes to determine
        # whether two jobs with the same name are identically
        # configured.  Useful upon reconfiguration.
        if not isinstance(other, Job):
            return False
        if self.name != other.name:
            return False
        for k, v in self.attributes.items():
            if getattr(self, k) != getattr(other, k):
                return False
        return True

    __hash__ = object.__hash__

    def __str__(self):
        return self.name

    def __repr__(self):
        ln = 0
        if self.start_mark:
            ln = self.start_mark.line + 1
        return ('<Job %s explicit: %s implied: %s '
                'source: %s#%s>' % (
                    self.name,
                    self.explicit_branch_matcher,
                    self.implied_branch_matcher,
                    self.source_context,
                    ln))

    def __getattr__(self, name):
        v = self.__dict__.get(name)
        if v is None:
            if name in self.attributes:
                return self.attributes[name]
            raise AttributeError
        return v

    def _get(self, name):
        return self.__dict__.get(name)

    def _resolveRequiredProjects(self, layout):
        # The configloader does not resolve project names in
        # required_projects because they may resolve differently in
        # different tenants.  This method will resolve the project
        # names in required-projects to canonical names and return a
        # new required_projects dict with new JobProject instances.
        new_projects = {}
        unknown_projects = []
        for job_project in self.required_projects.values():
            (trusted, project) = layout.tenant.getProject(
                job_project.project_name)
            if project is None:
                unknown_projects.append(job_project.project_name)
                continue
            job_project = JobProject(project.canonical_name,
                                     job_project.override_branch,
                                     job_project.override_checkout)
            new_projects[project.canonical_name] = job_project
        if unknown_projects:
            raise ProjectNotFoundError(unknown_projects)
        return new_projects

    def _resolveIncludeExcludeProjects(self, layout, projects):
        new_projects = set()
        unknown_projects = []
        for p in projects:
            ptype, pname = p
            # We only need to resolve "name" types
            if ptype == 'name':
                (trusted, project) = layout.tenant.getProject(
                    pname)
                if project is None:
                    unknown_projects.append(pname)
                    continue
                p = ('name', project.canonical_name)
            new_projects.add(p)
        if unknown_projects:
            raise ProjectNotFoundError(unknown_projects)
        return list(new_projects)

    def _resolveIncludeProjects(self, layout):
        if self.include_projects is None:
            return None
        return self._resolveIncludeExcludeProjects(
            layout,
            self.include_projects)

    def _resolveExcludeProjects(self, layout):
        if self.exclude_projects is None:
            return None
        return self._resolveIncludeExcludeProjects(
            layout,
            self.exclude_projects)

    def _resolveAllowedProjects(self, layout):
        # This method does the inverse of other resolve methods: it
        # returns the unqualified project name even if the input is
        # the canonical name.  That's because when we're performing
        # that check, we always use the unqualified name (ie, tempest
        # might be allowed to run a nova job regardless of whether
        # it's from the same connection.
        if self.allowed_projects is None:
            return None
        allowed = set()
        for project_name in self.allowed_projects:
            # Don't try to resolve the source context project
            # name; it's definitely in the tenant, and if it's
            # ambiguous, that doesn't matter.  It may have been
            # set automatically because of a secret.
            if project_name != self.source_context.project_name:
                (trusted, project) = layout.tenant.getProject(project_name)
                if project is None:
                    raise ProjectNotFoundError(project_name)
                project_name = project.name
            allowed.add(project_name)
        return allowed

    def _resolveIncludeVars(self, layout):
        new_include_vars = []
        for iv in self.include_vars:
            pname = None
            if iv.project_name is not None:
                (trusted, project) = layout.tenant.getProject(iv.project_name)
                if project is None:
                    raise ProjectNotFoundError(iv.project_name)
                pname = project.canonical_name
            job_include_vars = JobIncludeVars(iv.name,
                                              pname,
                                              iv.required,
                                              iv.use_ref)
            new_include_vars.append(job_include_vars)
        return new_include_vars

    def _resolveRoles(self, layout):
        new_roles = []
        for r in self.roles:
            pname = None
            if r.project_name is not None:
                (trusted, project) = layout.tenant.getProject(r.project_name)
                if project is None:
                    # TODO: failure to find roles has always been
                    # silently ignored.  Should this become an error?
                    # Note that getProject can raise an error if the
                    # name is ambiguous.
                    continue
                pname = project.canonical_name
            # Use copy to get the appropriate Role subclass
            role = r.copy()
            role.project_name = pname
            new_roles.append(role)
        return new_roles

    def setBase(self, layout, semaphore_handler):
        # A job in an untrusted repo that uses secrets requires
        # special care.  We must note this, and carry this flag
        # through inheritance to ensure that we don't run this job in
        # an unsafe check pipeline.  We must also set allowed-projects
        # to only the current project, as otherwise, other projects
        # might be able to cause something to happen with the secret
        # by using a depends-on header.
        if ((not self.post_review) and (
                self.secrets and not layout.tenant.isTrusted(
                    self.source_context.project_canonical_name))):
            self.post_review = True
            self.allowed_projects = set([self.source_context.project_name])
        if self._get('required_projects'):
            self.required_projects = self._resolveRequiredProjects(layout)
        if self._get('include_projects'):
            self.include_projects = self._resolveIncludeProjects(layout)
        if self._get('exclude_projects'):
            self.exclude_projects = self._resolveExcludeProjects(layout)
        if self._get('include_vars'):
            self.include_vars = self._resolveIncludeVars(layout)
        if self._get('roles'):
            self.roles = self._resolveRoles(layout)
        if self._get('run') is not None:
            self.run = self.freezePlaybooks(
                self.run, layout, semaphore_handler)
        if self._get('pre_run') is not None:
            self.pre_run = self.freezePlaybooks(
                self.pre_run, layout, semaphore_handler)
        if self._get('post_run') is not None:
            self.post_run = self.freezePlaybooks(
                self.post_run, layout, semaphore_handler)
        if self._get('cleanup_run') is not None:
            self.cleanup_run = self.freezePlaybooks(
                self.cleanup_run, layout, semaphore_handler)
        self.inheritance_path = self.inheritance_path + (repr(self),)

    def getNodeset(self, layout):
        if isinstance(self.nodeset, str):
            # This references an existing named nodeset in the layout.
            ns = layout.nodesets.get(self.nodeset)
            if ns is None:
                raise NodesetNotFoundError(self.nodeset)
            return ns
        return self.nodeset

    def validateReferences(self, layout, project_config=False):
        # Verify that references to other objects in the layout are
        # valid.
        if not self.isBase() and self.parent:
            parents = layout.getJobs(self.parent)
            if not parents:
                raise Exception("Job %s not defined" % (self.parent,))
            # For the following checks, we allow some leeway to
            # account for the possibility that there may be
            # nonconforming job variants that would not match and
            # therefore not trigger an error during job graph
            # freezing.  We only raise an error here if there is no
            # possibility of success, which may help prevent errors in
            # most cases.  If we don't raise an error here, the
            # possibility of later failure still remains.
            nonfinal_parent_found = False
            nonintermediate_parent_found = False
            nonprotected_parent_found = False
            for p in parents:
                if not p.final:
                    nonfinal_parent_found = True
                if not p.intermediate:
                    nonintermediate_parent_found = True
                if not p.protected:
                    nonprotected_parent_found = True
                if (nonfinal_parent_found and
                    nonintermediate_parent_found and
                    nonprotected_parent_found):
                    break

            if not nonfinal_parent_found:
                raise Exception(
                    f'The parent of job "{self.name}", "{self.parent}" '
                    'is final and can not act as a parent')
            if not nonintermediate_parent_found and not self.abstract:
                raise Exception(
                    f'The parent of job "{self.name}", "{self.parent}" '
                    f'is intermediate but "{self.name}" is not abstract')
            if (not nonprotected_parent_found and
                parents[0].source_context.project_canonical_name !=
                self.source_context.project_canonical_name):
                raise Exception(
                    f'The parent of job "{self.name}", "{self.parent}" '
                    f'is a protected job in a different project')

        for ns in self.flattenNodesetAlternatives(layout):
            if layout.tenant.max_nodes_per_job != -1 and \
               len(ns) > layout.tenant.max_nodes_per_job:
                raise Exception(
                    'The job "{job}" exceeds tenant '
                    'max-nodes-per-job {maxnodes}.'.format(
                        job=self.name,
                        maxnodes=layout.tenant.max_nodes_per_job))

        for dependency in self.dependencies:
            layout.getJob(dependency.name)
        for pb in self.all_playbooks:
            pb.validateReferences(layout)

        if not layout.tenant.use_nodepool:
            if isinstance(self.nodeset, NodeSet):
                self.nodeset.validateReferences(layout)

    def assertImagePermissions(self, image_build_name, config_object, layout):
        # config_object may be a project or an anonymous job variant
        image = layout.images.get(image_build_name)
        if image is None:
            raise JobConfigurationError(
                f'The job "{self.name}" references an unknown image '
                f'"{image_build_name}"')
        ppc_origin = config_object.source_context.project_canonical_name
        image_origin = image.source_context.project_canonical_name
        if ppc_origin != image_origin:
            raise JobConfigurationError(
                f'The image build job "{self.name}" may not be attached '
                f'to a pipeline in {ppc_origin} while referencing the '
                f'image "{image_build_name}" defined in {image_origin}')

    def addRoles(self, other, layout):
        # Get fully qualified project names
        roles = other._resolveRoles(layout)

        newroles = []
        # Start with a copy of the existing roles, but if any of them
        # are implicit roles which are identified as explicit in the
        # new roles list, replace them with the explicit version.
        changed = False
        for existing_role in self.roles:
            if existing_role in roles:
                new_role = roles[roles.index(existing_role)]
            else:
                new_role = None
            if (new_role and
                isinstance(new_role, ZuulRole) and
                isinstance(existing_role, ZuulRole) and
                existing_role.implicit and not new_role.implicit):
                newroles.append(new_role)
                changed = True
            else:
                newroles.append(existing_role)
        # Now add the new roles.
        for role in reversed(roles):
            if role not in newroles:
                newroles.insert(0, role)
                changed = True
        if changed:
            self.roles = tuple(newroles)

    def getBranches(self, tenant):
        # Return the raw branch list that match this job
        bm = self.getBranchMatcher(tenant)
        if bm:
            # bm is a ManchAny
            return [x._regex for x in bm.matchers]
        return None

    def setExplicitBranchMatchers(self, matchers):
        self.explicit_branch_matcher = change_matcher.MatchAny(matchers)

    def setImpliedBranchMatchers(self, matchers):
        self.implied_branch_matcher = change_matcher.MatchAny(matchers)

    def getBranchMatcher(self, tenant):
        # Explicit branch matchers always win
        if self.explicit_branch_matcher is not None:
            return self.explicit_branch_matcher

        # Project pipeline jobs never use implicit branch matchers
        if self.project_pipeline:
            return None

        # noop job has no source context:
        if self.source_context:
            source_tpc = tenant.getTPC(
                self.source_context.project_canonical_name)
            if self.source_context.implied_branch_matchers is not None:
                use_ibm = self.source_context.implied_branch_matchers
            else:
                use_ibm = source_tpc.implied_branch_matchers
            # If there was an explicit pragma to use implied branch
            # matchers or not, honor it
            if use_ibm is True:
                ibm = (self.source_context.implied_branch_matcher or
                       self.implied_branch_matcher)
                return ibm
            if use_ibm is False:
                return None
            # If we were defined in a trusted context in this tenant (and
            # there's no pragma), then we do not use implied branch
            # matchers.
            if source_tpc.trusted:
                return None

            # If this project only has one branch, don't use implied
            # branch matchers.  This way central job repos can work.
            branches = tenant.getProjectBranches(
                self.source_context.project_canonical_name)
            if len(branches) == 1:
                return None
        return self.implied_branch_matcher

    def setFileMatcher(self, files):
        # Set the file matcher to match any of the change files
        # Input is a list of ZuulRegex objects
        self._files = [x.toDict() for x in files]
        self.file_matcher = change_matcher.MatchAnyFiles(
            [change_matcher.FileMatcher(zuul_regex)
             for zuul_regex in sorted(files, key=lambda x: x.pattern)])

    def setIrrelevantFileMatcher(self, irrelevant_files):
        # Set the irrelevant file matcher to match any of the change files
        # Input is a list of ZuulRegex objects
        self._irrelevant_files = [x.toDict() for x in irrelevant_files]
        self.irrelevant_file_matcher = change_matcher.MatchAllFiles(
            [change_matcher.FileMatcher(zuul_regex)
             for zuul_regex in sorted(irrelevant_files,
                                      key=lambda x: x.pattern)])

    def _handleFinalControl(self, other, attr):
        if other._get(attr) is not None:
            if self.final_control[attr]:
                # This message says "job foo final attribute bar";
                # compare to "final job foo attribute bar" elsewhere
                # to distinguish final jobs from final attrs.
                raise JobConfigurationError(
                    "Unable to modify job %s final attribute "
                    "%s=%s with variant %s" % (
                        repr(self), attr, other._get(attr),
                        repr(other)))
        if other.final_control[attr]:
            fc = self.final_control.copy()
            fc[attr] = True
            self.final_control = types.MappingProxyType(fc)

    def _updateVariableAttribute(self, other, attr):
        self._handleFinalControl(other, attr)
        if other.override_control[attr]:
            setattr(self, attr, getattr(other, attr))
        else:
            setattr(self, attr, Job._deepUpdate(
                getattr(self, attr), getattr(other, attr)))

    def updateVariables(self, other):
        self._updateVariableAttribute(other, 'variables')
        self._updateVariableAttribute(other, 'extra_variables')
        self._updateVariableAttribute(other, 'host_variables')
        self._updateVariableAttribute(other, 'group_variables')

    def updateProjectVariables(self, project_vars):
        # Merge project/template variables directly into the job
        # variables.  Job variables override project variables.
        self.variables = Job._deepUpdate(project_vars, self.variables)

    def updateIncludeVars(self, other, layout):
        self._handleFinalControl(other, 'include_vars')
        if other.override_control['include_vars']:
            include_vars = other._resolveIncludeVars(layout)
        else:
            include_vars = list(self.include_vars)
            for iv in other._resolveIncludeVars(layout):
                if iv not in include_vars:
                    include_vars.append(iv)
        self.include_vars = include_vars

    def updateRequiredProjects(self, other, layout):
        self._handleFinalControl(other, 'required_projects')
        if other.override_control['required_projects']:
            required_projects = {}
        else:
            required_projects = self.required_projects.copy()
        required_projects.update(other._resolveRequiredProjects(layout))
        self.required_projects = required_projects

    def _updateIncludeExclude(self, other, this_projects, other_projects,
                              override, layout):
        if override:
            projects = other_projects
        else:
            projects = this_projects
        if projects is not None:
            projects = set(projects)
        if projects is None and other_projects is not None:
            projects = set()
        if other_projects is not None and not override:
            projects.update(other._resolveIncludeExcludeProjects(
                layout, other_projects))
        if projects is not None:
            projects = list(projects)
        return projects

    def updateIncludeProjects(self, other, layout):
        self._handleFinalControl(other, 'include_projects')
        self.include_projects = self._updateIncludeExclude(
            other,
            self.include_projects,
            other.include_projects,
            other.override_control['include_projects'],
            layout,
        )

    def updateExcludeProjects(self, other, layout):
        self._handleFinalControl(other, 'exclude_projects')
        self.exclude_projects = self._updateIncludeExclude(
            other,
            self.exclude_projects,
            other.exclude_projects,
            other.override_control['exclude_projects'],
            layout,
        )

    def updateAllowedProjects(self, other, layout):
        other_allowed_projects = other._resolveAllowedProjects(layout)
        if self._get('allowed_projects') is not None:
            self.allowed_projects = self.allowed_projects.intersection(
                other_allowed_projects)
        else:
            self.allowed_projects = other_allowed_projects

    @staticmethod
    def _deepUpdate(a, b):
        # Merge nested dictionaries if possible, otherwise, overwrite
        # the value in 'a' with the value in 'b'.

        ret = {}
        for k, av in a.items():
            if k not in b:
                ret[k] = av
        for k, bv in b.items():
            av = a.get(k)
            if (isinstance(av, (dict, types.MappingProxyType)) and
                isinstance(bv, (dict, types.MappingProxyType))):
                ret[k] = Job._deepUpdate(av, bv)
            else:
                ret[k] = bv
        return ret

    def copy(self):
        job = Job(self.name)
        for k in self.attributes:
            v = self._get(k)
            if v is not None:
                # If this is a config object, it's frozen, so it's
                # safe to shallow copy.
                setattr(job, k, v)
        job.final_control = self.final_control
        return job

    def freezePlaybooks(self, pblist, layout, semaphore_handler):
        """Take a list of playbooks, and return a copy of it updated with this
        job's roles.

        """

        ret = []
        for old_pb in pblist:
            pb = old_pb.copy()
            pb.roles = self.roles
            pb.freezeSecrets(layout)
            pb.freezeTrusted(layout)
            pb.freezeSemaphores(layout, semaphore_handler)
            pb.nesting_level = len(self.inheritance_path)
            ret.append(pb)
        return tuple(ret)

    def applyVariant(self, other, layout, semaphore_handler):
        """Copy the attributes which have been set on the other job to this
        job."""
        if not isinstance(other, Job):
            raise Exception("Job unable to inherit from %s" % (other,))

        for k in self.execution_attributes:
            if (other._get(k) is not None and
                k not in set(['final', 'abstract', 'protected',
                              'intermediate'])):
                if self.final:
                    raise JobConfigurationError(
                        "Unable to modify final job %s attribute "
                        "%s=%s with variant %s" % (
                            repr(self), k, other._get(k),
                            repr(other)))
                if self.protected_origin:
                    # this is a protected job, check origin of job definition
                    this_origin = self.protected_origin
                    other_origin = other.source_context.project_canonical_name
                    if this_origin != other_origin:
                        raise JobConfigurationError(
                            "Job %s which is defined in %s is "
                            "protected and cannot be inherited "
                            "from other projects."
                            % (repr(self), this_origin))
                self._handleFinalControl(other, k)
                if k not in set(['pre_run', 'run', 'post_run', 'cleanup_run',
                                 'roles', 'variables', 'extra_variables',
                                 'host_variables', 'group_variables',
                                 'required_projects', 'include_projects',
                                 'exclude_projects', 'allowed_projects',
                                 'semaphores', 'failure_output',
                                 'include_vars']):
                    setattr(self, k, other._get(k))

        # Don't set final above so that we don't trip an error halfway
        # through assignment.
        if other.final != self.attributes['final']:
            self.final = other.final

        # Abstract may not be reset by a variant, it may only be
        # cleared by inheriting.
        if other.name != self.name:
            self.abstract = other.abstract
        elif other.abstract:
            self.abstract = True

        # An intermediate job may only be inherited by an abstract
        # job.  Note intermediate jobs must be also be abstract, that
        # has been enforced during config reading.  Similar to
        # abstract, it is cleared by inheriting.
        if self.intermediate and not other.abstract:
            raise JobConfigurationError(
                "Intermediate job %s may only be inherited "
                "by another abstract job" %
                (repr(self)))
        if other.name != self.name:
            self.intermediate = other.intermediate
        elif other.intermediate:
            self.intermediate = True

        # Protected may only be set to true
        if other.protected is not None:
            # don't allow to reset protected flag
            if not other.protected and self.protected_origin:
                raise JobConfigurationError(
                    "Unable to reset protected attribute of job"
                    " %s by job %s" % (
                        repr(self), repr(other)))
            if not self.protected_origin:
                self.protected_origin = \
                    other.source_context.project_canonical_name

        # We must update roles before any playbook contexts
        if other._get('roles') is not None:
            self.addRoles(other, layout)

        # Freeze the nodeset
        self.nodeset = self.getNodeset(layout)

        # Pass secrets to parents
        secrets_for_parents = [s for s in other.secrets if s.pass_to_parent]
        if secrets_for_parents:
            frozen_secrets = []
            for secret_use in secrets_for_parents:
                secret = layout.secrets.get(secret_use.name)
                if secret is None:
                    raise SecretNotFoundError(
                        "Secret %s not found" % (secret_use.name,))
                secret_name = secret_use.alias
                encrypted_secret_data = secret.serialize(layout)
                # Use the other project, not the secret's, because we
                # want to decrypt with the other project's key key.
                connection_name = other.source_context.project_connection_name
                project_name = other.source_context.project_name
                frozen_secrets.append(FrozenSecret.construct_cached(
                    connection_name, project_name,
                    secret_name, encrypted_secret_data))

            # Add the secrets to any existing playbooks.  If any of
            # them are in an untrusted project, then we've just given
            # a secret to a playbook which can run in dynamic config,
            # therefore it's no longer safe to run this job
            # pre-review.  The only way pass-to-parent can work with
            # pre-review pipeline is if all playbooks are in the
            # trusted context.
            for pb in itertools.chain(
                    self.pre_run, self.run, self.post_run, self.cleanup_run):
                pb.addSecrets(frozen_secrets)
                if not layout.tenant.isTrusted(
                        pb.source_context.project_canonical_name):
                    self.post_review = True
        # A job in an untrusted repo that uses secrets requires
        # special care.  We must note this, and carry this flag
        # through inheritance to ensure that we don't run this job in
        # an unsafe check pipeline.  We must also set allowed-projects
        # to only the current project, as otherwise, other projects
        # might be able to cause something to happen with the secret
        # by using a depends-on header.
        if ((not self.post_review) and (
                other.secrets and not layout.tenant.isTrusted(
                    other.source_context.project_canonical_name))):
            self.post_review = True
            other_allowed_projects = set([other.source_context.project_name])
            if self._get('allowed_projects') is not None:
                self.allowed_projects = self.allowed_projects.intersection(
                    other_allowed_projects)
            else:
                self.allowed_projects = other_allowed_projects
        if other._get('run') is not None:
            other_run = self.freezePlaybooks(
                other.run, layout, semaphore_handler)
            self.run = other_run
        if other._get('pre_run') is not None:
            other_pre_run = self.freezePlaybooks(
                other.pre_run, layout, semaphore_handler)
            self.pre_run = self.pre_run + other_pre_run
        if other._get('post_run') is not None:
            other_post_run = self.freezePlaybooks(
                other.post_run, layout, semaphore_handler)
            self.post_run = other_post_run + self.post_run
        if other._get('cleanup_run') is not None:
            other_cleanup_run = self.freezePlaybooks(
                other.cleanup_run, layout, semaphore_handler)
            self.cleanup_run = other_cleanup_run + self.cleanup_run
        self.updateVariables(other)
        if other._get('include_vars') is not None:
            self.updateIncludeVars(other, layout)
        if other._get('required_projects') is not None:
            self.updateRequiredProjects(other, layout)
        if other._get('include_projects') is not None:
            self.updateIncludeProjects(other, layout)
        if other._get('exclude_projects') is not None:
            self.updateExcludeProjects(other, layout)
        if other._get('allowed_projects') is not None:
            self.updateAllowedProjects(other, layout)
        if other._get('semaphores') is not None:
            # Sort the list of semaphores to avoid issues with
            # contention (where two jobs try to start at the same time
            # and fail due to acquiring the same semaphores but in
            # reverse order.
            # Override control is explicitly not supported.
            semaphores = set(self.semaphores).union(set(other.semaphores))
            self.semaphores = tuple(sorted(semaphores, key=lambda x: x.name))
        if other._get('failure_output') is not None:
            self._handleFinalControl(other, 'failure_output')
            if other.override_control['failure_output']:
                failure_output = other.failure_output
            else:
                failure_output = set(self.failure_output).union(
                    set(other.failure_output))
            self.failure_output = tuple(sorted(failure_output))

        pb_semaphores = set()
        for pb in self.all_playbooks:
            pb_semaphores.update([x['name'] for x in pb.frozen_semaphores])
        common = (set([x.name for x in self.semaphores]) &
                  pb_semaphores)
        if common:
            raise JobConfigurationError(
                f"Semaphores {common} specified as both "
                "job and playbook semaphores but may only "
                "be used for one")

        for k in self.context_attributes:
            self._handleFinalControl(other, k)
            if (v := other._get(k)) is None:
                continue
            if other.override_control[k]:
                setattr(self, k, v)
            else:
                if isinstance(v, (set, frozenset)):
                    setattr(self, k, getattr(self, k).union(v))
                elif isinstance(v, change_matcher.AbstractMatcherCollection):
                    ours = getattr(self, k)
                    ours = ours and set(ours.matchers) or set()
                    matchers = ours.union(set(v.matchers))
                    if k == 'file_matcher':
                        self.setFileMatcher([m.regex for m in matchers])
                    elif k == 'irrelevant_file_matcher':
                        self.setIrrelevantFileMatcher(
                            [m.regex for m in matchers])
                    else:
                        raise NotImplementedError()
                elif k in ('_files', '_irrelevant_files',):
                    pass
                else:
                    raise NotImplementedError()

        self.inheritance_path = self.inheritance_path + (repr(other),)

    def changeMatchesBranch(self, tenant, change, override_branch=None):
        if override_branch is None:
            branch_change = change
        else:
            # If an override branch is supplied, create a very basic
            # change and set its branch to the override branch.
            branch_change = Branch(change.project)
            branch_change.branch = override_branch

        branch_matcher = self.getBranchMatcher(tenant)
        if branch_matcher and not branch_matcher.matches(branch_change):
            return False

        return True

    def changeMatchesFiles(self, change):
        if self.file_matcher and not self.file_matcher.matches(change):
            return False

        # NB: This is a negative match.
        if (self.irrelevant_file_matcher and
            self.irrelevant_file_matcher.matches(change)):
            return False

        return True

    def itemMatchesImage(self, item):
        if not self.image_build_name:
            return True

        if not (item.event and hasattr(item.event, 'image_names')):
            return True

        image_names = item.event.image_names or []
        if not image_names:
            return True

        if self.image_build_name in image_names:
            return True

        return False

    @property
    def all_playbooks(self):
        for k in ('pre_run', 'run', 'post_run', 'cleanup_run'):
            playbooks = getattr(self, k)
            yield from playbooks


class JobIncludeVars(ConfigObject):
    """ A reference to a variables file from a job. """

    def __init__(self, name, project_name, required, use_ref):
        super().__init__()
        self.name = name
        # The repo to look for the file in, or None for the zuul project
        self.project_name = project_name
        self.required = required
        self.use_ref = use_ref

    def toDict(self):
        d = dict()
        d['name'] = self.name
        d['project_name'] = self.project_name
        d['required'] = self.required
        d['use_ref'] = self.use_ref
        return d

    def __hash__(self):
        return hash(json.dumps(self.toDict(), sort_keys=True))

    def __eq__(self, other):
        if not isinstance(other, JobIncludeVars):
            return False
        return self.toDict() == other.toDict()


class JobProject(ConfigObject):
    """ A reference to a project from a job. """

    def __init__(self, project_name, override_branch=None,
                 override_checkout=None):
        super(JobProject, self).__init__()
        self.project_name = project_name
        self.override_branch = override_branch
        self.override_checkout = override_checkout

    def toDict(self):
        d = dict()
        d['project_name'] = self.project_name
        d['override_branch'] = self.override_branch
        d['override_checkout'] = self.override_checkout
        return d

    @classmethod
    def fromDict(cls, data):
        return cls(data['project_name'],
                   data['override_branch'],
                   data['override_checkout'])

    def __hash__(self):
        return hash(json.dumps(self.toDict(), sort_keys=True))

    def __eq__(self, other):
        if not isinstance(other, JobProject):
            return False
        return self.toDict() == other.toDict()


class JobSemaphore(ConfigObject):
    """ A reference to a semaphore from a job. """

    def __init__(self, semaphore_name, resources_first=False):
        super().__init__()
        self.name = semaphore_name
        self.resources_first = resources_first

    def __repr__(self):
        first = self.resources_first and ' resources first' or ''
        return '<JobSemaphore %s%s>' % (self.name, first)

    def toDict(self):
        d = dict()
        d['name'] = self.name
        d['resources_first'] = self.resources_first
        return d

    @classmethod
    def fromDict(cls, data):
        return cls(data['name'], data['resources_first'])

    def __hash__(self):
        return hash(json.dumps(self.toDict(), sort_keys=True))

    def __eq__(self, other):
        if not isinstance(other, JobSemaphore):
            return False
        return self.toDict() == other.toDict()


class JobList(ConfigObject):
    """ A list of jobs in a project's pipeline. """

    def __init__(self):
        super(JobList, self).__init__()
        self.jobs = OrderedDict()  # job.name -> [job, ...]

    def addJob(self, job):
        if job.name in self.jobs:
            self.jobs[job.name].append(job)
        else:
            self.jobs[job.name] = [job]

    def inheritFrom(self, other):
        for jobname, jobs in other.jobs.items():
            joblist = self.jobs.setdefault(jobname, [])
            for job in jobs:
                if job not in joblist:
                    joblist.append(job)


class JobDependency(ConfigObject):
    """ A reference to another job in the project-pipeline-config. """
    def __init__(self, name, soft=False):
        super(JobDependency, self).__init__()
        self.name = name
        self.soft = soft

    def __repr__(self):
        soft = self.soft and ' soft' or ''
        return '<JobDependency %s%s>' % (self.name, soft)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, JobDependency):
            return False
        return self.toDict() == other.toDict()

    def __hash__(self):
        return hash(json.dumps(self.toDict(), sort_keys=True))

    def toDict(self):
        return {'name': self.name,
                'soft': self.soft}

    @classmethod
    def fromDict(cls, data):
        return cls(data['name'], data['soft'])


class JobGraph(object):
    """A JobGraph represents the dependency graph between Jobs.

    This class is an attribute of the BuildSet, and should not be
    modified after its initial creation.
    """

    def __init__(self, job_map):
        # The jobs parameter is a reference to an attribute on the
        # BuildSet (either the real list of jobs, or a cached list of
        # "old" jobs for comparison).
        self._job_map = job_map
        # An ordered list of job UUIDs
        self.job_uuids = []
        # dependent_job_uuid -> dict(parent_job_name -> soft)
        self._dependencies = {}
        # The correct terminology is "dependencies" and "dependents",
        # which is confusing, but it's not appropriate to use terms
        # like "parent" and "child" since we use those for
        # inheritance.  This is another dimension.  But to clarify the
        # correct terminology using the wrong terms: dependency ~=
        # parent and dependent ~= child.  So if job A must run to
        # completion before job B, then job A is a dependency of job
        # B, and job B is a dependent of job A.
        # Dict of {job_uuid: {parent_uuid: {soft: bool}}}
        self.job_dependencies = {}
        # Dict of {job_uuid: {child_uuid: {soft: bool}}}
        self.job_dependents = {}
        self.project_metadata = {}

    def __repr__(self):
        return '<JobGraph %s>' % (self.job_uuids)

    def toDict(self):
        data = {
            "job_uuids": self.job_uuids,
            "dependencies": self._dependencies,
            "job_dependencies": self.job_dependencies,
            "job_dependents": self.job_dependents,
            "project_metadata": {
                k: v.toDict() for (k, v) in self.project_metadata.items()
            },
        }
        return data

    @classmethod
    def fromDict(klass, data, job_map):
        self = klass(job_map)
        self.job_uuids = data['job_uuids']
        self._dependencies = data['dependencies']
        self.project_metadata = {
            k: ProjectMetadata.fromDict(v)
            for (k, v) in data['project_metadata'].items()
        }
        self.job_dependencies = data['job_dependencies']
        self.job_dependents = data['job_dependents']
        return self

    def addJob(self, job):
        # A graph must be created after the job list is frozen,
        # therefore we should only get one job with the same name.
        self._job_map[job.uuid] = job
        self.job_uuids.append(job.uuid)
        # Append the dependency information
        self._dependencies.setdefault(job.uuid, {})
        for dependency in job.dependencies:
            self._dependencies[job.uuid][dependency.name] = dependency.soft

    def addInitializerJobs(self, log):
        init_jobs = []
        reg_jobs = []
        for job in self._job_map.values():
            if job.type == 'regular':
                reg_jobs.append(job)
            elif job.type == 'initializer':
                init_jobs.append(job)
        for reg_job in reg_jobs:
            for init_job in init_jobs:
                # True means a soft dependency; if the init job
                # doesn't run due to file matchers, we won't fail.
                self._dependencies[reg_job.uuid][init_job.name] = True
                log.debug("Adding initializer dependency %s -> %s",
                          reg_job, init_job)

    def _removeJob(self, job):
        # This should only be called internally during deduplication
        del self._job_map[job.uuid]
        self.job_uuids.remove(job.uuid)
        # Append the dependency information
        del self._dependencies[job.uuid]

    def getJobs(self):
        # Report in the order of layout cfg
        return [self._job_map[x] for x in self.job_uuids]

    def getJobFromUuid(self, job_uuid):
        return self._job_map[job_uuid]

    def getJob(self, name, ref):
        for job in self._job_map.values():
            if job.name == name and ref in job.all_refs:
                return job

    def getDirectDependentJobs(self, job):
        ret = []
        for dependent_uuid, dependent_data in \
            self.job_dependents.get(job.uuid, {}).items():
            ret.append(self.getJobFromUuid(dependent_uuid))
        return ret

    def getDependentJobsRecursively(self, job, skip_soft=False):
        all_dependent_uuids = set()
        uuids_to_iterate = set([(job.uuid, False)])
        while len(uuids_to_iterate) > 0:
            (current_uuid, current_data) = uuids_to_iterate.pop()
            current_dependent_uuids = self.job_dependents.get(current_uuid, {})
            if skip_soft:
                hard_dependent_uuids = {
                    j: d for j, d in current_dependent_uuids.items()
                    if not d['soft']
                }
                current_dependent_uuids = hard_dependent_uuids
            if job.uuid != current_uuid:
                all_dependent_uuids.add(current_uuid)
            new_dependent_uuids = (set(current_dependent_uuids.keys()) -
                                   all_dependent_uuids)
            for u in new_dependent_uuids:
                uuids_to_iterate.add((u, current_dependent_uuids[u]['soft']))
        return [self.getJobFromUuid(u) for u in all_dependent_uuids]

    def _freezeDependencies(self, log, layout=None):
        job_dependencies = {}
        job_dependents = {}
        for dependent_uuid, parents in self._dependencies.items():
            dependencies = job_dependencies.setdefault(dependent_uuid, {})
            for parent_name, parent_soft in parents.items():
                dependent_job = self._job_map[dependent_uuid]
                # We typically depend on jobs with the same ref, but
                # if we have been deduplicated, then we depend on
                # every job-ref for the given parent job.
                for ref in dependent_job.all_refs:
                    parent_job = self.getJob(parent_name, ref)
                    if parent_job is None:
                        if parent_soft:
                            if layout:
                                # If the caller spplied a layout,
                                # verify that the job exists to
                                # provide a helpful error message.
                                # Called for exception side effect:
                                layout.getJob(parent_name)
                            continue
                        raise JobConfigurationError(
                            "Job %s depends on %s which was not run." %
                            (dependent_job.name, parent_name))
                    dependencies[parent_job.uuid] = dict(soft=parent_soft)
                    dependents = job_dependents.setdefault(
                        parent_job.uuid, {})
                    dependents[dependent_uuid] = dict(soft=parent_soft)
        for dependent_uuid, parents in self._dependencies.items():
            dependent_job = self._job_map[dependent_uuid]
            # For the side effect of verifying no cycles
            self._getParentJobsRecursively(job_dependencies, dependent_job)
        return (job_dependencies, job_dependents)

    def freezeDependencies(self, log, layout=None):
        self.job_dependencies, self.job_dependents = \
            self._freezeDependencies(log, layout)

    def _getParentJobsRecursively(self, job_dependencies, job,
                                  skip_soft=False):
        all_dependency_uuids = set()
        uuids_to_iterate = set([job.uuid])
        ancestor_uuids = set()
        while len(uuids_to_iterate) > 0:
            current_uuid = uuids_to_iterate.pop()
            if current_uuid in ancestor_uuids:
                current_job = self.getJobFromUuid(current_uuid)
                raise Exception("Dependency cycle detected in job %s" %
                                current_job.name)
            ancestor_uuids.add(current_uuid)
            current_dependency_uuids = job_dependencies.get(
                current_uuid, {})
            if skip_soft:
                hard_dependency_uuids = {
                    j: d for j, d in current_dependency_uuids.items()
                    if not d['soft']
                }
                current_dependency_uuids = hard_dependency_uuids
            if job.uuid != current_uuid:
                all_dependency_uuids.add(current_uuid)
            new_dependency_uuids = (set(current_dependency_uuids.keys()) -
                                    all_dependency_uuids)
            uuids_to_iterate.update(new_dependency_uuids)
        return [self.getJobFromUuid(u) for u in all_dependency_uuids]

    def getParentJobsRecursively(self, job, skip_soft=False):
        return self._getParentJobsRecursively(self.job_dependencies, job,
                                              skip_soft)

    def getProjectMetadata(self, name):
        if name in self.project_metadata:
            return self.project_metadata[name]
        return None

    def removeNonMatchingJobs(self, log):
        # Get a copy of the frozen dependencies as they stand now so
        # that we can recursively find parents below.
        job_dependencies, job_dependents = self._freezeDependencies(log)
        for dependent_uuid, parents in self._dependencies.items():
            dependent_job = self._job_map[dependent_uuid]
            parents = self._getParentJobsRecursively(
                job_dependencies, dependent_job, skip_soft=True)
            for parent_job in parents:
                if not dependent_job.matches_change:
                    continue
                if not parent_job.matches_change:
                    log.debug(
                        "Forcing non-matching hard dependency "
                        "%s to run for %s", parent_job, dependent_job)
                    parent_job._set(matches_change=True)

        # Afer removing duplicates and walking the dependency graph,
        # remove any jobs that we shouldn't run because of file
        # matchers.
        for job in list(self._job_map.values()):
            if not job.matches_change:
                log.debug("Removing non-matching job %s", job)
                self._removeJob(job)

    def deduplicateJobs(self, log, item):
        # Jobs are deduplicated before they start, so returned data
        # are not considered at all.
        #
        # If a to-be-deduplicated job depends on a non-deduplicated
        # job, it will treat each (job, ref) instance as a parent.
        #
        # Otherwise, each job will depend only on jobs for the same
        # ref.

        event_ref = item.event.ref if item.event else None
        # A list of lists where the inner list is a set of identical
        # jobs to merge:
        all_combine = []
        job_list = list(self._job_map.values())
        while job_list:
            job = job_list.pop(0)
            if job.deduplicate is False:
                continue
            job_combine = [job]
            for other_job in job_list[:]:
                if other_job.deduplicate is False:
                    continue
                if not other_job.isEqual(job):
                    continue
                job_change = job.buildset.item.getChangeForJob(job)
                other_job_change = other_job.buildset.item.getChangeForJob(
                    other_job)
                if job.deduplicate == 'auto':
                    # Deduplicate if there are required projects
                    # or the item project is the same.
                    if (not job.required_projects and
                        job_change.project !=
                        other_job_change.project):
                        continue
                # Deduplicate!
                job_combine.append(other_job)
                job_list.remove(other_job)
            if len(job_combine) > 1:
                all_combine.append(job_combine)
        for to_combine in all_combine:
            # to_combine is a list of jobs that should be
            # deduplicated; pick the zuul change and deduplicate the
            # rest into its job.
            primary_job = to_combine[0]
            for job in to_combine:
                job_change = job.buildset.item.getChangeForJob(job)
                if job_change.cache_key == event_ref:
                    primary_job = job
                    break
            to_combine.remove(primary_job)
            primary_job_change = job.buildset.item.getChangeForJob(primary_job)
            for other_job in to_combine:
                other_job_change = other_job.buildset.item.getChangeForJob(
                    other_job)
                log.info("Deduplicating %s for %s into %s for %s",
                         other_job, other_job_change,
                         primary_job, primary_job_change)
                primary_job.other_refs.append(other_job.ref)
                # If the jobs matched for any of the changes, we need
                # to run it.
                matches_change = (primary_job.matches_change
                                  or other_job.matches_change)
                primary_job._set(matches_change=matches_change)
                self._removeJob(other_job)


@total_ordering
class JobRequest:
    # States:
    UNSUBMITTED = "unsubmitted"
    REQUESTED = "requested"
    HOLD = "hold"  # Used by tests to stall processing
    RUNNING = "running"
    COMPLETED = "completed"

    ALL_STATES = (UNSUBMITTED, REQUESTED, HOLD, RUNNING, COMPLETED)

    # This object participates in transactions, and therefore must
    # remain small and unsharded.
    def __init__(self, uuid, precedence=None, state=None, result_path=None,
                 span_context=None):
        self.uuid = uuid
        if precedence is None:
            self.precedence = 0
        else:
            self.precedence = precedence

        if state is None:
            self.state = self.UNSUBMITTED
        else:
            self.state = state
        # Path to the future result if requested
        self.result_path = result_path
        # Reference to the parent span
        if span_context:
            self.span_context = span_context
        else:
            span = trace.get_current_span()
            self.span_context = tracing.getSpanContext(span)

        # ZK related data not serialized
        self.path = None
        self._zstat = None
        self.lock = None
        self.is_locked = False
        self.lock_contenders = set()
        self.thread_lock = threading.Lock()

    def toDict(self):
        return {
            "uuid": self.uuid,
            "state": self.state,
            "precedence": self.precedence,
            "result_path": self.result_path,
            "span_context": self.span_context,
        }

    def updateFromDict(self, data):
        self.precedence = data["precedence"]
        self.state = data["state"]
        self.result_path = data["result_path"]
        self.span_context = data.get("span_context")

    @classmethod
    def fromDict(cls, data):
        return cls(
            data["uuid"],
            precedence=data["precedence"],
            state=data["state"],
            result_path=data["result_path"],
            span_context=data.get("span_context"),
        )

    def __lt__(self, other):
        # Sort requests by precedence and their creation time in
        # ZooKeeper in ascending order to prevent older requests from
        # starving.
        if self.precedence == other.precedence:
            if self._zstat and other._zstat:
                return self._zstat.ctime < other._zstat.ctime
            # NOTE (felix): As the _zstat should always be set when retrieving
            # the request from ZooKeeper, this branch shouldn't matter
            # much. It's just there, because the _zstat could - theoretically -
            # be None.
            return self.uuid < other.uuid
        return self.precedence < other.precedence

    def __eq__(self, other):
        same_prec = self.precedence == other.precedence
        if self._zstat and other._zstat:
            same_ctime = self._zstat.ctime == other._zstat.ctime
        else:
            same_ctime = self.uuid == other.uuid
        return same_prec and same_ctime

    def __repr__(self):
        return (f"<JobRequest {self.uuid}, state={self.state}, "
                f"path={self.path} zone={self.zone}>")


class MergeRequest(JobRequest):

    # Types:
    MERGE = "merge"
    CAT = "cat"
    REF_STATE = "refstate"
    FILES_CHANGES = "fileschanges"

    def __init__(self, uuid, job_type, build_set_uuid, tenant_name,
                 pipeline_name, event_id, precedence=None, state=None,
                 result_path=None, span_context=None, span_info=None):
        super().__init__(uuid, precedence, state, result_path, span_context)
        self.job_type = job_type
        self.build_set_uuid = build_set_uuid
        self.tenant_name = tenant_name
        self.pipeline_name = pipeline_name
        self.event_id = event_id
        self.span_info = span_info

    def toDict(self):
        d = super().toDict()
        d.update({
            "job_type": self.job_type,
            "build_set_uuid": self.build_set_uuid,
            "tenant_name": self.tenant_name,
            "pipeline_name": self.pipeline_name,
            "event_id": self.event_id,
            "span_info": self.span_info,
        })
        return d

    @classmethod
    def fromDict(cls, data):
        return cls(
            data["uuid"],
            data["job_type"],
            data["build_set_uuid"],
            data["tenant_name"],
            data["pipeline_name"],
            data["event_id"],
            precedence=data["precedence"],
            state=data["state"],
            result_path=data["result_path"],
            span_context=data.get("span_context"),
            span_info=data.get("span_info"),
        )

    def __repr__(self):
        return (
            f"<MergeRequest {self.uuid}, job_type={self.job_type}, "
            f"state={self.state}, path={self.path}>"
        )


class BuildRequest(JobRequest):
    """A request for a build in a specific zone"""

    # States:
    PAUSED = 'paused'

    ALL_STATES = JobRequest.ALL_STATES + (PAUSED,)

    def __init__(self, uuid, zone, build_set_uuid, job_uuid,
                 tenant_name, pipeline_name, event_id,
                 precedence=None, state=None, result_path=None,
                 span_context=None):
        super().__init__(uuid, precedence, state, result_path, span_context)
        self.zone = zone
        self.build_set_uuid = build_set_uuid
        self.job_uuid = job_uuid
        self.tenant_name = tenant_name
        self.pipeline_name = pipeline_name
        self.event_id = event_id
        # The executor sets the worker info when it locks the build
        # request so that zuul web can use this information to
        # build the url for the live log stream.
        self.worker_info = None

    def toDict(self):
        d = super().toDict()
        d.update({
            "zone": self.zone,
            "build_set_uuid": self.build_set_uuid,
            "job_uuid": self.job_uuid,
            "tenant_name": self.tenant_name,
            "pipeline_name": self.pipeline_name,
            "event_id": self.event_id,
            "worker_info": self.worker_info,
        })
        return d

    @classmethod
    def fromDict(cls, data):
        request = cls(
            data["uuid"],
            data["zone"],
            data["build_set_uuid"],
            data["job_uuid"],
            data["tenant_name"],
            data["pipeline_name"],
            data["event_id"],
            precedence=data["precedence"],
            state=data["state"],
            result_path=data["result_path"],
            span_context=data.get("span_context"),
        )

        request.worker_info = data["worker_info"]

        return request

    def __repr__(self):
        return (
            f"<BuildRequest {self.uuid}, job={self.job_uuid}, "
            f"state={self.state}, path={self.path} zone={self.zone}>"
        )


class BuildReference:
    def __init__(self, _path):
        self._path = _path


class BuildEvent:
    TYPE_PAUSED = "paused"
    TYPE_RESUMED = "resumed"
    TYPE_SNAPSHOT_STARTED = "snapshot started"
    TYPE_SNAPSHOT_COMPLETED = "snapshot completed"

    def __init__(self, event_time, event_type, description=None):
        self.event_time = event_time
        self.event_type = event_type
        self.description = description

    def toDict(self):
        return {
            "event_time": self.event_time,
            "event_type": self.event_type,
            "description": self.description,
        }

    @classmethod
    def fromDict(cls, data):
        return cls(data["event_time"], data["event_type"], data["description"])


class Build(zkobject.ZKObject):
    """A Build is an instance of a single execution of a Job.

    While a Job describes what to run, a Build describes an actual
    execution of that Job.  Each build is associated with exactly one
    Job (related builds are grouped together in a BuildSet).
    """

    # If data/variables are more than 10k, we offload them to another
    # object, otherwise we store them on this one.
    MAX_DATA_LEN = 10 * 1024

    log = logging.getLogger("zuul.Build")

    job_data_attributes = ('result_data',
                           'secret_result_data',
                           )

    def __init__(self):
        super().__init__()
        self._set(
            job=None,
            build_set=None,
            uuid=uuid4().hex,
            url=None,
            result=None,
            _result_data=None,
            _secret_result_data=None,
            error_detail=None,
            execute_time=time.time(),
            start_time=None,
            end_time=None,
            estimated_time=None,
            canceled=False,
            paused=False,
            retry=False,
            held=False,
            unreachable=False,
            zuul_event_id=None,
            build_request_ref=None,
            span_info=None,
            # A list of build events like paused, resume, ...
            events=[],
            pre_fail=False,
            snapshotting=False,
        )

    def serialize(self, context):
        data = {
            "uuid": self.uuid,
            "url": self.url,
            "result": self.result,
            "error_detail": self.error_detail,
            "execute_time": self.execute_time,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "estimated_time": self.estimated_time,
            "canceled": self.canceled,
            "paused": self.paused,
            "pre_fail": self.pre_fail,
            "retry": self.retry,
            "snapshotting": self.snapshotting,
            "held": self.held,
            "unreachable": self.unreachable,
            "zuul_event_id": self.zuul_event_id,
            "build_request_ref": self.build_request_ref,
            "span_info": self.span_info,
            "events": [e.toDict() for e in self.events],
        }
        for k in self.job_data_attributes:
            v = getattr(self, '_' + k)
            if isinstance(v, JobData):
                v = {'storage': 'offload', 'path': v.getPath(),
                     'hash': v.hash}
            else:
                v = {'storage': 'local', 'data': v}
            data[k] = v

        return json.dumps(data, sort_keys=True).encode("utf8")

    def deserialize(self, raw, context, extra=None):
        data = super().deserialize(raw, context)

        # Deserialize build events
        data["events"] = [
            BuildEvent.fromDict(e) for e in data.get("events", [])
        ]

        # Result data can change (between a pause and build
        # completion).

        for job_data_key in self.job_data_attributes:
            job_data = data.pop(job_data_key, None)
            if job_data:
                # This is a dict which tells us where the actual data is.
                if job_data['storage'] == 'local':
                    # The data are stored locally in this dict
                    data['_' + job_data_key] = job_data['data']
                elif job_data['storage'] == 'offload':
                    existing_job_data = getattr(self, f"_{job_data_key}", None)
                    if (getattr(existing_job_data, 'hash', None) ==
                        job_data['hash']
                        and job_data['hash'] is not None):
                        # Re-use the existing object since it's the same
                        data['_' + job_data_key] = existing_job_data
                    else:
                        if job_data['hash'] is None:
                            context.log.error("JobData hash is None on %s",
                                              self)
                        # Load the object from ZK
                        data['_' + job_data_key] = JobData.fromZK(
                            context, job_data['path'])

        return data

    def getPath(self):
        return f"{self.job.getPath()}/build/{self.uuid}"

    def _save(self, context, *args, **kw):
        # Before saving, update the buildset with the new job version
        # so that future readers know to refresh it.
        self.job.buildset.updateBuildVersion(context, self)
        return super()._save(context, *args, **kw)

    def __repr__(self):
        return ('<Build %s of %s voting:%s>' %
                (self.uuid, self.job.name, self.job.voting))

    def _getJobData(self, name):
        val = getattr(self, name)
        if isinstance(val, JobData):
            return val.data
        return val

    @property
    def result_data(self):
        return self._getJobData('_result_data') or {}

    @property
    def secret_result_data(self):
        return self._getJobData('_secret_result_data') or {}

    def setResultData(self, result_data, secret_result_data):
        if not self._active_context:
            raise Exception(
                "setResultData must be used with a context manager")
        self._result_data = JobData.new(
            self._active_context,
            data=result_data,
            _path=self.getPath() + '/result_data')
        self._secret_result_data = JobData.new(
            self._active_context,
            data=secret_result_data,
            _path=self.getPath() + '/secret_result_data')

    def addEvent(self, event):
        if not self._active_context:
            raise Exception(
                "addEvent must be used with a context manager")
        self.events.append(event)

    @property
    def failed(self):
        if self.pre_fail:
            return True
        if self.result and self.result not in ['SUCCESS', 'SKIPPED', 'RETRY']:
            return True
        return False

    @property
    def pipeline(self):
        return self.build_set.item.manager.pipeline

    @property
    def log_url(self):
        log_url = self.result_data.get('zuul', {}).get('log_url')
        if log_url and log_url[-1] != '/':
            log_url = log_url + '/'
        return log_url

    def getSafeAttributes(self):
        return Attributes(uuid=self.uuid,
                          result=self.result,
                          error_detail=self.error_detail,
                          result_data=self.result_data)


class RepoFiles(zkobject.ShardedZKObject):
    """RepoFiles holds config-file content for per-project job config.

    When Zuul asks a merger to prepare a future multiple-repo state
    and collect Zuul configuration files so that we can dynamically
    load our configuration, this class provides cached access to that
    data for use by the Change which updated the config files and any
    changes that follow it in a ChangeQueue.

    It is attached to a BuildSet since the content of Zuul
    configuration files can change with each new BuildSet.
    """

    # If the node exists already, it is probably a half-written state
    # from a crash; truncate it and continue.
    truncate_on_create = True

    def __init__(self):
        super().__init__()
        self._set(connections={})

    def __repr__(self):
        return '<RepoFiles %s>' % self.connections

    def getFile(self, connection_name, project_name, branch, fn):
        host = self.connections.get(connection_name, {})
        return host.get(project_name, {}).get(branch, {}).get(fn)

    def getPath(self):
        return f"{self._buildset_path}/files"

    def serialize(self, context):
        data = {
            "connections": self.connections,
            "_buildset_path": self._buildset_path,
        }
        return json.dumps(data, sort_keys=True).encode("utf8")


class RepoState:
    def __init__(self):
        self.state = {}
        self.state_keys = {}

    def load(self, blobstore, key):
        # Load a single project-repo-state from the blobstore and
        # combine it with existing projects in this repo state.
        if key in self.state_keys.values():
            return
        data = blobstore.get(key)
        repo_state = json.loads(data.decode('utf-8'))
        # Format is {connection: {project: state}}
        for connection_name, connection_data in repo_state.items():
            projects = self.state.setdefault(connection_name, {})
            projects.update(connection_data)
            for project_name, project_data in connection_data.items():
                self.state_keys[(connection_name, project_name)] = key

    def add(self, blobstore, repo_state):
        # Split the incoming repo_state into individual
        # project-repo-state objects in the blob store.
        for connection_name, connection_data in repo_state.items():
            projects = self.state.setdefault(connection_name, {})
            for project_name, project_data in connection_data.items():
                project_dict = {project_name: project_data}
                connection_dict = {connection_name: project_dict}
                serialized = json_dumps(
                    connection_dict, sort_keys=True).encode("utf8")
                key = blobstore.put(serialized)
                projects.update(project_dict)
                self.state_keys[(connection_name, project_name)] = key

    def getKeys(self):
        return self.state_keys.values()


class BaseRepoState(zkobject.ShardedZKObject):
    """RepoState holds the repo state for a buildset

    When Zuul performs a speculative merge before enqueing an item,
    the starting state of the repo (and the repos in any items ahead)
    before that merge is encoded in a RepoState so the process can be
    repeated by the executor.

    If jobs add required-projects, a second merge operation is
    performed for any repos not in the original.  A second RepoState
    object holds the additional information.  A second object is used
    instead of updating the first since these objects are sharded --
    this simplifies error detection and recovery if a scheduler
    crashes while writing them.  They are effectively immutable once
    written.

    It is attached to a BuildSet since the content of Zuul
    configuration files can change with each new BuildSet.
    """

    # If the node exists already, it is probably a half-written state
    # from a crash; truncate it and continue.
    truncate_on_create = True

    def __init__(self):
        super().__init__()
        self._set(state={})

    def serialize(self, context):
        data = {
            "state": self.state,
            "_buildset_path": self._buildset_path,
        }
        return json.dumps(data, sort_keys=True).encode("utf8")


class BuildSet(zkobject.ZKObject):
    """A collection of Builds for one specific potential future repository
    state.

    When Zuul executes Builds for a change, it creates a Build to
    represent each execution of each job and a BuildSet to keep track
    of all the Builds running for that Change.  When Zuul re-executes
    Builds for a Change with a different configuration, all of the
    running Builds in the BuildSet for that change are aborted, and a
    new BuildSet is created to hold the Builds for the Jobs being
    run with the new configuration.

    A BuildSet also holds the UUID used to produce the Zuul Ref that
    builders check out.

    """

    log = logging.getLogger("zuul.BuildSet")

    # Merge states:
    NEW = 1
    PENDING = 2
    COMPLETE = 3

    states_map = {
        1: 'NEW',
        2: 'PENDING',
        3: 'COMPLETE',
    }

    def __init__(self):
        super().__init__()
        self._set(
            item=None,
            builds={},
            retry_builds={},
            result=None,
            uuid=uuid4().hex,
            dependent_changes=None,
            merger_items=None,
            unable_to_merge=False,
            config_errors=None,  # ConfigurationErrorList or None
            failing_reasons=[],
            debug_messages=[],
            warning_messages=[],
            merge_state=self.NEW,
            nodeset_info={},  # job -> NodesetInfo
            node_requests={},  # job -> request id
            _files=None,  # The files object if loaded
            _files_path=None,  # The ZK path to the files object
            repo_state_keys=[],  # Refs (links) to blobstore repo_state
            tries={},
            files_state=self.NEW,
            repo_state_state=self.NEW,
            span_info=None,
            configured=False,
            configured_time=None,  # When setConfigured was called
            start_time=None,  # When the buildset reported start
            repo_state_request_time=None,  # When the refstate job was called
            fail_fast=False,
            job_graph=None,
            jobs={},
            job_versions={},
            build_versions={},
            # Cached job graph of previous layout; not serialized
            _old_job_graph=None,
            _old_jobs={},
            _repo_state=RepoState(),
        )

    def setFiles(self, items):
        if self._files_path is not None:
            raise Exception("Repo files can not be updated")
        if not self._active_context:
            raise Exception("setFiles must be used with a context manager")
        connections = {}
        for item in items:
            connection = connections.setdefault(item['connection'], {})
            project = connection.setdefault(item['project'], {})
            # We are only interested in the most recent set of
            # config files for a project-branch combination.
            project[item['branch']] = item['files']
        repo_files = RepoFiles.new(self._active_context,
                                   connections=connections,
                                   _buildset_path=self.getPath())
        self._files = repo_files
        self._files_path = repo_files.getPath()

    def hasFiles(self):
        return bool(self._files_path)

    def getFiles(self, context):
        if self._files is not None:
            return self._files
        try:
            self._set(_files=RepoFiles.fromZK(context,
                                              self._files_path))
        except Exception:
            self.log.exception("Failed to restore repo files")
        return self._files

    def setConfigErrors(self, config_errors):
        if not self._active_context:
            raise Exception("setConfigErrors must be used "
                            "with a context manager")
        path = self.getPath() + '/config_errors/' + uuid4().hex
        el = ConfigurationErrorList.new(self._active_context,
                                        errors=config_errors,
                                        _path=path)
        self.config_errors = el

    @property
    def has_blocking_errors(self):
        if not self.config_errors:
            return False
        errs = filter_severity(self.config_errors.errors,
                               errors=True, warnings=False)
        return bool(errs)

    def setMergeRepoState(self, repo_state):
        if not self._active_context:
            raise Exception("setMergeRepoState must be used "
                            "with a context manager")
        blobstore = BlobStore(self._active_context)
        self._repo_state.add(blobstore, repo_state)
        for key in self._repo_state.getKeys():
            if key not in self.repo_state_keys:
                self.repo_state_keys.append(key)

    def setExtraRepoState(self, repo_state):
        if not self._active_context:
            raise Exception("setExtraRepoState must be used "
                            "with a context manager")
        blobstore = BlobStore(self._active_context)
        self._repo_state.add(blobstore, repo_state)
        for key in self._repo_state.getKeys():
            if key not in self.repo_state_keys:
                self.repo_state_keys.append(key)

    def getPath(self):
        return f"{self.item.getPath()}/buildset/{self.uuid}"

    @classmethod
    def parsePath(self, path):
        """Return path components for use by the REST API"""
        item_path, bs, uuid = path.rsplit('/', 2)
        tenant, pipeline, item_uuid = QueueItem.parsePath(item_path)
        return (tenant, pipeline, item_uuid, uuid)

    def serialize(self, context):
        data = {
            # "item": self.item,
            "builds": {j: b.getPath() for j, b in self.builds.items()},
            "retry_builds": {j: [b.getPath() for b in l]
                             for j, l in self.retry_builds.items()},
            "result": self.result,
            "uuid": self.uuid,
            "dependent_changes": self.dependent_changes,
            "merger_items": self.merger_items,
            "unable_to_merge": self.unable_to_merge,
            "config_errors": (self.config_errors.getPath()
                              if self.config_errors else None),
            "failing_reasons": self.failing_reasons,
            "debug_messages": self.debug_messages,
            "warning_messages": self.warning_messages,
            "merge_state": self.merge_state,
            "nodeset_info": {i: ni.toDict()
                             for i, ni in self.nodeset_info.items()},
            "node_requests": self.node_requests,
            "files": self._files_path,
            "repo_state_keys": self.repo_state_keys,
            "tries": self.tries,
            "files_state": self.files_state,
            "repo_state_state": self.repo_state_state,
            "configured": self.configured,
            "fail_fast": self.fail_fast,
            "job_graph": (self.job_graph.toDict()
                          if self.job_graph else None),
            "span_info": self.span_info,
            "configured_time": self.configured_time,
            "start_time": self.start_time,
            "repo_state_request_time": self.repo_state_request_time,
            "job_versions": self.job_versions,
            "build_versions": self.build_versions,
            # jobs (serialize as separate objects)
        }
        return json.dumps(data, sort_keys=True).encode("utf8")

    def deserialize(self, raw, context, extra=None):
        data = super().deserialize(raw, context)
        # Set our UUID so that getPath() returns the correct path for
        # child objects.
        self._set(uuid=data["uuid"])

        # These three values are immutable, and not kept in memory
        # unless accessed.
        data['_files_path'] = data.pop('files')

        data['nodeset_info'] = {i: NodesetInfo.fromDict(d)
                                for i, d in data['nodeset_info'].items()}

        config_errors = data.get('config_errors')
        if config_errors:
            if (self.config_errors and
                self.config_errors._path == config_errors):
                data['config_errors'] = self.config_errors
            else:
                data['config_errors'] = ConfigurationErrorList.fromZK(
                    context, data['config_errors'],
                    _path=data['config_errors'])
        else:
            data['config_errors'] = None

        # Job graphs are immutable
        if self.job_graph is not None:
            data['job_graph'] = self.job_graph
        elif data['job_graph']:
            data['job_graph'] = JobGraph.fromDict(data['job_graph'], self.jobs)

        builds = {}
        retry_builds = defaultdict(list)
        # Flatten dict with lists of retried builds
        existing_retry_builds = {b.getPath(): b
                                 for bl in self.retry_builds.values()
                                 for b in bl}
        # This is a tuple of (kind, job_uuid, Future),
        # where kind is None if no action needs to be taken, or a
        # string to indicate which kind of job it was.  This structure
        # allows us to execute async ZK reads and perform local data
        # updates in order.
        tpe_jobs = []
        tpe = context.executor[BuildSet]
        job_versions = data.get('job_versions', {})
        build_versions = data.get('build_versions', {})
        # jobs (deserialize as separate objects)
        if job_graph := data['job_graph']:
            for job_uuid in job_graph.job_uuids:
                # If we have a current build before refreshing, we may
                # be able to skip refreshing some items since they
                # will not have changed.
                build_path = data["builds"].get(job_uuid)
                old_build = self.builds.get(job_uuid)
                old_build_exists = (old_build
                                    and old_build.getPath() == build_path)

                if job_uuid in self.jobs:
                    job = self.jobs[job_uuid]
                    if ((not old_build_exists) or
                        self.shouldRefreshJob(job, job_versions)):
                        tpe_jobs.append((None, job_uuid,
                                         tpe.submit(job.refresh, context)))
                else:
                    job_path = FrozenJob.jobPath(job_uuid, self.getPath())
                    tpe_jobs.append(('job', job_uuid, tpe.submit(
                        FrozenJob.fromZK, context, job_path, buildset=self)))

                if build_path:
                    build = self.builds.get(job_uuid)
                    builds[job_uuid] = build
                    if build and build.getPath() == build_path:
                        if self.shouldRefreshBuild(build, build_versions):
                            tpe_jobs.append((
                                None, job_uuid, tpe.submit(
                                    build.refresh, context)))
                    else:
                        tpe_jobs.append((
                            'build', job_uuid, tpe.submit(
                                Build.fromZK, context, build_path,
                                build_set=self)))

                for retry_path in data["retry_builds"].get(job_uuid, []):
                    retry_build = existing_retry_builds.get(retry_path)
                    if retry_build and retry_build.getPath() == retry_path:
                        # Retry builds never change.
                        retry_builds[job_uuid].append(retry_build)
                    else:
                        tpe_jobs.append((
                            'retry', job_uuid, tpe.submit(
                                Build.fromZK, context, retry_path,
                                build_set=self)))

        for (kind, job_uuid, future) in tpe_jobs:
            result = future.result()
            if kind == 'job':
                self.jobs[job_uuid] = result
            elif kind == 'build':
                # We normally set the job on the constructor, but we
                # may not have had it in time.  At this point though,
                # the job future is guaranteed to have completed, so
                # we can look it up now.
                result._set(job=self.jobs[job_uuid])
                builds[job_uuid] = result
            elif kind == 'retry':
                result._set(job=self.jobs[job_uuid])
                retry_builds[job_uuid].append(result)

        data.update({
            "builds": builds,
            "retry_builds": retry_builds,
            # These are local cache objects only valid for one pipeline run
            "_old_job_graph": None,
            "_old_jobs": {},
        })
        return data

    def updateBuildVersion(self, context, build):
        # It is common for a lot of builds/jobs to be added at once,
        # so to avoid writing this buildset object repeatedly during
        # that time, we only update the version after the initial
        # creation.
        version = build.getZKVersion()
        # If zstat is None, we created the object
        if version is not None:
            self.build_versions[build.uuid] = version + 1
            self.updateAttributes(context, build_versions=self.build_versions)

    def updateJobVersion(self, context, job):
        version = job.getZKVersion()
        if version is not None:
            self.job_versions[job.uuid] = version + 1
            self.updateAttributes(context, job_versions=self.job_versions)

    def shouldRefreshBuild(self, build, build_versions):
        current = build.getZKVersion()
        expected = build_versions.get(build.uuid, 0)
        return expected != current

    def shouldRefreshJob(self, job, job_versions):
        current = job.getZKVersion()
        expected = job_versions.get(job.uuid, 0)
        return expected != current

    @property
    def ref(self):
        # NOTE(jamielennox): The concept of buildset ref is to be removed and a
        # buildset UUID identifier available instead. Currently the ref is
        # checked to see if the BuildSet has been configured.
        return 'Z' + self.uuid if self.configured else None

    def __repr__(self):
        return '<BuildSet item: %s #builds: %s merge state: %s>' % (
            self.item,
            len(self.builds),
            self.getStateName(self.merge_state))

    def setConfiguration(self, context):
        with self.activeContext(context):
            # The change isn't enqueued until after it's created
            # so we don't know what the other changes ahead will be
            # until jobs start.
            if not self.configured:
                items = [self.item]
                # The time is used by makeMergerItem
                self.configured_time = time.time()
                items.extend(i for i in self.item.items_ahead
                             if i not in items)
                items.reverse()

                self.dependent_changes = [
                    self._toChangeDict(i, c) for i in items for c in i.changes
                ]
                self.merger_items = [
                    i.makeMergerItem(c) for i in items for c in i.changes
                ]
                self.configured = True

    def _toChangeDict(self, item, change):
        change_dict = dict(
            ref=change.cache_key,
        )
        # Inject bundle_id to dict if available, this can be used to decide
        # if changes belongs to the same bunbdle
        if len(item.changes) > 1:
            change_dict['bundle_id'] = item.uuid
        return change_dict

    def getStateName(self, state_num):
        return self.states_map.get(
            state_num, 'UNKNOWN (%s)' % state_num)

    def addBuild(self, build):
        self.addBuilds([build])

    def addBuilds(self, builds):
        with self.activeContext(self.item.manager.current_context):
            for build in builds:
                self._addBuild(build)

    def _addBuild(self, build):
        self.builds[build.job.uuid] = build
        if build.job.uuid not in self.tries:
            self.tries[build.job.uuid] = 1

    def addRetryBuild(self, build):
        with self.activeContext(self.item.manager.current_context):
            self.retry_builds.setdefault(
                build.job.uuid, []).append(build)

    def removeBuild(self, build):
        with self.activeContext(self.item.manager.current_context):
            self.tries[build.job.uuid] += 1
            del self.builds[build.job.uuid]

    def getBuild(self, job):
        return self.builds.get(job.uuid)

    def getBuilds(self):
        builds = list(self.builds.values())
        builds.sort(key=lambda b: b.job.name)
        return builds

    def getRetryBuildsForJob(self, job):
        return self.retry_builds.get(job.uuid, [])

    def getJobNodeSetInfo(self, job):
        # Return None if not provisioned; dict of info about nodes otherwise
        return self.nodeset_info.get(job.uuid)

    def getJobNodeProvider(self, job):
        info = self.getJobNodeSetInfo(job)
        if info:
            return info.provider

    def getJobNodeExecutorZone(self, job):
        info = self.getJobNodeSetInfo(job)
        if info:
            return info.zone

    def getJobNodeList(self, job):
        info = self.getJobNodeSetInfo(job)
        if info:
            return info.nodes

    def setJobNodeSetInfo(self, job, nodeset_info):
        if job.uuid in self.nodeset_info:
            raise Exception("Prior node request for %s", job.name)
        with self.activeContext(self.item.manager.current_context):
            self.nodeset_info[job.uuid] = nodeset_info

    def removeJobNodeSetInfo(self, job):
        if job.uuid not in self.nodeset_info:
            raise Exception("No job nodeset for %s" % (job.name))
        with self.activeContext(self.item.manager.current_context):
            del self.nodeset_info[job.uuid]

    def setJobNodeRequestID(self, job, request_id):
        if job.uuid in self.node_requests:
            raise Exception("Prior node request for %s" % (job.name))
        with self.activeContext(self.item.manager.current_context):
            self.node_requests[job.uuid] = request_id

    def getJobNodeRequestID(self, job):
        return self.node_requests.get(job.uuid)

    def getNodeRequests(self):
        for job_uuid, request in self.node_requests.items():
            yield self.job_graph.getJobFromUuid(job_uuid), request

    def removeJobNodeRequestID(self, job):
        if job.uuid in self.node_requests:
            with self.activeContext(
                    self.item.manager.current_context):
                del self.node_requests[job.uuid]

    def getTries(self, job):
        return self.tries.get(job.uuid, 0)

    def getMergeMode(self, change):
        # We may be called before this build set has a shadow layout
        # (ie, we are called to perform the merge to create that
        # layout).  It's possible that the change we are merging will
        # update the merge-mode for the project, but there's not much
        # we can do about that here.  Instead, do the best we can by
        # using the nearest shadow layout to determine the merge mode,
        # or if that fails, the current live layout, or if that fails,
        # use the default: merge-resolve.
        item = self.item
        project = change.project
        project_metadata = None
        while item:
            if item.current_build_set.job_graph:
                project_metadata = item.current_build_set.job_graph.\
                    getProjectMetadata(
                        project.canonical_name)
                if project_metadata:
                    break
            item = item.item_ahead
        if not project_metadata:
            layout = self.item.manager.tenant.layout
            if layout:
                project_metadata = layout.getProjectMetadata(
                    project.canonical_name
                )
        if project_metadata:
            return project_metadata.merge_mode
        return MERGER_MERGE_RESOLVE

    def getSafeAttributes(self):
        return Attributes(uuid=self.uuid)


class EventInfo:

    def __init__(self):
        self.zuul_event_id = None
        self.timestamp = time.time()
        self.span_context = None
        self.ref = None
        # Image build related events carry information that's needed
        # by the reporter.
        self.image_names = None
        self.image_upload_uuid = None
        self.image_build_uuid = None
        self.user_id = None

    @classmethod
    def fromEvent(cls, event, event_ref_key):
        tinfo = cls()
        tinfo.zuul_event_id = event.zuul_event_id
        tinfo.timestamp = event.timestamp
        tinfo.span_context = event.span_context
        if event_ref_key:
            tinfo.ref = event_ref_key
        else:
            tinfo.ref = None
        tinfo.image_names = getattr(event, 'image_names', None)
        tinfo.image_upload_uuid = getattr(event, 'image_upload_uuid', None)
        tinfo.image_build_uuid = getattr(event, 'image_build_uuid', None)
        tinfo.user_id = getattr(event, 'user_id', None)
        return tinfo

    @classmethod
    def fromDict(cls, d):
        tinfo = cls()
        tinfo.zuul_event_id = d["zuul_event_id"]
        tinfo.timestamp = d["timestamp"]
        tinfo.span_context = d["span_context"]
        tinfo.ref = d["ref"]
        tinfo.image_names = d.get("image_names")
        tinfo.image_upload_uuid = d.get("image_upload_uuid")
        tinfo.image_build_uuid = d.get("image_build_uuid")
        tinfo.user_id = d.get("user_id")
        return tinfo

    def isSuperset(self, other):
        other_image_names = getattr(other, 'image_names', None)
        image_names = self.image_names
        if image_names is None:
            # We will build all images
            return True
        if other_image_names is None:
            # They want to build all images, and if we passed the test
            # above, we are building a subset.
            return False

        if self.image_upload_uuid:
            # This is an image-validate event so we need to compare
            # the image upload UUID.
            return self.image_upload_uuid == other.image_upload_uuid
        elif self.image_build_uuid:
            # This is an image-delete event so we need to compare
            # the image build UUID.
            return self.image_build_uuid == other.image_build_uuid

        # This is an image-build event so check if we include all
        # requested images.
        return set(image_names) >= set(other_image_names)

    def toDict(self):
        return {
            "zuul_event_id": self.zuul_event_id,
            "timestamp": self.timestamp,
            "span_context": self.span_context,
            "ref": self.ref,
            "image_names": self.image_names,
            "image_upload_uuid": self.image_upload_uuid,
            "image_build_uuid": self.image_build_uuid,
            "user_id": self.user_id,
        }


class QueueItem(zkobject.ZKObject):

    """Represents the position of a Change in a ChangeQueue.

    All Changes are enqueued into ChangeQueue in a QueueItem. The QueueItem
    holds the current `BuildSet` as well as all previous `BuildSets` that were
    produced for this `QueueItem`.
    """

    log = logging.getLogger("zuul.QueueItem")

    class State(StrEnum):
        PENDING = "pending"

    def __init__(self):
        super().__init__()
        self._set(
            uuid=uuid4().hex,
            queue=None,
            changes=[],  # a list of refs
            dequeued_needing_change=None,
            dequeued_missing_requirements=False,
            current_build_set=None,
            item_ahead=None,
            items_behind=[],
            span_info=None,
            enqueue_time=None,
            report_time=None,
            dequeue_time=None,
            first_job_start_time=None,
            reported=False,
            reported_enqueue=False,
            reported_start=False,
            quiet=False,
            active=False,  # Whether an item is within an active window
            live=True,  # Whether an item is intended to be processed at all
            layout_uuid=None,
            _cached_sql_results={},
            event=None,  # Info about the event that lead to this queue item
            debug=False,
            user_id=None,
            parameters=None,
            reporter_jobs_state=None,
            # Change.cache_key -> sha for any changes in this item
            # that were merged
            merged_change_shas={},
            # Additional container for connection specific information to be
            # used by reporters throughout the lifecycle
            dynamic_state=defaultdict(dict),
        )

    @property
    def manager(self):
        if self.queue:
            return self.queue.manager
        return None

    @classmethod
    def new(klass, context, **kw):
        obj = klass()
        obj._set(**kw)

        data = obj._trySerialize(context)
        obj._save(context, data, create=True)
        # Skip the initial merge for branch/ref items as we don't need it in
        # order to build a job graph. The merger items will be included as
        # part of the extra repo state if there are jobs to run.
        should_merge = any(isinstance(o, (Change, Tag)) for o in obj.changes)
        merge_state = (BuildSet.NEW if should_merge else BuildSet.COMPLETE)
        should_files = any(o.files is None for o in obj.changes)
        files_state = (BuildSet.NEW if should_files else BuildSet.COMPLETE)

        with trace.use_span(tracing.restoreSpan(obj.span_info)):
            buildset_span_info = tracing.startSavedSpan("BuildSet")
            obj.updateAttributes(context, current_build_set=BuildSet.new(
                context, item=obj, files_state=files_state,
                merge_state=merge_state,
                span_info=buildset_span_info))
        return obj

    def getPath(self):
        return self.itemPath(PipelineState.pipelinePath(self.manager),
                             self.uuid)

    @classmethod
    def itemPath(cls, pipeline_path, item_uuid):
        return f"{pipeline_path}/item/{item_uuid}"

    @classmethod
    def parsePath(self, path):
        """Return path components for use by the REST API"""
        pipeline_path, item, uuid = path.rsplit('/', 2)
        tenant, pipeline = PipelineState.parsePath(pipeline_path)
        return (tenant, pipeline, uuid)

    def serialize(self, context):
        event_type = "EventInfo"
        data = {
            "uuid": self.uuid,
            # TODO: we need to also store some info about the change in
            # Zookeeper in order to show the change info on the status page.
            # This needs change cache and the API to resolve change by key.
            "changes": [c.cache_key for c in self.changes],
            "dequeued_needing_change": self.dequeued_needing_change,
            "dequeued_missing_requirements":
            self.dequeued_missing_requirements,
            "current_build_set": (self.current_build_set and
                                  self.current_build_set.getPath()),
            "item_ahead": self.item_ahead and self.item_ahead.getPath(),
            "items_behind": [i.getPath() for i in self.items_behind],
            "span_info": self.span_info,
            "enqueue_time": self.enqueue_time,
            "report_time": self.report_time,
            "dequeue_time": self.dequeue_time,
            "reported": self.reported,
            "reported_enqueue": self.reported_enqueue,
            "reported_start": self.reported_start,
            "quiet": self.quiet,
            "active": self.active,
            "live": self.live,
            "layout_uuid": self.layout_uuid,
            "event": {
                "type": event_type,
                "data": self.event.toDict(),
            },
            "parameters": self.parameters,
            "dynamic_state": self.dynamic_state,
            "first_job_start_time": self.first_job_start_time,
            "debug": self.debug,
            "reporter_jobs_state": self.reporter_jobs_state,
            "merged_change_shas": self.merged_change_shas,
        }
        return json.dumps(data, sort_keys=True).encode("utf8")

    def deserialize(self, raw, context, extra=None):
        data = super().deserialize(raw, context)
        # Set our UUID so that getPath() returns the correct path for
        # child objects.
        self._set(uuid=data["uuid"])

        event = EventInfo.fromDict(data["event"]["data"])
        changes = self.manager.resolveChangeReferences(
            data["changes"])
        build_set = self.current_build_set
        if build_set and build_set.getPath() == data["current_build_set"]:
            build_set.refresh(context)
        else:
            build_set = (data["current_build_set"] and
                         BuildSet.fromZK(context, data["current_build_set"],
                                         item=self))

        data.update({
            "event": event,
            "changes": changes,
            "log": get_annotated_logger(self.log, event),
            "dynamic_state": defaultdict(dict, data["dynamic_state"]),
            "current_build_set": build_set,
        })
        data['_item_ahead'] = data.pop('item_ahead')
        data['_items_behind'] = data.pop('items_behind')
        return data

    def annotateLogger(self, logger):
        """Return an annotated logger with the trigger event"""
        return get_annotated_logger(logger, self.event)

    def __repr__(self):
        if self.manager:
            pipeline = self.manager.pipeline.name
        else:
            pipeline = None
        if self.live:
            live = 'live'
        else:
            live = 'non-live'
        return '<QueueItem %s %s for %s in %s>' % (
            self.uuid, live, self.changes, pipeline)

    def resetAllBuilds(self):
        context = self.manager.current_context
        old_build_set = self.current_build_set
        have_all_files = all(c.files is not None for c in self.changes)
        files_state = (BuildSet.COMPLETE if have_all_files else BuildSet.NEW)

        with trace.use_span(tracing.restoreSpan(self.span_info)):
            old_buildset_span = tracing.restoreSpan(old_build_set.span_info)
            link = trace.Link(
                old_buildset_span.get_span_context(),
                attributes={"previous_buildset": old_build_set.uuid})
            buildset_span_info = tracing.startSavedSpan(
                "BuildSet", links=[link])

            self.updateAttributes(
                context,
                current_build_set=BuildSet.new(context, item=self,
                                               files_state=files_state,
                                               span_info=buildset_span_info),
                layout_uuid=None)
        old_build_set.delete(context)

    def addBuild(self, build):
        self.current_build_set.addBuild(build)

    def addBuilds(self, builds):
        self.current_build_set.addBuilds(builds)

    def addRetryBuild(self, build):
        self.current_build_set.addRetryBuild(build)

    def removeBuild(self, build):
        self.current_build_set.removeBuild(build)

    def setReportedResult(self, result):
        self.updateAttributes(self.manager.current_context,
                              report_time=time.time())
        self.current_build_set.updateAttributes(
            self.manager.current_context, result=result)

    def warning(self, msgs):
        with self.current_build_set.activeContext(
                self.manager.current_context):
            if not isinstance(msgs, list):
                msgs = [msgs]
            for msg in msgs:
                self.current_build_set.warning_messages.append(msg)
                self.log.info(msg)

    def getChangeForJob(self, job):
        for change in self.changes:
            if change.cache_key == job.ref:
                return change
        return None

    def freezeJobGraph(self, layout, context,
                       skip_file_matcher,
                       redact_secrets_and_keys):
        """Find or create actual matching jobs for this item's changes and
        store the resulting job tree."""

        try:
            results = layout.createJobGraph(context, self, skip_file_matcher,
                                            redact_secrets_and_keys)
            job_graph = results['job_graph']

            # Write the jobs out to ZK
            for frozen_job in job_graph._job_map.values():
                frozen_job.internalCreate(context)

            self.current_build_set.updateAttributes(context, **results)
        except Exception:
            self.current_build_set.updateAttributes(
                context, job_graph=None, _old_job_graph=None)
            raise

    def hasJobGraph(self):
        """Returns True if the item has a job graph."""
        return self.current_build_set.job_graph is not None

    def getJobs(self, include_reporter=None):
        if not self.live or not self.current_build_set.job_graph:
            return []
        if include_reporter is None:
            include_reporter = bool(self.reporter_jobs_state)
        ret = []
        for job in self.current_build_set.job_graph.getJobs():
            if include_reporter or job.type != 'reporter':
                ret.append(job)
        return ret

    def getJob(self, job_uuid):
        return self.current_build_set.job_graph.getJobFromUuid(job_uuid)

    @property
    def items_ahead(self):
        item_ahead = self.item_ahead
        while item_ahead:
            yield item_ahead
            item_ahead = item_ahead.item_ahead

    def getReporterJobs(self):
        if not self.live or not self.current_build_set.job_graph:
            return []
        ret = []
        for job in self.current_build_set.job_graph.getJobs():
            if job.type == 'reporter':
                ret.append(job)
        return ret

    def areAllReporterJobsComplete(self):
        for job in self.getReporterJobs():
            build = self.current_build_set.getBuild(job)
            if not build or not build.result:
                return False
        return True

    def areAllChangesMerged(self):
        for change in self.changes:
            if not getattr(change, 'is_merged', True):
                return False
        return True

    def haveAllJobsStarted(self):
        if not self.hasJobGraph():
            return False
        for job in self.getJobs():
            build = self.current_build_set.getBuild(job)
            if not build or not build.start_time:
                return False
        return True

    def areAllJobsComplete(self):
        if (self.current_build_set.has_blocking_errors or
            self.current_build_set.unable_to_merge):
            return True
        if not self.hasJobGraph():
            return False
        for job in self.getJobs():
            build = self.current_build_set.getBuild(job)
            if not build or not build.result:
                return False
        return True

    def _didAllJobsSucceed(self, jobs):
        """Check if all jobs have completed with status SUCCESS.

        Return True if all voting jobs have completed with status
        SUCCESS.  Non-voting jobs are ignored.  Skipped jobs are
        ignored, but skipping all jobs returns a failure.  Incomplete
        builds are considered a failure, hence this is unlikely to be
        useful unless all builds are complete.

        """
        if not self.hasJobGraph():
            return False

        all_jobs_skipped = True
        for job in jobs:
            build = self.current_build_set.getBuild(job)
            if build:
                # If the build ran, record whether or not it was skipped
                # and return False if the build was voting and has an
                # unsuccessful return value
                if build.result != 'SKIPPED':
                    all_jobs_skipped = False
                if job.voting and build.result not in ['SUCCESS', 'SKIPPED']:
                    return False
            elif job.voting:
                # If the build failed to run and was voting that is an
                # unsuccessful build. But we don't count against it if not
                # voting.
                return False

        # NOTE(pabelanger): We shouldn't be able to skip all jobs.
        if all_jobs_skipped:
            return False

        return True

    def didAllJobsSucceed(self):
        return self._didAllJobsSucceed(self.getJobs())

    def didAllReporterJobsSucceed(self):
        return self._didAllJobsSucceed(self.getReporterJobs())

    def hasAnyJobFailed(self):
        """Check if any jobs have finished with a non-success result.

        Return True if any job in the job graph has returned with a
        status not equal to SUCCESS or SKIPPED, else return False.
        Non-voting and in-flight jobs are ignored.

        """
        if not self.hasJobGraph():
            return False
        for job in self.getJobs():
            if not job.voting:
                continue
            build = self.current_build_set.getBuild(job)
            if (build and build.failed):
                return True
        return False

    def didMergerFail(self):
        return self.current_build_set.unable_to_merge

    def getConfigErrors(self, errors=True, warnings=True):
        if self.current_build_set.config_errors:
            return filter_severity(self.current_build_set.config_errors.errors,
                                   errors=errors, warnings=warnings)
        return []

    def wasDequeuedNeedingChange(self):
        return bool(self.dequeued_needing_change)

    def wasDequeuedMissingRequirements(self):
        return self.dequeued_missing_requirements

    def includesConfigUpdates(self):
        """Returns whether the changes include updates to the
        trusted and untrusted configs"""
        includes_trusted = False
        includes_untrusted = False
        tenant = self.manager.tenant
        item = self

        while item:
            for change in item.changes:
                if change.updatesConfig(tenant):
                    (trusted, project) = tenant.getProject(
                        change.project.canonical_name)
                    if trusted:
                        includes_trusted = True
                    else:
                        includes_untrusted = True
                if includes_trusted and includes_untrusted:
                    # We're done early
                    return (includes_trusted, includes_untrusted)
            item = item.item_ahead
        return (includes_trusted, includes_untrusted)

    def updatesConfig(self):
        """Returns whether the changes update the config"""
        for change in self.changes:
            if change.updatesConfig(self.manager.tenant):
                tenant_project = self.manager.tenant.getProject(
                    change.project.canonical_name
                )[1]
                # If the cycle doesn't update the config or a change
                # in the cycle updates the config but the that
                # change's project is not part of the tenant
                # (e.g. when dealing w/ cross-tenant cycles), return
                # False.
                if tenant_project is None:
                    continue
                return True
        return False

    def isHoldingFollowingChanges(self):
        if not self.live:
            return False
        if not self.hasJobGraph():
            return False
        for job in self.getJobs():
            if not job.hold_following_changes:
                continue
            build = self.current_build_set.getBuild(job)
            if not build:
                return True
            if build.result != 'SUCCESS':
                return True

        if not self.item_ahead:
            return False
        return self.item_ahead.isHoldingFollowingChanges()

    def _getRequirementsResultFromSQL(self, job):
        # This either returns data or raises an exception
        requirements = job.requires
        self.log.debug("Checking DB for requirements")
        requirements_tuple = tuple(sorted(requirements))
        if requirements_tuple not in self._cached_sql_results:
            conn = self.manager.sched.connections.getSqlConnection()
            if conn:
                for change in self.changes:
                    builds = conn.getBuilds(
                        tenant=self.manager.tenant.name,
                        project=change.project.name,
                        pipeline=self.manager.pipeline.name,
                        change=change.number,
                        branch=change.branch,
                        patchset=change.patchset,
                        provides=requirements_tuple)
                    if builds:
                        break
            else:
                builds = []
            # Just look at the most recent buildset.
            # TODO: query for a buildset instead of filtering.
            builds = [b for b in builds
                      if b.buildset.uuid == builds[0].buildset.uuid]
            self._cached_sql_results[requirements_tuple] = builds

        builds = self._cached_sql_results[requirements_tuple]
        data = []
        if not builds:
            self.log.debug("No artifacts matching requirements found in DB")
            return data

        for build in builds:
            if build.result != 'SUCCESS':
                provides = [x.name for x in build.provides]
                requirement = list(requirements.intersection(set(provides)))
                raise RequirementsError(
                    'Job %s requires artifact(s) %s provided by build %s '
                    '(triggered by change %s on project %s), but that build '
                    'failed with result "%s"' % (
                        job.name, ', '.join(requirement), build.uuid,
                        build.ref.change, build.ref.project,
                        build.result))
            else:
                for a in build.artifacts:
                    artifact = {'name': a.name,
                                'url': a.url,
                                'project': build.ref.project,
                                'change': str(build.ref.change),
                                'patchset': build.ref.patchset,
                                'job': build.job_name}
                    if a.meta:
                        artifact['metadata'] = json.loads(a.meta)
                    data.append(artifact)
        self.log.debug("Found artifacts in DB: %s", repr(data))
        return data

    def providesRequirements(self, job, data=None, recurse=True):
        # Returns true/false if requirements are satisfied and updates
        # the 'data' dictionary if provided.
        requirements = job.requires
        if not requirements:
            return True
        if not self.live:
            self.log.debug("Checking whether non-live item %s provides %s",
                           self, requirements)
            # Look for this item in other queues in the pipeline.
            item = None
            found = False
            for item in self.manager.state.getAllItems():
                if item.live and set(item.changes) == set(self.changes):
                    found = True
                    break
            if found:
                if not item.providesRequirements(job, data=data,
                                                 recurse=False):
                    return False
            else:
                # Try to get the requirements from the databse for
                # the side-effect of raising an exception when the
                # found build failed.
                artifacts = self._getRequirementsResultFromSQL(job)
                if data is not None:
                    data.extend(artifacts)
        if self.hasJobGraph():
            for _job in self.getJobs():
                if _job.provides.intersection(requirements):
                    build = self.current_build_set.getBuild(_job)
                    if not build:
                        return False
                    if build.result and build.result != 'SUCCESS':
                        return False
                    if not build.result and not build.paused:
                        return False
                    change = self.getChangeForJob(_job)
                    if not isinstance(change, Change):
                        # Speculative artifacts for non speculative objects
                        # does not make sense.
                        return False
                    if data is not None:
                        artifacts = get_artifacts_from_result_data(
                            build.result_data,
                            logger=self.log)
                        for a in artifacts:
                            a.update({'project': change.project.name,
                                      'change': change.number,
                                      'patchset': change.patchset,
                                      'job': build.job.name})
                        self.log.debug(
                            "Found live artifacts: %s", repr(artifacts))
                        data.extend(artifacts)
        if not self.item_ahead:
            return True
        if not recurse:
            return True
        return self.item_ahead.providesRequirements(job, data=data)

    def jobRequirementsReady(self, job):
        if not self.item_ahead:
            return True
        try:
            data = None
            ret = self.item_ahead.providesRequirements(job, data=data)
            if data:
                data.reverse()
                job.setArtifactData(data)
            return ret
        except RequirementsError as e:
            self.log.info(str(e))
            fakebuild = Build.new(self.manager.current_context,
                                  job=job, build_set=self.current_build_set,
                                  error_detail=str(e), result='FAILURE')
            self.addBuild(fakebuild)
            self.manager.sched.reportBuildEnd(
                fakebuild,
                tenant=self.manager.tenant.name,
                final=True)
            self.setResult(fakebuild)
        return False

    def getArtifactData(self, job):
        data = []
        self.providesRequirements(job, data)
        data.reverse()
        return data

    def getJobParentData(self, job):
        job_graph = self.current_build_set.job_graph
        parent_builds_with_data = {}
        for parent_job in job_graph.getParentJobsRecursively(job):
            parent_build = self.current_build_set.getBuild(parent_job)
            if parent_build and parent_build.result_data:
                parent_builds_with_data[parent_job.uuid] = parent_build

        parent_data = {}
        secret_parent_data = {}
        # We may have artifact data from
        # jobRequirementsReady, so we preserve it.
        # updateParentData de-duplicates it.
        artifact_data = job.artifact_data or self.getArtifactData(job)
        # Iterate over all jobs of the graph (which is
        # in sorted config order) and apply parent data of the jobs we
        # already found.
        for parent_job in job_graph.getJobs():
            parent_build = parent_builds_with_data.get(parent_job.uuid)
            if not parent_build:
                continue
            (parent_data, secret_parent_data, artifact_data
                ) = FrozenJob.updateParentData(
                    parent_data,
                    secret_parent_data,
                    artifact_data,
                    parent_build)
        return parent_data, secret_parent_data, artifact_data

    def findJobsToRun(self, semaphore_handler):
        torun = []
        item_jobs = list(self.getJobs())
        if not item_jobs:
            return torun
        if self.item_ahead:
            # Only run jobs if any 'hold' jobs on the change ahead
            # have completed successfully.
            if self.item_ahead.isHoldingFollowingChanges():
                return torun

        job_graph = self.current_build_set.job_graph
        failed_job_ids = set()  # Jobs that run and failed
        ignored_job_ids = set()  # Jobs that were skipped or canceled
        unexecuted_job_ids = set()  # Jobs that were not started yet
        jobs_not_started = set()
        for job in item_jobs:
            build = self.current_build_set.getBuild(job)
            if build:
                if build.result == 'SUCCESS' or build.paused:
                    pass
                elif build.result == 'SKIPPED':
                    ignored_job_ids.add(job.uuid)
                else:  # elif build.result in ('FAILURE', 'CANCELED', ...):
                    failed_job_ids.add(job.uuid)
            else:
                unexecuted_job_ids.add(job.uuid)
                jobs_not_started.add(job)

        for job in item_jobs:
            if job not in jobs_not_started:
                continue
            if not self.jobRequirementsReady(job):
                continue
            all_parent_jobs_successful = True
            parent_builds_with_data = {}
            for parent_job in job_graph.getParentJobsRecursively(job):
                if parent_job.uuid in unexecuted_job_ids \
                        or parent_job.uuid in failed_job_ids:
                    all_parent_jobs_successful = False
                    break
                parent_build = self.current_build_set.getBuild(parent_job)
                if parent_build.result_data:
                    parent_builds_with_data[parent_job.uuid] = parent_build

            for parent_job in job_graph.getParentJobsRecursively(
                    job, skip_soft=True):
                if parent_job.uuid in ignored_job_ids:
                    all_parent_jobs_successful = False
                    break

            if all_parent_jobs_successful:
                nodeset = self.current_build_set.getJobNodeSetInfo(job)
                if nodeset is None:
                    # The nodes for this job are not ready, skip
                    # it for now.
                    continue
                if semaphore_handler.acquire(self, job, False):
                    # If this job needs a semaphore, either acquire it or
                    # make sure that we have it before running the job.
                    torun.append(job)
        return torun

    def findJobsToRequest(self, semaphore_handler):
        build_set = self.current_build_set
        toreq = []
        item_jobs = list(self.getJobs())
        if not item_jobs:
            return toreq
        if self.item_ahead:
            if self.item_ahead.isHoldingFollowingChanges():
                return toreq

        job_graph = self.current_build_set.job_graph
        failed_job_ids = set()       # Jobs that run and failed
        ignored_job_ids = set()      # Jobs that were skipped or canceled
        unexecuted_job_ids = set()   # Jobs that were not started yet
        jobs_not_requested = set()
        for job in item_jobs:
            build = build_set.getBuild(job)
            if build and (build.result == 'SUCCESS' or build.paused):
                pass
            elif build and build.result == 'SKIPPED':
                ignored_job_ids.add(job.uuid)
            elif build and build.result in ('FAILURE', 'CANCELED'):
                failed_job_ids.add(job.uuid)
            else:
                unexecuted_job_ids.add(job.uuid)
                nodeset = build_set.getJobNodeSetInfo(job)
                if nodeset is None:
                    req_id = build_set.getJobNodeRequestID(job)
                    if req_id is None:
                        jobs_not_requested.add(job)
                    else:
                        # This may have been reset due to a reconfig;
                        # since we know there is a queued request for
                        # it, set it here.
                        if job.queued is not True:
                            job.updateAttributes(
                                self.manager.current_context,
                                queued=True)

        # Attempt to request nodes for jobs in the order jobs appear
        # in configuration.
        for job in item_jobs:
            if job not in jobs_not_requested:
                continue
            if not self.jobRequirementsReady(job):
                job.setWaitingStatus('requirements: {}'.format(
                    ', '.join(job.requires)))
                continue

            # Some set operations to figure out what jobs we really need:
            all_dep_jobs_successful = True
            # Every parent job (dependency), whether soft or hard:
            all_dep_job_ids = set(
                [x.uuid for x in
                 job_graph.getParentJobsRecursively(job)])
            # Only the hard deps:
            hard_dep_job_ids = set(
                [x.uuid for x in job_graph.getParentJobsRecursively(
                    job, skip_soft=True)])
            # Any dep that hasn't finished (or started) running
            unexecuted_dep_job_ids = unexecuted_job_ids & all_dep_job_ids
            # Any dep that has finished and failed
            failed_dep_job_ids = failed_job_ids & all_dep_job_ids
            ignored_hard_dep_job_ids = hard_dep_job_ids & ignored_job_ids
            # We can't proceed if there are any:
            # * Deps that haven't finished running
            #     (this includes soft deps that haven't skipped)
            # * Deps that have failed
            # * Hard deps that were skipped
            required_dep_job_ids = (
                unexecuted_dep_job_ids |
                failed_dep_job_ids |
                ignored_hard_dep_job_ids)
            if required_dep_job_ids:
                deps = [self.getJob(i).name
                        for i in required_dep_job_ids]
                job.setWaitingStatus('dependencies: {}'.format(
                    ', '.join(deps)))
                all_dep_jobs_successful = False

            if all_dep_jobs_successful:
                if semaphore_handler.acquire(self, job, True):
                    # If this job needs a semaphore, either acquire it or
                    # make sure that we have it before requesting the nodes.
                    toreq.append(job)
                    if job.queued is not True:
                        job.updateAttributes(
                            self.manager.current_context,
                            queued=True)
                else:
                    sem_names = ','.join([s.name for s in job.semaphores])
                    job.setWaitingStatus('semaphores: {}'.format(sem_names))
        return toreq

    def setResult(self, build):
        if build.retry:
            self.addRetryBuild(build)
            self.removeBuild(build)
            return

        job_graph = self.current_build_set.job_graph
        skipped = []
        # We may skip several jobs, but if we do, they will all be for
        # the same reason.
        skipped_reason = None
        # NOTE(pabelanger): Check successful/paused jobs to see if
        # zuul_return includes zuul.child_jobs.
        build_result = build.result_data.get('zuul', {})
        if ((build.result == 'SUCCESS' or build.paused)
                and 'child_jobs' in build_result):
            skipped_reason = ('Skipped due to child_jobs return value in job '
                              + build.job.name)
            zuul_return = build_result.get('child_jobs', [])
            dependent_jobs = set(job_graph.getDirectDependentJobs(build.job))

            if not zuul_return:
                # If zuul.child_jobs exists and is empty, the user
                # wants to skip all child jobs.
                to_skip = job_graph.getDependentJobsRecursively(
                    build.job, skip_soft=True)
                skipped += to_skip
            else:
                # The user supplied a list of jobs to run.
                intersect_jobs = set([
                    j for j in dependent_jobs if j.name in zuul_return
                ])

                for skip in (dependent_jobs - intersect_jobs):
                    skipped.append(skip)
                    to_skip = job_graph.getDependentJobsRecursively(
                        skip, skip_soft=True)
                    skipped += to_skip

        elif build.result not in ('SUCCESS', 'SKIPPED') and not build.paused:
            skipped_reason = 'Skipped due to failed job ' + build.job.name
            to_skip = job_graph.getDependentJobsRecursively(
                build.job)
            skipped += to_skip

        fake_builds = []
        for job in skipped:
            child_build = self.current_build_set.getBuild(job)
            if not child_build:
                fake_builds.append(
                    Build.new(self.manager.current_context,
                              job=job,
                              build_set=self.current_build_set,
                              error_detail=skipped_reason,
                              result='SKIPPED'))
        if fake_builds:
            self.addBuilds(fake_builds)
            self.manager.sched.reportBuildEnds(
                fake_builds,
                tenant=self.manager.tenant.name,
                final=True)

    def setNodeRequestFailure(self, job, error):
        fakebuild = Build.new(
            self.manager.current_context,
            job=job,
            build_set=self.current_build_set,
            start_time=time.time(),
            end_time=time.time(),
            error_detail=error,
            result='NODE_FAILURE',
        )
        self.addBuild(fakebuild)
        self.manager.sched.reportBuildEnd(
            fakebuild,
            tenant=self.manager.tenant.name,
            final=True)
        self.setResult(fakebuild)

    def setDequeuedNeedingChange(self, msg):
        self.updateAttributes(
            self.manager.current_context,
            dequeued_needing_change=msg)
        self._setAllJobsSkipped(msg)

    def setDequeuedMissingRequirements(self):
        self.updateAttributes(
            self.manager.current_context,
            dequeued_missing_requirements=True)
        self._setAllJobsSkipped('Missing pipeline requirements')

    def setUnableToMerge(self, errors=None):
        with self.current_build_set.activeContext(
                self.manager.current_context):
            self.current_build_set.unable_to_merge = True
            if errors:
                for msg in errors:
                    self.current_build_set.warning_messages.append(msg)
                    self.log.info(msg)
        self._setAllJobsSkipped('Unable to merge')

    def setConfigError(self, error):
        err = ConfigurationError(None, None, error)
        self.setConfigErrors([err])

    def setConfigErrors(self, errors):
        # The manager may call us with the same errors object to
        # trigger side effects of setting jobs to 'skipped'.
        if (self.current_build_set.config_errors and
            self.current_build_set.config_errors != errors):
            # TODO: This is not expected, but if it happens we should
            # look into cleaning up leaked config_errors objects in
            # zk.
            self.log.warning("Differing config errors set on item %s",
                             self)
        if self.current_build_set.config_errors != errors:
            with self.current_build_set.activeContext(
                    self.manager.current_context):
                self.current_build_set.setConfigErrors(errors)
        if [x for x in errors if x.severity == SEVERITY_ERROR]:
            self._setAllJobsSkipped('Buildset configuration error')

    def _setAllJobsSkipped(self, msg):
        fake_builds = []
        for job in self.getJobs():
            fake_builds.append(Build.new(
                self.manager.current_context,
                job=job, build_set=self.current_build_set,
                error_detail=msg, result='SKIPPED'))
        if fake_builds:
            self.addBuilds(fake_builds)
            self.manager.sched.reportBuildEnds(
                fake_builds,
                tenant=self.manager.tenant.name,
                final=True)

    def _setMissingJobsSkipped(self, msg):
        fake_builds = []
        for job in self.getJobs():
            build = self.current_build_set.getBuild(job)
            if build:
                # We already have a build for this job
                continue
            fake_builds.append(Build.new(
                self.manager.current_context,
                job=job, build_set=self.current_build_set,
                error_detail=msg, result='SKIPPED'))
        if fake_builds:
            self.addBuilds(fake_builds)
            self.manager.sched.reportBuildEnds(
                fake_builds,
                tenant=self.manager.tenant.name,
                final=True)

    def formatUrlPattern(self, url_pattern, job=None, build=None):
        url = None
        # Produce safe versions of objects which may be useful in
        # result formatting, but don't allow users to crawl through
        # the entire data structure where they might be able to access
        # secrets, etc.
        if job:
            change = self.getChangeForJob(job)
            safe_change = change.getSafeAttributes()
        else:
            safe_change = self.changes[0].getSafeAttributes()
        safe_pipeline = self.manager.pipeline.getSafeAttributes()
        safe_tenant = self.manager.tenant.getSafeAttributes()
        safe_buildset = self.current_build_set.getSafeAttributes()
        safe_job = job.getSafeAttributes() if job else {}
        safe_build = build.getSafeAttributes() if build else {}
        try:
            url = url_pattern.format(change=safe_change,
                                     pipeline=safe_pipeline,
                                     tenant=safe_tenant,
                                     buildset=safe_buildset,
                                     job=safe_job,
                                     build=safe_build)
        except KeyError as e:
            self.log.error("Error while formatting url for job %s: unknown "
                           "key %s in pattern %s"
                           % (job, e.args[0], url_pattern))
        except AttributeError as e:
            self.log.error("Error while formatting url for job %s: unknown "
                           "attribute %s in pattern %s"
                           % (job, e.args[0], url_pattern))
        except Exception:
            self.log.exception("Error while formatting url for job %s with "
                               "pattern %s:" % (job, url_pattern))

        return url

    def formatJobResult(self, job, build=None):
        if build is None:
            build = self.current_build_set.getBuild(job)
        pattern = urllib.parse.urljoin(self.manager.tenant.web_root,
                                       'build/{build.uuid}')
        url = self.formatUrlPattern(pattern, job, build)
        result = build.result
        if result == 'SUCCESS':
            if job.success_message:
                result = job.success_message
        else:
            if job.failure_message:
                result = job.failure_message
        return (result, url)

    def formatItemUrl(self):
        # If we don't have a web root set, we can't format any url
        if not self.manager.tenant.web_root:
            # Apparently we have no website
            return None

        if self.current_build_set.result:
            # We have reported (or are reporting) and so we should
            # send the buildset page url
            pattern = urllib.parse.urljoin(
                self.manager.tenant.web_root, "buildset/{buildset.uuid}"
            )
            return self.formatUrlPattern(pattern)

        # We haven't reported yet (or we don't have a database), so
        # the best we can do at the moment is send the status page
        # url.  TODO: require a database, insert buildsets into it
        # when they are created, and remove this case.
        pattern = urllib.parse.urljoin(
            self.manager.tenant.web_root,
            "status/change/{change.number},{change.patchset}",
        )
        return self.formatUrlPattern(pattern)

    def formatJSON(self, websocket_url=None):
        ret = {}
        ret['active'] = self.active
        ret['live'] = self.live
        refs = []
        for change in self.changes:
            ret_ref = {}
            if hasattr(change, 'url') and change.url is not None:
                ret_ref['url'] = change.url
            else:
                ret_ref['url'] = None
            if hasattr(change, 'ref') and change.ref is not None:
                ret_ref['ref'] = change.ref
            else:
                ret_ref['ref'] = None
            if change.project:
                ret_ref['project'] = change.project.name
                ret_ref['project_canonical'] = change.project.canonical_name
            else:
                # For cross-project dependencies with the depends-on
                # project not known to zuul, the project is None
                # Set it to a static value
                ret_ref['project'] = "Unknown Project"
                ret_ref['project_canonical'] = "Unknown Project"
            if hasattr(change, 'owner'):
                ret_ref['owner'] = change.owner
            else:
                ret_ref['owner'] = None
            ret_ref['id'] = change._id()
            refs.append(ret_ref)
        ret['id'] = self.uuid
        ret['refs'] = refs
        if self.item_ahead:
            ret['item_ahead'] = self.item_ahead.uuid
        else:
            ret['item_ahead'] = None
        ret['items_behind'] = [i.uuid for i in self.items_behind]
        ret['failing_reasons'] = self.current_build_set.failing_reasons
        ret['zuul_ref'] = self.current_build_set.ref
        ret['enqueue_time'] = int(self.enqueue_time * 1000)
        ret['jobs'] = []
        max_remaining = 0
        for job in self.getJobs(include_reporter=True):
            now = time.time()
            build = self.current_build_set.getBuild(job)
            elapsed = None
            remaining = None
            result = None
            build_url = None
            finger_url = None
            report_url = None
            if build:
                result = build.result
                finger_url = build.url
                # TODO(tobiash): add support for custom web root
                urlformat = 'stream/{build.uuid}?' \
                            'logfile=console.log'
                if websocket_url:
                    urlformat += '&websocket_url={websocket_url}'
                build_url = urlformat.format(
                    build=build, websocket_url=websocket_url)
                (unused, report_url) = self.formatJobResult(job)
                if build.start_time:
                    if build.end_time:
                        elapsed = int((build.end_time -
                                       build.start_time) * 1000)
                        remaining = 0
                    else:
                        elapsed = int((now - build.start_time) * 1000)
                        if build.estimated_time:
                            remaining = max(
                                int(build.estimated_time * 1000) - elapsed,
                                0)
            if remaining and remaining > max_remaining:
                max_remaining = remaining

            waiting_status = None
            if elapsed is None:
                waiting_status = job.waiting_status

            ret['jobs'].append({
                'name': job.name,
                'dependencies': [x.name for x in job.dependencies],
                'elapsed_time': elapsed,
                'remaining_time': remaining,
                'url': build_url,
                'finger_url': finger_url,
                'report_url': report_url,
                'result': result,
                'voting': job.voting,
                'uuid': build.uuid if build else None,
                'execute_time': build.execute_time if build else None,
                'start_time': build.start_time if build else None,
                'end_time': build.end_time if build else None,
                'estimated_time': build.estimated_time if build else None,
                'pipeline': build.pipeline.name if build else None,
                'canceled': build.canceled if build else None,
                'paused': build.paused if build else None,
                'pre_fail': build.pre_fail if build else None,
                'retry': build.retry if build else None,
                'snapshotting': build.snapshotting if build else None,
                'tries': self.current_build_set.getTries(job),
                'queued': job.queued,
                'waiting_status': waiting_status,
            })

        if self.haveAllJobsStarted():
            ret['remaining_time'] = max_remaining
        else:
            ret['remaining_time'] = None
        return ret

    def formatStatus(self, indent=0):
        indent_str = ' ' * indent
        ret = '%s%s based on %s\n' % (
            indent_str,
            self,
            self.item_ahead)
        for job in self.getJobs():
            build = self.current_build_set.getBuild(job)
            if build:
                result = build.result
            else:
                result = None
            job_name = job.name
            if not job.voting:
                voting = ' (non-voting)'
            else:
                voting = ''
            ret += '%s  %s: %s%s' % (indent_str, job_name, result, voting)
            ret += '\n'
        return ret

    def makeMergerItem(self, change):
        # Create a dictionary with all info about the item needed by
        # the merger.
        number = None
        patchset = None
        oldrev = None
        newrev = None
        branch = None
        if hasattr(change, 'number'):
            number = change.number
            patchset = change.patchset
        if hasattr(change, 'newrev'):
            oldrev = change.oldrev
            newrev = change.newrev
        if hasattr(change, 'branch'):
            branch = change.branch

        source = change.project.source
        connection_name = source.connection.connection_name
        project = change.project

        return dict(project=project.name,
                    connection=connection_name,
                    merge_mode=self.current_build_set.getMergeMode(change),
                    ref=change.ref,
                    branch=branch,
                    buildset_uuid=self.current_build_set.uuid,
                    number=number,
                    patchset=patchset,
                    oldrev=oldrev,
                    newrev=newrev,
                    configured_time=self.current_build_set.configured_time,
                    )

    def updatesJobConfig(self, job, change, layout):
        log = self.annotateLogger(self.log)

        if hasattr(change, 'files'):
            files = {c for c in change.files if c != '/COMMIT_MSG'}
        else:
            files = set()
        project_cn = change.project.canonical_name

        for iv in job.include_vars:
            if iv.project_name is not None:
                _, iv_project = layout.tenant.getProject(iv.project_name)
                if iv_project is None:
                    raise ProjectNotFoundError(iv.project_name)
                iv_project_cn = iv_project.canonical_name
            else:
                iv_project_cn = project_cn
            if iv_project_cn == project_cn:
                # Either the include-vars explicitly matches this
                # change's project, or we are instructed to look in
                # the Zuul project, which is this change's project.
                if iv.name in files:
                    log.debug("Include-vars %s is altered in this change",
                              iv.name)
                    return True

        for pb in job.all_playbooks:
            # noop job playbook has no source context
            if (pb.source_context and
                pb.source_context.project_canonical_name == project_cn):
                if pb.path in files:
                    log.debug("Playbook %s is altered in this change",
                              pb.path)
                    return True

        layout_ahead = None
        if self.manager:
            layout_ahead = self.manager.getFallbackLayout(self)
        if layout_ahead and layout and layout is not layout_ahead:
            # This change updates the layout.  Calculate the job as it
            # would be if the layout had not changed.
            if self.current_build_set._old_job_graph is None:
                try:
                    log.debug("Creating job graph for config change detection")
                    results = layout_ahead.createJobGraph(
                        None, self,
                        skip_file_matcher=True,
                        redact_secrets_and_keys=False,
                        old=True)
                    self.current_build_set._set(
                        _old_job_graph=results['job_graph'])
                    log.debug("Done creating job graph for "
                              "config change detection")
                except Exception as e:
                    self.log.debug(
                        "Error freezing job graph in job update check:",
                        exc_info=not isinstance(e, JobConfigurationError))
                    # The config was broken before, we have no idea
                    # which jobs have changed, so rather than run them
                    # all, just rely on the file matchers as-is.
                    return False
            old_job = self.current_build_set._old_job_graph.getJob(
                job.name, change.cache_key)
            if old_job is None:
                log.debug("Found a newly created job")
                return True  # A newly created job
            if (job.getConfigHash(self.manager.tenant) !=
                old_job.config_hash):
                log.debug("Found an updated job")
                return True  # This job's configuration has changed
        return False

    def getBlobKeys(self):
        keys = set(self.current_build_set.repo_state_keys)
        job_graph = self.current_build_set.job_graph
        if not job_graph:
            return keys
        # Return a set of blob keys used by this item
        # for each job in the frozen job graph
        for job in job_graph.getJobs():
            for pb in job.all_playbooks:
                for secret in pb['secrets'].values():
                    if isinstance(secret, dict) and 'blob' in secret:
                        keys.add(secret['blob'])
        return keys

    def getEventChange(self):
        if not self.event:
            return None
        if not self.event.ref:
            return None
        sched = self.manager.sched
        key = ChangeKey.fromReference(self.event.ref)
        source = sched.connections.getSource(key.connection_name)
        return source.getChange(key)


# Cache info of a ref
CacheStat = namedtuple("CacheStat",
                       ["key", "uuid", "version", "mzxid", "last_modified",
                        "compressed_size", "uncompressed_size"])


class Ref(object):
    """An existing state of a Project."""

    def __init__(self, project):
        self.project = project
        self.ref = None
        self.oldrev = None
        self.newrev = None
        self.files = []
        # A list of files that can be commented upon, if different than files.
        self.commentable_files = None
        # The url for browsing the ref/tag/branch/change
        self.url = None
        # Cache info about this ref:
        # CacheStat(cache key, uuid, version, mzxid, last_modified)
        self.cache_stat = None

    @property
    def cache_key(self):
        return self.cache_stat.key.reference

    @property
    def cache_version(self):
        return -1 if self.cache_stat is None else self.cache_stat.version

    def serialize(self):
        return {
            "project": self.project.name,
            "ref": self.ref,
            "oldrev": self.oldrev,
            "newrev": self.newrev,
            "files": self.files,
            "commentable_files": self.commentable_files,
            "url": self.url,
        }

    def deserialize(self, data):
        self.ref = data.get("ref")
        self.oldrev = data.get("oldrev")
        self.newrev = data.get("newrev")
        self.files = data.get("files", [])
        self.commentable_files = data.get("commentable_files", None)
        self.url = data.get("url")

    def _id(self):
        return self.newrev

    def __repr__(self):
        rep = None
        pname = None
        if self.project and self.project.name:
            pname = self.project.name
        if self.newrev == '0000000000000000000000000000000000000000':
            rep = '<%s 0x%x %s deletes %s from %s' % (
                type(self).__name__, id(self), pname,
                self.ref, self.oldrev)
        elif self.oldrev == '0000000000000000000000000000000000000000':
            rep = '<%s 0x%x %s creates %s on %s>' % (
                type(self).__name__, id(self), pname,
                self.ref, self.newrev)
        else:
            # Catch all
            rep = '<%s 0x%x %s %s updated %s..%s>' % (
                type(self).__name__, id(self), pname,
                self.ref, self.oldrev, self.newrev)
        return rep

    def toString(self):
        # Not using __str__ because of prevalence in log lines and we
        # prefer the repr syntax.
        rep = None
        pname = None
        if self.project and self.project.name:
            pname = self.project.name
        if self.newrev == '0000000000000000000000000000000000000000':
            rep = '%s %s deletes %s from %s' % (
                type(self).__name__, pname,
                self.ref, self.oldrev)
        elif self.oldrev == '0000000000000000000000000000000000000000':
            rep = '%s %s creates %s on %s' % (
                type(self).__name__, pname,
                self.ref, self.newrev)
        else:
            # Catch all
            rep = '%s %s %s updated %s..%s' % (
                type(self).__name__, pname,
                self.ref, self.oldrev, self.newrev)
        return rep

    def equals(self, other):
        if (self.project == other.project
            and self.ref == other.ref
            and self.newrev == other.newrev):
            return True
        return False

    def isUpdateOf(self, other):
        return False

    def getRelatedChanges(self, sched, related):
        """Recursively update a set of related changes

        :arg Scheduler sched: The scheduler instance
        :arg set related: The cache keys of changes which have been inspected
           so far.  Will be updated with additional changes by this method.
        """
        related.add(self.cache_stat.key)

    def updatesConfig(self, tenant):
        tpc = tenant.project_configs.get(self.project.canonical_name)
        if tpc is None:
            return False
        if hasattr(self, 'branch'):
            if tpc.isAlwaysDynamicBranch(self.branch):
                return True
        if self.files is None:
            # If self.files is None we don't know if this change updates the
            # config so assume it does as this is a safe default if we don't
            # know.
            return True
        for fn in self.files:
            if fn == 'zuul.yaml':
                return True
            if fn == '.zuul.yaml':
                return True
            if fn.startswith("zuul.d/"):
                return True
            if fn.startswith(".zuul.d/"):
                return True
            for ef in tpc.extra_config_files:
                if fn.startswith(ef):
                    return True
            for ed in tpc.extra_config_dirs:
                if fn.startswith(ed):
                    return True
        return False

    def getSafeAttributes(self):
        return Attributes(project=self.project.name,
                          ref=self.ref,
                          oldrev=self.oldrev,
                          newrev=self.newrev)

    def toDict(self):
        # Render to a dict to use in passing json to the executor
        d = dict()
        d['project'] = dict(
            name=self.project.name,
            short_name=self.project.name.split('/')[-1],
            canonical_hostname=self.project.canonical_hostname,
            canonical_name=self.project.canonical_name,
        )
        d['change_url'] = self.url

        commit_id = None
        if self.oldrev and self.oldrev != '0' * 40:
            d['oldrev'] = self.oldrev
            commit_id = self.oldrev
        if self.newrev and self.newrev != '0' * 40:
            d['newrev'] = self.newrev
            commit_id = self.newrev
        if commit_id:
            d['commit_id'] = commit_id
        return d


class Branch(Ref):
    """An existing branch state for a Project."""
    def __init__(self, project):
        super(Branch, self).__init__(project)
        self.branch = None

    def toDict(self):
        # Render to a dict to use in passing json to the executor
        d = super(Branch, self).toDict()
        d['branch'] = self.branch
        return d

    def serialize(self):
        d = super().serialize()
        d["branch"] = self.branch
        return d

    def deserialize(self, data):
        super().deserialize(data)
        self.branch = data.get("branch")


class Tag(Ref):
    """An existing tag state for a Project."""
    def __init__(self, project):
        super(Tag, self).__init__(project)
        self.tag = None
        self.containing_branches = []

    def toDict(self):
        # Render to a dict to use in passing json to the executor
        d = super(Tag, self).toDict()
        d["tag"] = self.tag
        return d

    def serialize(self):
        d = super().serialize()
        d["containing_branches"] = self.containing_branches
        d["tag"] = self.tag
        return d

    def deserialize(self, data):
        super().deserialize(data)
        self.containing_branches = data.get("containing_branches")
        self.tag = data.get("tag")


class Change(Branch):
    """A proposed new state for a Project."""
    def __init__(self, project):
        super(Change, self).__init__(project)
        self.number = None
        # URIs for this change which may appear in depends-on headers.
        # Note this omits the scheme; i.e., is hostname/path.
        self.uris = []
        self.patchset = None

        # Changes that the source determined are needed due to the
        # git DAG:
        self.git_needs_changes = []
        self.git_needed_by_changes = []

        # Changes that the source determined are needed by backwards
        # compatible processing of Depends-On headers (Gerrit only):
        self.compat_needs_changes = []
        self.compat_needed_by_changes = []

        # Changes that the pipeline manager determined are needed due
        # to Depends-On headers (all drivers):
        self.commit_needs_changes = None

        # Needed changes by topic (all
        # drivers in theory, but Gerrit only in practice for
        # emulate-submit-whole-topic):
        self.topic_needs_changes = None
        self.topic_query_ltime = 0

        self.is_current_patchset = True
        self.can_merge = False
        self.is_merged = False
        self.failed_to_merge = False
        self.open = None
        self.owner = None
        self.topic = None

        # This may be the commit message, or it may be a cover message
        # in the case of a PR.  Either way, it's the place where we
        # look for depends-on headers.
        self.message = None
        # This can be the commit id of the patchset enqueued or
        # in the case of a PR the id of HEAD of the branch.
        self.commit_id = None
        # The sha of the base commit, e.g. the sha of the target branch
        # from which the feature branch is created.
        self.base_sha = None

    def deserialize(self, data):
        super().deserialize(data)
        self.number = data.get("number")
        self.uris = data.get("uris", [])
        self.patchset = data.get("patchset")
        self.git_needs_changes = data.get("git_needs_changes", [])
        self.git_needed_by_changes = data.get("git_needed_by_changes", [])
        self.compat_needs_changes = data.get("compat_needs_changes", [])
        self.compat_needed_by_changes = data.get(
            "compat_needed_by_changes", [])
        self.commit_needs_changes = (
            None if data.get("commit_needs_changes") is None
            else data.get("commit_needs_changes", [])
        )
        self.topic_needs_changes = data.get("topic_needs_changes")
        self.topic_query_ltime = data.get("topic_query_ltime", 0)
        self.is_current_patchset = data.get("is_current_patchset", True)
        self.can_merge = data.get("can_merge", False)
        self.is_merged = data.get("is_merged", False)
        self.failed_to_merge = data.get("failed_to_merge", False)
        self.open = data.get("open")
        self.owner = data.get("owner")
        self.topic = data.get("topic")
        self.message = data.get("message")
        self.commit_id = data.get("commit_id")
        self.base_sha = data.get("base_sha")

    def serialize(self):
        d = super().serialize()
        d.update({
            "number": self.number,
            "uris": self.uris,
            "patchset": self.patchset,
            "git_needs_changes": self.git_needs_changes,
            "git_needed_by_changes": self.git_needed_by_changes,
            "compat_needs_changes": self.compat_needs_changes,
            "compat_needed_by_changes": self.git_needed_by_changes,
            "commit_needs_changes": self.commit_needs_changes,
            "topic_needs_changes": self.topic_needs_changes,
            "topic_query_ltime": self.topic_query_ltime,
            "is_current_patchset": self.is_current_patchset,
            "can_merge": self.can_merge,
            "is_merged": self.is_merged,
            "failed_to_merge": self.failed_to_merge,
            "open": self.open,
            "owner": self.owner,
            "topic": self.topic,
            "message": self.message,
            "commit_id": self.commit_id,
            "base_sha": self.base_sha,
        })
        return d

    def _id(self):
        return '%s,%s' % (self.number, self.patchset)

    def __repr__(self):
        pname = None
        if self.project and self.project.name:
            pname = self.project.name
        return '<Change 0x%x %s %s>' % (id(self), pname, self._id())

    def toString(self):
        pname = None
        if self.project and self.project.name:
            pname = self.project.name
        return '%s %s %s' % (type(self).__name__, pname, self._id())

    def equals(self, other):
        if (super().equals(other) and
            isinstance(other, Change) and
            self.number == other.number and self.patchset == other.patchset):
            return True
        return False

    def getNeedsChanges(self, include_topic_needs=False):
        commit_needs_changes = self.commit_needs_changes or []
        topic_needs_changes = (
            include_topic_needs and self.topic_needs_changes) or []
        r = OrderedDict()
        for x in (self.git_needs_changes + self.compat_needs_changes +
                  commit_needs_changes + topic_needs_changes):
            r[x] = None
        return tuple(r.keys())

    def getNeededByChanges(self):
        r = OrderedDict()
        for x in (self.git_needed_by_changes + self.compat_needed_by_changes):
            r[x] = None
        return tuple(r.keys())

    def isUpdateOf(self, other):
        if (self.project == other.project and
            (hasattr(other, 'number') and self.number == other.number) and
            (hasattr(other, 'patchset') and
             self.patchset is not None and
             other.patchset is not None and
             int(self.patchset) > int(other.patchset))):
            return True
        return False

    def getRelatedChanges(self, sched, related):
        """Recursively update a set of related changes

        :arg Scheduler sched: The scheduler instance
        :arg set related: The cache keys of changes which have been inspected
           so far.  Will be updated with additional changes by this method.
        """
        related.add(self.cache_stat.key)
        for reference in itertools.chain(
                self.getNeedsChanges(include_topic_needs=True),
                self.getNeededByChanges()):
            key = ChangeKey.fromReference(reference)
            if key not in related:
                source = sched.connections.getSource(key.connection_name)
                change = source.getChange(key)
                change.getRelatedChanges(sched, related)

    def getSafeAttributes(self):
        return Attributes(project=self.project.name,
                          number=self.number,
                          patchset=self.patchset)

    def toDict(self):
        # Render to a dict to use in passing json to the executor
        d = super(Change, self).toDict()
        d.pop('oldrev', None)
        d.pop('newrev', None)
        d['change'] = str(self.number)
        d['patchset'] = str(self.patchset)
        d['commit_id'] = self.commit_id
        d['change_message'] = self.message
        d['topic'] = self.topic
        return d


class EventTypeIndex(type(abc.ABC)):
    """A metaclass used to maintain a mapping of Event class names to
    class definitions when serializing and deserializing to and from
    ZooKeeper
    """
    event_type_mapping = {}

    def __init__(self, name, bases, clsdict):
        EventTypeIndex.event_type_mapping[name] = self
        super().__init__(name, bases, clsdict)


class AbstractEvent(abc.ABC, metaclass=EventTypeIndex):
    """Base class defining the interface for all events."""

    # Opaque identifier in order to acknowledge an event
    ack_ref = None

    @abc.abstractmethod
    def toDict(self):
        pass

    @abc.abstractmethod
    def updateFromDict(self, d) -> None:
        pass

    @classmethod
    def fromDict(cls, data):
        event = cls()
        event.updateFromDict(data)
        return event


class ConnectionEvent(AbstractEvent, UserDict):

    def toDict(self):
        return self.data

    def updateFromDict(self, d):
        self.data.update(d)


class ManagementEvent(AbstractEvent):
    """An event that should be processed within the main queue run loop"""

    def __init__(self):
        self.traceback = None
        self.zuul_event_id = None
        # Logical timestamp of the event (Zookeeper creation transaction ID).
        # This will be automatically set when the event is consumed from
        # the event queue in case it is None.
        self.zuul_event_ltime = None
        # Opaque identifier in order to report the result of an event
        self.result_ref = None

    def exception(self, tb: str):
        self.traceback = tb

    def toDict(self):
        return {
            "zuul_event_id": self.zuul_event_id,
            "zuul_event_ltime": self.zuul_event_ltime,
        }

    def updateFromDict(self, d):
        self.zuul_event_id = d.get("zuul_event_id")
        self.zuul_event_ltime = d.get("zuul_event_ltime")


class ReconfigureEvent(ManagementEvent):
    """Reconfigure the scheduler.  The layout will be (re-)loaded from
    the path specified in the configuration."""

    def __init__(self, smart=False, tenants=None):
        super(ReconfigureEvent, self).__init__()
        self.smart = smart
        self.tenants = tenants

    def toDict(self):
        d = super().toDict()
        d["smart"] = self.smart
        d["tenants"] = self.tenants
        return d

    @classmethod
    def fromDict(cls, data):
        event = cls(data.get("smart", False),
                    data.get("tenants", None))
        event.updateFromDict(data)
        return event


class TenantReconfigureEvent(ManagementEvent):
    """Reconfigure the given tenant.  The layout will be (re-)loaded from
    the path specified in the configuration.

    :arg str tenant_name: the tenant to reconfigure
    :arg str project_name: if supplied, clear the cached configuration
         from this project first
    :arg str branch_name: if supplied along with project, only remove the
         configuration of the specific branch from the cache
    """
    def __init__(self, tenant_name, project_name, branch_name):
        super(TenantReconfigureEvent, self).__init__()
        self.tenant_name = tenant_name
        self.project_branches = set([(project_name, branch_name)])
        self.branch_cache_ltimes = {}
        self.trigger_event_ltime = -1
        self.merged_events = []

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, TenantReconfigureEvent):
            return False
        # We don't check projects because they will get combined when
        # merged.
        return (self.tenant_name == other.tenant_name)

    def merge(self, other):
        if self.tenant_name != other.tenant_name:
            raise Exception("Can not merge events from different tenants")
        self.project_branches |= other.project_branches
        for connection_name, ltime in other.branch_cache_ltimes.items():
            self.branch_cache_ltimes[connection_name] = max(
                self.branch_cache_ltimes.get(connection_name, ltime), ltime)
        self.zuul_event_ltime = max(self.zuul_event_ltime,
                                    other.zuul_event_ltime)
        self.trigger_event_ltime = max(self.trigger_event_ltime,
                                       other.trigger_event_ltime)
        self.merged_events.append(other)

    def toDict(self):
        d = super().toDict()
        d["tenant_name"] = self.tenant_name
        d["project_branches"] = list(self.project_branches)
        d["branch_cache_ltimes"] = self.branch_cache_ltimes
        d["trigger_event_ltime"] = self.trigger_event_ltime
        return d

    @classmethod
    def fromDict(cls, data):
        project, branch = next(iter(data["project_branches"]))
        event = cls(data.get("tenant_name"), project, branch)
        event.updateFromDict(data)
        # In case the dictionary was deserialized from JSON we get
        # [[project, branch]] instead of [(project, branch]).
        # Because of that we need to make sure we have a hashable
        # project-branch tuple.
        event.project_branches = set(
            tuple(pb) for pb in data["project_branches"]
        )
        event.branch_cache_ltimes = data.get("branch_cache_ltimes", {})
        event.trigger_event_ltime = data.get("trigger_event_ltime", -1)
        return event


class PromoteEvent(ManagementEvent):
    """Promote one or more changes to the head of the queue.

    :arg str tenant_name: the name of the tenant
    :arg str pipeline_name: the name of the pipeline
    :arg list change_ids: a list of strings of change ids in the form
        1234,1
    """

    def __init__(self, tenant_name, pipeline_name, change_ids):
        super(PromoteEvent, self).__init__()
        self.tenant_name = tenant_name
        self.pipeline_name = pipeline_name
        self.change_ids = change_ids

    def toDict(self):
        d = super().toDict()
        d["tenant_name"] = self.tenant_name
        d["pipeline_name"] = self.pipeline_name
        d["change_ids"] = list(self.change_ids)
        return d

    @classmethod
    def fromDict(cls, data):
        event = cls(
            data.get("tenant_name"),
            data.get("pipeline_name"),
            list(data.get("change_ids", [])),
        )
        event.updateFromDict(data)
        return event


class PipelinePostConfigEvent(ManagementEvent):
    """Enqueued after a pipeline has been reconfigured in order
    to trigger a processing run"""
    pass


class PipelineSemaphoreReleaseEvent(ManagementEvent):
    """Enqueued after a semaphore has been released in order
    to trigger a processing run"""
    pass


class ChangeManagementEvent(ManagementEvent):
    """Base class for events that dequeue/enqueue changes

    :arg str tenant_name: the name of the tenant
    :arg str pipeline_name: the name of the pipeline
    :arg str project_hostname: the hostname of the project
    :arg str project_name: the name of the project
    :arg str change: optional, the change
    :arg str ref: optional, the ref
    :arg str oldrev: optional, the old revision
    :arg str newrev: optional, the new revision
    :arg dict orig_ref: optional, the cache key reference of the
        ref of the original triggering event
    :arg str user_id: optional, event originator user_id
    :arg dict parameters: optional, pipeline parameters
    """

    def __init__(self, tenant_name, pipeline_name, project_hostname,
                 project_name, change=None, ref=None, oldrev=None,
                 newrev=None, orig_ref=None, user_id=None,
                 parameters=None):
        super().__init__()
        self.type = None
        self.tenant_name = tenant_name
        self.pipeline_name = pipeline_name
        self.project_hostname = project_hostname
        self.project_name = project_name
        self.change = change
        if change is not None:
            self.change_number, self.patch_number = change.split(',')
        else:
            self.change_number, self.patch_number = (None, None)
        self.ref = ref
        self.oldrev = oldrev
        self.newrev = newrev
        self.orig_ref = orig_ref
        self.timestamp = time.time()
        span = trace.get_current_span()
        self.span_context = tracing.getSpanContext(span)
        self.user_id = user_id
        self.parameters = parameters

    @property
    def canonical_project_name(self):
        return self.project_hostname + '/' + self.project_name

    def toDict(self):
        d = super().toDict()
        d["type"] = self.type
        d["tenant_name"] = self.tenant_name
        d["pipeline_name"] = self.pipeline_name
        d["project_hostname"] = self.project_hostname
        d["project_name"] = self.project_name
        d["change"] = self.change
        d["ref"] = self.ref
        d["oldrev"] = self.oldrev
        d["newrev"] = self.newrev
        d["timestamp"] = self.timestamp
        d["span_context"] = self.span_context
        d["orig_ref"] = self.orig_ref
        d["user_id"] = self.user_id
        d["parameters"] = self.parameters
        return d

    def updateFromDict(self, d):
        super().updateFromDict(d)
        self.type = d.get("type")
        self.timestamp = d.get("timestamp")
        self.span_context = d.get("span_context")

    @classmethod
    def fromDict(cls, data):
        event = cls(
            data.get("tenant_name"),
            data.get("pipeline_name"),
            data.get("project_hostname"),
            data.get("project_name"),
            data.get("change"),
            data.get("ref"),
            data.get("oldrev"),
            data.get("newrev"),
            data.get("orig_ref"),
            data.get("user_id"),
            data.get("parameters"),
        )
        event.updateFromDict(data)
        return event


class DequeueEvent(ChangeManagementEvent):
    """Dequeue a change from a pipeline"""
    type = "dequeue"


class EnqueueEvent(ChangeManagementEvent):
    """Enqueue a change into a pipeline"""
    type = "enqueue"


class SupercedeEvent(ChangeManagementEvent):
    """Supercede a change in a pipeline"""
    type = "supercede"


class ResultEvent(AbstractEvent):
    """An event that needs to modify the pipeline state due to a
    result from an external system."""

    def updateFromDict(self, d) -> None:
        pass


class SemaphoreReleaseEvent(ManagementEvent):
    """Enqueued after a semaphore has been released in order
    to trigger a processing run.

    This is emitted when a playbook semaphore is released to instruct
    the scheduler to emit fan-out PipelineSemaphoreReleaseEvents for
    every potentially affected tenant-pipeline.
    """

    def __init__(self, semaphore_name):
        super().__init__()
        self.semaphore_name = semaphore_name

    def toDict(self):
        d = super().toDict()
        d["semaphore_name"] = self.semaphore_name
        return d

    @classmethod
    def fromDict(cls, data):
        event = cls(data.get("semaphore_name"))
        event.updateFromDict(data)
        return event

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} semaphore_name={self.semaphore_name}>"
        )


class BuildResultEvent(ResultEvent):
    """Base class for all build result events.

    This class provides the common data structure for all build result
    events.

    :arg str build_uuid: The UUID of the build for which this event is
                         emitted.
    :arg str build_set_uuid: The UUID of the buildset of which the build
                             is part of.
    :arg str job_uuid: The uuid of the job the build is executed for.
    :arg str build_request_ref: The path to the build request that is
                                stored in ZooKeeper.
    :arg dict data: The event data.
    """

    def __init__(self, build_uuid, build_set_uuid, job_uuid,
                 build_request_ref, data, zuul_event_id=None):
        self.build_uuid = build_uuid
        self.build_set_uuid = build_set_uuid
        self.job_uuid = job_uuid
        self.build_request_ref = build_request_ref
        self.data = data
        self.zuul_event_id = zuul_event_id

    def toDict(self):
        d = {
            "build_uuid": self.build_uuid,
            "build_set_uuid": self.build_set_uuid,
            "job_uuid": self.job_uuid,
            "build_request_ref": self.build_request_ref,
            "data": self.data,
            "zuul_event_id": self.zuul_event_id,
        }
        return d

    @classmethod
    def fromDict(cls, data):
        return cls(
            data.get("build_uuid"), data.get("build_set_uuid"),
            data.get("job_uuid"),
            data.get("build_request_ref"), data.get("data"),
            data.get("zuul_event_id"))

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} build={self.build_uuid} "
            f"job={self.job_uuid}>"
        )


class BuildStartedEvent(BuildResultEvent):
    """A build has started."""

    pass


class BuildStatusEvent(BuildResultEvent):
    """Worker info and log URL for a build are available."""

    pass


class BuildPausedEvent(BuildResultEvent):
    """A build has been paused."""

    pass


class BuildCompletedEvent(BuildResultEvent):
    """A build has completed."""

    pass


class MergeCompletedEvent(ResultEvent):
    """A remote merge operation has completed

    :arg str request_uuid: The UUID of the merge request job.
    :arg str build_set_uuid: The UUID of the build_set which is ready.
    :arg bool merged: Whether the merge succeeded (changes with refs).
    :arg bool updated: Whether the repo was updated (changes without refs).
    :arg str commit: The SHA of the merged commit (changes with refs).
    :arg dict repo_state: The starting repo state before the merge.
    :arg list item_in_branches: A list of branches in which the final
        commit in the merge list appears (changes without refs).
    :arg list errors: A list of error message strings.
    :arg float elapsed_time: Elapsed time of merge op in seconds.
    """

    def __init__(self, request_uuid, build_set_uuid, merged, updated,
                 commit, files, repo_state, item_in_branches,
                 errors, elapsed_time, span_info=None, zuul_event_id=None):
        self.request_uuid = request_uuid
        self.build_set_uuid = build_set_uuid
        self.merged = merged
        self.updated = updated
        self.commit = commit
        self.files = files or []
        self.repo_state = repo_state or {}
        self.item_in_branches = item_in_branches or []
        self.errors = errors or []
        self.elapsed_time = elapsed_time
        self.span_info = span_info
        self.zuul_event_id = zuul_event_id

    def __repr__(self):
        return ('<MergeCompletedEvent job: %s buildset: %s merged: %s '
                'updated: %s commit: %s errors: %s>' % (
                    self.request_uuid, self.build_set_uuid,
                    self.merged, self.updated, self.commit,
                    self.errors))

    def toDict(self):
        return {
            "request_uuid": self.request_uuid,
            "build_set_uuid": self.build_set_uuid,
            "merged": self.merged,
            "updated": self.updated,
            "commit": self.commit,
            "files": list(self.files),
            "repo_state": dict(self.repo_state),
            "item_in_branches": list(self.item_in_branches),
            "errors": list(self.errors),
            "elapsed_time": self.elapsed_time,
            "span_info": self.span_info,
            "zuul_event_id": self.zuul_event_id,
        }

    @classmethod
    def fromDict(cls, data):
        return cls(
            data.get("request_uuid"),
            data.get("build_set_uuid"),
            data.get("merged"),
            data.get("updated"),
            data.get("commit"),
            list(data.get("files", [])),
            dict(data.get("repo_state", {})),
            list(data.get("item_in_branches", [])),
            list(data.get("errors", [])),
            data.get("elapsed_time"),
            data.get("span_info"),
            data.get("zuul_event_id"),
        )


class FilesChangesCompletedEvent(ResultEvent):
    """A remote fileschanges operation has completed

    :arg BuildSet build_set: The build_set which is ready.
    :arg list files: List of files changed.
    :arg float elapsed_time: Elapsed time of merge op in seconds.
    """

    def __init__(self, request_uuid, build_set_uuid, files, elapsed_time,
                 span_info=None):
        self.request_uuid = request_uuid
        self.build_set_uuid = build_set_uuid
        self.files = files or []
        self.elapsed_time = elapsed_time
        self.span_info = span_info

    def toDict(self):
        return {
            "request_uuid": self.request_uuid,
            "build_set_uuid": self.build_set_uuid,
            "files": list(self.files),
            "elapsed_time": self.elapsed_time,
            "span_info": self.span_info,
        }

    @classmethod
    def fromDict(cls, data):
        return cls(
            data.get("request_uuid"),
            data.get("build_set_uuid"),
            list(data.get("files", [])),
            data.get("elapsed_time"),
            data.get("span_info"),
        )


class NodesProvisionedEvent(ResultEvent):
    """Nodes have been provisioned for a build_set

    :arg int request_id: The id of the fulfilled node request.
    :arg str build_set_uuid: UUID of the buildset this node request belongs to
    """

    def __init__(self, request_id, build_set_uuid):
        self.request_id = request_id
        self.build_set_uuid = build_set_uuid

    def toDict(self):
        return {
            "request_id": self.request_id,
            "build_set_uuid": self.build_set_uuid,
        }

    @classmethod
    def fromDict(cls, data):
        return cls(
            data.get("request_id"),
            data.get("build_set_uuid"),
        )


class TriggerEvent(AbstractEvent):
    """Incoming event from an external system."""
    def __init__(self):
        # TODO(jeblair): further reduce this list
        self.data = None
        # common
        self.type = None
        self.branch_updated = False
        self.branch_created = False
        self.branch_deleted = False
        self.branch_protected = True
        self.ref = None
        # For reconfiguration sequencing
        self.min_reconfigure_ltime = -1
        self.zuul_event_ltime = None
        # For management events (eg: enqueue / promote)
        self.tenant_name = None
        self.project_hostname = None
        self.project_name = None
        self.trigger_name = None
        self.connection_name = None
        # Representation of the user account that performed the event.
        self.account = None
        # patchset-created, comment-added, etc.
        self.change_number = None
        self.change_url = None
        self.patch_number = None
        self.branch = None
        self.comment = None
        self.state = None
        # ref-updated
        self.oldrev = None
        self.newrev = None
        # For events that arrive with a destination pipeline (eg, from
        # an admin command, etc):
        self.forced_pipeline = None
        # For logging
        self.zuul_event_id = None
        self.timestamp = None
        self.arrived_at_scheduler_timestamp = None
        self.driver_name = None
        self.branch_cache_ltime = -1
        span = trace.get_current_span()
        self.span_context = tracing.getSpanContext(span)

    def toDict(self):
        return {
            "data": self.data,
            "type": self.type,
            "branch_updated": self.branch_updated,
            "branch_created": self.branch_created,
            "branch_deleted": self.branch_deleted,
            "branch_protected": self.branch_protected,
            "ref": self.ref,
            "min_reconfigure_ltime": self.min_reconfigure_ltime,
            "zuul_event_ltime": self.zuul_event_ltime,
            "tenant_name": self.tenant_name,
            "project_hostname": self.project_hostname,
            "project_name": self.project_name,
            "trigger_name": self.trigger_name,
            "connection_name": self.connection_name,
            "account": self.account,
            "change_number": self.change_number,
            "change_url": self.change_url,
            "patch_number": self.patch_number,
            "branch": self.branch,
            "comment": self.comment,
            "state": self.state,
            "oldrev": self.oldrev,
            "newrev": self.newrev,
            "forced_pipeline": self.forced_pipeline,
            "zuul_event_id": self.zuul_event_id,
            "timestamp": self.timestamp,
            "arrived_at_scheduler_timestamp": (
                self.arrived_at_scheduler_timestamp
            ),
            "driver_name": self.driver_name,
            "branch_cache_ltime": self.branch_cache_ltime,
            "span_context": self.span_context,
        }

    def updateFromDict(self, d):
        self.data = d["data"]
        self.type = d["type"]
        self.branch_updated = d["branch_updated"]
        self.branch_created = d["branch_created"]
        self.branch_deleted = d["branch_deleted"]
        self.branch_protected = d["branch_protected"]
        self.ref = d["ref"]
        self.min_reconfigure_ltime = d.get("min_reconfigure_ltime", -1)
        self.zuul_event_ltime = d.get("zuul_event_ltime", None)
        self.tenant_name = d["tenant_name"]
        self.project_hostname = d["project_hostname"]
        self.project_name = d["project_name"]
        self.trigger_name = d["trigger_name"]
        self.connection_name = d["connection_name"]
        self.account = d["account"]
        self.change_number = d["change_number"]
        self.change_url = d["change_url"]
        self.patch_number = d["patch_number"]
        self.branch = d["branch"]
        self.comment = d["comment"]
        self.state = d["state"]
        self.oldrev = d["oldrev"]
        self.newrev = d["newrev"]
        self.forced_pipeline = d["forced_pipeline"]
        self.zuul_event_id = d["zuul_event_id"]
        self.timestamp = d["timestamp"]
        self.arrived_at_scheduler_timestamp = (
            d["arrived_at_scheduler_timestamp"]
        )
        self.driver_name = d["driver_name"]
        self.branch_cache_ltime = d.get("branch_cache_ltime", -1)
        self.span_context = d.get("span_context")

    @property
    def canonical_project_name(self):
        return self.project_hostname + '/' + self.project_name

    def isPatchsetCreated(self):
        return False

    def isMessageChanged(self):
        return False

    def isChangeAbandoned(self):
        return False

    def isBranchProtectionChanged(self):
        return False

    def isDefaultBranchChanged(self):
        return False

    def _repr(self):
        flags = [str(self.type)]
        if self.project_name:
            flags.append(self.project_name)
        if self.ref:
            flags.append(self.ref)
        if self.branch_updated:
            flags.append('branch_updated')
        if self.branch_created:
            flags.append('branch_created')
        if self.branch_deleted:
            flags.append('branch_deleted')
        return ' '.join(flags)

    def __repr__(self):
        return '<%s 0x%x %s>' % (self.__class__.__name__,
                                 id(self), self._repr())


class FalseWithReason(object):
    """Event filter result"""
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.reason

    def __bool__(self):
        return False


class BaseFilter(ConfigObject):
    """Base Class for filtering which Changes and Events to process."""
    pass


class EventFilter(BaseFilter):
    """Allows a Pipeline to only respond to certain events."""
    def __init__(self, connection_name, trigger, debug=None):
        super(EventFilter, self).__init__()
        self.connection_name = connection_name
        self.trigger = trigger
        self.debug = bool(debug)

    def matches(self, event, ref):
        # TODO(jeblair): consider removing ref argument

        # Event came from wrong connection
        if self.connection_name != event.connection_name:
            return False

        return True


class RefFilter(BaseFilter):
    """Allows a Manager to only enqueue Changes that meet certain criteria."""
    def __init__(self, connection_name):
        super(RefFilter, self).__init__()
        self.connection_name = connection_name

    def matches(self, change):
        return True


class TenantProjectConfig(object):
    """A project in the context of a tenant.

    A Project is globally unique in the system, however, when used in
    a tenant, some metadata about the project local to the tenant is
    stored in a TenantProjectConfig.
    """

    def __init__(self, project):
        self.project = project
        self.load_classes = set()
        self.shadow_projects = set()
        self.branches = []
        self.dynamic_branches = []
        # The tenant's default setting of exclude_unprotected_branches will
        # be overridden by this one if not None.
        self.exclude_unprotected_branches = None
        self.exclude_locked_branches = None
        self.include_branches = None
        self.exclude_branches = None
        self.always_dynamic_branches = None
        # The list of paths to look for extra zuul config files
        self.extra_config_files = ()
        # The list of paths to look for extra zuul config dirs
        self.extra_config_dirs = ()
        # Load config from a different branch if this is a config project
        self.load_branch = None
        self.merge_modes = None
        self.implied_branch_matchers = None
        self.trusted = False
        # A list of project names/regexes that this project is allowed
        # to configure
        self.configure_projects = None
        self.allow_reporter_jobs = None

    def canConfigureProject(self, other_tpc, validation_only=False):
        if self.trusted:
            return True
        if other_tpc.project.canonical_name == self.project.canonical_name:
            return True
        if not self.configure_projects:
            return False
        if not validation_only and other_tpc.trusted:
            return False
        for r in self.configure_projects:
            for name in (other_tpc.project.name,
                         other_tpc.project.canonical_name):
                if r.fullmatch(name):
                    return True
        return False

    def isAlwaysDynamicBranch(self, branch):
        if self.always_dynamic_branches is None:
            return False
        for r in self.always_dynamic_branches:
            if r.fullmatch(branch):
                return True
        return False

    def includesBranch(self, branch):
        if self.include_branches is not None:
            for r in self.include_branches:
                if r.fullmatch(branch):
                    return True
            return False

        if self.exclude_branches is not None:
            for r in self.exclude_branches:
                if r.fullmatch(branch):
                    return False
        return True


class ProjectParameterConfig(ConfigObject):
    # Represents a project parameter
    def __init__(self):
        super().__init__()
        self.name = None
        self.description = None
        self.type = None
        self.values = None
        self.default = None
        self.required = False

    def toDict(self):
        return {
            'name': self.name,
            'description': self.description,
            'type': self.type,
            'values': self.values,
            'default': self.default,
            'required': self.required,
        }


class ProjectPipelineConfig(ConfigObject):
    # Represents a project cofiguration in the context of a pipeline
    def __init__(self):
        super(ProjectPipelineConfig, self).__init__()
        self.job_list = JobList()
        self.debug = False
        self.debug_messages = []
        self.fail_fast = None
        self.variables = {}
        self.parameters = {}

    def addDebug(self, msg):
        self.debug_messages.append(msg)

    def update(self, other):
        if not isinstance(other, ProjectPipelineConfig):
            raise Exception("Unable to update from %s" % (other,))
        if other.debug:
            self.debug = other.debug
        if self.fail_fast is None:
            self.fail_fast = other.fail_fast
        self.job_list.inheritFrom(other.job_list)
        for p in other.parameters.values():
            if p.name not in self.parameters:
                self.parameters[p.name] = p

    def updateVariables(self, other):
        # We need to keep this separate to update() because we wish to
        # apply the project variables all the time, even if its jobs
        # only come from templates.
        self.variables = Job._deepUpdate(self.variables, other)

    def toDict(self):
        return {
            'parameters': [x.toDict() for x in self.parameters.values()],
        }


class ProjectConfig(ConfigObject):
    # Represents a project configuration
    def __init__(self, name):
        super(ProjectConfig, self).__init__()
        # Note: name may not be resolved to a fully qualified project
        # name; use the layout to get relevant project configs for a
        # given canonical name.
        self.name = name
        self.is_template = False
        self.templates = []
        # Pipeline name -> ProjectPipelineConfig
        self.pipelines = {}
        self.explicit_branch_matcher = None
        self.implied_branch_matcher = None
        self.source_implied_branch_matcher = None
        self.variables = {}
        # These represent the values from the config file, but should
        # not be used directly; instead, use the ProjectMetadata to
        # find the computed value from across all project config
        # stanzas.
        self.merge_mode = None
        self.default_branch = None
        self.queue_name = None

    def __repr__(self):
        return ('<ProjectConfig %s source: %s '
                'explicit: %s implied: %s source: %s>' % (
                    self.name, self.source_context,
                    self.explicit_branch_matcher,
                    self.implied_branch_matcher,
                    self.source_implied_branch_matcher,
                ))

    def embeddedJobs(self):
        # Return all embedded job definitions in this project config
        # for validation purposes.
        for ppc in self.pipelines.values():
            for jobs in ppc.job_list.jobs.values():
                for job in jobs:
                    if isinstance(job, Job):
                        yield job

    def _makeBranchMatcher(self, matchers):
        if len(matchers) == 0:
            return None
        elif len(matchers) > 1:
            return change_matcher.MatchAny(matchers)
        else:
            return matchers[0]

    def setExplicitBranchMatchers(self, matchers):
        self.explicit_branch_matcher = self._makeBranchMatcher(matchers)

    def setImpliedBranchMatchers(self, matchers):
        self.implied_branch_matcher = self._makeBranchMatcher(matchers)

    def setSourceImpliedBranchMatchers(self, matchers):
        self.source_implied_branch_matcher = self._makeBranchMatcher(matchers)

    def getBranchMatcher(self, tenant, project_canonical_name):
        # Explicit branch matchers always win
        if self.explicit_branch_matcher is not None:
            return self.explicit_branch_matcher

        source_tpc = tenant.getTPC(self.source_context.project_canonical_name)
        # Pragmas can cause templates to end up with implied branch
        # matchers for arbitrary branches, but project stanzas should
        # not.  They should either have the current branch or no
        # branch matcher.  But if we're configuring a different
        # project, then we'll allow the pragma-supplied branch
        # matchers.
        if not self.is_template:
            this_tpc = tenant.getTPC(project_canonical_name)
            same_project = (this_tpc.project.canonical_name ==
                            self.source_context.project_canonical_name)
            if same_project:
                if source_tpc.trusted:
                    return None
                else:
                    return self.source_implied_branch_matcher

        # If there was an explicit pragma to use implied branch
        # matchers or not, honor it
        if self.source_context.implied_branch_matchers is True:
            return self.source_context.implied_branch_matcher
        if self.source_context.implied_branch_matchers is False:
            return None
        # If we were defined in a trusted context in this tenant (and
        # there's no pragma), then we do not use implied branch
        # matchers.
        if source_tpc.trusted:
            return None
        branches = tenant.getProjectBranches(
            self.source_context.project_canonical_name)
        if len(branches) == 1:
            return None
        return self.implied_branch_matcher

    def changeMatches(self, tenant, project_canonical_name, change):
        # project_canonical_name is the name of the project that this
        # ProjectConfig is targeting, provided by the caller since
        # self.name is not fully resolved.
        branch_matcher = self.getBranchMatcher(tenant, project_canonical_name)
        if branch_matcher and not branch_matcher.matches(change):
            return False
        return True

    def toDict(self):
        d = {}
        d['source_context'] = self.source_context.toDict()
        d['default_branch'] = self.default_branch
        if self.merge_mode:
            d['merge_mode'] = list(filter(lambda x: x[1] == self.merge_mode,
                                          MERGER_MAP.items()))[0][0]
        else:
            d['merge_mode'] = None
        d['is_template'] = self.is_template
        d['templates'] = self.templates
        d['queue_name'] = self.queue_name
        return d


class ProjectMetadata:
    """Information about a Project

    A Layout holds one of these for each project it knows about.
    Information about the project which is synthesized from multiple
    ProjectConfig objects is stored here.
    """

    def __init__(self):
        self.merge_mode = None
        self.default_branch = None
        self.is_template = False
        self.queue_name = None

    def toDict(self):
        return {
            'merge_mode': self.merge_mode,
            'default_branch': self.default_branch,
            'is_template': self.is_template,
            'queue_name': self.queue_name,
        }

    @classmethod
    def fromDict(cls, data):
        o = cls()
        o.merge_mode = data['merge_mode']
        o.default_branch = data['default_branch']
        o.is_template = data.get('is_template', False)
        o.queue_name = data['queue_name']
        return o


class SystemAttributes:
    """Global system attributes from the Zuul config.

    Those runtime related settings are expected to be consistent on
    all schedulers and will be synchronized via Zookeeper.
    """

    _default_oidc_signing_key_rotation_interval = 60 * 60 * 24 * 7  # 1 week
    # TODO: When more algorithms are supported, this should be
    # fallback to all supported algorithms
    _default_oidc_supported_signing_algorithms = ['RS256']
    _default_oidc_default_signing_algorithm = "RS256"

    def __init__(self):
        self.use_relative_priority = False
        self.max_hold_expiration = 0
        self.default_hold_expiration = 0
        self.default_ansible_version = None
        self.web_root = None
        self.websocket_url = None
        self.oidc_signing_key_rotation_interval = None
        self.oidc_supported_signing_algorithms = None
        self.oidc_default_signing_algorithm = None

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self.use_relative_priority == other.use_relative_priority
            and self.max_hold_expiration == other.max_hold_expiration
            and self.default_hold_expiration == other.default_hold_expiration
            and self.default_ansible_version == other.default_ansible_version
            and self.web_root == other.web_root
            and self.websocket_url == other.websocket_url
            and (self.oidc_signing_key_rotation_interval ==
                 other.oidc_signing_key_rotation_interval)
            and (self.oidc_supported_signing_algorithms ==
                 other.oidc_supported_signing_algorithms)
            and (self.oidc_default_signing_algorithm ==
                 other.oidc_default_signing_algorithm))

    @classmethod
    def fromConfig(cls, config):
        sys_attrs = cls()
        sys_attrs.updateFromConfig(config)
        return sys_attrs

    def updateFromConfig(self, config):
        """Set runtime related system attributes from config."""
        self.use_relative_priority = False
        if config.has_option('scheduler', 'relative_priority'):
            if config.getboolean('scheduler', 'relative_priority'):
                self.use_relative_priority = True

        max_hold = get_default(config, 'scheduler', 'max_hold_expiration', 0)
        default_hold = get_default(
            config, 'scheduler', 'default_hold_expiration', max_hold)

        # If the max hold is not infinite, we need to make sure that
        # our default value does not exceed it.
        if (max_hold and default_hold != max_hold
                and (default_hold == 0 or default_hold > max_hold)):
            default_hold = max_hold
        self.max_hold_expiration = max_hold
        self.default_hold_expiration = default_hold

        # Reload the ansible manager in case the default ansible version
        # changed.
        self.default_ansible_version = get_default(
            config, 'scheduler', 'default_ansible_version', None)

        web_root = get_default(config, 'web', 'root', None)
        if web_root:
            web_root = urllib.parse.urljoin(web_root, 't/{tenant.name}/')
        self.web_root = web_root

        self.websocket_url = get_default(config, 'web', 'websocket_url', None)
        self.oidc_signing_key_rotation_interval = int(get_default(
            config, 'oidc', 'signing_key_rotation_interval',
            self._default_oidc_signing_key_rotation_interval))

        oidc_signing_algorithms_config = get_default(
            config, 'oidc', 'supported_signing_algorithms', None)
        self.oidc_supported_signing_algorithms = [
            alg.strip() for alg in oidc_signing_algorithms_config.split(',')
        ] if oidc_signing_algorithms_config else \
            self._default_oidc_supported_signing_algorithms

        self.oidc_default_signing_algorithm = get_default(
            config, 'oidc', 'default_signing_algorithm',
            self._default_oidc_default_signing_algorithm)

    def toDict(self):
        attributes = {
            "use_relative_priority": self.use_relative_priority,
            "max_hold_expiration": self.max_hold_expiration,
            "default_hold_expiration": self.default_hold_expiration,
            "default_ansible_version": self.default_ansible_version,
            "web_root": self.web_root,
            "websocket_url": self.websocket_url,
            "oidc_signing_key_rotation_interval":
                self.oidc_signing_key_rotation_interval,
            "oidc_supported_signing_algorithms":
                self.oidc_supported_signing_algorithms,
            "oidc_default_signing_algorithm":
                self.oidc_default_signing_algorithm,
        }
        if COMPONENT_REGISTRY.model_api < 34:
            attributes["web_status_url"] = ""
        return attributes

    @classmethod
    def fromDict(cls, data):
        sys_attrs = cls()
        sys_attrs.use_relative_priority = data["use_relative_priority"]
        sys_attrs.max_hold_expiration = data["max_hold_expiration"]
        sys_attrs.default_hold_expiration = data["default_hold_expiration"]
        sys_attrs.default_ansible_version = data["default_ansible_version"]
        sys_attrs.web_root = data["web_root"]
        sys_attrs.websocket_url = data["websocket_url"]
        # For the newly added system attributes, we need to use get()
        # method to avoid KeyError in scheduler prime() method.
        sys_attrs.oidc_signing_key_rotation_interval = data.get(
            "oidc_signing_key_rotation_interval",
            cls._default_oidc_signing_key_rotation_interval)
        sys_attrs.oidc_supported_signing_algorithms = data.get(
            "oidc_supported_signing_algorithms",
            cls._default_oidc_supported_signing_algorithms)
        sys_attrs.oidc_default_signing_algorithm = data.get(
            "oidc_default_signing_algorithm",
            cls._default_oidc_default_signing_algorithm)
        return sys_attrs


# TODO(ianw) : this would clearly be better if it recorded the
# original file and made line-relative comments, however the contexts
# the subclasses are raised in don't have that info currently, so this
# is a best-effort to show you something that clues you into the
# error.
class ConfigItemErrorException(Exception):
    def __init__(self, conf):
        super(ConfigItemErrorException, self).__init__(
            self.message + self._generate_extract(conf))

    def _generate_extract(self, conf):
        context = textwrap.dedent("""\

        The incorrect values are around:

        """)
        # Not sorting the keys makes it look closer to what is in the
        # file and works best with >= Python 3.7 where dicts are
        # ordered by default.  If this is a foreign config file or
        # something the dump might be really long; hence the
        # truncation.
        extract = yaml.encrypted_dump(conf, sort_keys=False)
        lines = extract.split('\n')
        if len(lines) > 5:
            lines = lines[0:4]
            lines.append('...')

        return context + '\n'.join(lines)


class ConfigItemNotListError(ConfigItemErrorException):
    message = textwrap.dedent("""\
    Configuration file is not a list.  Each zuul.yaml configuration
    file must be a list of items, for example:

    - job:
        name: foo

    - project:
        name: bar

    Ensure that every item starts with "- " so that it is parsed as a
    YAML list.
    """)


class ConfigItemNotDictError(ConfigItemErrorException):
    message = textwrap.dedent("""\
    Configuration item is not a dictionary.  Each zuul.yaml
    configuration file must be a list of dictionaries, for
    example:

    - job:
        name: foo

    - project:
        name: bar

    Ensure that every item in the list is a dictionary with one
    key (in this example, 'job' and 'project').
    """)


class ConfigItemMultipleKeysError(ConfigItemErrorException):
    message = textwrap.dedent("""\
    Configuration item has more than one key.  Each zuul.yaml
    configuration file must be a list of dictionaries with a
    single key, for example:

    - job:
        name: foo

    - project:
        name: bar

    Ensure that every item in the list is a dictionary with only
    one key (in this example, 'job' and 'project').  This error
    may be caused by insufficient indentation of the keys under
    the configuration item ('name' in this example).
    """)


class ConfigItemUnknownError(ConfigItemErrorException):
    message = textwrap.dedent("""\
    Configuration item not recognized.  Each zuul.yaml
    configuration file must be a list of dictionaries, for
    example:

    - job:
        name: foo

    - project:
        name: bar

    The dictionary keys must match one of the configuration item
    types recognized by zuul (for example, 'job' or 'project').
    """)


class UnparsedAbideConfig(object):

    """A collection of yaml lists that has not yet been parsed into objects.

    An Abide is a collection of tenants and access rules to those tenants.
    """

    def __init__(self):
        self.uuid = uuid4().hex
        self.ltime = -1
        self.tenants = {}
        self.authz_rules = []
        self.authz_roles = []
        self.semaphores = []
        self.api_roots = []

    def extend(self, conf):
        if isinstance(conf, UnparsedAbideConfig):
            self.tenants.update(conf.tenants)
            self.authz_rules.extend(conf.authz_rules)
            self.authz_roles.extend(conf.authz_roles)
            self.semaphores.extend(conf.semaphores)
            self.api_roots.extend(conf.api_roots)
            if len(self.api_roots) > 1:
                raise Exception("More than one api-root object")
            return

        if not isinstance(conf, list):
            raise ConfigItemNotListError(conf)

        for item in conf:
            if not isinstance(item, dict):
                raise ConfigItemNotDictError(item)
            if len(item.keys()) > 1:
                raise ConfigItemMultipleKeysError(item)
            key, value = list(item.items())[0]
            if key == 'tenant':
                if value["name"] in self.tenants:
                    raise Exception("Duplicate configuration for "
                                    f"tenant {value['name']}")
                self.tenants[value["name"]] = value
            # TODO: remove deprecated "admin-rule"
            elif key == 'admin-rule':
                self.authz_rules.append(value)
            elif key == 'authorization-rule':
                self.authz_rules.append(value)
            elif key == 'role':
                self.authz_roles.append(value)
            elif key == 'global-semaphore':
                self.semaphores.append(value)
            elif key == 'api-root':
                if self.api_roots:
                    raise Exception("More than one api-root object")
                self.api_roots.append(value)
            else:
                raise ConfigItemUnknownError(item)

    def toDict(self):
        d = {
            "uuid": self.uuid,
            "tenants": self.tenants,
            "semaphores": self.semaphores,
            "api_roots": self.api_roots,
        }
        d["authz_rules"] = self.authz_rules
        d["authz_roles"] = self.authz_roles
        return d

    @classmethod
    def fromDict(cls, data, ltime):
        unparsed_abide = cls()
        unparsed_abide.uuid = data["uuid"]
        unparsed_abide.ltime = ltime
        unparsed_abide.tenants = data["tenants"]
        unparsed_abide.authz_rules = data.get('authz_rules',
                                              data.get('admin_rules',
                                                       []))
        unparsed_abide.authz_roles = data.get('authz_roles', [])
        unparsed_abide.semaphores = data.get("semaphores", [])
        unparsed_abide.api_roots = data.get("api_roots", [])
        return unparsed_abide


class UnparsedConfig(object):
    """A collection of yaml lists that has not yet been parsed into objects."""

    def __init__(self):
        self.pragmas = []
        self.pipelines = []
        self.jobs = []
        self.project_templates = []
        self.projects = []
        self.nodesets = []
        self.secrets = []
        self.semaphores = []
        self.queues = []
        self.images = []
        self.flavors = []
        self.labels = []
        self.sections = []
        self.providers = []

        # The list of files/dirs which this represents.
        self.files_examined = set()
        self.dirs_examined = set()

    def copy(self):
        r = UnparsedConfig()
        for attr in ['pragmas', 'pipelines', 'jobs', 'project_templates',
                     'projects', 'nodesets', 'secrets', 'semaphores',
                     'queues', 'images', 'flavors', 'labels', 'sections',
                     'providers']:
            # Make a deep copy of each of our attributes
            old_objlist = getattr(self, attr)
            new_objlist = copy.deepcopy(old_objlist)
            setattr(r, attr, new_objlist)
        return r

    def extend(self, conf):
        # conf might be None in the case of a file with only comments.
        if conf is None:
            return

        if isinstance(conf, UnparsedConfig):
            self.pragmas.extend(conf.pragmas)
            self.pipelines.extend(conf.pipelines)
            self.jobs.extend(conf.jobs)
            self.project_templates.extend(conf.project_templates)
            self.projects.extend(conf.projects)
            self.nodesets.extend(conf.nodesets)
            self.secrets.extend(conf.secrets)
            self.semaphores.extend(conf.semaphores)
            self.queues.extend(conf.queues)
            self.images.extend(conf.images)
            self.flavors.extend(conf.flavors)
            self.labels.extend(conf.labels)
            self.sections.extend(conf.sections)
            self.providers.extend(conf.providers)
            return

        if not isinstance(conf, list):
            raise ConfigItemNotListError(conf)

        for item in conf:
            if not isinstance(item, dict):
                raise ConfigItemNotDictError(item)
            if len(item.keys()) > 1:
                raise ConfigItemMultipleKeysError(item)
            key, value = list(item.items())[0]
            if not isinstance(value, dict):
                raise ConfigItemNotDictError(item)
            if key == 'project':
                self.projects.append(value)
            elif key == 'job':
                self.jobs.append(value)
            elif key == 'project-template':
                self.project_templates.append(value)
            elif key == 'pipeline':
                self.pipelines.append(value)
            elif key == 'nodeset':
                self.nodesets.append(value)
            elif key == 'secret':
                self.secrets.append(value)
            elif key == 'semaphore':
                self.semaphores.append(value)
            elif key == 'queue':
                self.queues.append(value)
            elif key == 'pragma':
                self.pragmas.append(value)
            elif key == 'image':
                self.images.append(value)
            elif key == 'flavor':
                self.flavors.append(value)
            elif key == 'label':
                self.labels.append(value)
            elif key == 'section':
                self.sections.append(value)
            elif key == 'provider':
                self.providers.append(value)
            else:
                raise ConfigItemUnknownError(item)


class ParsedConfig:
    """A collection of parsed config objects."""

    def __init__(self):
        self.pragmas = []
        self.pragma_errors = []
        self.pipelines = []
        self.pipeline_errors = []
        self.jobs = []
        self.job_errors = []
        self.project_templates = []
        self.project_template_errors = []
        self.projects = []
        self.project_errors = []
        # projects_by_regex uses the same error list as project_errors
        self.projects_by_regex = {}
        self.nodesets = []
        self.nodeset_errors = []
        self.secrets = []
        self.secret_errors = []
        self.semaphores = []
        self.semaphore_errors = []
        self.queues = []
        self.queue_errors = []
        self.images = []
        self.image_errors = []
        self.flavors = []
        self.flavor_errors = []
        self.labels = []
        self.label_errors = []
        self.sections = []
        self.section_errors = []
        self.providers = []
        self.provider_errors = []

    def copy(self):
        r = self.__class__()
        r.pragmas = self.pragmas[:]
        r.pragma_errors = self.pragma_errors[:]
        r.pipelines = self.pipelines[:]
        r.pipeline_errors = self.pipeline_errors[:]
        r.jobs = self.jobs[:]
        r.job_errors = self.job_errors[:]
        r.project_templates = self.project_templates[:]
        r.project_template_errors = self.project_template_errors[:]
        r.projects = self.projects[:]
        r.project_errors = self.project_errors[:]
        r.projects_by_regex = copy.copy(self.projects_by_regex)
        r.nodesets = self.nodesets[:]
        r.nodeset_errors = self.nodeset_errors[:]
        r.secrets = self.secrets[:]
        r.secret_errors = self.secret_errors[:]
        r.semaphores = self.semaphores[:]
        r.semaphore_errors = self.semaphore_errors[:]
        r.queues = self.queues[:]
        r.queue_errors = self.queue_errors[:]
        r.images = self.images[:]
        r.image_errors = self.image_errors[:]
        r.flavors = self.flavors[:]
        r.flavor_errors = self.flavor_errors[:]
        r.labels = self.labels[:]
        r.label_errors = self.label_errors[:]
        r.sections = self.sections[:]
        r.section_errors = self.section_errors[:]
        r.providers = self.providers[:]
        r.provider_errors = self.provider_errors[:]
        return r

    def extend(self, conf):
        if isinstance(conf, ParsedConfig):
            self.pragmas.extend(conf.pragmas)
            self.pragma_errors.extend(conf.pragma_errors)
            self.pipelines.extend(conf.pipelines)
            self.pipeline_errors.extend(conf.pipeline_errors)
            self.jobs.extend(conf.jobs)
            self.job_errors.extend(conf.job_errors)
            self.project_templates.extend(conf.project_templates)
            self.project_template_errors.extend(conf.project_template_errors)
            self.projects.extend(conf.projects)
            self.project_errors.extend(conf.project_errors)
            self.nodesets.extend(conf.nodesets)
            self.nodeset_errors.extend(conf.nodeset_errors)
            self.secrets.extend(conf.secrets)
            self.secret_errors.extend(conf.secret_errors)
            self.semaphores.extend(conf.semaphores)
            self.semaphore_errors.extend(conf.semaphore_errors)
            self.queues.extend(conf.queues)
            self.queue_errors.extend(conf.queue_errors)
            self.images.extend(conf.images)
            self.image_errors.extend(conf.image_errors)
            self.flavors.extend(conf.flavors)
            self.flavor_errors.extend(conf.flavor_errors)
            self.labels.extend(conf.labels)
            self.label_errors.extend(conf.label_errors)
            self.sections.extend(conf.sections)
            self.section_errors.extend(conf.section_errors)
            self.providers.extend(conf.providers)
            self.provider_errors.extend(conf.provider_errors)
            for regex, projects in conf.projects_by_regex.items():
                self.projects_by_regex.setdefault(regex, []).extend(projects)
            return
        else:
            raise TypeError()


class Layout(object):
    """Holds all of the Pipelines."""

    log = logging.getLogger("zuul.layout")

    def __init__(self, tenant, uuid=None):
        # Allow setting the UUID in case we are re-creating the "same" layout
        # on another scheduler.
        # Queue items will reference their (dynamic) layout via the layout's
        # UUID. An item's layout UUID will change if any of it's input to the
        # layout creation changed. Those inputs are the tenant layout and list
        # of items ahead. This means that during a re-enqueue and in case of a
        # gate reset we will set the layout UUID of an item back to None.
        self.uuid = uuid or uuid4().hex
        self.tenant = tenant
        self.project_configs = {}
        self.project_templates = {}
        self.project_metadata = {}
        self.pipelines = OrderedDict()
        self.pipeline_managers = OrderedDict()
        # This is a dictionary of name -> [jobs].  The first element
        # of the list is the first job added with that name.  It is
        # the reference definition for a given job.  Subsequent
        # elements are aspects of that job with different matchers
        # that override some attribute of the job.  These aspects all
        # inherit from the reference definition.
        noop = Job('noop')
        noop.description = 'A job that will always succeed, no operation.'
        noop.parent = noop.BASE_JOB_MARKER
        noop.deduplicate = True
        noop.run = (PlaybookContext(None, 'noop.yaml', [], [], []),)
        noop.final = True
        self.jobs = {'noop': [noop]}
        self.nodesets = {}
        self.secrets = {}
        self.semaphores = {}
        self.queues = {}
        self.images = {}
        self.flavors = {}
        self.labels = {}
        self.sections = {}
        self.provider_configs = {}
        self.providers = {}
        self.loading_errors = LoadingErrors()

    def getJob(self, name):
        if name in self.jobs:
            return self.jobs[name][0]
        raise JobNotDefinedError("Job %s not defined" % (name,))

    def hasJob(self, name):
        return name in self.jobs

    def getJobs(self, name):
        return self.jobs.get(name, [])

    def _checkAddJob(self, job):
        if isinstance(job.nodeset, NodeSet):
            self._checkAddNodeset(job.nodeset)

        # Call resolveRequiredProjects for the side-effect of raising an
        # exception; ignore the return value.
        if job.required_projects:
            job._resolveRequiredProjects(self)

        if job.include_projects:
            job._resolveIncludeProjects(self)

        if job.exclude_projects:
            job._resolveExcludeProjects(self)

        if job.include_vars:
            job._resolveIncludeVars(self)

        if job.allowed_projects:
            job._resolveAllowedProjects(self)

        if job.roles:
            job._resolveRoles(self)

        if (job.pre_timeout and
            self.tenant.max_job_timeout != -1 and
            job.pre_timeout > self.tenant.max_job_timeout):
            raise MaxTimeoutError(job, self.tenant)

        # pre-timeout counts against timeout so can't be any larger than
        # timeout
        if (job.pre_timeout and job.timeout and
            job.pre_timeout > job.timeout):
            raise PreTimeoutExceedsTimeoutError(job)

        if (job.timeout and
            self.tenant.max_job_timeout != -1 and
            job.timeout > self.tenant.max_job_timeout):
            raise MaxTimeoutError(job, self.tenant)

        if (job.post_timeout and
            self.tenant.max_job_timeout != -1 and
            job.post_timeout > self.tenant.max_job_timeout):
            raise MaxTimeoutError(job, self.tenant)

        if job.isBase() and not self.tenant.isTrusted(
                job.source_context.project_canonical_name):
            raise Exception(
                "Base jobs must be defined in config projects")

    def addJob(self, job):
        self._checkAddJob(job)
        # We can have multiple variants of a job all with the same
        # name, but these variants must all be defined in the same repo.
        # Unless the repo is permitted to shadow another.  If so, and
        # the job we are adding is from a repo that is permitted to
        # shadow the one with the older jobs, skip adding this job.
        job_project = job.source_context.project_canonical_name
        job_tpc = self.tenant.project_configs[job_project]
        skip_add = False
        prior_jobs = self.jobs.get(job.name, [])
        if prior_jobs:
            # All jobs we've added so far should be from the same
            # project, so pick the first one.
            prior_job = prior_jobs[0]
            if (prior_job.source_context.project_canonical_name !=
                job.source_context.project_canonical_name):
                prior_project = prior_job.source_context.project_canonical_name
                if prior_project in job_tpc.shadow_projects:
                    skip_add = True
                else:
                    raise Exception("Job %s in %s is not permitted to shadow "
                                    "job %s in %s" % (
                                        job,
                                        job.source_context.project_name,
                                        prior_job,
                                        prior_job.source_context.project_name))

        if skip_add:
            return False
        if prior_jobs:
            self.jobs[job.name].append(job)
        else:
            self.jobs[job.name] = [job]
        return True

    def _addIdenticalObject(self, obj_type, obj_dict, new_obj):
        """Helper method to add an object if it's new, ignore it if we
        already have a duplicate on a different branch in the same
        project, or raise an error if it duplicates an object in a
        different project.

        :param str obj_type: A capitalized string describing the
            object (eg "Nodeset").
        :param dict obj_dict: The layout dict for this object
            type (eg self.nodesets).
        :param object new_obj: The new object (eg a Nodeset instance).
        """

        other = obj_dict.get(new_obj.name)
        if other is not None:
            if not new_obj.source_context.isSameProject(other.source_context):
                raise Exception(
                    "%s %s already defined in project %s" %
                    (obj_type, new_obj.name,
                     other.source_context.project_name))
            if new_obj.source_context.branch == other.source_context.branch:
                raise Exception("%s %s already defined" % (
                    obj_type, new_obj.name))
            if new_obj != other:
                raise Exception("%s %s does not match existing definition"
                                " in branch %s" %
                                (obj_type, new_obj.name,
                                 other.source_context.branch))
            # Identical data in a different branch of the same project;
            # ignore the duplicate definition
            return
        obj_dict[new_obj.name] = new_obj

    def _checkAddNodeset(self, nodeset):
        # Verify that we can add a nodeset; used by top-level nodeset
        # objects as well as nested nodesets.
        # Not applicable to NIZ since we which labels are
        # allowed/disallowed is implicit due to their presence in
        # configuration.
        if self.tenant.use_nodepool:
            allowed_labels = self.tenant.allowed_labels
            disallowed_labels = self.tenant.disallowed_labels

            requested_labels = nodeset.flattenAlternativeLabels()
            filtered_labels = set(filter_allowed_disallowed(
                requested_labels, allowed_labels, disallowed_labels))
            rejected_labels = requested_labels - filtered_labels
            for name in rejected_labels:
                raise LabelForbiddenError(
                    label=name,
                    allowed_labels=allowed_labels,
                    disallowed_labels=disallowed_labels)

    def addNodeSet(self, nodeset):
        self._checkAddNodeset(nodeset)
        self._addIdenticalObject('Nodeset', self.nodesets, nodeset)

    def _checkAddSecret(self, secret):
        if secret.secret_oidc:
            ttl = secret.secret_oidc.get('ttl', self.tenant.default_oidc_ttl)
            if int(ttl) > self.tenant.max_oidc_ttl:
                raise MaxOIDCTTLError(secret, self.tenant)

            iss = secret.secret_oidc.get('iss')
            if iss and iss not in self.tenant.allowed_oidc_issuers:
                raise OIDCIssuerNotAllowedError(secret, iss)

    def addSecret(self, secret):
        self._checkAddSecret(secret)
        self._addIdenticalObject('Secret', self.secrets, secret)

    def addSemaphore(self, semaphore):
        if semaphore.name in self.tenant.global_semaphores:
            raise Exception("Semaphore %s shadows a global semaphore and "
                            "will be ignored" % (semaphore.name))
        self._addIdenticalObject('Semaphore', self.semaphores, semaphore)

    def getSemaphore(self, abide, semaphore_name):
        if semaphore_name in self.tenant.global_semaphores:
            return abide.semaphores[semaphore_name]
        semaphore = self.semaphores.get(semaphore_name)
        if semaphore:
            return semaphore
        # Return an implied semaphore with max=1
        # TODO: consider deprecating implied semaphores to avoid typo
        # config errors
        return Semaphore(semaphore_name)

    def addQueue(self, queue):
        self._addIdenticalObject('Queue', self.queues, queue)

    def addImage(self, image):
        self._addIdenticalObject('Image', self.images, image)

    def addFlavor(self, flavor):
        self._addIdenticalObject('Flavor', self.flavors, flavor)

    def addLabel(self, label):
        self._addIdenticalObject('Label', self.labels, label)

    def addSection(self, section):
        self._addIdenticalObject('Section', self.sections, section)

    def addProviderConfig(self, provider_config):
        self._addIdenticalObject('Provider', self.provider_configs,
                                 provider_config)

    def addProvider(self, provider):
        if provider.name in self.providers:
            raise Exception(
                "Provider %s is already defined" % provider.name)
        self.providers[provider.name] = provider

    def addPipelineManager(self, manager):
        if manager.pipeline.name in self.pipeline_managers:
            raise Exception(
                "Pipeline %s is already defined" % manager.pipeline.name)

        if self.tenant.allowed_reporters is not None:
            for action in manager.pipeline.actions:
                if (action.connection.connection_name not in
                    self.tenant.allowed_reporters):
                    raise UnknownConnection(action.connection.connection_name)

        if self.tenant.allowed_triggers is not None:
            for trigger in manager.pipeline.triggers:
                if (trigger.connection.connection_name not in
                    self.tenant.allowed_triggers):
                    raise UnknownConnection(trigger.connection.connection_name)

        self.pipeline_managers[manager.pipeline.name] = manager

    def _checkAddPPC(self, pipeline_name, ppc):
        manager = self.pipeline_managers.get(pipeline_name)
        # TODO: Consaider raising errors on incorrect pipeline names
        if manager:
            pipeline = manager.pipeline
            if ppc.parameters and not pipeline.allow_parameters:
                raise Exception("Pipeline does not allow parameters")

    def addProjectTemplate(self, project_template):
        for job in project_template.embeddedJobs():
            self._checkAddJob(job)

        for pipeline_name, ppc in project_template.pipelines.items():
            self._checkAddPPC(pipeline_name, ppc)

        template_list = self.project_templates.get(project_template.name)
        if template_list is not None:
            reference = template_list[0]
            if (reference.source_context.project_canonical_name !=
                project_template.source_context.project_canonical_name):
                raise Exception("Project template %s is already defined" %
                                (project_template.name,))
        else:
            template_list = self.project_templates.setdefault(
                project_template.name, [])
        template_list.append(project_template)

    def getProjectTemplates(self, name):
        pt = self.project_templates.get(name, None)
        if pt is None:
            raise TemplateNotFoundError("Project template %s not found" % name)
        return pt

    def addProjectConfig(self, project_canonical_name, project_config):
        source_tpc = self.tenant.getTPC(
            project_config.source_context.project_canonical_name)
        this_tpc = self.tenant.getTPC(project_canonical_name)
        if not source_tpc.canConfigureProject(this_tpc):
            raise ProjectNotPermittedError()

        for job in project_config.embeddedJobs():
            self._checkAddJob(job)

        for pipeline_name, ppc in project_config.pipelines.items():
            self._checkAddPPC(pipeline_name, ppc)

        if project_canonical_name in self.project_configs:
            self.project_configs[project_canonical_name].append(project_config)
        else:
            self.project_configs[project_canonical_name] = [project_config]
            self.project_metadata[project_canonical_name] = ProjectMetadata()
        md = self.project_metadata[project_canonical_name]
        if md.merge_mode is None and project_config.merge_mode is not None:
            md.merge_mode = project_config.merge_mode
        if (md.default_branch is None and
            project_config.default_branch is not None):
            md.default_branch = project_config.default_branch
        if (
            md.queue_name is None
            and project_config.queue_name is not None
        ):
            md.queue_name = project_config.queue_name

    def getProjectConfigs(self, name):
        return self.project_configs.get(name, [])

    def getAllProjectConfigs(self, name):
        # Get all the project configs (project and project-template
        # stanzas) for a project.
        try:
            ret = []
            for pc in self.getProjectConfigs(name):
                ret.append(pc)
                for template_name in pc.templates:
                    templates = self.getProjectTemplates(template_name)
                    ret.extend(templates)
            return ret
        except TemplateNotFoundError as e:
            self.log.warning("%s for project %s" % (e, name))
            return []

    def getProjectMetadata(self, name):
        if name in self.project_metadata:
            return self.project_metadata[name]
        return None

    def getProjectPipelineConfig(self, item, change):
        log = item.annotateLogger(self.log)
        # Create a project-pipeline config for the given item, taking
        # its branch (if any) into consideration.  If the project does
        # not participate in the pipeline at all (in this branch),
        # return None.

        # A pc for a project can appear only in a config-project
        # (unbranched, always applies), or in the project itself (it
        # should have an implied branch matcher and it must match the
        # item).
        ppc = ProjectPipelineConfig()
        project_in_pipeline = False
        for pc in self.getProjectConfigs(change.project.canonical_name):
            if not pc.changeMatches(
                    self.tenant, change.project.canonical_name, change):
                msg = "Project %s did not match" % (pc,)
                ppc.addDebug(msg)
                log.debug("%s item %s", msg, item)
                continue
            msg = "Project %s matched" % (pc,)
            ppc.addDebug(msg)
            log.debug("%s item %s", msg, item)
            for template_name in pc.templates:
                templates = self.getProjectTemplates(template_name)
                for template in templates:
                    template_ppc = template.pipelines.get(
                        item.manager.pipeline.name)
                    if template_ppc:
                        if not template.changeMatches(
                                self.tenant, change.project.canonical_name,
                                change):
                            msg = "Project template %s did not match" % (
                                template,)
                            ppc.addDebug(msg)
                            log.debug("%s item %s", msg, item)
                            continue
                        msg = "Project template %s matched" % (
                            template,)
                        ppc.addDebug(msg)
                        log.debug("%s item %s", msg, item)
                        project_in_pipeline = True
                        ppc.update(template_ppc)
                        ppc.updateVariables(template.variables)

            # Now merge in project variables (they will override
            # template variables; later job variables may override
            # these again)
            ppc.updateVariables(pc.variables)

            project_ppc = pc.pipelines.get(item.manager.pipeline.name)
            if project_ppc:
                project_in_pipeline = True
                ppc.update(project_ppc)
        if project_in_pipeline:
            return ppc
        return None

    def _updateOverrideCheckouts(self, override_checkouts, job):
        # Update the values in an override_checkouts dict with those
        # in a job.  Used in collectJobVariants.
        if job.override_checkout:
            override_checkouts[None] = job.override_checkout
        # TODO: we will end up resolving these projects again if they
        # match; consider caching the results.
        for req in job._resolveRequiredProjects(self).values():
            if req.override_checkout:
                override_checkouts[req.project_name] = req.override_checkout

    def _collectJobVariants(self, item, jobname, change, path, jobs, stack,
                            override_checkouts, indent, debug_messages):
        log = item.annotateLogger(self.log)
        matched = False
        local_override_checkouts = override_checkouts.copy()
        override_branch = None
        project = None
        for variant in self.getJobs(jobname):
            if project is None and variant.source_context:
                project = variant.source_context.project_canonical_name
                if override_checkouts.get(None) is not None:
                    override_branch = override_checkouts.get(None)
                override_branch = override_checkouts.get(
                    project, override_branch)
                branches = self.tenant.getProjectBranches(project)
                if override_branch not in branches:
                    override_branch = None
            if not variant.changeMatchesBranch(
                    self.tenant,
                    change,
                    override_branch=override_branch):
                log.debug("Variant %s did not match %s", repr(variant), change)
                add_debug_line(debug_messages,
                               "Variant {variant} did not match".format(
                                   variant=repr(variant)), indent=indent)
                continue
            else:
                log.debug("Variant %s matched %s", repr(variant), change)
                add_debug_line(debug_messages,
                               "Variant {variant} matched".format(
                                   variant=repr(variant)), indent=indent)
            if not variant.isBase():
                parent = variant.parent
                if not jobs and parent is None:
                    parent = self.tenant.default_base_job
            else:
                parent = None
            self._updateOverrideCheckouts(local_override_checkouts, variant)
            if parent and parent not in path:
                if parent in stack:
                    raise Exception("Dependency cycle in jobs: %s" % stack)
                self.collectJobs(item, parent, change, path, jobs,
                                 stack + [jobname], local_override_checkouts,
                                 debug_messages=debug_messages)

            matched = True
            if variant not in jobs:
                jobs.append(variant)
        return matched

    def collectJobs(self, item, jobname, change, path=None, jobs=None,
                    stack=None, override_checkouts=None, debug_messages=None):
        log = item.annotateLogger(self.log)
        # Stack is the recursion stack of job parent names.  Each time
        # we go up a level, we add to stack, and it's popped as we
        # descend.
        if stack is None:
            stack = []
        # Jobs is the list of jobs we've accumulated.
        if jobs is None:
            jobs = []
        # Path is the list of job names we've examined.  It
        # accumulates and never reduces.  If more than one job has the
        # same parent, this will prevent us from adding it a second
        # time.
        if path is None:
            path = []
        # Override_checkouts is a dictionary of canonical project
        # names -> branch names.  It is not mutated, but instead new
        # copies are made and updated as we ascend the hierarchy, so
        # higher levels don't affect lower levels after we descend.
        # It's used to override the branch matchers for jobs.
        if override_checkouts is None:
            override_checkouts = {}
        path.append(jobname)
        matched = False
        indent = len(path) + 1
        msg = "Collecting job variants for {jobname}".format(jobname=jobname)
        log.debug(msg)
        add_debug_line(debug_messages, msg, indent=indent)
        matched = self._collectJobVariants(
            item, jobname, change, path, jobs, stack, override_checkouts,
            indent, debug_messages)
        if not matched:
            log.debug("No matching parents for job %s and change %s",
                      jobname, change)
            add_debug_line(debug_messages,
                           "No matching parents for {jobname}".format(
                               jobname=repr(jobname)), indent=indent)
            raise NoMatchingParentError()
        return jobs

    def collectParameters(self, item, ppc):
        # Filter supplied parameters against the parameter
        # specifications in the ppc
        parameters = {}
        if not item.parameters:
            return parameters
        for (k, v) in item.parameters.items():
            param_def = ppc.parameters.get(k)
            if param_def:
                if param_def.type == 'multiple-selection':
                    if isinstance(v, list) and set(v) <= set(param_def.values):
                        parameters[k] = v
                elif param_def.type == 'selection':
                    if v in param_def.values:
                        parameters[k] = v
                else:
                    parameters[k] = v
        return parameters

    def extendJobGraph(self, context, item, change, ppc, job_graph,
                       skip_file_matcher, redact_secrets_and_keys,
                       debug_messages, pending_errors):
        log = item.annotateLogger(self.log)
        semaphore_handler = item.manager.tenant.semaphore_handler
        job_list = ppc.job_list
        pipeline = item.manager.pipeline
        add_debug_line(debug_messages, "Freezing job graph")
        for jobname in job_list.jobs:
            # This is the final job we are constructing
            final_job = None
            log.debug("Collecting jobs %s for %s", jobname, change)
            add_debug_line(debug_messages,
                           "Freezing job {jobname}".format(
                               jobname=jobname), indent=1)
            # Create the initial list of override_checkouts, which are
            # used as we walk up the hierarchy to expand the set of
            # jobs which match.
            override_checkouts = {}
            for variant in job_list.jobs[jobname]:
                if variant.changeMatchesBranch(self.tenant, change):
                    self._updateOverrideCheckouts(override_checkouts, variant)
            try:
                variants = self.collectJobs(
                    item, jobname, change,
                    override_checkouts=override_checkouts,
                    debug_messages=debug_messages)
            except NoMatchingParentError:
                variants = None
            log.debug("Collected jobs %s for %s", jobname, change)
            if not variants:
                # A change must match at least one defined job variant
                # (that is to say that it must match more than just
                # the job that is defined in the tree).
                add_debug_line(debug_messages,
                               "No matching variants for {jobname}".format(
                                   jobname=jobname), indent=2)
                continue
            for variant in variants:
                if final_job is None:
                    final_job = variant.copy()
                    final_job.setBase(self, semaphore_handler)
                else:
                    final_job.applyVariant(variant, self, semaphore_handler)
                    final_job.name = variant.name
            final_job.name = jobname

            # Don't accept any parameters that aren't specified in our PPC
            parameters = self.collectParameters(item, ppc)
            # Pipeline parameters behave like project variables, but
            # have higher precedence
            final_job.updateProjectVariables(parameters)
            # Now merge variables set from this parent ppc
            # (i.e. project+templates) directly into the job vars
            final_job.updateProjectVariables(ppc.variables)

            # If the job does not specify an ansible version default to the
            # tenant default.
            if not final_job.ansible_version:
                final_job.ansible_version = \
                    self.tenant.default_ansible_version

            log.debug("Froze job %s for %s", jobname, change)
            # Whether the change matches any of the project pipeline
            # variants
            matched = False
            reporter_job_ok = False
            for variant in job_list.jobs[jobname]:
                if variant.changeMatchesBranch(self.tenant, change):
                    final_job.applyVariant(variant, self, semaphore_handler)
                    if self.tenant.isTrusted(
                            variant.source_context.project_canonical_name):
                        # A config project has attached this job to a
                        # project-pipeline.  In this case, we can
                        # ignore allowed-projects -- the superuser has
                        # stated they want it to run.  This can be
                        # useful to allow untrusted jobs with secrets
                        # to be run in other untrusted projects.
                        final_job.ignore_allowed_projects = True
                    if not reporter_job_ok and final_job.type == 'reporter':
                        source_tpc = self.tenant.getTPC(
                            variant.source_context.project_canonical_name)
                        if not source_tpc.allow_reporter_jobs:
                            raise ReporterJobNotPermittedError(
                                source_tpc.project)
                        reporter_job_ok = True
                    matched = True
                    log.debug("Pipeline variant %s matched %s",
                              repr(variant), change)
                    add_debug_line(debug_messages,
                                   "Pipeline variant {variant} matched".format(
                                       variant=repr(variant)), indent=2)
                    if final_job.image_build_name:
                        final_job.assertImagePermissions(
                            final_job.image_build_name, variant, self)
                else:
                    log.debug("Pipeline variant %s did not match %s",
                              repr(variant), change)
                    add_debug_line(debug_messages,
                                   "Pipeline variant {variant} did not match".
                                   format(variant=repr(variant)), indent=2)
            if not matched:
                # A change must match at least one project pipeline
                # job variant.
                add_debug_line(debug_messages,
                               "No matching pipeline variants for {jobname}".
                               format(jobname=jobname), indent=2)
                continue
            if not final_job.itemMatchesImage(item):
                log.debug("Job %s did not match images", jobname)
                add_debug_line(debug_messages,
                               f"Job {jobname} did not match images",
                               indent=2)
                continue
            updates_job_config = False
            if not skip_file_matcher and \
               not final_job.changeMatchesFiles(change):
                matched_files = False
                if final_job.match_on_config_updates:
                    updates_job_config = item.updatesJobConfig(
                        final_job, change, self)
            else:
                matched_files = True
            matches_change = True
            if not matched_files:
                if updates_job_config:
                    # Log the reason we're ignoring the file matcher
                    log.debug("The configuration of job %s is "
                              "changed by %s; ignoring file matcher",
                              repr(final_job), change)
                    add_debug_line(debug_messages,
                                   "The configuration of job {jobname} is "
                                   "changed; ignoring file matcher".
                                   format(jobname=jobname), indent=2)
                else:
                    log.debug("Job %s did not match files in %s",
                              repr(final_job), change)
                    add_debug_line(debug_messages,
                                   "Job {jobname} did not match files".
                                   format(jobname=jobname), indent=2)
                    # A decision not to run based on a file matcher
                    # can be overridden later, so we just note our
                    # initial decision here.
                    matches_change = False
            frozen_job = final_job.freezeJob(
                context, self.tenant, self, item, change,
                redact_secrets_and_keys)
            frozen_job._set(matches_change=matches_change)
            job_graph.addJob(frozen_job)

            # These are only errors if we actually decide to run the job
            if final_job.abstract:
                pending_errors[frozen_job.uuid] = JobConfigurationError(
                    "Job %s is abstract and may not be directly run" %
                    (final_job.name,))
            elif (not final_job.ignore_allowed_projects and
                  final_job.allowed_projects is not None and
                  change.project.name not in final_job.allowed_projects):
                pending_errors[frozen_job.uuid] = JobConfigurationError(
                    "Project %s is not allowed to run job %s" %
                    (change.project.name, final_job.name))
            elif ((not pipeline.post_review) and final_job.post_review):
                pending_errors[frozen_job.uuid] = JobConfigurationError(
                    "Pre-review pipeline %s does not allow "
                    "post-review job %s" % (
                        pipeline.name, final_job.name))
            elif not final_job.run:
                pending_errors[frozen_job.uuid] = JobConfigurationError(
                    "Job %s does not specify a run playbook" % (
                        final_job.name,))

    def createJobGraph(self, context, item,
                       skip_file_matcher,
                       redact_secrets_and_keys,
                       old=False):
        """Find or create actual matching jobs for this item's change and
        store the resulting job tree."""

        enable_debug = item.debug
        fail_fast = item.current_build_set.fail_fast
        debug_messages = []
        if old:
            job_map = item.current_build_set._old_jobs
            if context is not None:
                raise RuntimeError("Context should be none for old job graphs")
            context = zkobject.LocalZKContext(self.log)
        else:
            job_map = item.current_build_set.jobs
        job_graph = JobGraph(job_map)
        pending_errors = {}
        for change in item.changes:
            ppc = self.getProjectPipelineConfig(item, change)
            if not ppc:
                continue
            if ppc.debug:
                # Any ppc that sets debug=True enables debugging
                enable_debug = True
                debug_messages.extend(ppc.debug_messages)
            self.extendJobGraph(
                context, item, change, ppc, job_graph, skip_file_matcher,
                redact_secrets_and_keys, debug_messages, pending_errors)
            if ppc.fail_fast is not None:
                # Any explicit setting of fail_fast takes effect,
                # last one wins.
                fail_fast = ppc.fail_fast

        log = item.annotateLogger(self.log)
        job_graph.addInitializerJobs(log)
        job_graph.deduplicateJobs(log, item)
        job_graph.removeNonMatchingJobs(log)
        job_graph.freezeDependencies(log, self)
        for job in job_map.values():
            if job.uuid in pending_errors:
                raise pending_errors[job.uuid]

        # Copy project metadata to job_graph since this must be independent
        # of the layout as we need it in order to prepare the context for
        # job execution.
        # The layout might be no longer available at this point, as the
        # scheduler submitting the job can be different from the one that
        # created the layout.
        job_graph.project_metadata = self.project_metadata

        if not enable_debug:
            debug_messages = item.current_build_set.debug_messages

        return dict(
            debug_messages=debug_messages,
            fail_fast=fail_fast,
            job_graph=job_graph,
        )


class Semaphore(ConfigObject):
    def __init__(self, name, max=1, global_scope=False):
        super(Semaphore, self).__init__()
        self.name = name
        self.global_scope = global_scope
        self.max = int(max)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Semaphore):
            return False
        return (self.name == other.name and
                self.max == other.max)


class Queue(ConfigObject):
    class Type(StrEnum):
        ALL_BRANCHES = 'all-branches'
        PER_BRANCH = 'per-branch'
        BRANCH_ASSIGNED = 'branch-assigned'

    def __init__(self, name,
                 allow_circular_dependencies=False,
                 dependencies_by_topic=False,
                 type=None):
        super().__init__()
        self.name = name
        self.allow_circular_dependencies = allow_circular_dependencies
        self.dependencies_by_topic = dependencies_by_topic
        self.type = type

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if not isinstance(other, Queue):
            return False
        return (
            self.name == other.name and
            self.allow_circular_dependencies ==
            other.allow_circular_dependencies and
            self.dependencies_by_topic ==
            other.dependencies_by_topic and
            self.type == other.type
        )


class Tenant(object):
    def __init__(self, name):
        self.name = name
        self.max_nodes_per_job = 5
        self.max_job_timeout = 10800
        self.max_oidc_ttl = 10800
        self.default_oidc_ttl = 3600
        self.allowed_oidc_issuers = []
        self.max_changes_per_pipeline = None
        self.max_dependencies = None
        self.exclude_unprotected_branches = False
        self.exclude_locked_branches = False
        self.default_base_job = None
        self.layout = None
        self.allowed_triggers = None
        self.allowed_reporters = None
        self.use_nodepool = True
        self.private_ssh_key = None
        self.public_ssh_key = None
        # The unparsed configuration from the main zuul config for
        # this tenant.
        self.unparsed_config = None
        # The parsed config from the config-projects
        self.config_projects_config = None
        # The parsed config from untrusted-projects
        self.untrusted_projects_config = None
        self.semaphore_handler = None
        # Metadata about projects for this tenant
        # canonical project name -> TenantProjectConfig
        self.project_configs = {}

        # A mapping of project names to projects.  project_name ->
        # VALUE where VALUE is a further dictionary of
        # canonical_hostname -> Project.
        self.projects = {}
        self.canonical_hostnames = set()

        # The per tenant default ansible version
        self.default_ansible_version = None

        self.access_rules = []
        self.admin_rules = []
        self.default_auth_realm = None
        self.global_semaphores = set()

        # Apply a cache to this method since it is called frequently;
        # it is applied here so we don't leak Tenant objects due to
        # cache self-reference.
        self.isTrusted = lru_cache(maxsize=None)(self.isTrusted)

    def __repr__(self):
        return f"<Tenant {self.name}>"

    @property
    def all_projects(self):
        """
        Return a generator for all projects of the tenant.
        """
        for hostname_dict in self.projects.values():
            for project in hostname_dict.values():
                yield project

    @property
    def all_tpcs(self):
        """
        Return a generator for all tenant TPCS.
        """
        for tpc in self.project_configs.values():
            yield tpc

    @property
    def config_projects(self):
        """
        Return a generator for all config projects.
        """
        for tpc in self.project_configs.values():
            if tpc.trusted:
                yield tpc.project

    @property
    def untrusted_projects(self):
        """
        Return a generator for all tenant TPCS.
        """
        for tpc in self.project_configs.values():
            if not tpc.trusted:
                yield tpc.project

    def _addProject(self, tpc):
        """Add a project to the project index

        :arg TenantProjectConfig tpc: The TenantProjectConfig (with
        associated project) to add.

        """
        project = tpc.project
        self.canonical_hostnames.add(project.canonical_hostname)
        hostname_dict = self.projects.setdefault(project.name, {})
        if project.canonical_hostname in hostname_dict:
            raise Exception("Project %s is already in project index" %
                            (project,))
        hostname_dict[project.canonical_hostname] = project
        self.project_configs[project.canonical_name] = tpc

    def isTrusted(self, name):
        # This method has a cache applied in the constructor
        trusted, project = self.getProject(name)
        return trusted

    def getProject(self, name):
        """Return a project given its name.

        :arg str name: The name of the project.  It may be fully
            qualified (E.g., "git.example.com/subpath/project") or may
            contain only the project name name may be supplied (E.g.,
            "subpath/project").

        :returns: A tuple (trusted, project) or (None, None) if the
            project is not found or ambiguous.  The "trusted" boolean
            indicates whether or not the project is trusted by this
            tenant.
        :rtype: (bool, Project)

        """
        path = name.split('/', 1)
        if path[0] in self.canonical_hostnames:
            hostname = path[0]
            project_name = path[1]
        else:
            hostname = None
            project_name = name
        hostname_dict = self.projects.get(project_name)
        project = None
        if hostname_dict:
            if hostname:
                project = hostname_dict.get(hostname)
            else:
                values = list(hostname_dict.values())
                if len(values) == 1:
                    project = values[0]
                else:
                    raise Exception("Project name '%s' is ambiguous, "
                                    "please fully qualify the project "
                                    "with a hostname" % (name,))
        if project is None:
            return (None, None)
        tpc = self.project_configs.get(project.canonical_name)
        if tpc is None:
            # This should never happen:
            raise Exception("Project %s is neither trusted nor untrusted" %
                            (project,))
        return (tpc.trusted, project)

    def getTPCsByRegex(self, regex):
        """Return all TPCs with a full match to either project name or
        canonical project name.

        :arg str regex: The regex to match
        :returns: A list of TenantProjectConfigs describing the found
            projects.
        """

        matcher = re2.compile(regex)
        projects = []
        result = []

        for name, hostname_dict in self.projects.items():
            if matcher.fullmatch(name):
                projects.extend(hostname_dict.values())
            else:
                # It is possible for the regex to match specific connection
                # prefixes. Check these more specific names if we didn't add
                # all of the possible canonical names already.
                for project in hostname_dict.values():
                    if matcher.fullmatch(project.canonical_name):
                        projects.append(project)

        for project in projects:
            tpc = self.project_configs.get(project.canonical_name)
            if tpc is None:
                # This should never happen:
                raise Exception("Project %s is neither trusted nor untrusted" %
                                (project,))
            result.append(tpc)
        return result

    def getProjectsByRegex(self, regex):
        """Return all projects with a full match to either project name or
        canonical project name.

        :arg str regex: The regex to match
        :returns: A list of tuples (trusted, project) describing the found
            projects.
        """
        return [(tpc.trusted, tpc.project)
                for tpc in self.getTPCsByRegex(regex)]

    def getProjectBranches(self, project_canonical_name,
                           include_always_dynamic=False):
        """Return a project's branches (filtered by this tenant config)

        :arg str project_canonical: The project's canonical name.
        :arg bool include_always_dynamic: Whether to include
             always-dynamic-branches

        :returns: A list of branch names.
        :rtype: [str]

        """
        tpc = self.project_configs[project_canonical_name]
        if include_always_dynamic:
            return tpc.branches + tpc.dynamic_branches
        return tpc.branches

    def getExcludeUnprotectedBranches(self, project):
        # Evaluate if unprotected branches should be excluded or not. The first
        # match wins. The order is project -> tenant (default is false).
        project_config = self.project_configs.get(project.canonical_name)
        if project_config.exclude_unprotected_branches is not None:
            return project_config.exclude_unprotected_branches
        return self.exclude_unprotected_branches

    def getExcludeLockedBranches(self, project):
        # Evaluate if locked branches should be excluded or not. The first
        # match wins. The order is project -> tenant (default is false).
        project_config = self.project_configs.get(project.canonical_name)
        if project_config.exclude_locked_branches is not None:
            return project_config.exclude_locked_branches
        return self.exclude_locked_branches

    def addTPC(self, tpc):
        self._addProject(tpc)

    def getTPC(self, name):
        (_, project) = self.getProject(name)
        if project is None:
            return None
        return self.project_configs[project.canonical_name]

    def getSafeAttributes(self):
        return Attributes(name=self.name)


# data can be None here to mean "this path has been checked and as of
# ltime there was nothing in the repo at that path".

class ConfigObjectCacheEntry:
    """A cache entry holding config objects for a given project-branch-path"""
    def __init__(self, ltime, parsed_config=None):
        self.ltime = ltime
        self.parsed_config = parsed_config


class ConfigObjectCache:
    """A cache of config objects stored by project-branch-path"""
    def __init__(self):
        # This is a dict of path -> ConfigObjectCacheEntry items.
        # If a path exists here, it means it has been checked as of
        # ConfigObjectCacheEntry.ltime if anything was found, then
        # ConfigObjectCacheEntry.parsed_config will have the contents.  If it
        # was checked and no data was found, then
        # ConfigObjectCacheEntry.parsed_config will be None.
        # If it has not been checked, then there will be no entry.
        self.entries = {}

    def _getPaths(self, tpc):
        # Return a list of paths we have entries for that match the TPC.
        files_list = self.entries.keys()
        fns1 = []
        fns2 = []
        fns3 = []
        fns4 = []
        for fn in files_list:
            if fn.startswith("zuul.d/"):
                fns1.append(fn)
            if fn.startswith(".zuul.d/"):
                fns2.append(fn)
            for ef in tpc.extra_config_files:
                if fn.startswith(ef):
                    fns3.append(fn)
            for ed in tpc.extra_config_dirs:
                if fn.startswith(ed):
                    fns4.append(fn)
        fns = (["zuul.yaml"] + sorted(fns1) + [".zuul.yaml"] +
               sorted(fns2) + fns3 + sorted(fns4))
        return fns

    def getValidFor(self, tpc, conf_root, min_ltime):
        """Return the oldest ltime if this has valid cache results for the
        extra files/dirs in the tpc.  Otherwise, return None.

        """
        oldest_ltime = None
        for path in conf_root + tpc.extra_config_files + tpc.extra_config_dirs:
            entry = self.entries.get(path)
            if entry is None:
                return None
            if entry.ltime < min_ltime:
                return None
            if oldest_ltime is None or entry.ltime < oldest_ltime:
                oldest_ltime = entry.ltime
        return oldest_ltime

    def setValidFor(self, tpc, conf_root, ltime):
        """Indicate that the cache has just been made current for the given
        TPC as of ltime"""

        seen = set()
        # Identify any entries we have that match the TPC, and remove
        # them if they are not up to date.
        for path in self._getPaths(tpc):
            entry = self.entries.get(path)
            if entry is None:
                # Probably "zuul.yaml" or similar hardcoded path that
                # is unused.
                continue
            else:
                # If this is a real entry (i.e., actually has data),
                # then mark it as seen so we don't create a dummy
                # entry for it later, and also check to see if it can
                # be pruned.
                if entry.parsed_config is not None:
                    seen.add(path)
                    if entry.ltime < ltime:
                        # This is an old entry which is not in the present
                        # update but should have been if it existed in the
                        # repo.  That means it was deleted and we can
                        # remove it.
                        del self.entries[path]
        # Set the ltime for any paths that did not appear in our list
        # (so that we know they have been checked and the cache is
        # valid for that path+ltime).
        for path in conf_root + tpc.extra_config_files + tpc.extra_config_dirs:
            if path in seen:
                continue
            self.entries[path] = ConfigObjectCacheEntry(ltime)

    def put(self, path, parsed_config, ltime):
        entry = self.entries.get(path)
        if entry is not None:
            if ltime < entry.ltime:
                # We don't want the entry to go backward
                return
        self.entries[path] = ConfigObjectCacheEntry(ltime, parsed_config)

    def get(self, tpc, conf_root):
        ret = ParsedConfig()
        loaded = False
        for fn in self._getPaths(tpc):
            entry = self.entries.get(fn)
            if entry is not None and entry.parsed_config is not None:
                # Don't load from more than one configuration in a
                # project-branch (unless an "extra" file/dir).
                fn_root = fn.split('/')[0]
                if (fn_root in conf_root):
                    if (loaded and loaded != fn_root):
                        # "Multiple configuration files in source_context"
                        continue
                    loaded = fn_root
                ret.extend(entry.parsed_config)
        return ret


class TenantTPCRegistry:
    def __init__(self):
        # The project TPCs are stored as a list as we don't check for
        # duplicate projects here.
        self.config_tpcs = defaultdict(list)
        self.untrusted_tpcs = defaultdict(list)

    def addConfigTPC(self, tpc):
        self.config_tpcs[tpc.project.name].append(tpc)

    def addUntrustedTPC(self, tpc):
        self.untrusted_tpcs[tpc.project.name].append(tpc)

    def getConfigTPCs(self):
        return list(itertools.chain.from_iterable(
            self.config_tpcs.values()))

    def getUntrustedTPCs(self):
        return list(itertools.chain.from_iterable(
            self.untrusted_tpcs.values()))


class Abide(object):
    def __init__(self):
        self.authz_rules = {}
        self.authz_roles = {}
        self.semaphores = {}
        self.tenants = {}
        self.tenant_lock = threading.Lock()
        # tenant -> TenantTPCRegistry
        self.tpc_registry = {}
        # project -> branch -> ConfigObjectCache
        self.config_object_cache = {}
        self.api_root = None

    def clearTPCRegistry(self, tenant_name):
        try:
            del self.tpc_registry[tenant_name]
        except KeyError:
            pass

    def hasTPCRegistry(self, tenant_name):
        return tenant_name in self.tpc_registry

    def getTPCRegistry(self, tenant_name):
        r = self.tpc_registry.get(tenant_name)
        if r is None:
            return TenantTPCRegistry()
        return r

    def setTPCRegistry(self, tenant_name, tpc_registry):
        self.tpc_registry[tenant_name] = tpc_registry

    def getAllTPCs(self, tenant_name):
        # Hold a reference to the registry to make sure it doesn't
        # change between the two calls below.
        registry = self.getTPCRegistry(tenant_name)
        return list(itertools.chain(
            itertools.chain.from_iterable(
                registry.config_tpcs.values()),
            itertools.chain.from_iterable(
                registry.untrusted_tpcs.values()),
        ))

    def _allProjectTPCs(self, project_name):
        # Flatten the lists of a project TPCs from all tenants
        # Force to a list to avoid iteration errors since the clear
        # method can mutate the dictionary.
        for tpc_registry in list(self.tpc_registry.values()):
            for config_tpc in tpc_registry.config_tpcs.get(
                    project_name, []):
                yield config_tpc
            for untrusted_tpc in tpc_registry.untrusted_tpcs.get(
                    project_name, []):
                yield untrusted_tpc

    def getExtraConfigFiles(self, project_name):
        """Get all extra config files for a project accross tenants."""
        return set(itertools.chain.from_iterable(
            tpc.extra_config_files
            for tpc in self._allProjectTPCs(project_name)))

    def getExtraConfigDirs(self, project_name):
        """Get all extra config dirs for a project accross tenants."""
        return set(itertools.chain.from_iterable(
            tpc.extra_config_dirs
            for tpc in self._allProjectTPCs(project_name)))

    def hasConfigObjectCache(self, canonical_project_name, branch):
        config_object_cache = self.config_object_cache.setdefault(
            canonical_project_name, {})
        cache = config_object_cache.get(branch)
        if cache is None:
            return False
        return True

    def getConfigObjectCache(self, canonical_project_name, branch):
        config_object_cache = self.config_object_cache.setdefault(
            canonical_project_name, {})
        return config_object_cache.setdefault(branch, ConfigObjectCache())


class Capabilities(object):
    """The set of capabilities this Zuul installation has.

    Some plugins add elements to the external API. In order to
    facilitate consumers knowing if functionality is available
    or not, keep track of distinct capability flags.
    """
    def __init__(self, **kwargs):
        self.capabilities = kwargs

    def __repr__(self):
        return '<Capabilities 0x%x %s>' % (id(self), self._renderFlags())

    def _renderFlags(self):
        return " ".join(['{k}={v}'.format(k=k, v=repr(v))
                         for (k, v) in self.capabilities.items()])

    def copy(self):
        # Use a deep copy since zuul-web may modify dict entries
        return Capabilities(**copy.deepcopy(self.toDict()))

    def toDict(self):
        return self.capabilities


class WebInfo(object):
    """Information about the system needed by zuul-web /info."""

    def __init__(self, websocket_url=None,
                 capabilities=None, stats_url=None,
                 stats_prefix=None, stats_type=None):
        _caps = capabilities
        if _caps is None:
            _caps = Capabilities(**capabilities_registry.capabilities)
        self.capabilities = _caps
        self.stats_prefix = stats_prefix
        self.stats_type = stats_type
        self.stats_url = stats_url
        self.tenant = None
        self.websocket_url = websocket_url

    def __repr__(self):
        return '<WebInfo 0x%x capabilities=%s>' % (
            id(self), str(self.capabilities))

    def copy(self):
        return WebInfo(
            capabilities=self.capabilities.copy(),
            stats_prefix=self.stats_prefix,
            stats_type=self.stats_type,
            stats_url=self.stats_url,
            websocket_url=self.websocket_url)

    @staticmethod
    def fromConfig(config):
        return WebInfo(
            stats_prefix=get_default(config, 'statsd', 'prefix'),
            stats_type=get_default(config, 'web', 'stats_type', 'graphite'),
            stats_url=get_default(config, 'web', 'stats_url', None),
            websocket_url=get_default(config, 'web', 'websocket_url', None),
        )

    def toDict(self):
        d = dict()
        d['capabilities'] = self.capabilities.toDict()
        d['websocket_url'] = self.websocket_url
        stats = dict()
        stats['prefix'] = self.stats_prefix
        stats['type'] = self.stats_type
        stats['url'] = self.stats_url
        d['stats'] = stats
        if self.tenant:
            d['tenant'] = self.tenant
        return d


class HoldRequest(object):
    def __init__(self):
        self.lock = None
        self.stat = None
        self.id = None
        self.expired = None
        self.tenant = None
        self.project = None
        self.job = None
        self.ref_filter = None
        self.reason = None
        self.node_expiration = None
        # When max_count == current_count, hold request can no longer be used.
        self.max_count = 1
        self.current_count = 0

        # The hold request 'nodes' attribute is a list of dictionaries
        # (one list entry per hold request count) containing the build
        # ID (build) and a list of nodes (nodes) held for that build.
        # Example:
        #
        #  hold_request.nodes = [
        #    { 'build': 'ca01...', 'nodes': ['00000001', '00000002'] },
        #    { 'build': 'fb72...', 'nodes': ['00000003', '00000004'] },
        #  ]
        self.nodes = []

    def __str__(self):
        return "<HoldRequest %s: tenant=%s project=%s job=%s ref_filter=%s>" \
            % (self.id, self.tenant, self.project, self.job, self.ref_filter)

    @staticmethod
    def fromDict(data):
        '''
        Return a new object from the given data dictionary.
        '''
        obj = HoldRequest()
        obj.expired = data.get('expired')
        obj.tenant = data.get('tenant')
        obj.project = data.get('project')
        obj.job = data.get('job')
        obj.ref_filter = data.get('ref_filter')
        obj.max_count = data.get('max_count')
        obj.current_count = data.get('current_count')
        obj.reason = data.get('reason')
        obj.node_expiration = data.get('node_expiration')
        obj.nodes = data.get('nodes', [])
        return obj

    def toDict(self):
        '''
        Return a dictionary representation of the object.
        '''
        d = dict()
        d['id'] = self.id
        d['expired'] = self.expired
        d['tenant'] = self.tenant
        d['project'] = self.project
        d['job'] = self.job
        d['ref_filter'] = self.ref_filter
        d['max_count'] = self.max_count
        d['current_count'] = self.current_count
        d['reason'] = self.reason
        d['node_expiration'] = self.node_expiration
        d['nodes'] = self.nodes
        return d

    def updateFromDict(self, d):
        '''
        Update current object with data from the given dictionary.
        '''
        self.expired = d.get('expired')
        self.tenant = d.get('tenant')
        self.project = d.get('project')
        self.job = d.get('job')
        self.ref_filter = d.get('ref_filter')
        self.max_count = d.get('max_count', 1)
        self.current_count = d.get('current_count', 0)
        self.reason = d.get('reason')
        self.node_expiration = d.get('node_expiration')

    def serialize(self):
        '''
        Return a representation of the object as a string.

        Used for storing the object data in ZooKeeper.
        '''
        return json.dumps(self.toDict(), sort_keys=True).encode('utf8')


# AuthZ models

class AuthZRule(object):
    """The base class for authorization rules"""

    def __ne__(self, other):
        return not self.__eq__(other)


class ClaimRule(AuthZRule):
    """This rule checks the value of a claim.
    The check tries to be smart by assessing the type of the tested value."""
    def __init__(self, claim=None, value=None):
        super(ClaimRule, self).__init__()
        self.claim = claim or 'sub'
        self.value = value

    def templated(self, value, tenant=None):
        template_dict = {}
        if tenant is not None:
            template_dict['tenant'] = tenant.getSafeAttributes()
        return value.format(**template_dict)

    def _match_jsonpath(self, claims, tenant):
        matches = [match.value
                   for match in jsonpath_rw.parse(self.claim).find(claims)]
        value = self.value
        if isinstance(self.value, str):
            value = self.templated(value, tenant)
        if len(matches) == 1:
            match = matches[0]
            if isinstance(match, list):
                return value in match
            elif isinstance(match, str) or isinstance(match, bool):
                return value == match
            else:
                # unsupported type - don't raise, but this should be notified
                return False
        else:
            # TODO we should differentiate no match and 2+ matches
            return False

    def _match_dict(self, claims, tenant):
        def _compare(value, claim):
            if isinstance(value, list):
                t_value = map(self.templated, value, [tenant] * len(value))
                if isinstance(claim, list):
                    # if the claim is empty, the value must be empty too:
                    if claim == []:
                        return t_value == []
                    else:
                        return (set(claim) <= set(t_value))
                else:
                    return claim in value
            elif isinstance(value, dict):
                if not isinstance(claim, dict):
                    return False
                elif value == {}:
                    return claim == {}
                else:
                    return all(_compare(value[x], claim.get(x, {}))
                               for x in value.keys())
            else:
                t_value = self.templated(value, tenant)
                return t_value == claim

        return _compare(self.value, claims.get(self.claim, {}))

    def __call__(self, claims, tenant=None):
        if isinstance(self.value, dict):
            return self._match_dict(claims, tenant)
        else:
            return self._match_jsonpath(claims, tenant)

    def __eq__(self, other):
        if not isinstance(other, ClaimRule):
            return False
        return (self.claim == other.claim and self.value == other.value)

    def __repr__(self):
        return '<ClaimRule "%s":"%s">' % (self.claim, self.value)

    def __hash__(self):
        return hash(repr(self))


class OrRule(AuthZRule):

    def __init__(self, subrules):
        super(OrRule, self).__init__()
        self.rules = set(subrules)

    def __call__(self, claims, tenant=None):
        return any(rule(claims, tenant) for rule in self.rules)

    def __eq__(self, other):
        if not isinstance(other, OrRule):
            return False
        return self.rules == other.rules

    def __repr__(self):
        return '<OrRule %s>' % ('  || '.join(repr(r) for r in self.rules))

    def __hash__(self):
        return hash(repr(self))


class AndRule(AuthZRule):

    def __init__(self, subrules):
        super(AndRule, self).__init__()
        self.rules = set(subrules)

    def __call__(self, claims, tenant=None):
        return all(rule(claims, tenant) for rule in self.rules)

    def __eq__(self, other):
        if not isinstance(other, AndRule):
            return False
        return self.rules == other.rules

    def __repr__(self):
        return '<AndRule %s>' % (' && '.join(repr(r) for r in self.rules))

    def __hash__(self):
        return hash(repr(self))


class AuthZRuleTree(object):

    def __init__(self, name):
        self.name = name
        # initialize actions as unauthorized
        self.ruletree = None

    def __call__(self, claims, tenant=None):
        return self.ruletree(claims, tenant)

    def __eq__(self, other):
        if not isinstance(other, AuthZRuleTree):
            return False
        return (self.name == other.name and
                self.ruletree == other.ruletree)

    def __repr__(self):
        return '<AuthZRuleTree [ %s ]>' % self.ruletree


class AuthZRole(object):
    admin = False

    def __init__(self, conf):
        self.name = conf['name']
        self.permissions = conf['permissions']

    def __call__(self, permission, params):
        return False


class AuthZAdminRole(AuthZRole):
    # Special role that always returns true to checks to enable admin action
    admin = True

    def __init__(self, *args, **kwargs):
        self.name = 'admin'
        self.permissions = {'admin': True}

    def __call__(self, permission, params):
        return True


class AuthZReadRole(AuthZRole):
    # Special role that limits access to read only endpoints
    admin = False

    def __init__(self, *args, **kwargs):
        self.name = 'read'
        self.permissions = {'read': True}

    def __call__(self, permission, params):
        return True


class AuthZConfigRole(AuthZRole):
    # Roles that are configured in the zuul config

    # Admin in this case really means has permission to do more than read.
    # If we have a configured role explicitly allowing something then that
    # rule has permission to do more than read. This is "admin" while we
    # carry the old backward compatbile code.
    admin = True

    def __call__(self, permission, params):
        if permission:
            for perm, conditions in self.permissions.items():
                if permission == perm:
                    if conditions is True:
                        return True
                    elif conditions and isinstance(conditions, dict):
                        for cond, val in conditions['conditions'].items():
                            if not params.get(cond) == val:
                                return False
                        return True
        elif self.permissions.get('read'):
            # If we are called with permission=None we're checking
            # read access.
            return True
        return False


class TenantEventState(zkobject.ZKObject):
    def __init__(self, path):
        super().__init__()
        self._set(_path=path)
        self.reset()

    def reset(self):
        self._set(
            trigger_queue_discarding=False,
            trigger_queue_paused=False,
            result_queue_paused=False,
            reason=None,
        )

    def getPath(self):
        return self._path

    def toDict(self):
        data = {
            "trigger_queue_discarding": self.trigger_queue_discarding,
            "trigger_queue_paused": self.trigger_queue_paused,
            "result_queue_paused": self.result_queue_paused,
            "reason": self.reason,
        }
        return data

    def serialize(self, context):
        data = self.toDict()
        r = json.dumps(data, sort_keys=True).encode("utf8")
        return r

    def deserialize(self, raw, context, extra=None):
        if not raw:
            raw = {}
        r = super().deserialize(raw, context)
        return r

    def internalCreate(self, context):
        data = self._trySerialize(context)
        try:
            self._save(context, data)
        except NoNodeError:
            try:
                self._save(context, data, create=True)
            except NodeExistsError:
                self._save(context, data)
