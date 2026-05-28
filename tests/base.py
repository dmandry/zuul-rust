# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2016 Red Hat, Inc.
# Copyright 2021-2024 Acme Gating, LLC
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

import configparser
from collections import OrderedDict, Counter
from configparser import ConfigParser
from contextlib import contextmanager
import errno
import gc
from io import StringIO
import itertools
import json
import logging
import os
import pickle
import random
import re
from collections import defaultdict, namedtuple
from queue import Queue
from typing import Generator, List
from unittest.case import skipIf
import zlib

from apscheduler.triggers.interval import IntervalTrigger
import prometheus_client
import requests
import responses
import select
import shutil
import socket
import string
import subprocess
import sys
import tempfile
import threading
import traceback
import time
import uuid
import socketserver
import http.server

import git
import fixtures
import kazoo.client
import kazoo.exceptions
import pymysql
import psycopg2
import psycopg2.extensions
import testtools
import testtools.content
import testtools.content_type
from git.exc import NoSuchPathError
import yaml
import paramiko
import sqlalchemy

from kazoo.exceptions import NoNodeError

from zuul import model
from zuul.model import (
    BuildRequest, MergeRequest, WebInfo, HoldRequest
)

from zuul.driver.zuul import ZuulDriver
from zuul.driver.git import GitDriver
from zuul.driver.smtp import SMTPDriver
from zuul.driver.github import GithubDriver
from zuul.driver.timer import TimerDriver
from zuul.driver.sql import SQLDriver
from zuul.driver.bubblewrap import BubblewrapDriver
from zuul.driver.nullwrap import NullwrapDriver
from zuul.driver.mqtt import MQTTDriver
from zuul.driver.pagure import PagureDriver
from zuul.driver.gitlab import GitlabDriver
from zuul.driver.gerrit import GerritDriver
from zuul.driver.elasticsearch import ElasticsearchDriver
from zuul.driver.aws import AwsDriver
from zuul.driver.azure import AzureDriver
from zuul.driver.openstack import OpenstackDriver
from zuul.driver.static import StaticDriver
from zuul.driver.kubernetes import KubernetesDriver
from zuul.lib.collections import DefaultKeyDict
from zuul.lib.connections import ConnectionRegistry
from zuul.zk import zkobject, ZooKeeperClient
from zuul.zk.components import SchedulerComponent, COMPONENT_REGISTRY
from zuul.zk.event_queues import ConnectionEventQueue, PipelineResultEventQueue
from zuul.zk.executor import ExecutorApi
from zuul.zk.locks import tenant_read_lock, pipeline_lock, SessionAwareLock
from zuul.zk.merger import MergerApi
from psutil import Popen

import zuul.driver.gerrit.gerritsource as gerritsource
import zuul.driver.github
import zuul.driver.elasticsearch.connection as elconnection
import zuul.driver.sql
import zuul.scheduler
import zuul.executor.server
import zuul.executor.client
import zuul.launcher.server
import zuul.launcher.client
import zuul.lib.ansible
import zuul.lib.connections
import zuul.lib.auth
import zuul.lib.keystorage
import zuul.merger.client
import zuul.merger.merger
import zuul.merger.server
import zuul.nodepool
import zuul.configloader
from zuul.lib.logutil import get_annotated_logger

from tests.util import FIXTURE_DIR
import tests.fakegerrit
import tests.fakegithub
import tests.fakegitlab
import tests.fakepagure
from tests.otlp_fixture import OTLPFixture
import opentelemetry.sdk.trace.export

KEEP_TEMPDIRS = bool(os.environ.get('KEEP_TEMPDIRS', False))
SCHEDULER_COUNT = int(os.environ.get('ZUUL_SCHEDULER_COUNT', 1))
ZOOKEEPER_SESSION_TIMEOUT = 60


def skipIfMultiScheduler(reason=None):
    if not reason:
        reason = "Test is failing with multiple schedulers"
    return skipIf(SCHEDULER_COUNT > 1, reason)


def repack_repo(path):
    cmd = ['git', '--git-dir=%s/.git' % path, 'repack', '-afd']
    output = subprocess.Popen(cmd, close_fds=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    out = output.communicate()
    if output.returncode:
        raise Exception("git repack returned %d" % output.returncode)
    return out


def iterate_timeout(max_seconds, purpose):
    start = time.time()
    count = 0
    while (time.time() < start + max_seconds):
        count += 1
        yield count
        time.sleep(0.01)
    raise Exception("Timeout waiting for %s" % purpose)


def model_version(version):
    """Specify a model version for a model upgrade test

    This creates a dummy scheduler component with the specified model
    API version.  The component is created before any other, so it
    will appear to Zuul that it is joining an existing cluster with
    data at the old version.
    """

    def decorator(test):
        test.__model_version__ = version
        return test
    return decorator


def simple_layout(path, driver='gerrit', enable_nodepool=False,
                  replace=None):
    """Specify a layout file for use by a test method.

    :arg str path: The path to the layout file.
    :arg str driver: The source driver to use, defaults to gerrit.
    :arg bool enable_nodepool: Enable additional nodepool objects.

    Some tests require only a very simple configuration.  For those,
    establishing a complete config directory hierachy is too much
    work.  In those cases, you can add a simple zuul.yaml file to the
    test fixtures directory (in fixtures/layouts/foo.yaml) and use
    this decorator to indicate the test method should use that rather
    than the tenant config file specified by the test class.

    The decorator will cause that layout file to be added to a
    config-project called "common-config" and each "project" instance
    referenced in the layout file will have a git repo automatically
    initialized.

    The enable_nodepool argument is a temporary facility for
    convenience during the initial stages of the nodepool-in-zuul
    work.  It enables the additional nodepool config objects (which
    are not otherwise enabled by default, but will be later).

    The replace argument, if provided, is expected to be a callable
    which returns a dict to use with python template replacement.  It
    is called with the test as an argument.

    """

    def decorator(test):
        test.__simple_layout__ = (path, driver, replace)
        test.__enable_nodepool__ = enable_nodepool
        return test
    return decorator


def never_capture():
    """Never capture logs/output

    Due to high volume, log files are normally captured and attached
    to the subunit stream only on error.  This can make diagnosing
    some problems difficult.  Use this dectorator on a test to
    indicate that logs and output should not be captured.

    """

    def decorator(test):
        test.__never_capture__ = True
        return test
    return decorator


def gerrit_config(submit_whole_topic=False):
    """Configure the fake gerrit

    This allows us to configure the fake gerrit at startup.
    """

    def decorator(test):
        test.__gerrit_config__ = dict(
            submit_whole_topic=submit_whole_topic,
        )
        return test
    return decorator


def driver_config(driver, **kw):
    """A generic driver config.  Use this instead of making a new
    decorator like gerrit_config.
    """

    def decorator(test):
        driver_dict = getattr(test, '__driver_config__', None)
        if driver_dict is None:
            driver_dict = {}
            test.__driver_config__ = driver_dict
        driver_dict[driver] = kw
        return test
    return decorator


def return_data(job, ref, data):
    """Add return data for a job

    This allows configuring job return data for jobs that start
    immediately.

    """

    def decorator(test):
        if not hasattr(test, '__return_data__'):
            test.__return_data__ = []
        test.__return_data__.append(dict(
            job=job,
            ref=ref,
            data=data,
        ))
        return test
    return decorator


def okay_tracebacks(*args):
    """A list of substrings that, if they appear in a traceback, indicate
    that it's okay for that traceback to appear in logs."""

    def decorator(test):
        test.__okay_tracebacks__ = args
        return test
    return decorator


def zuul_config(section, key, value):
    """Set a zuul.conf value."""

    def decorator(test):
        config_dict = getattr(test, '__zuul_config__', None)
        if config_dict is None:
            config_dict = {}
            test.__zuul_config__ = config_dict
        section_dict = config_dict.setdefault(section, {})
        section_dict[key] = value
        return test
    return decorator


def registerProjects(source_name, client, config):
    path = config.get('scheduler', 'tenant_config')
    with open(os.path.join(FIXTURE_DIR, path)) as f:
        tenant_config = yaml.safe_load(f.read())
    for tenant in tenant_config:
        sources = tenant['tenant']['source']
        conf = sources.get(source_name)
        if not conf:
            return

        projects = conf.get('config-projects', [])
        projects.extend(conf.get('untrusted-projects', []))

        for project in projects:
            if isinstance(project, dict):
                # This can be a dict with the project as the only key
                client.addProjectByName(
                    list(project.keys())[0])
            else:
                client.addProjectByName(project)


def copyConfig(config):
    # Since the config contains a regex we cannot use copy.deepcopy()
    # as this will raise an exception with Python <3.7
    config_data = StringIO()
    config.write(config_data)
    config_data.seek(0)
    new_config = ConfigParser()
    new_config.read_file(config_data)
    return new_config


class FakeChangeDB:
    def __init__(self):
        # A dictionary of server -> dict as below
        self.servers = {}

    def getServerChangeDB(self, server):
        """Returns a dictionary for the specified server; key -> Change.  The
        key is driver dependent, but typically the change/PR/MR id.

        """
        return self.servers.setdefault(server, {})

    def save(self, path):
        with open(path, 'wb') as f:
            pickle.dump(self.servers, f, pickle.HIGHEST_PROTOCOL)

    def load(self, path):
        with open(path, 'rb') as f:
            self.servers = pickle.load(f)


class StatException(Exception):
    # Used by assertReportedStat
    pass


class GerritDriverMock(GerritDriver):
    def __init__(self, registry, test_config, upstream_root,
                 additional_event_queues, poller_events, add_cleanup):
        super(GerritDriverMock, self).__init__()
        self.registry = registry
        self.test_config = test_config
        self.changes = test_config.changes
        self.upstream_root = upstream_root
        self.additional_event_queues = additional_event_queues
        self.poller_events = poller_events
        self.add_cleanup = add_cleanup

    def getConnection(self, name, config):
        server = config['server']
        db = self.changes.getServerChangeDB(server)
        poll_event = self.poller_events.setdefault(name, threading.Event())
        ref_event = self.poller_events.setdefault(name + '-ref',
                                                  threading.Event())
        submit_whole_topic = self.test_config.gerrit_config.get(
            'submit_whole_topic', False)
        connection = tests.fakegerrit.FakeGerritConnection(
            self, name, config,
            changes_db=db,
            upstream_root=self.upstream_root,
            poller_event=poll_event,
            ref_watcher_event=ref_event,
            submit_whole_topic=submit_whole_topic)
        if connection.web_server:
            self.add_cleanup(connection.web_server.stop)

        setattr(self.registry, 'fake_' + name, connection)
        return connection


class GithubDriverMock(GithubDriver):
    def __init__(self, registry, test_config, config, upstream_root,
                 additional_event_queues, git_url_with_auth):
        super(GithubDriverMock, self).__init__()
        self.registry = registry
        self.test_config = test_config
        self.changes = test_config.changes
        self.config = config
        self.upstream_root = upstream_root
        self.additional_event_queues = additional_event_queues
        self.git_url_with_auth = git_url_with_auth

    def getConnection(self, name, config):
        server = config.get('server', 'github.com')
        db = self.changes.getServerChangeDB(server)
        connection = tests.fakegithub.FakeGithubConnection(
            self, name, config,
            changes_db=db,
            upstream_root=self.upstream_root,
            git_url_with_auth=self.git_url_with_auth)
        setattr(self.registry, 'fake_' + name, connection)
        client = connection.getGithubClient(None)
        registerProjects(connection.source.name, client, self.config)
        return connection


class PagureDriverMock(PagureDriver):
    def __init__(self, registry, test_config, upstream_root,
                 additional_event_queues):
        super(PagureDriverMock, self).__init__()
        self.registry = registry
        self.changes = test_config.changes
        self.upstream_root = upstream_root
        self.additional_event_queues = additional_event_queues

    def getConnection(self, name, config):
        server = config.get('server', 'pagure.io')
        db = self.changes.getServerChangeDB(server)
        connection = tests.fakepagure.FakePagureConnection(
            self, name, config,
            changes_db=db,
            upstream_root=self.upstream_root)
        setattr(self.registry, 'fake_' + name, connection)
        return connection


class GitlabDriverMock(GitlabDriver):
    def __init__(self, registry, test_config, config, upstream_root,
                 additional_event_queues):
        super(GitlabDriverMock, self).__init__()
        self.registry = registry
        self.changes = test_config.changes
        self.config = config
        self.upstream_root = upstream_root
        self.additional_event_queues = additional_event_queues

    def getConnection(self, name, config):
        server = config.get('server', 'gitlab.com')
        db = self.changes.getServerChangeDB(server)
        connection = tests.fakegitlab.FakeGitlabConnection(
            self, name, config,
            changes_db=db,
            upstream_root=self.upstream_root)
        setattr(self.registry, 'fake_' + name, connection)
        registerProjects(connection.source.name, connection,
                         self.config)
        return connection


class TestConnectionRegistry(ConnectionRegistry):
    def __init__(self, config, test_config,
                 additional_event_queues, upstream_root,
                 poller_events, git_url_with_auth, add_cleanup):
        self.connections = OrderedDict()
        self.drivers = {}

        self.registerDriver(ZuulDriver())
        self.registerDriver(GerritDriverMock(
            self, test_config, upstream_root, additional_event_queues,
            poller_events, add_cleanup))
        self.registerDriver(GitDriver())
        self.registerDriver(GithubDriverMock(
            self, test_config, config, upstream_root, additional_event_queues,
            git_url_with_auth))
        self.registerDriver(SMTPDriver())
        self.registerDriver(TimerDriver())
        self.registerDriver(SQLDriver())
        self.registerDriver(BubblewrapDriver())
        self.registerDriver(NullwrapDriver())
        self.registerDriver(MQTTDriver())
        self.registerDriver(PagureDriverMock(
            self, test_config, upstream_root, additional_event_queues))
        self.registerDriver(GitlabDriverMock(
            self, test_config, config, upstream_root, additional_event_queues))
        self.registerDriver(ElasticsearchDriver())
        self.registerDriver(AwsDriver())
        self.registerDriver(AzureDriver())
        self.registerDriver(OpenstackDriver())
        self.registerDriver(StaticDriver())
        self.registerDriver(KubernetesDriver())


class FakeAnsibleManager(zuul.lib.ansible.AnsibleManager):

    def validate(self):
        return True

    def copyAnsibleFiles(self):
        pass


class FakeElasticsearchConnection(elconnection.ElasticsearchConnection):

    log = logging.getLogger("zuul.test.FakeElasticsearchConnection")

    def __init__(self, driver, connection_name, connection_config):
        self.driver = driver
        self.connection_name = connection_name
        self.source_it = None

    def add_docs(self, source_it, index):
        self.source_it = source_it
        self.index = index


class BuildHistory(object):
    def __init__(self, **kw):
        self.__dict__.update(kw)

    def __repr__(self):
        return ("<Completed build, result: %s name: %s uuid: %s "
                "changes: %s ref: %s>" %
                (self.result, self.name, self.uuid,
                 self.changes, self.ref))


class FakeStatsd(threading.Thread):
    log = logging.getLogger("zuul.test.FakeStatsd")

    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = True
        self.sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        self.sock.bind(('', 0))
        self.port = self.sock.getsockname()[1]
        self.wake_read, self.wake_write = os.pipe()
        self.stats = []

    def clear(self):
        self.stats = []

    def run(self):
        while True:
            poll = select.poll()
            poll.register(self.sock, select.POLLIN)
            poll.register(self.wake_read, select.POLLIN)
            ret = poll.poll()
            for (fd, event) in ret:
                if fd == self.sock.fileno():
                    data = self.sock.recvfrom(1024)
                    if not data:
                        return
                    # self.log.debug("Appending: %s" % data[0])
                    self.stats.append(data[0])
                if fd == self.wake_read:
                    return

    def stop(self):
        os.write(self.wake_write, b'1\n')
        self.join()
        self.sock.close()

    def __del__(self):
        os.close(self.wake_read)
        os.close(self.wake_write)


class FakeBuild(object):
    log = logging.getLogger("zuul.test")

    def __init__(self, executor_server, build_request, params):
        self.daemon = True
        self.executor_server = executor_server
        self.build_request = build_request
        self.jobdir = None
        self.uuid = build_request.uuid
        self.parameters = params
        self.job = model.FrozenJob.fromZK(executor_server.zk_context,
                                          params["job_ref"])
        self.parameters["zuul"].update(
            zuul.executor.server.zuul_params_from_job(self.job))
        # TODOv3(jeblair): self.node is really "the label of the node
        # assigned".  We should rename it (self.node_label?) if we
        # keep using it like this, or we may end up exposing more of
        # the complexity around multi-node jobs here
        # (self.nodes[0].label?)
        self.node = None
        if len(self.job.nodeset.nodes) == 1:
            self.node = next(iter(self.job.nodeset.nodes.values())).label
        self.unique = self.parameters['zuul']['build']
        self.pipeline = self.parameters['zuul']['pipeline']
        self.project = self.parameters['zuul']['project']['name']
        self.name = self.job.name
        self.wait_condition = threading.Condition()
        self.waiting = False
        self.paused = False
        self.aborted = False
        self.requeue = False
        self.should_fail = False
        self.should_retry = False
        self.created = time.time()
        self.changes = None
        items = self.parameters['zuul']['items']
        self.changes = ' '.join(['%s,%s' % (x['change'], x['patchset'])
                                 for x in items if 'change' in x])
        if 'change' in items[-1]:
            self.change = ' '.join((items[-1]['change'],
                                    items[-1]['patchset']))
        else:
            self.change = None

    def __repr__(self):
        waiting = ''
        if self.waiting:
            waiting = ' [waiting]'
        return '<FakeBuild %s:%s %s%s>' % (self.pipeline, self.name,
                                           self.changes, waiting)

    def release(self):
        """Release this build."""
        self.wait_condition.acquire()
        self.wait_condition.notify()
        self.waiting = False
        self.log.debug("Build %s released" % self.unique)
        self.wait_condition.release()

    def isWaiting(self):
        """Return whether this build is being held.

        :returns: Whether the build is being held.
        :rtype: bool
        """

        self.wait_condition.acquire()
        if self.waiting:
            ret = True
        else:
            ret = False
        self.wait_condition.release()
        return ret

    def _wait(self):
        self.wait_condition.acquire()
        self.waiting = True
        self.log.debug("Build %s waiting" % self.unique)
        self.wait_condition.wait()
        self.wait_condition.release()

    def run(self):
        self.log.debug('Running build %s' % self.unique)

        if self.executor_server.hold_jobs_in_build:
            self.log.debug('Holding build %s' % self.unique)
            self._wait()
        self.log.debug("Build %s continuing" % self.unique)

        self.writeReturnData()

        result = (RecordingAnsibleJob.RESULT_NORMAL, 0)  # Success
        if self.shouldFail():
            result = (RecordingAnsibleJob.RESULT_NORMAL, 1)  # Failure
        if self.shouldRetry():
            result = (RecordingAnsibleJob.RESULT_NORMAL, None)
        if self.aborted:
            result = (RecordingAnsibleJob.RESULT_ABORTED, None)
        if self.requeue:
            result = (RecordingAnsibleJob.RESULT_UNREACHABLE, None)

        return result

    def shouldFail(self):
        if self.should_fail:
            return True
        changes = self.executor_server.fail_tests.get(self.name, [])
        for change in changes:
            if self.hasChanges(change):
                return True
        return False

    def shouldRetry(self):
        if self.should_retry:
            return True
        entries = self.executor_server.retry_tests.get(self.name, [])
        for entry in entries:
            if self.hasChanges(entry['change']):
                if entry['retries'] is None:
                    return True
                if entry['retries']:
                    entry['retries'] = entry['retries'] - 1
                    return True
        return False

    def writeReturnData(self):
        data_changes = self.executor_server.return_data.get(self.name, {})
        secret_data_changes = self.executor_server.return_secret_data.get(
            self.name, {})
        data = data_changes.get(self.parameters['zuul']['ref'])
        secret_data = secret_data_changes.get(self.parameters['zuul']['ref'])
        if data is None and secret_data is None:
            return
        with open(self.jobdir.result_data_file, 'w') as f:
            f.write(json.dumps({'data': data, 'secret_data': secret_data}))

    def hasChanges(self, *changes):
        """Return whether this build has certain changes in its git repos.

        :arg FakeChange changes: One or more changes (varargs) that
            are expected to be present (in order) in the git repository of
            the active project.

        :returns: Whether the build has the indicated changes.
        :rtype: bool

        """
        for change in changes:
            hostname = change.source_hostname
            path = os.path.join(self.jobdir.src_root, hostname, change.project)
            try:
                repo = git.Repo(path)
            except NoSuchPathError as e:
                self.log.debug('%s' % e)
                return False
            repo_messages = [c.message.strip() for c in repo.iter_commits()]
            commit_message = '%s-1' % change.subject
            self.log.debug("Checking if build %s has changes; commit_message "
                           "%s; repo_messages %s" % (self, commit_message,
                                                     repo_messages))
            if commit_message not in repo_messages:
                self.log.debug("  messages do not match")
                return False
        self.log.debug("  OK")
        return True

    def getWorkspaceRepos(self, projects):
        """Return workspace git repo objects for the listed projects

        :arg list projects: A list of strings, each the canonical name
                            of a project.

        :returns: A dictionary of {name: repo} for every listed
                  project.
        :rtype: dict

        """

        repos = {}
        for project in projects:
            path = os.path.join(self.jobdir.src_root, project)
            repo = git.Repo(path)
            repos[project] = repo
        return repos


class RecordingAnsibleJob(zuul.executor.server.AnsibleJob):
    result = None
    semaphore_sleep_time = 5

    def _execute(self):
        for _ in iterate_timeout(60, 'wait for merge'):
            if not self.executor_server.hold_jobs_in_start:
                break
            time.sleep(1)

        super()._execute()

    def doMergeChanges(self, *args, **kw):
        # Get a merger in order to update the repos involved in this job.
        commit = super(RecordingAnsibleJob, self).doMergeChanges(
            *args, **kw)
        if not commit:
            self.recordResult('MERGE_CONFLICT')

        return commit

    def recordResult(self, result):
        self.executor_server.lock.acquire()
        build = self.executor_server.job_builds.get(self.build_request.uuid)
        if not build:
            self.executor_server.lock.release()
            # Already recorded
            return
        self.executor_server.build_history.append(
            BuildHistory(name=build.name, result=result, changes=build.changes,
                         node=build.node, uuid=build.unique, job=build.job,
                         ref=build.parameters['zuul']['ref'],
                         newrev=build.parameters['zuul'].get('newrev'),
                         parameters=build.parameters, jobdir=build.jobdir,
                         pipeline=build.parameters['zuul']['pipeline'],
                         build_request_ref=build.build_request.path)
        )
        self.executor_server.running_builds.remove(build)
        del self.executor_server.job_builds[self.build_request.uuid]
        self.executor_server.lock.release()

    def runPlaybooks(self, args):
        build = self.executor_server.job_builds[self.build_request.uuid]
        build.jobdir = self.jobdir

        self.result, unreachable, error_detail = super(
            RecordingAnsibleJob, self).runPlaybooks(args)
        self.recordResult(self.result)
        return self.result, unreachable, error_detail

    def runAnsible(self, cmd, timeout, playbook, ansible_version,
                   allow_pre_fail, wrapped=True, cleanup=False,
                   phase=None, index=None):
        build = self.executor_server.job_builds[self.build_request.uuid]

        if self.executor_server._run_ansible:
            # Call run on the fake build omitting the result so we also can
            # hold real ansible jobs.
            if playbook not in [self.jobdir.setup_playbook,
                                self.jobdir.freeze_playbook]:
                build.run()

            result = super(RecordingAnsibleJob, self).runAnsible(
                cmd, timeout, playbook, ansible_version, allow_pre_fail,
                wrapped, cleanup, phase, index)
        else:
            if playbook not in [self.jobdir.setup_playbook,
                                self.jobdir.freeze_playbook]:
                result = build.run()
            else:
                result = (self.RESULT_NORMAL, 0)
        return result

    def getHostList(self, args, nodes):
        self.log.debug("hostlist %s", nodes)
        hosts = super(RecordingAnsibleJob, self).getHostList(args, nodes)
        for host in hosts:
            if not host['host_vars'].get('ansible_connection'):
                host['host_vars']['ansible_connection'] = 'local'
                # Ansible will find our test venv python interpreter
                # due to the "local" connection, but it won't be in
                # the bwrap environment.  Force it to use the system
                # python instead.
                if host['host_vars'].get(
                        'ansible_python_interpreter', 'auto') == 'auto':
                    host['host_vars']['ansible_python_interpreter'] =\
                        '/usr/bin/python3'
        return hosts

    def pause(self):
        build = self.executor_server.job_builds[self.build_request.uuid]
        build.paused = True
        super().pause()

    def resume(self):
        build = self.executor_server.job_builds.get(self.build_request.uuid)
        if build:
            build.paused = False
        super().resume()

    def _send_aborted(self):
        self.recordResult('ABORTED')
        super()._send_aborted()


FakeMergeRequest = namedtuple(
    "FakeMergeRequest", ("uuid", "job_type", "payload")
)


class HoldableMergerApi(MergerApi):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hold_in_queue = False
        self.history = {}

    def submit(self, request, params, needs_result=False):
        self.log.debug("Appending merge job to history: %s", request.uuid)
        self.history.setdefault(request.job_type, [])
        self.history[request.job_type].append(
            FakeMergeRequest(request.uuid, request.job_type, params)
        )
        return super().submit(request, params, needs_result)

    @property
    def initial_state(self):
        if self.hold_in_queue:
            return MergeRequest.HOLD
        return MergeRequest.REQUESTED


class TestingMergerApi(HoldableMergerApi):

    log = logging.getLogger("zuul.test.TestingMergerApi")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_types_to_fail = []

    def _test_getMergeJobsInState(self, *states):
        # As this method is used for assertions in the tests, it should look up
        # the merge requests directly from ZooKeeper and not from a cache
        # layer.
        all_merge_requests = []
        for merge_uuid in self._getAllRequestIds():
            merge_request = self._get("/".join(
                [self.REQUEST_ROOT, merge_uuid]))
            if merge_request and (not states or merge_request.state in states):
                all_merge_requests.append(merge_request)

        return sorted(all_merge_requests)

    def failJobs(self, job_type):
        """
        Lets all merge jobs of the given type fail.

        The actual implementation is done by overriding reportResult().
        """
        # As the params of a merge job are cleared directly after it's
        # picked up by a merger, we don't have much information to
        # identify a running merge job. The easiest solution is to
        # fail all merge jobs of a given type, which should be fine
        # for test purposes.
        # We are failing merge jobs by type as the params are directly
        # cleared when the merge job is picked up by a merger. Thus, we
        # cannot compare information like project or branch in
        # reportResult() where we set the merge job's result to failed.
        self.job_types_to_fail.append(job_type)

    def reportResult(self, request, result):
        # Set updated to False to mark a merge job as failed
        if request.job_type in self.job_types_to_fail:
            result["updated"] = False
        return super().reportResult(request, result)

    def release(self, merge_request=None):
        """
        Releases a merge request which was previously put on hold for testing.

        If no merge_request is provided, all merge request that are currently
        in state HOLD will be released.
        """
        # Either release all jobs in HOLD state or the one specified.
        if merge_request is not None:
            merge_request.state = MergeRequest.REQUESTED
            self.update(merge_request)
            return

        for merge_request in self._test_getMergeJobsInState(MergeRequest.HOLD):
            merge_request.state = MergeRequest.REQUESTED
            self.update(merge_request)

    def queued(self):
        return self._test_getMergeJobsInState(
            MergeRequest.REQUESTED, MergeRequest.HOLD
        )

    def all(self):
        return self._test_getMergeJobsInState()


class HoldableMergeClient(zuul.merger.client.MergeClient):

    _merger_api_class = HoldableMergerApi


class HoldableExecutorApi(ExecutorApi):
    def __init__(self, *args, **kwargs):
        self.hold_in_queue = False
        super().__init__(*args, **kwargs)

    def _getInitialState(self):
        if self.hold_in_queue:
            return BuildRequest.HOLD
        return BuildRequest.REQUESTED


class HoldableLauncherClient(zuul.launcher.client.LauncherClient):

    hold_in_queue = False

    def _getInitialRequestStates(self, job):
        if self.hold_in_queue:
            return dict(
                state=model.NodesetRequest.State.TEST_HOLD,
            )
        return super()._getInitialRequestStates(job)


class TestingExecutorApi(HoldableExecutorApi):
    log = logging.getLogger("zuul.test.TestingExecutorApi")

    def _test_getBuildsInState(self, *states):
        # As this method is used for assertions in the tests, it
        # should look up the build requests directly from ZooKeeper
        # and not from a cache layer.

        all_builds = []
        for zone in self._getAllZones():
            queue = self.zone_queues[zone]
            for build_uuid in queue._getAllRequestIds():
                build = queue._get(f'{queue.REQUEST_ROOT}/{build_uuid}')
                if build and (not states or build.state in states):
                    all_builds.append(build)

        all_builds.sort()
        return all_builds

    def _getJobForBuildRequest(self, build_request):
        # The parameters for the build request are removed immediately
        # after the job starts in order to reduce impact to ZK, so if
        # we want to inspect them in the tests, we need to save them.
        # This adds them to a private internal cache for that purpose.
        if not hasattr(self, '_test_build_request_job_map'):
            self._test_build_request_job_map = {}
        if build_request.uuid in self._test_build_request_job_map:
            return self._test_build_request_job_map[build_request.uuid]

        params = self.getParams(build_request)
        job_name = params['zuul']['job']
        self._test_build_request_job_map[build_request.uuid] = job_name
        return job_name

    def release(self, what=None):
        """
        Releases a build request which was previously put on hold for testing.

        The what parameter specifies what to release. This can be a concrete
        build request or a regular expression matching a job name.
        """
        self.log.debug("Releasing builds matching %s", what)
        if isinstance(what, BuildRequest):
            self.log.debug("Releasing build %s", what)
            what.state = BuildRequest.REQUESTED
            self.update(what)
            return

        for build_request in self._test_getBuildsInState(
                BuildRequest.HOLD):
            # Either release all build requests in HOLD state or the ones
            # matching the given job name pattern.
            if what is None or (
                    re.match(what,
                             self._getJobForBuildRequest(build_request))):
                self.log.debug("Releasing build %s", build_request)
                build_request.state = BuildRequest.REQUESTED
                self.update(build_request)

    def queued(self):
        return self._test_getBuildsInState(
            BuildRequest.REQUESTED, BuildRequest.HOLD
        )

    def all(self):
        return self._test_getBuildsInState()


class HoldableExecutorClient(zuul.executor.client.ExecutorClient):
    _executor_api_class = HoldableExecutorApi


class RecordingExecutorServer(zuul.executor.server.ExecutorServer):
    """An Ansible executor to be used in tests.

    :ivar bool hold_jobs_in_build: If true, when jobs are executed
        they will report that they have started but then pause until
        released before reporting completion.  This attribute may be
        changed at any time and will take effect for subsequently
        executed builds, but previously held builds will still need to
        be explicitly released.

    """

    _job_class = RecordingAnsibleJob
    _merger_api_class = TestingMergerApi

    def __init__(self, *args, **kw):
        self._run_ansible = kw.pop('_run_ansible', False)
        self._test_root = kw.pop('_test_root', False)
        if self._run_ansible:
            self._ansible_manager_class = zuul.lib.ansible.AnsibleManager
        else:
            self._ansible_manager_class = FakeAnsibleManager
        super(RecordingExecutorServer, self).__init__(*args, **kw)
        self.hold_jobs_in_build = False
        self.hold_jobs_in_start = False
        self.lock = threading.Lock()
        self.running_builds = []
        self.build_history = []
        self.fail_tests = {}
        self.retry_tests = {}
        self.return_data = {}
        self.return_secret_data = {}
        self.job_builds = {}

    def failJob(self, name, change):
        """Instruct the executor to report matching builds as failures.

        :arg str name: The name of the job to fail.
        :arg Change change: The :py:class:`~tests.base.FakeChange`
            instance which should cause the job to fail.  This job
            will also fail for changes depending on this change.

        """
        l = self.fail_tests.get(name, [])
        l.append(change)
        self.fail_tests[name] = l

    def retryJob(self, name, change, retries=None):
        """Instruct the executor to report matching builds as retries.

        :arg str name: The name of the job to fail.
        :arg Change change: The :py:class:`~tests.base.FakeChange`
            instance which should cause the job to fail.  This job
            will also fail for changes depending on this change.

        """
        self.retry_tests.setdefault(name, []).append(
            dict(change=change,
                 retries=retries))

    def returnData(self, name, change, data, secret_data={}):
        """Instruct the executor to return data for this build.

        :arg str name: The name of the job to return data.
        :arg Change change: The :py:class:`~tests.base.FakeChange`
            instance which should cause the job to return data.
            Or pass a ref as a string.
        :arg dict data: The data to return

        """
        data_changes = self.return_data.setdefault(name, {})
        secret_data_changes = self.return_secret_data.setdefault(name, {})
        if hasattr(change, 'number'):
            cid = change.data['currentPatchSet']['ref']
        elif isinstance(change, str):
            cid = change
        else:
            # Not actually a change, but a ref update event for tags/etc
            # In this case a key of None is used by writeReturnData
            cid = None
        data_changes[cid] = data
        secret_data_changes[cid] = secret_data

    def release(self, regex=None, change=None):
        """Release a held build.

        :arg str regex: A regular expression which, if supplied, will
            cause only builds with matching names to be released.  If
            not supplied, all builds will be released.

        """
        released = []
        builds = self.running_builds[:]
        if len(builds) == 0:
            self.log.debug('No running builds to release')
            return []

        self.log.debug("Releasing build %s %s (%s)" % (
            regex, change, len(builds)))
        for build in builds:
            if ((not regex or re.match(regex, build.name)) and
                (not change or build.change == change)):
                self.log.debug("Releasing build %s" %
                               (build.parameters['zuul']['build']))
                released.append(build)
                build.release()
            else:
                self.log.debug("Not releasing build %s" %
                               (build.parameters['zuul']['build']))
        self.log.debug("Done releasing builds %s (%s)" %
                       (regex, len(builds)))
        return released

    def executeJob(self, build_request, params):
        build = FakeBuild(self, build_request, params)
        self.running_builds.append(build)
        self.job_builds[build_request.uuid] = build
        params['zuul']['_test'] = dict(test_root=self._test_root)
        super(RecordingExecutorServer, self).executeJob(build_request, params)

    def stopJob(self, build_request: BuildRequest):
        self.log.debug("handle stop")
        uuid = build_request.uuid
        for build in self.running_builds:
            if build.unique == uuid:
                build.aborted = True
                build.release()
        super(RecordingExecutorServer, self).stopJob(build_request)

    def stop(self):
        for build in self.running_builds:
            build.aborted = True
            build.release()
        super(RecordingExecutorServer, self).stop()


class TestScheduler(zuul.scheduler.Scheduler):
    _merger_client_class = HoldableMergeClient
    _executor_client_class = HoldableExecutorClient
    _launcher_client_class = HoldableLauncherClient


class TestLauncher(zuul.launcher.server.Launcher):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._test_lock = threading.Lock()

    def _run(self):
        with self._test_lock:
            return super()._run()


class FakeSMTP(object):
    log = logging.getLogger('zuul.FakeSMTP')

    def __init__(self, messages, server, port):
        self.server = server
        self.port = port
        self.messages = messages

    def sendmail(self, from_email, to_email, msg):
        self.log.info("Sending email from %s, to %s, with msg %s" % (
                      from_email, to_email, msg))

        headers = msg.split('\n\n', 1)[0]
        body = msg.split('\n\n', 1)[1]

        self.messages.append(dict(
            from_email=from_email,
            to_email=to_email,
            msg=msg,
            headers=headers,
            body=body,
        ))

        return True

    def quit(self):
        return True


class FakeNodepool(object):
    REQUEST_ROOT = '/nodepool/requests'
    NODE_ROOT = '/nodepool/nodes'
    COMPONENT_ROOT = '/nodepool/components'

    log = logging.getLogger("zuul.test.FakeNodepool")

    def __init__(self, zk_chroot_fixture):
        self.complete_event = threading.Event()
        self.host_keys = None

        self.client = kazoo.client.KazooClient(
            hosts='%s:%s%s' % (
                zk_chroot_fixture.zookeeper_host,
                zk_chroot_fixture.zookeeper_port,
                zk_chroot_fixture.zookeeper_chroot),
            use_ssl=True,
            keyfile=zk_chroot_fixture.zookeeper_key,
            certfile=zk_chroot_fixture.zookeeper_cert,
            ca=zk_chroot_fixture.zookeeper_ca,
            timeout=ZOOKEEPER_SESSION_TIMEOUT,
        )
        self.client.start()
        self.registerLauncher()
        self._running = True
        self.paused = False
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()
        self.fail_requests = set()
        self.remote_ansible = False
        self.attributes = None
        self.resources = None
        self.python_path = 'auto'
        self.override_python_path = None
        self.shell_type = None
        self.connection_port = None
        self.history = []

    def stop(self):
        self._running = False
        self.thread.join()
        self.client.stop()
        self.client.close()

    def pause(self):
        self.complete_event.wait()
        self.paused = True

    def unpause(self):
        self.paused = False

    def run(self):
        while self._running:
            self.complete_event.clear()
            try:
                self._run()
            except Exception:
                self.log.exception("Error in fake nodepool:")
            self.complete_event.set()
            time.sleep(0.1)

    def _run(self):
        if self.paused:
            return
        for req in self.getNodeRequests():
            self.fulfillRequest(req)

    def registerLauncher(self, labels=["label1"], id="FakeLauncher"):
        path = os.path.join(self.COMPONENT_ROOT, 'pool', id)
        data = {'id': id, 'supported_labels': labels}
        self.client.create(
            path, json.dumps(data).encode('utf8'),
            ephemeral=True, makepath=True, sequence=True)

    def getNodeRequests(self):
        try:
            reqids = self.client.get_children(self.REQUEST_ROOT)
        except kazoo.exceptions.NoNodeError:
            return []
        reqs = []
        for oid in reqids:
            path = self.REQUEST_ROOT + '/' + oid
            try:
                data, stat = self.client.get(path)
                data = json.loads(data.decode('utf8'))
                data['_oid'] = oid
                reqs.append(data)
            except kazoo.exceptions.NoNodeError:
                pass
        reqs.sort(key=lambda r: (r['_oid'].split('-')[0],
                                 r['relative_priority'],
                                 r['_oid'].split('-')[1]))
        return reqs

    def getNodes(self):
        try:
            nodeids = self.client.get_children(self.NODE_ROOT)
        except kazoo.exceptions.NoNodeError:
            return []
        nodes = []
        for oid in sorted(nodeids):
            path = self.NODE_ROOT + '/' + oid
            data, stat = self.client.get(path)
            data = json.loads(data.decode('utf8'))
            data['_oid'] = oid
            try:
                lockfiles = self.client.get_children(path + '/lock')
            except kazoo.exceptions.NoNodeError:
                lockfiles = []
            if lockfiles:
                data['_lock'] = True
            else:
                data['_lock'] = False
            nodes.append(data)
        return nodes

    def makeNode(self, request_id, node_type, request):
        now = time.time()
        path = '/nodepool/nodes/'
        remote_ip = os.environ.get('ZUUL_REMOTE_IPV4', '127.0.0.1')
        if self.remote_ansible and not self.host_keys:
            self.host_keys = self.keyscan(remote_ip)
        if self.host_keys is None:
            host_keys = ["fake-key1", "fake-key2"]
        else:
            host_keys = self.host_keys
        data = dict(type=node_type,
                    cloud='test-cloud',
                    provider='test-provider',
                    region='test-region',
                    az='test-az',
                    attributes=self.attributes,
                    host_id='test-host-id',
                    interface_ip=remote_ip,
                    public_ipv4=remote_ip,
                    private_ipv4=None,
                    public_ipv6=None,
                    private_ipv6=None,
                    python_path=self.python_path,
                    shell_type=self.shell_type,
                    allocated_to=request_id,
                    state='ready',
                    state_time=now,
                    created_time=now,
                    updated_time=now,
                    image_id=None,
                    host_keys=host_keys,
                    executor='fake-nodepool',
                    hold_expiration=None,
                    node_properties={"spot": False})
        if self.resources:
            data['resources'] = self.resources
        if self.remote_ansible:
            data['connection_type'] = 'ssh'
        if os.environ.get("ZUUL_REMOTE_USER"):
            data['username'] = os.environ.get("ZUUL_REMOTE_USER")
        if 'fakeuser' in node_type:
            data['username'] = 'fakeuser'
        if 'windows' in node_type:
            data['connection_type'] = 'winrm'
        if 'network' in node_type:
            data['connection_type'] = 'network_cli'
        if self.connection_port:
            data['connection_port'] = self.connection_port
        if 'kubernetes-namespace' in node_type or 'fedora-pod' in node_type:
            data['connection_type'] = 'namespace'
            data['connection_port'] = {
                'name': 'zuul-ci',
                'namespace': 'zuul-ci-abcdefg',
                'host': 'localhost',
                'skiptls': True,
                'token': 'FakeToken',
                'ca_crt': 'FakeCA',
                'user': 'zuul-worker',
            }
            if 'fedora-pod' in node_type:
                data['connection_type'] = 'kubectl'
                data['connection_port']['pod'] = 'fedora-abcdefg'
        if 'remote-pod' in node_type:
            data['connection_type'] = 'kubectl'
            data['connection_port'] = {
                'name': os.environ['ZUUL_POD_REMOTE_NAME'],
                'namespace': os.environ.get(
                    'ZUUL_POD_REMOTE_NAMESPACE', 'default'),
                'host': os.environ['ZUUL_POD_REMOTE_SERVER'],
                'skiptls': False,
                'token': os.environ['ZUUL_POD_REMOTE_TOKEN'],
                'ca_crt': os.environ['ZUUL_POD_REMOTE_CA'],
                'user': os.environ['ZUUL_POD_REMOTE_USER'],
                'pod': os.environ['ZUUL_POD_REMOTE_NAME'],
            }
            data['interface_ip'] = data['connection_port']['pod']
            data['public_ipv4'] = None
        if not data.get('connection_type') and self.override_python_path:
            # We aren't setting a specific connection type so will
            # connect locally. On certain platforms (like tumbleweed)
            # ansible auto python interpreter discovery does the wrong
            # thing so we allow test runners to override a set value.
            data['python_path'] = self.override_python_path
        data['tenant_name'] = request['tenant_name']
        data['requestor'] = request['requestor']

        data = json.dumps(data).encode('utf8')
        path = self.client.create(path, data,
                                  makepath=True,
                                  sequence=True)
        nodeid = path.split("/")[-1]
        return nodeid

    def removeNode(self, node):
        path = self.NODE_ROOT + '/' + node["_oid"]
        self.client.delete(path, recursive=True)

    def addFailRequest(self, request):
        self.fail_requests.add(request['_oid'])

    def fulfillRequest(self, request):
        if request['state'] != 'requested':
            return
        request = request.copy()
        self.history.append(request)
        oid = request['_oid']
        del request['_oid']

        if oid in self.fail_requests:
            request['state'] = 'failed'
        else:
            request['state'] = 'fulfilled'
            nodes = request.get('nodes', [])
            for node in request['node_types']:
                nodeid = self.makeNode(oid, node, request)
                nodes.append(nodeid)
            request['nodes'] = nodes

        request['state_time'] = time.time()
        path = self.REQUEST_ROOT + '/' + oid
        data = json.dumps(request).encode('utf8')
        self.log.debug("Fulfilling node request: %s %s" % (oid, data))
        try:
            self.client.set(path, data)
        except kazoo.exceptions.NoNodeError:
            self.log.debug("Node request %s %s disappeared" % (oid, data))

    def keyscan(self, ip, port=22, timeout=60):
        '''
        Scan the IP address for public SSH keys.

        Keys are returned formatted as: "<type> <base64_string>"
        '''
        addrinfo = socket.getaddrinfo(ip, port)[0]
        family = addrinfo[0]
        sockaddr = addrinfo[4]

        keys = []
        key = None
        for count in iterate_timeout(timeout, "ssh access"):
            sock = None
            t = None
            try:
                sock = socket.socket(family, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                sock.connect(sockaddr)
                t = paramiko.transport.Transport(sock)
                t.start_client(timeout=timeout)
                key = t.get_remote_server_key()
                break
            except socket.error as e:
                if e.errno not in [
                        errno.ECONNREFUSED, errno.EHOSTUNREACH, None]:
                    self.log.exception(
                        'Exception with ssh access to %s:' % ip)
            except Exception as e:
                self.log.exception("ssh-keyscan failure: %s", e)
            finally:
                try:
                    if t:
                        t.close()
                except Exception as e:
                    self.log.exception('Exception closing paramiko: %s', e)
                try:
                    if sock:
                        sock.close()
                except Exception as e:
                    self.log.exception('Exception closing socket: %s', e)

        # Paramiko, at this time, seems to return only the ssh-rsa key, so
        # only the single key is placed into the list.
        if key:
            keys.append("%s %s" % (key.get_name(), key.get_base64()))

        return keys


class ResponsesFixture(fixtures.Fixture):
    def __init__(self):
        super().__init__()
        self.requests_mock = responses.RequestsMock(
            assert_all_requests_are_fired=False)
        self.requests_mock.add_passthru("http://localhost")

    def _setUp(self):
        self.requests_mock.start()
        self.addCleanup(self.requests_mock.stop)
        self.addCleanup(self.requests_mock.reset)


class ChrootedKazooFixture(fixtures.Fixture):
    def __init__(self, test_id, random_databases, delete_databases):
        super(ChrootedKazooFixture, self).__init__()

        if 'ZOOKEEPER_2181_TCP' in os.environ:
            # prevent any nasty hobbits^H^H^H suprises
            if 'ZUUL_ZK_HOST' in os.environ:
                raise Exception(
                    'Looks like tox-docker is being used but you have also '
                    'configured ZUUL_ZK_HOST. Either avoid using the '
                    'docker environment or unset ZUUL_ZK_HOST.')

            zk_host = 'localhost:' + os.environ['ZUUL_2181_TCP']
        elif 'ZUUL_ZK_HOST' in os.environ:
            zk_host = os.environ['ZUUL_ZK_HOST']
        else:
            zk_host = 'localhost'

        if ':' in zk_host:
            host, port = zk_host.split(':')
        else:
            host = zk_host
            port = None

        zk_ca = os.environ.get('ZUUL_ZK_CA', None)
        if not zk_ca:
            zk_ca = os.path.join(os.path.dirname(__file__),
                                 '../tools/ca/certs/cacert.pem')
        self.zookeeper_ca = zk_ca
        zk_cert = os.environ.get('ZUUL_ZK_CERT', None)
        if not zk_cert:
            zk_cert = os.path.join(os.path.dirname(__file__),
                                   '../tools/ca/certs/client.pem')
        self.zookeeper_cert = zk_cert
        zk_key = os.environ.get('ZUUL_ZK_KEY', None)
        if not zk_key:
            zk_key = os.path.join(os.path.dirname(__file__),
                                  '../tools/ca/keys/clientkey.pem')
        self.zookeeper_key = zk_key
        self.zookeeper_host = host

        if not port:
            self.zookeeper_port = 2281
        else:
            self.zookeeper_port = int(port)

        self.test_id = test_id
        self.random_databases = random_databases
        self.delete_databases = delete_databases

    def _setUp(self):
        if self.random_databases:
            # Make sure the test chroot paths do not conflict
            random_bits = ''.join(random.choice(string.ascii_lowercase +
                                                string.ascii_uppercase)
                                  for x in range(8))

            test_path = '%s_%s_%s' % (random_bits, os.getpid(), self.test_id)
        else:
            test_path = self.test_id.split('.')[-1]
        self.zookeeper_chroot = f"/test/{test_path}"

        self.zk_hosts = '%s:%s%s' % (
            self.zookeeper_host,
            self.zookeeper_port,
            self.zookeeper_chroot)

        if self.delete_databases:
            self.addCleanup(self._cleanup)

        # Ensure the chroot path exists and clean up any pre-existing znodes.
        _tmp_client = kazoo.client.KazooClient(
            hosts=f'{self.zookeeper_host}:{self.zookeeper_port}',
            use_ssl=True,
            keyfile=self.zookeeper_key,
            certfile=self.zookeeper_cert,
            ca=self.zookeeper_ca,
            timeout=ZOOKEEPER_SESSION_TIMEOUT,
        )
        _tmp_client.start()

        if self.random_databases:
            if _tmp_client.exists(self.zookeeper_chroot):
                _tmp_client.delete(self.zookeeper_chroot, recursive=True)

        _tmp_client.ensure_path(self.zookeeper_chroot)
        _tmp_client.stop()
        _tmp_client.close()

    def _cleanup(self):
        '''Remove the chroot path.'''
        # Need a non-chroot'ed client to remove the chroot path
        _tmp_client = kazoo.client.KazooClient(
            hosts='%s:%s' % (self.zookeeper_host, self.zookeeper_port),
            use_ssl=True,
            keyfile=self.zookeeper_key,
            certfile=self.zookeeper_cert,
            ca=self.zookeeper_ca,
            timeout=ZOOKEEPER_SESSION_TIMEOUT,
        )
        _tmp_client.start()
        _tmp_client.delete(self.zookeeper_chroot, recursive=True)
        _tmp_client.stop()
        _tmp_client.close()


class WebProxyFixture(fixtures.Fixture):
    def __init__(self, rules):
        super(WebProxyFixture, self).__init__()
        self.rules = rules

    def _setUp(self):
        rules = self.rules

        class Proxy(http.server.SimpleHTTPRequestHandler):
            log = logging.getLogger('zuul.WebProxyFixture.Proxy')

            def do_GET(self):
                path = self.path
                for (pattern, replace) in rules:
                    path = re.sub(pattern, replace, path)
                resp = requests.get(path)
                self.send_response(resp.status_code)
                if resp.status_code >= 300:
                    self.end_headers()
                    return
                for key, val in resp.headers.items():
                    self.send_header(key, val)
                self.end_headers()
                self.wfile.write(resp.content)

            def log_message(self, fmt, *args):
                self.log.debug(fmt, *args)

        self.httpd = socketserver.ThreadingTCPServer(('', 0), Proxy)
        self.port = self.httpd.socket.getsockname()[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever)
        self.thread.start()
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        self.httpd.shutdown()
        self.thread.join()
        self.httpd.server_close()


class ZuulWebFixture(fixtures.Fixture):
    def __init__(self, config, test_config,
                 additional_event_queues, upstream_root,
                 poller_events, git_url_with_auth, add_cleanup,
                 test_root, info=None, stats_interval=None):
        super(ZuulWebFixture, self).__init__()
        self.config = config
        self.connections = TestConnectionRegistry(
            config, test_config,
            additional_event_queues, upstream_root,
            poller_events, git_url_with_auth, add_cleanup)
        self.connections.configure(config, database=True, sources=True,
                                   triggers=True, reporters=True,
                                   providers=True)

        self.authenticators = zuul.lib.auth.AuthenticatorRegistry()
        self.authenticators.configure(config)
        if info is None:
            self.info = WebInfo.fromConfig(config)
        else:
            self.info = info
        self.test_root = test_root
        self.stats_interval = stats_interval

    def _setUp(self):
        # Start the web server
        self.web = zuul.web.ZuulWeb(
            config=self.config,
            info=self.info,
            connections=self.connections,
            authenticators=self.authenticators)
        self.connections.load(self.web.zk_client, self.web.component_registry)
        if self.stats_interval:
            self.web._stats_interval = IntervalTrigger(
                seconds=self.stats_interval)
        self.web.start()
        self.addCleanup(self.stop)

        self.host = 'localhost'
        # Wait until web server is started
        while True:
            self.port = self.web.port
            try:
                with socket.create_connection((self.host, self.port)):
                    break
            except ConnectionRefusedError:
                pass

    def stop(self):
        self.web.stop()
        self.connections.stop()


class MySQLSchemaFixture(fixtures.Fixture):
    log = logging.getLogger('zuul.test.MySQLSchemaFixture')

    def __init__(self, test_id, random_databases, delete_databases):
        super().__init__()
        self.test_id = test_id
        self.random_databases = random_databases
        self.delete_databases = delete_databases

    def setUp(self):
        super().setUp()

        if self.random_databases:
            random_bits = ''.join(random.choice(string.ascii_lowercase +
                                                string.ascii_uppercase)
                                  for x in range(8))
            self.name = '%s_%s' % (random_bits, os.getpid())
            self.passwd = uuid.uuid4().hex
        else:
            self.name = self.test_id.split('.')[-1]
            self.passwd = self.name
        self.host = os.environ.get('ZUUL_MYSQL_HOST', '127.0.0.1')
        self.port = int(os.environ.get('ZUUL_MYSQL_PORT', 3306))
        self.log.debug("Creating database %s:%s:%s",
                       self.host, self.port, self.name)
        connected = False
        # Set this to True to enable pymyql connection debugging. It is very
        # verbose so we leave it off by default.
        pymysql.connections.DEBUG = False
        try:
            db = pymysql.connect(host=self.host,
                                 port=self.port,
                                 user="openstack_citest",
                                 passwd="openstack_citest",
                                 db="openstack_citest")
            pymysql.connections.DEBUG = False
            connected = True
            with db.cursor() as cur:
                cur.execute("create database %s" % self.name)
                cur.execute(
                    "create user '{user}'@'' identified by '{passwd}'".format(
                        user=self.name, passwd=self.passwd))
                cur.execute("grant all on {name}.* to '{name}'@''".format(
                    name=self.name))
                # Do not flush privileges here. It is only necessary when
                # modifying the db tables directly (INSERT, UPDATE, DELETE)
                # not when using CREATE USER, DROP USER, and/or GRANT.
        except pymysql.err.ProgrammingError as e:
            if e.args[0] == 1007:
                # Database exists
                pass
            else:
                raise
        finally:
            pymysql.connections.DEBUG = False
            if connected:
                db.close()

        self.dburi = 'mariadb+pymysql://{name}:{passwd}@{host}:{port}/{name}'\
            .format(
                name=self.name,
                passwd=self.passwd,
                host=self.host,
                port=self.port
            )
        self.addDetail('dburi', testtools.content.text_content(self.dburi))
        if self.delete_databases:
            self.addCleanup(self.cleanup)

    def cleanup(self):
        self.log.debug("Deleting database %s:%s:%s",
                       self.host, self.port, self.name)
        connected = False
        # Set this to True to enable pymyql connection debugging. It is very
        # verbose so we leave it off by default.
        pymysql.connections.DEBUG = False
        try:
            db = pymysql.connect(host=self.host,
                                 port=self.port,
                                 user="openstack_citest",
                                 passwd="openstack_citest",
                                 db="openstack_citest",
                                 read_timeout=90)
            connected = True
            pymysql.connections.DEBUG = False
            with db.cursor() as cur:
                cur.execute("drop database %s" % self.name)
                cur.execute("drop user '%s'@''" % self.name)
                # Do not flush privileges here. It is only necessary when
                # modifying the db tables directly (INSERT, UPDATE, DELETE)
                # not when using CREATE USER, DROP USER, and/or GRANT.
        finally:
            pymysql.connections.DEBUG = False
            if connected:
                db.close()


class PostgresqlSchemaFixture(fixtures.Fixture):
    def __init__(self, test_id, random_databases, delete_databases):
        super().__init__()
        self.test_id = test_id
        self.random_databases = random_databases
        self.delete_databases = delete_databases

    def setUp(self):
        super().setUp()

        if self.random_databases:
            # Postgres lowercases user and table names during creation but not
            # during authentication. Thus only use lowercase chars.
            random_bits = ''.join(random.choice(string.ascii_lowercase)
                                  for x in range(8))
            self.name = '%s_%s' % (random_bits, os.getpid())
        else:
            self.name = self.test_id.split('.')[-1]
        self.passwd = uuid.uuid4().hex
        self.host = os.environ.get('ZUUL_POSTGRES_HOST', '127.0.0.1')
        db = psycopg2.connect(host=self.host,
                              user="openstack_citest",
                              password="openstack_citest",
                              database="openstack_citest")
        db.autocommit = True
        cur = db.cursor()
        cur.execute("create role %s with login password '%s';" % (
            self.name, self.passwd))
        cur.execute("create database %s OWNER %s TEMPLATE template0 "
                    "ENCODING 'UTF8';" % (self.name, self.name))

        self.dburi = 'postgresql://{name}:{passwd}@{host}/{name}'.format(
            name=self.name, passwd=self.passwd, host=self.host)

        self.addDetail('dburi', testtools.content.text_content(self.dburi))
        if self.delete_databases:
            self.addCleanup(self.cleanup)

    def cleanup(self):
        db = psycopg2.connect(host=self.host,
                              user="openstack_citest",
                              password="openstack_citest",
                              database="openstack_citest")
        db.autocommit = True
        cur = db.cursor()
        cur.execute("drop database %s" % self.name)
        cur.execute("drop user %s" % self.name)


class PrometheusFixture(fixtures.Fixture):
    def _setUp(self):
        # Save a list of collectors which exist at the start of the
        # test (ie, the standard prometheus_client collectors)
        self.collectors = list(
            prometheus_client.registry.REGISTRY._collector_to_names.keys())
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        # Avoid the "Duplicated timeseries in CollectorRegistry" error
        # by removing any collectors added during the test.
        collectors = list(
            prometheus_client.registry.REGISTRY._collector_to_names.keys())
        for collector in collectors:
            if collector not in self.collectors:
                prometheus_client.registry.REGISTRY.unregister(collector)


class GlobalRegistryFixture(fixtures.Fixture):
    def _setUp(self):
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        # Remove our component registry from the global
        COMPONENT_REGISTRY.clearRegistry()


class FakeCPUTimes:
    def __init__(self):
        self.user = 0
        self.system = 0
        self.children_user = 0
        self.children_system = 0


def cpu_times(self):
    return FakeCPUTimes()


class LogExceptionHandler(logging.Handler):
    def __init__(self, loglist):
        super().__init__()
        self.__loglist = loglist

    def emit(self, record):
        if record.exc_info:
            self.__loglist.append(record)


class BaseTestCase(testtools.TestCase):
    log = logging.getLogger("zuul.test")
    wait_timeout = 90
    # These can be unset to use predictable database fixtures that
    # persist across an upgrade functional test run.
    random_databases = True
    delete_databases = True
    use_tmpdir = True
    always_attach_logs = False

    def checkLogs(self, *args):
        for record in self._exception_logs:
            okay = False
            for substr in self.test_config.okay_tracebacks:
                if substr in record.exc_text:
                    okay = True
                    break
            if okay:
                continue
            self.fail(f"Traceback found in logs: {record.msg}")

    def attachLogs(self, *args):
        def reader():
            self._log_stream.seek(0)
            while True:
                x = self._log_stream.read(4096)
                if not x:
                    break
                yield x.encode('utf8')
        content = testtools.content.content_from_reader(
            reader,
            testtools.content_type.UTF8_TEXT,
            False)
        self.addDetail('logging', content)

    def initTestConfig(self):
        # Some tests may need to do this before we setUp
        if not hasattr(self, 'test_config'):
            self.test_config = TestConfig(self)

    def setUp(self):
        super(BaseTestCase, self).setUp()

        self.initTestConfig()

        self.useFixture(PrometheusFixture())
        self.useFixture(GlobalRegistryFixture())
        test_timeout = os.environ.get('OS_TEST_TIMEOUT', 0)
        try:
            test_timeout = int(test_timeout)
        except ValueError:
            # If timeout value is invalid do not set a timeout.
            test_timeout = 0
        if test_timeout > 0:
            # Try a gentle timeout first and as a safety net a hard timeout
            # later.
            self.useFixture(fixtures.Timeout(test_timeout, gentle=True))
            self.useFixture(fixtures.Timeout(test_timeout + 20, gentle=False))

        if not self.test_config.never_capture:
            if (os.environ.get('OS_STDOUT_CAPTURE') == 'True' or
                os.environ.get('OS_STDOUT_CAPTURE') == '1'):
                stdout = self.useFixture(
                    fixtures.StringStream('stdout')).stream
                self.useFixture(fixtures.MonkeyPatch('sys.stdout', stdout))
            if (os.environ.get('OS_STDERR_CAPTURE') == 'True' or
                os.environ.get('OS_STDERR_CAPTURE') == '1'):
                stderr = self.useFixture(
                    fixtures.StringStream('stderr')).stream
                self.useFixture(fixtures.MonkeyPatch('sys.stderr', stderr))
            if (os.environ.get('OS_LOG_CAPTURE') == 'True' or
                os.environ.get('OS_LOG_CAPTURE') == '1'):
                self._log_stream = StringIO()
                if self.always_attach_logs:
                    self.addCleanup(self.attachLogs)
                else:
                    self.addOnException(self.attachLogs)
            else:
                self._log_stream = sys.stdout
        else:
            self._log_stream = sys.stdout

        handler = logging.StreamHandler(self._log_stream)
        formatter = logging.Formatter('%(asctime)s %(name)-32s '
                                      '%(levelname)-8s %(message)s')
        handler.setFormatter(formatter)

        logger = logging.getLogger()
        # It is possible that a stderr log handler is inserted before our
        # addHandler below. If that happens we will emit all logs to stderr
        # even when we don't want to. Error here to make it clear there is
        # a problem as early as possible as it is easy to overlook.
        self.assertEqual(logger.handlers, [])
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)

        # Make sure we don't carry old handlers around in process state
        # which slows down test runs
        self.addCleanup(logger.removeHandler, handler)

        self._exception_logs = []
        log_exc_handler = LogExceptionHandler(self._exception_logs)
        logger.addHandler(log_exc_handler)
        self.addCleanup(self.checkLogs)
        self.addCleanup(logger.removeHandler, log_exc_handler)

        # NOTE(notmorgan): Extract logging overrides for specific
        # libraries from the OS_LOG_DEFAULTS env and create loggers
        # for each. This is used to limit the output during test runs
        # from libraries that zuul depends on.
        log_defaults_from_env = os.environ.get(
            'OS_LOG_DEFAULTS',
            'git.cmd=INFO,'
            'git.util=INFO,'
            'kazoo.client=WARNING,kazoo.recipe=WARNING,'
            'botocore=WARNING'
        )

        if log_defaults_from_env:
            for default in log_defaults_from_env.split(','):
                try:
                    name, level_str = default.split('=', 1)
                    level = getattr(logging, level_str, logging.DEBUG)
                    logger = logging.getLogger(name)
                    logger.setLevel(level)
                    logger.addHandler(handler)
                    self.addCleanup(logger.removeHandler, handler)
                    logger.propagate = False
                except ValueError:
                    # NOTE(notmorgan): Invalid format of the log default,
                    # skip and don't try and apply a logger for the
                    # specified module
                    pass
        self.addCleanup(handler.close)
        self.addCleanup(handler.flush)

        if sys.platform == 'darwin':
            # Popen.cpu_times() is broken on darwin so patch it with a fake.
            Popen.cpu_times = cpu_times

    def setupZK(self):
        self.zk_chroot_fixture = self.useFixture(
            ChrootedKazooFixture(self.id(),
                                 self.random_databases,
                                 self.delete_databases,
                                 ))

    def getZKWatches(self):
        # TODO: The client.command method used here returns only the
        # first 8k of data.  That means this method can return {} when
        # there actually are watches (and this happens in practice in
        # heavily loaded test environments).  We should replace that
        # method with something more robust.
        chroot = self.zk_chroot_fixture.zookeeper_chroot
        data = self.zk_client.client.command(b'wchp')
        ret = {}
        sessions = None
        for line in data.split('\n'):
            if line.startswith('\t'):
                if sessions is not None:
                    sessions.append(line.strip())
            else:
                line = line.strip()
                if not line:
                    continue
                if line.startswith(chroot):
                    line = line[len(chroot):]
                    sessions = []
                    ret[line] = sessions
                else:
                    sessions = None
        return ret

    def getZKTree(self, path, ret=None):
        """Return the contents of a ZK tree as a dictionary"""
        if ret is None:
            ret = {}
        for key in self.zk_client.client.get_children(path):
            subpath = os.path.join(path, key)
            ret[subpath] = self.zk_client.client.get(
                os.path.join(path, key))[0]
            self.getZKTree(subpath, ret)
        return ret

    def getZKPaths(self, path):
        return list(self.getZKTree(path).keys())

    def getZKObject(self, path):
        compressed_data, zstat = self.zk_client.client.get(path)
        try:
            data = zlib.decompress(compressed_data)
        except zlib.error:
            # Fallback for old, uncompressed data
            data = compressed_data
        return data

    def setupModelPin(self):
        # Add a fake scheduler to the system that is on the old model
        # version.
        test_name = self.id().split('.')[-1]
        test = getattr(self, test_name)
        if hasattr(test, '__model_version__'):
            version = getattr(test, '__model_version__')
            self.model_test_component_info = SchedulerComponent(
                self.zk_client, 'test_component')
            self.model_test_component_info.register(version)


class SymLink(object):
    def __init__(self, target):
        self.target = target


class SchedulerTestApp:
    def __init__(self, log, config, test_config,
                 additional_event_queues,
                 upstream_root, poller_events,
                 git_url_with_auth, add_cleanup, validate_tenants,
                 wait_for_init, disable_pipelines, instance_id):
        self.log = log
        self.config = config
        self.test_config = test_config
        self.validate_tenants = validate_tenants
        self.wait_for_init = wait_for_init
        self.disable_pipelines = disable_pipelines

        # Register connections from the config using fakes
        self.connections = TestConnectionRegistry(
            self.config,
            self.test_config,
            additional_event_queues,
            upstream_root,
            poller_events,
            git_url_with_auth,
            add_cleanup,
        )
        self.connections.configure(self.config, database=True,
                                   sources=True, triggers=True,
                                   reporters=True, providers=True)

        self.sched = TestScheduler(self.config, self.connections, self,
                                   wait_for_init, disable_pipelines)
        self.sched.log = logging.getLogger(f"zuul.Scheduler-{instance_id}")
        self.sched._stats_interval = 1

        if validate_tenants is None:
            self.connections.registerScheduler(self.sched)
            self.connections.load(self.sched.zk_client,
                                  self.sched.component_registry)

        # TODO (swestphahl): Can be removed when we no longer use global
        # management events.
        self.event_queues = [
            self.sched.reconfigure_event_queue,
        ]

    def start(self, validate_tenants=None):
        if validate_tenants is None:
            self.sched.start()
            self.sched.prime(self.config)
        else:
            self.sched.validateTenants(self.config, validate_tenants)

    def fullReconfigure(self, command_socket=False):
        try:
            if command_socket:
                command_socket = self.sched.config.get(
                    'scheduler', 'command_socket')
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                    s.connect(command_socket)
                    s.sendall('full-reconfigure\n'.encode('utf8'))
            else:
                self.sched.reconfigure(self.config)
        except Exception:
            self.log.exception("Reconfiguration failed:")

    def smartReconfigure(self, command_socket=False):
        try:
            if command_socket:
                command_socket = self.sched.config.get(
                    'scheduler', 'command_socket')
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                    s.connect(command_socket)
                    s.sendall('smart-reconfigure\n'.encode('utf8'))
            else:
                self.sched.reconfigure(self.config, smart=True)
        except Exception:
            self.log.exception("Reconfiguration failed:")

    def tenantReconfigure(self, tenants, command_socket=False):
        try:
            if command_socket:
                command_socket = self.sched.config.get(
                    'scheduler', 'command_socket')
                args = json.dumps(tenants)
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                    s.connect(command_socket)
                    s.sendall(f'tenant-reconfigure {args}\n'.
                              encode('utf8'))
            else:
                self.sched.reconfigure(
                    self.config, smart=False, tenants=tenants)
        except Exception:
            self.log.exception("Reconfiguration failed:")


class SchedulerTestManager:
    def __init__(self, validate_tenants, wait_for_init, disable_pipelines):
        self.instances = []

    def create(self, log, config, test_config, additional_event_queues,
               upstream_root, poller_events, git_url_with_auth,
               add_cleanup, validate_tenants, wait_for_init,
               disable_pipelines):
        scheduler_config = copyConfig(config)
        instance_id = len(self.instances)
        # Ensure a unique command socket per scheduler instance
        command_socket = os.path.join(
            os.path.dirname(config.get("scheduler", "command_socket")),
            f"scheduler-{instance_id}.socket"
        )
        scheduler_config.set("scheduler", "command_socket", command_socket)

        app = SchedulerTestApp(log, scheduler_config, test_config,
                               additional_event_queues, upstream_root,
                               poller_events, git_url_with_auth,
                               add_cleanup, validate_tenants,
                               wait_for_init, disable_pipelines,
                               instance_id)
        self.instances.append(app)
        return app

    def __len__(self):
        return len(self.instances)

    def __getitem__(self, item):
        return self.instances[item]

    def __setitem__(self, key, value):
        raise Exception("Not implemented, use create method!")

    def __delitem__(self, key):
        del self.instances[key]

    def __iter__(self):
        return iter(self.instances)

    @property
    def first(self):
        if len(self.instances) == 0:
            raise Exception("No scheduler!")
        return self.instances[0]

    def filter(self, matcher=None):
        thefcn = None
        if type(matcher) is list:
            def fcn(_, app):
                return app in matcher
            thefcn = fcn
        elif type(matcher).__name__ == 'function':
            thefcn = matcher
        return [e[1] for e in enumerate(self.instances)
                if thefcn is None or thefcn(e[0], e[1])]

    def execute(self, function, matcher=None):
        for instance in self.filter(matcher):
            function(instance)


class DriverTestConfig:
    def __init__(self, test_config):
        self.test_config = test_config

    def __getattr__(self, name):
        if name in self.test_config.driver_config:
            return self.test_config.driver_config[name]
        return {}


class TestConfig:
    def __init__(self, testobj):
        test_name = testobj.id().split('.')[-1]
        test = getattr(testobj, test_name)
        default_okay_tracebacks = [
            # We log git merge errors at debug level with tracebacks;
            # these are typically safe to ignore
            'ERROR: content conflict',
            'mapLines',
            # These errors occasionally show up on legitimate tests
            # due to race conditions and timing.  They are recoverable
            # errors.  It would be nice if they didn't happen, but
            # until we understand them more, we can't fail on them.
            'RolledBackError',
            'manager.change_list.refresh',
            'kazoo.exceptions.ConnectionClosedError',
        ]
        self.simple_layout = getattr(test, '__simple_layout__', None)
        self.gerrit_config = getattr(test, '__gerrit_config__', {})
        self.never_capture = getattr(test, '__never_capture__', None)
        self.okay_tracebacks = getattr(test, '__okay_tracebacks__',
                                       default_okay_tracebacks)
        self.enable_nodepool = getattr(test, '__enable_nodepool__', False)
        self.return_data = getattr(test, '__return_data__', [])
        self.driver_config = getattr(test, '__driver_config__', {})
        self.zuul_config = getattr(test, '__zuul_config__', {})
        self.driver = DriverTestConfig(self)
        self.changes = FakeChangeDB()


class ZuulTestCase(BaseTestCase):
    """A test case with a functioning Zuul.

    The following class variables are used during test setup and can
    be overidden by subclasses but are effectively read-only once a
    test method starts running:

    :cvar str config_file: This points to the main zuul config file
        within the fixtures directory.  Subclasses may override this
        to obtain a different behavior.

    :cvar str tenant_config_file: This is the tenant config file
        (which specifies from what git repos the configuration should
        be loaded).  It defaults to the value specified in
        `config_file` but can be overidden by subclasses to obtain a
        different tenant/project layout while using the standard main
        configuration.  See also the :py:func:`simple_layout`
        decorator.

    :cvar str tenant_config_script_file: This is the tenant config script
        file. This attribute has the same meaning than tenant_config_file
        except that the tenant configuration is loaded from a script.
        When this attribute is set then tenant_config_file is ignored
        by the scheduler.

    :cvar bool create_project_keys: Indicates whether Zuul should
        auto-generate keys for each project, or whether the test
        infrastructure should insert dummy keys to save time during
        startup.  Defaults to False.

    :cvar int log_console_port: The zuul_stream/zuul_console port.

    The following are instance variables that are useful within test
    methods:

    :ivar FakeGerritConnection fake_<connection>:
        A :py:class:`~tests.base.FakeGerritConnection` will be
        instantiated for each connection present in the config file
        and stored here.  For instance, `fake_gerrit` will hold the
        FakeGerritConnection object for a connection named `gerrit`.

    :ivar RecordingExecutorServer executor_server: An instance of
        :py:class:`~tests.base.RecordingExecutorServer` which is the
        Ansible execute server used to run jobs for this test.

    :ivar list builds: A list of :py:class:`~tests.base.FakeBuild` objects
        representing currently running builds.  They are appended to
        the list in the order they are executed, and removed from this
        list upon completion.

    :ivar list history: A list of :py:class:`~tests.base.BuildHistory`
        objects representing completed builds.  They are appended to
        the list in the order they complete.

    """

    config_file: str = 'zuul.conf'
    run_ansible: bool = False
    create_project_keys: bool = False
    use_ssl: bool = False
    git_url_with_auth: bool = False
    log_console_port: int = 19885
    validate_tenants = None
    wait_for_init = None
    disable_pipelines = False
    scheduler_count = SCHEDULER_COUNT
    init_repos = True
    load_change_db = False

    def __getattr__(self, name):
        """Allows to access fake connections the old way, e.g., using
        `fake_gerrit` for FakeGerritConnection.

        This will access the connection of the first (default) scheduler
        (`self.scheds.first`). To access connections of a different
        scheduler use `self.scheds[{X}].connections.fake_{NAME}`.
        """
        if name.startswith('fake_') and\
                hasattr(self.scheds.first.connections, name):
            return getattr(self.scheds.first.connections, name)
        raise AttributeError("'ZuulTestCase' object has no attribute '%s'"
                             % name)

    def _startMerger(self):
        self.merge_server = zuul.merger.server.MergeServer(
            self.config, self.scheds.first.connections
        )
        self.merge_server.start()

    def setUp(self):
        super(ZuulTestCase, self).setUp()

        self.setupZK()
        self.fake_nodepool = FakeNodepool(self.zk_chroot_fixture)

        if self.use_tmpdir:
            if not KEEP_TEMPDIRS:
                tmp_root = self.useFixture(fixtures.TempDir(
                    rootdir=os.environ.get("ZUUL_TEST_ROOT")
                )).path
            else:
                tmp_root = tempfile.mkdtemp(
                    dir=os.environ.get("ZUUL_TEST_ROOT", None))
        else:
            tmp_root = os.environ.get("ZUUL_TEST_ROOT", '/tmp')
        self.test_root = os.path.join(tmp_root, "zuul-test")
        self.upstream_root = os.path.join(self.test_root, "upstream")
        self.merger_src_root = os.path.join(self.test_root, "merger-git")
        self.executor_src_root = os.path.join(self.test_root, "executor-git")
        self.state_root = os.path.join(self.test_root, "lib")
        self.merger_state_root = os.path.join(self.test_root, "merger-lib")
        self.executor_state_root = os.path.join(self.test_root, "executor-lib")
        self.jobdir_root = os.path.join(self.test_root, "builds")

        if os.path.exists(self.test_root) and self.init_repos:
            shutil.rmtree(self.test_root)
        os.makedirs(self.test_root, exist_ok=True)
        os.makedirs(self.upstream_root, exist_ok=True)
        os.makedirs(self.state_root, exist_ok=True)
        os.makedirs(self.merger_state_root, exist_ok=True)
        os.makedirs(self.executor_state_root, exist_ok=True)
        os.makedirs(self.jobdir_root, exist_ok=True)

        # Make per test copy of Configuration.
        self.config = self.setup_config(self.config_file)
        self.private_key_file = os.path.join(self.test_root, 'test_id_rsa')
        if not os.path.exists(self.private_key_file):
            src_private_key_file = os.environ.get(
                'ZUUL_SSH_KEY',
                os.path.join(FIXTURE_DIR, 'test_id_rsa'))
            shutil.copy(src_private_key_file, self.private_key_file)
            shutil.copy('{}.pub'.format(src_private_key_file),
                        '{}.pub'.format(self.private_key_file))
            os.chmod(self.private_key_file, 0o0600)
        for cfg_attr in ('tenant_config', 'tenant_config_script'):
            if self.config.has_option('scheduler', cfg_attr):
                cfg_value = self.config.get('scheduler', cfg_attr)
                self.config.set(
                    'scheduler', cfg_attr,
                    os.path.join(FIXTURE_DIR, cfg_value))

        self.config.set('scheduler', 'state_dir', self.state_root)
        self.config.set(
            'scheduler', 'command_socket',
            os.path.join(self.test_root, 'scheduler.socket'))
        if not self.config.has_option("keystore", "password"):
            self.config.set("keystore", "password", 'keystorepassword')
        self.config.set('merger', 'git_dir', self.merger_src_root)
        self.config.set('executor', 'git_dir', self.executor_src_root)
        self.config.set('executor', 'private_key_file', self.private_key_file)
        self.config.set('executor', 'state_dir', self.executor_state_root)
        self.config.set(
            'executor', 'command_socket',
            os.path.join(self.test_root, 'executor.socket'))
        self.config.set(
            'merger', 'command_socket',
            os.path.join(self.test_root, 'merger.socket'))
        self.config.set('web', 'listen_address', '::')
        self.config.set('web', 'port', '0')
        self.config.set(
            'web', 'command_socket',
            os.path.join(self.test_root, 'web.socket'))
        self.config.set(
            'launcher', 'command_socket',
            os.path.join(self.test_root, 'launcher.socket'))
        self.config.set('launcher', 'temp_dir', self.test_root)

        self.statsd = FakeStatsd()
        if self.config.has_section('statsd'):
            self.config.set('statsd', 'port', str(self.statsd.port))
        self.statsd.start()

        self.config.set('zookeeper', 'hosts', self.zk_chroot_fixture.zk_hosts)
        self.config.set('zookeeper', 'session_timeout',
                        str(ZOOKEEPER_SESSION_TIMEOUT))
        self.config.set('zookeeper', 'tls_cert',
                        self.zk_chroot_fixture.zookeeper_cert)
        self.config.set('zookeeper', 'tls_key',
                        self.zk_chroot_fixture.zookeeper_key)
        self.config.set('zookeeper', 'tls_ca',
                        self.zk_chroot_fixture.zookeeper_ca)

        # Speed up operations in tests
        gerritsource.GerritSource.replication_timeout = 1.5
        gerritsource.GerritSource.replication_retry_interval = 0.5
        zuul.driver.aws.awsendpoint.CACHE_TTL = 0.1
        zuul.driver.openstack.openstackendpoint.CACHE_TTL = 0.1
        zuul.launcher.server.NodescanWorker.TIMEOUT = 0.1
        zuul.launcher.server.Launcher.MAX_SLEEP = 0.1

        if self.load_change_db:
            self.loadChangeDB()

        self.additional_event_queues = []
        self.zk_client = ZooKeeperClient.fromConfig(self.config)
        self.zk_client.connect()

        self.setupModelPin()

        self._context_lock = SessionAwareLock(
            self.zk_client.client, f"/test/{uuid.uuid4().hex}")

        self.connection_event_queues = DefaultKeyDict(
            lambda cn: ConnectionEventQueue(self.zk_client, cn, None)
        )
        # requires zk client
        self.setupAllProjectKeys(self.config)

        self.poller_events = {}
        self._configureSmtp()
        self._configureMqtt()
        self._configureElasticsearch()

        executor_connections = TestConnectionRegistry(
            self.config, self.test_config,
            self.additional_event_queues,
            self.upstream_root, self.poller_events,
            self.git_url_with_auth, self.addCleanup)
        executor_connections.configure(self.config, sources=True)
        self.executor_api = TestingExecutorApi(self.zk_client)
        self.merger_api = TestingMergerApi(self.zk_client)
        self.executor_server = RecordingExecutorServer(
            self.config,
            executor_connections,
            jobdir_root=self.jobdir_root,
            _run_ansible=self.run_ansible,
            _test_root=self.test_root,
            keep_jobdir=KEEP_TEMPDIRS,
            log_console_port=self.log_console_port)
        for return_data in self.test_config.return_data:
            self.executor_server.returnData(
                return_data['job'],
                return_data['ref'],
                return_data['data'],
            )
        self.executor_server.start()
        self.history = self.executor_server.build_history
        self.builds = self.executor_server.running_builds

        self.launcher = self.createLauncher()

        self.scheds = SchedulerTestManager(self.validate_tenants,
                                           self.wait_for_init,
                                           self.disable_pipelines)
        for _ in range(self.scheduler_count):
            self.createScheduler()

        self.merge_server = None

        # Cleanups are run in reverse order
        self.addCleanup(self.assertCleanShutdown)
        self.addCleanup(self.shutdown)
        self.addCleanup(self.assertFinalState)

        self.scheds.execute(
            lambda app: app.start(self.validate_tenants))

    def createScheduler(self):
        return self.scheds.create(
            self.log, self.config, self.test_config,
            self.additional_event_queues, self.upstream_root,
            self.poller_events, self.git_url_with_auth,
            self.addCleanup, self.validate_tenants, self.wait_for_init,
            self.disable_pipelines)

    def createLauncher(self, instance_id=0, connection_filter=None):
        launcher_config = copyConfig(self.config)
        # Ensure a unique command socket per instance
        command_socket = os.path.join(
            os.path.dirname(launcher_config.get("launcher", "command_socket")),
            f"launcher-{instance_id}.socket"
        )
        launcher_config.set("launcher", "command_socket", command_socket)
        hostname = f"launcher-{instance_id}"

        if connection_filter:
            launcher_config.set(
                "launcher", "connection_filter", connection_filter)

        launcher_connections = TestConnectionRegistry(
            launcher_config, self.test_config,
            self.additional_event_queues,
            self.upstream_root, self.poller_events,
            self.git_url_with_auth, self.addCleanup)
        launcher_connections.configure(launcher_config, providers=True)
        launcher = TestLauncher(
            launcher_config,
            launcher_connections,
            _hostname=hostname)
        launcher._start_cleanup = False
        launcher._stats_interval = 1
        launcher.start()
        return launcher

    def createZKContext(self, lock=None):
        if lock is None:
            # Just make sure the lock is acquired
            self._context_lock.acquire(blocking=False)
            lock = self._context_lock
        return zkobject.ZKContext(self.zk_client, lock,
                                  None, self.log)

    def __event_queues(self, matcher) -> List[Queue]:
        # TODO (swestphahl): Can be removed when we no longer use global
        # management events.
        sched_queues = map(lambda app: app.event_queues,
                           self.scheds.filter(matcher))
        return [item for sublist in sched_queues for item in sublist] + \
            self.additional_event_queues

    def _configureSmtp(self):
        # Set up smtp related fakes
        # TODO(jhesketh): This should come from lib.connections for better
        # coverage
        # Register connections from the config
        self.smtp_messages = []

        def FakeSMTPFactory(*args, **kw):
            args = [self.smtp_messages] + list(args)
            return FakeSMTP(*args, **kw)

        self.useFixture(fixtures.MonkeyPatch('smtplib.SMTP', FakeSMTPFactory))

    def _configureMqtt(self):
        # Set up mqtt related fakes
        self.mqtt_messages = []

        def fakeMQTTPublish(_, topic, msg, qos, zuul_event_id):
            log = logging.getLogger('zuul.FakeMQTTPubish')
            log.info('Publishing message via mqtt')
            self.mqtt_messages.append({'topic': topic, 'msg': msg, 'qos': qos})
        self.useFixture(fixtures.MonkeyPatch(
            'zuul.driver.mqtt.mqttconnection.MQTTConnection.publish',
            fakeMQTTPublish))

    def _configureElasticsearch(self):
        # Set up Elasticsearch related fakes
        def getElasticsearchConnection(driver, name, config):
            con = FakeElasticsearchConnection(
                driver, name, config)
            return con

        self.useFixture(fixtures.MonkeyPatch(
            'zuul.driver.elasticsearch.ElasticsearchDriver.getConnection',
            getElasticsearchConnection))

    def setup_config(self, config_file: str):
        # This creates the per-test configuration object.  It can be
        # overridden by subclasses, but should not need to be since it
        # obeys the config_file and tenant_config_file attributes.
        config = configparser.ConfigParser()
        config.read(os.path.join(FIXTURE_DIR, config_file))

        sections = [
            'zuul', 'scheduler', 'executor', 'merger', 'web', 'launcher',
            'zookeeper', 'keystore', 'database',
        ]
        for section in sections:
            if not config.has_section(section):
                config.add_section(section)

        def _setup_fixture(config, section_name):
            if (config.get(section_name, 'dburi') ==
                    '$MYSQL_FIXTURE_DBURI$'):
                f = MySQLSchemaFixture(self.id(), self.random_databases,
                                       self.delete_databases)
                self.useFixture(f)
                config.set(section_name, 'dburi', f.dburi)
            elif (config.get(section_name, 'dburi') ==
                  '$POSTGRESQL_FIXTURE_DBURI$'):
                f = PostgresqlSchemaFixture(self.id(), self.random_databases,
                                            self.delete_databases)
                self.useFixture(f)
                config.set(section_name, 'dburi', f.dburi)

        for section_name in config.sections():
            con_match = re.match(r'^connection ([\'\"]?)(.*)(\1)$',
                                 section_name, re.I)
            if not con_match:
                continue

            if config.get(section_name, 'driver') == 'sql':
                _setup_fixture(config, section_name)

        if 'database' in config.sections():
            _setup_fixture(config, 'database')

        if 'tracing' in config.sections():
            self.otlp = OTLPFixture()
            self.useFixture(self.otlp)
            self.useFixture(fixtures.MonkeyPatch(
                'zuul.lib.tracing.Tracing.processor_class',
                opentelemetry.sdk.trace.export.SimpleSpanProcessor))
            config.set('tracing', 'endpoint',
                       f'http://localhost:{self.otlp.port}')

        if not self.setupSimpleLayout(config):
            tenant_config = None
            for cfg_attr in ('tenant_config', 'tenant_config_script'):
                if hasattr(self, cfg_attr + '_file'):
                    if getattr(self, cfg_attr + '_file'):
                        value = getattr(self, cfg_attr + '_file')
                        config.set('scheduler', cfg_attr, value)
                        tenant_config = value
                    else:
                        config.remove_option('scheduler', cfg_attr)

            if tenant_config:
                if self.init_repos:
                    git_path = os.path.join(
                        os.path.dirname(
                            os.path.join(FIXTURE_DIR, tenant_config)),
                        'git')
                    if os.path.exists(git_path):
                        for reponame in os.listdir(git_path):
                            project = reponame.replace('_', '/')
                            self.copyDirToRepo(
                                project,
                                os.path.join(git_path, reponame))
        for section, section_dict in self.test_config.zuul_config.items():
            for k, v in section_dict.items():
                config.set(section, k, v)
        return config

    def setupSimpleLayout(self, config):
        # If the test method has been decorated with a simple_layout,
        # use that instead of the class tenant_config_file.  Set up a
        # single config-project with the specified layout, and
        # initialize repos for all of the 'project' entries which
        # appear in the layout.
        if not self.test_config.simple_layout:
            return False
        path, driver, replace = self.test_config.simple_layout

        files = {}
        path = os.path.join(FIXTURE_DIR, path)
        with open(path) as f:
            data = f.read()
            if replace:
                kw = replace(self)
                data = data.format(**kw)
            layout = yaml.safe_load(data)
            files['zuul.yaml'] = data
        config_projects = []
        types = list(zuul.configloader.ZuulSafeLoader.zuul_node_types)
        types.remove('pragma')
        if self.test_config.enable_nodepool:
            config_projects.append({
                'org/common-config': {
                    'include': types,
                }
            })
        else:
            config_projects.append('org/common-config')

        untrusted_projects = []
        for item in layout:
            if 'project' in item:
                name = item['project']['name']
                if name.startswith('^'):
                    continue
                if name == 'org/common-config':
                    continue
                if name in untrusted_projects:
                    continue
                if self.test_config.enable_nodepool:
                    untrusted_projects.append({
                        name: {
                            'include': types,
                        }
                    })
                else:
                    untrusted_projects.append(name)
                if self.init_repos:
                    self.init_repo(name)
                    self.addCommitToRepo(name, 'initial commit',
                                         files={'README': ''},
                                         branch='master', tag='init')
            if 'job' in item:
                if 'run' in item['job']:
                    files['%s' % item['job']['run']] = ''
                for fn in zuul.configloader.as_list(
                        item['job'].get('pre-run', [])):
                    files['%s' % fn] = ''
                for fn in zuul.configloader.as_list(
                        item['job'].get('post-run', [])):
                    files['%s' % fn] = ''

        root = os.path.join(self.test_root, "config")
        if not os.path.exists(root):
            os.makedirs(root)
        f = tempfile.NamedTemporaryFile(dir=root, delete=False)
        temp_config = [{
            'tenant': {
                'name': 'tenant-one',
                'source': {
                    driver: {
                        'config-projects': config_projects,
                        'untrusted-projects': untrusted_projects}}}}]
        f.write(yaml.dump(temp_config).encode('utf8'))
        f.close()
        config.set('scheduler', 'tenant_config',
                   os.path.join(FIXTURE_DIR, f.name))

        if self.init_repos:
            self.init_repo('org/common-config')
            self.addCommitToRepo('org/common-config',
                                 'add content from fixture',
                                 files, branch='master', tag='init')

        return True

    def setupAllProjectKeys(self, config: ConfigParser):
        if self.create_project_keys:
            return

        path = config.get('scheduler', 'tenant_config')
        with open(os.path.join(FIXTURE_DIR, path)) as f:
            tenant_config = yaml.safe_load(f.read())
        for tenant in tenant_config:
            if 'tenant' not in tenant.keys():
                continue
            sources = tenant['tenant']['source']
            self.setupTenantKeys(tenant['tenant']['name'])
            for source, conf in sources.items():
                for project in conf.get('config-projects', []):
                    self.setupProjectKeys(source, project)
                for project in conf.get('untrusted-projects', []):
                    self.setupProjectKeys(source, project)

    def setupProjectKeys(self, source, project):
        # Make sure we set up an RSA key for the project so that we
        # don't spend time generating one:
        if isinstance(project, dict):
            project = list(project.keys())[0]

        password = self.config.get("keystore", "password")
        keystore = zuul.lib.keystorage.KeyStorage(
            self.zk_client, password=password, start_cache=False)
        import_keys = {}
        import_data = {'keys': import_keys}

        path = keystore.getProjectSecretsKeysPath(source, project)
        with open(os.path.join(FIXTURE_DIR, 'secrets.json'), 'rb') as i:
            import_keys[path] = json.load(i)

        # ssh key
        path = keystore.getProjectSSHKeysPath(source, project)
        with open(os.path.join(FIXTURE_DIR, 'ssh.json'), 'rb') as i:
            import_keys[path] = json.load(i)

        keystore.importKeys(import_data, False)
        keystore.stop()

    def setupTenantKeys(self, tenant_name):
        # Make sure we set up an RSA key for the tenant so that we
        # don't spend time generating one:
        password = self.config.get("keystore", "password")
        keystore = zuul.lib.keystorage.KeyStorage(
            self.zk_client, password=password, start_cache=False)
        import_keys = {}
        import_data = {'keys': import_keys}

        # ssh key
        path = keystore.getTenantSSHKeysPath(tenant_name)
        with open(os.path.join(FIXTURE_DIR, 'ssh.json'), 'rb') as i:
            import_keys[path] = json.load(i)

        keystore.importKeys(import_data, False)
        keystore.stop()

    def copyDirToRepo(self, project, source_path):
        self.init_repo(project)

        files = {}
        for (dirpath, dirnames, filenames) in os.walk(source_path):
            for filename in filenames:
                test_tree_filepath = os.path.join(dirpath, filename)
                common_path = os.path.commonprefix([test_tree_filepath,
                                                    source_path])
                relative_filepath = test_tree_filepath[len(common_path) + 1:]
                with open(test_tree_filepath, 'rb') as f:
                    content = f.read()
                    # dynamically create symlinks if the content is of the form
                    # symlink: <target>
                    match = re.match(rb'symlink: ([^\s]+)', content)
                    if match:
                        content = SymLink(match.group(1))

                files[relative_filepath] = content
        self.addCommitToRepo(project, 'add content from fixture',
                             files, branch='master', tag='init')

    def addImageBuildEvent(self, tenant_name, project_canonical_name,
                           branch, image_names):
        project_hostname, project_name = \
            project_canonical_name.split('/', 1)
        driver = self.launcher.connections.drivers['zuul']
        event = driver.getImageBuildEvent(
            list(image_names), project_hostname, project_name, branch)
        self.log.info("Submitting image build event for %s %s",
                      tenant_name, image_names)
        self.launcher.trigger_events[tenant_name].put(
            event.trigger_name, event)

    def assertNodepoolState(self):
        # Make sure that there are no pending requests

        requests = None
        for x in iterate_timeout(30, "zk getNodeRequests"):
            try:
                requests = self.fake_nodepool.getNodeRequests()
                break
            except kazoo.exceptions.ConnectionLoss:
                # NOTE(pabelanger): We lost access to zookeeper, iterate again
                pass
        self.assertEqual(len(requests), 0)

        nodes = None

        for x in iterate_timeout(30, "zk getNodeRequests"):
            try:
                nodes = self.fake_nodepool.getNodes()
                break
            except kazoo.exceptions.ConnectionLoss:
                # NOTE(pabelanger): We lost access to zookeeper, iterate again
                pass

        for node in nodes:
            self.assertFalse(node['_lock'], "Node %s is locked" %
                             (node['_oid'],))

    def assertNoGeneratedKeys(self):
        # Make sure that Zuul did not generate any project keys
        # (unless it was supposed to).

        if self.create_project_keys:
            return

        test_keys = []
        key_fns = ['private.pem', 'ssh.pem']
        for fn in key_fns:
            with open(os.path.join(FIXTURE_DIR, fn)) as i:
                test_keys.append(i.read())

        key_root = os.path.join(self.state_root, 'keys')
        for root, dirname, files in os.walk(key_root):
            for fn in files:
                if fn == '.version':
                    continue
                with open(os.path.join(root, fn)) as f:
                    self.assertTrue(f.read() in test_keys)

    def assertSQLState(self):
        reporter = self.scheds.first.connections.getSqlReporter(None)
        with self.scheds.first.connections.getSqlConnection().\
             engine.connect() as conn:

            try:
                result = conn.execute(
                    sqlalchemy.sql.select(
                        reporter.connection.zuul_buildset_table)
                )
            except sqlalchemy.exc.ProgrammingError:
                # Table doesn't exist
                return

            for buildset in result.fetchall():
                self.assertIsNotNone(buildset.result)

            result = conn.execute(
                sqlalchemy.sql.select(reporter.connection.zuul_build_table)
            )

            for build in result.fetchall():
                self.assertIsNotNone(build.result)
                self.assertIsNotNone(build.start_time)
                self.assertIsNotNone(build.end_time)

    def assertFinalState(self):
        self.log.debug("Assert final state")
        # Make sure no jobs are running
        self.assertEqual({}, self.executor_server.job_workers)
        # Make sure that git.Repo objects have been garbage collected.
        gc.disable()
        try:
            gc.collect()
            for obj in gc.get_objects():
                if isinstance(obj, git.Repo):
                    self.log.debug("Leaked git repo object: 0x%x %s" %
                                   (id(obj), repr(obj)))
        finally:
            gc.enable()
        if len(self.scheds) > 1:
            self.refreshPipelines(self.scheds.first.sched)
        self.assertEmptyQueues()
        self.assertNodepoolState()
        self.assertNoGeneratedKeys()
        self.assertSQLState()
        self.assertCleanZooKeeper()
        ipm = zuul.manager.independent.IndependentPipelineManager
        for tenant in self.scheds.first.sched.abide.tenants.values():
            for manager in tenant.layout.pipeline_managers.values():
                if isinstance(manager, ipm):
                    self.assertEqual(len(manager.state.queues), 0)

    def getAllItems(self, tenant_name, pipeline_name):
        tenant = self.scheds.first.sched.abide.tenants.get(tenant_name)
        manager = tenant.layout.pipeline_managers[pipeline_name]
        items = manager.state.getAllItems()
        return items

    def getAllQueues(self, tenant_name, pipeline_name):
        tenant = self.scheds.first.sched.abide.tenants.get(tenant_name)
        manager = tenant.layout.pipeline_managers[pipeline_name]
        return manager.state.queues

    def shutdown(self):
        # Note: when making changes to this sequence, check if
        # corresponding changes need to happen in
        # tests/upgrade/test_upgrade_old.py
        self.log.debug("Shutting down after tests")
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.scheds.execute(lambda app: app.sched.executor.stop())
        if self.merge_server:
            self.merge_server.stop()
            self.merge_server.join()

        self.executor_server.stop()
        self.executor_server.join()
        self.launcher.stop()
        self.launcher.join()
        if self.validate_tenants is None:
            self.scheds.execute(lambda app: app.sched.stop())
            self.scheds.execute(lambda app: app.sched.join())
        else:
            self.scheds.execute(lambda app: app.sched.stopConnections())
        self.statsd.stop()
        self.statsd.join()
        self.fake_nodepool.stop()
        self.zk_client.disconnect()
        self.printHistory()
        # We whitelist watchdog threads as they have relatively long delays
        # before noticing they should exit, but they should exit on their own.
        whitelist = ['watchdog',
                     'socketserver_Thread',
                     'cleanup start',
                     ]
        # Ignore threads that start with
        # * Thread- : Kazoo TreeCache
        # * Dummy-  : Seen during debugging in VS Code
        # * pydevd  : Debug helper threads of pydevd (used by many IDEs)
        # * ptvsd   : Debug helper threads used by VS Code
        threads = [t for t in threading.enumerate()
                   if t.name not in whitelist
                   and not t.name.startswith("Thread-")
                   and not t.name.startswith('Dummy-')
                   and not t.name.startswith('pydevd.')
                   and not t.name.startswith('ptvsd.')
                   and not t.name.startswith('OTLPFixture_')
                   ]
        if len(threads) > 1:
            thread_map = dict(map(lambda x: (x.ident, x.name),
                                  threading.enumerate()))
            log_str = ""
            for thread_id, stack_frame in sys._current_frames().items():
                log_str += "Thread id: %s, name: %s\n" % (
                    thread_id, thread_map.get(thread_id, 'UNKNOWN'))
                log_str += "".join(traceback.format_stack(stack_frame))
            self.log.debug(log_str)
            raise Exception("More than one thread is running: %s" % threads)
        self.cleanupTestServers()

    def cleanupTestServers(self):
        del self.executor_server
        del self.scheds
        del self.launcher
        del self.fake_nodepool
        del self.zk_client

    def assertCleanShutdown(self):
        pass

    def init_repo(self, project, tag=None):
        parts = project.split('/')
        path = os.path.join(self.upstream_root, *parts[:-1])
        if not os.path.exists(path):
            os.makedirs(path)
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo.init(path)

        with repo.config_writer() as config_writer:
            config_writer.set_value('user', 'email', 'user@example.com')
            config_writer.set_value('user', 'name', 'User Name')

        repo.index.commit('initial commit')
        master = repo.create_head('master')
        if tag:
            repo.create_tag(tag)

        repo.head.reference = master
        repo.head.reset(working_tree=True)
        repo.git.clean('-x', '-f', '-d')

    def create_branch(self, project, branch, commit_filename='README'):
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo(path)
        fn = os.path.join(path, commit_filename)

        branch_head = repo.create_head(branch)
        repo.head.reference = branch_head
        f = open(fn, 'a')
        f.write("test %s\n" % branch)
        f.close()
        repo.index.add([fn])
        repo.index.commit('%s commit' % branch)

        repo.head.reference = repo.heads['master']
        repo.head.reset(working_tree=True)
        repo.git.clean('-x', '-f', '-d')

    def delete_branch(self, project, branch):
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo(path)
        repo.head.reference = repo.heads['master']
        repo.head.reset(working_tree=True)
        repo.delete_head(repo.heads[branch], force=True)

    def create_commit(self, project, files=None, delete_files=None,
                      head='master', message='Creating a fake commit',
                      **kwargs):
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo(path)
        repo.head.reference = repo.heads[head]
        repo.head.reset(index=True, working_tree=True)

        files = files or {"README": "creating fake commit\n"}
        for name, content in files.items():
            file_name = os.path.join(path, name)
            with open(file_name, 'a') as f:
                f.write(content)
            repo.index.add([file_name])

        delete_files = delete_files or []
        for name in delete_files:
            file_name = os.path.join(path, name)
            repo.index.remove([file_name])

        commit = repo.index.commit(message, **kwargs)
        return commit.hexsha

    def orderedRelease(self, count=None):
        # Run one build at a time to ensure non-race order:
        i = 0
        while len(self.builds):
            self.release(self.builds[0])
            self.waitUntilSettled()
            i += 1
            if count is not None and i >= count:
                break

    def getSortedBuilds(self):
        "Return the list of currently running builds sorted by name"

        return sorted(self.builds, key=lambda x: x.name)

    def getCurrentBuilds(self):
        for tenant in self.scheds.first.sched.abide.tenants.values():
            for manager in tenant.layout.pipeline_managers.values():
                for item in manager.state.getAllItems():
                    for build in item.current_build_set.builds.values():
                        yield build

    def release(self, job):
        job.release()

    @property
    def sched_zk_nodepool(self):
        return self.scheds.first.sched.nodepool.zk_nodepool

    @property
    def hold_jobs_in_queue(self):
        return self.executor_api.hold_in_queue

    @hold_jobs_in_queue.setter
    def hold_jobs_in_queue(self, hold_in_queue):
        """Helper method to set hold_in_queue on all involved Executor APIs"""

        self.executor_api.hold_in_queue = hold_in_queue
        for app in self.scheds:
            app.sched.executor.executor_api.hold_in_queue = hold_in_queue

    @property
    def hold_merge_jobs_in_queue(self):
        return self.merger_api.hold_in_queue

    @hold_merge_jobs_in_queue.setter
    def hold_merge_jobs_in_queue(self, hold_in_queue):
        """Helper method to set hold_in_queue on all involved Merger APIs"""

        self.merger_api.hold_in_queue = hold_in_queue
        for app in self.scheds:
            app.sched.merger.merger_api.hold_in_queue = hold_in_queue
        self.executor_server.merger_api.hold_in_queue = hold_in_queue

    @property
    def hold_nodeset_requests_in_queue(self):
        return self.scheds.first.sched.launcher.hold_in_queue

    @hold_nodeset_requests_in_queue.setter
    def hold_nodeset_requests_in_queue(self, hold_in_queue):
        for app in self.scheds:
            app.sched.launcher.hold_in_queue = hold_in_queue

    def releaseNodesetRequests(self, *requests):
        ctx = self.createZKContext(None)
        if not requests:
            requests = self.launcher.api.getNodesetRequests()
        for req in requests:
            req.updateAttributes(ctx, state=req.State.REQUESTED)

    @property
    def merge_job_history(self):
        history = defaultdict(list)
        for app in self.scheds:
            for job_type, jobs in app.sched.merger.merger_api.history.items():
                history[job_type].extend(jobs)
        return history

    @merge_job_history.deleter
    def merge_job_history(self):
        for app in self.scheds:
            app.sched.merger.merger_api.history.clear()

    def waitUntilNodeCacheSync(self, zk_nodepool):
        """Wait until the node cache on the zk_nodepool object is in sync"""
        for _ in iterate_timeout(60, 'wait for node cache sync'):
            cache_state = {}
            zk_state = {}
            for n in self.fake_nodepool.getNodes():
                zk_state[n['_oid']] = n['state']
            for nid in zk_nodepool.getNodes(cached=True):
                n = zk_nodepool.getNode(nid)
                cache_state[n.id] = n.state
            if cache_state == zk_state:
                return

    def waitForArtifacts(self, image_name, count, format=None):
        for _ in iterate_timeout(30, "artifacts to settle"):
            artifacts = self.launcher.image_build_registry.\
                getArtifactsForImage(image_name)
            if format:
                artifacts = [a for a in artifacts if a.format == format]
            if len(artifacts) == count:
                return artifacts

    def waitForUploads(self, image_name, **states):
        for _ in iterate_timeout(60, "upload to complete"):
            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                image_name)
            uploads_by_state = Counter(u.state for u in uploads)
            if uploads_by_state == states:
                return

    def __haveAllBuildsReported(self):
        # The build requests will be deleted from ZooKeeper once the
        # scheduler processed their result event. Thus, as long as
        # there are build requests left in ZooKeeper, the system is
        # not stable.
        for build in self.history:
            try:
                self.zk_client.client.get(build.build_request_ref)
            except NoNodeError:
                # It has already been reported
                continue
            # It hasn't been reported yet.
            return False
        return True

    def __areAllBuildsWaiting(self):
        # Look up the queued build requests directly from ZooKeeper
        queued_build_requests = list(self.executor_api.all())
        seen_builds = set()
        # Always ignore builds which are on hold
        for build_request in queued_build_requests:
            seen_builds.add(build_request.uuid)
            if build_request.state in (BuildRequest.HOLD):
                continue
            # Check if the build is currently processed by the
            # RecordingExecutorServer.
            worker_build = self.executor_server.job_builds.get(
                build_request.uuid)
            if worker_build:
                if worker_build.paused:
                    # Avoid a race between setting the resume flag and
                    # the job actually resuming.  If the build is
                    # paused, make sure that there is no resume flag
                    # and if that's true, that the build is still
                    # paused.  If there's no resume flag between two
                    # checks of the paused attr, it should still be
                    # paused.
                    if not self.zk_client.client.exists(
                            build_request.path + '/resume'):
                        if worker_build.paused:
                            continue
                if worker_build.isWaiting():
                    continue
                self.log.debug("%s is running", worker_build)
                return False
            else:
                self.log.debug("%s is unassigned", build_request)
                return False
        # Wait until all running builds have finished on the executor
        # and that all job workers are cleaned up. Otherwise there
        # could be a short window in which the build is finished
        # (and reported), but the job cleanup is not yet finished on
        # the executor. During this time the test could settle, but
        # assertFinalState() will fail because there are still
        # job_workers present on the executor.
        for build_uuid in self.executor_server.job_workers.keys():
            if build_uuid not in seen_builds:
                log = get_annotated_logger(self.log, build=build_uuid)
                log.debug("Build is not finalized")
                return False
        return True

    def __areAllNodeRequestsComplete(self, matcher=None):
        if self.fake_nodepool.paused:
            return True
        # Check ZK and the scheduler cache and make sure they are
        # in sync.
        for app in self.scheds.filter(matcher):
            sched = app.sched
            nodepool = app.sched.nodepool
            with nodepool.zk_nodepool._callback_lock:
                for req in self.fake_nodepool.getNodeRequests():
                    if req['state'] != model.STATE_FULFILLED:
                        return False
                    r2 = nodepool.zk_nodepool._node_request_cache.get(
                        req['_oid'])
                    if r2 and r2.state != req['state']:
                        return False
                    if req and not r2:
                        return False
                    tenant_name = r2.tenant_name
                    pipeline_name = r2.pipeline_name
                    if sched.pipeline_result_events[tenant_name][
                            pipeline_name
                    ].hasEvents():
                        return False
        return True

    def __areAllNodesetRequestsComplete(self, matcher=None):
        # Check ZK and the scheduler cache and make sure they are
        # in sync.
        for app in self.scheds.filter(matcher):
            sched = app.sched
            for request in self.launcher.api.getNodesetRequests():
                if request.state == request.State.TEST_HOLD:
                    continue
                if request.state not in model.NodesetRequest.FINAL_STATES:
                    return False
                if sched.pipeline_result_events[request.tenant_name][
                    request.pipeline_name
                ].hasEvents():
                    return False
        return True

    def __areAllMergeJobsWaiting(self):
        # Look up the queued merge jobs directly from ZooKeeper
        queued_merge_jobs = list(self.merger_api.all())
        # Always ignore merge jobs which are on hold
        for job in queued_merge_jobs:
            if job.state != MergeRequest.HOLD:
                return False
        return True

    def __eventQueuesEmpty(self, matcher=None) -> Generator[bool, None, None]:
        for event_queue in self.__event_queues(matcher):
            yield not event_queue.unfinished_tasks

    def __eventQueuesJoin(self, matcher) -> None:
        for app in self.scheds.filter(matcher):
            for event_queue in app.event_queues:
                event_queue.join()
        for event_queue in self.additional_event_queues:
            event_queue.join()

    def __areZooKeeperEventQueuesEmpty(self, matcher=None, debug=False):
        for sched in map(lambda app: app.sched, self.scheds.filter(matcher)):
            for connection_name in sched.connections.connections:
                if self.connection_event_queues[connection_name].hasEvents():
                    if debug:
                        self.log.debug(
                            f"Connection queue {connection_name} not empty")
                    return False
            for tenant in sched.abide.tenants.values():
                if sched.management_events[tenant.name].hasEvents():
                    if debug:
                        self.log.debug(
                            f"Tenant management queue {tenant.name} not empty")
                    return False
                if sched.trigger_events[tenant.name].hasEvents():
                    if debug:
                        self.log.debug(
                            f"Tenant trigger queue {tenant.name} not empty")
                    return False
                for pipeline_name in tenant.layout.pipeline_managers:
                    if sched.pipeline_management_events[tenant.name][
                        pipeline_name
                    ].hasEvents():
                        if debug:
                            self.log.debug(
                                "Pipeline management queue "
                                f"{tenant.name} {pipeline_name} not empty")
                        return False
                    if sched.pipeline_trigger_events[tenant.name][
                        pipeline_name
                    ].hasEvents():
                        if debug:
                            self.log.debug(
                                "Pipeline trigger queue "
                                f"{tenant.name} {pipeline_name} not empty")
                        return False
                    if sched.pipeline_result_events[tenant.name][
                        pipeline_name
                    ].hasEvents():
                        if debug:
                            self.log.debug(
                                "Pipeline result queue "
                                f"{tenant.name} {pipeline_name} not empty")
                        return False
        return True

    def __areAllSchedulersPrimed(self, matcher=None):
        for app in self.scheds.filter(matcher):
            if app.sched.last_reconfigured is None:
                return False
        return True

    def __areAllLaunchersSynced(self):
        return (self.scheds.first.sched.local_layout_state ==
                self.launcher.local_layout_state)

    def __areAllImagesUploaded(self):
        for upload in self.launcher.image_upload_registry.getItems():
            if upload.uuid in getattr(
                    self, '_finished_image_uploads', set()):
                return True
            if not upload.external_id:
                return False
        return True

    def _addFinishedUpload(self, upload_uuid):
        if not hasattr(self, '_finished_image_uploads'):
            self._finished_image_uploads = set()
        self._finished_image_uploads.add(upload_uuid)

    def waitUntilSettled(self, msg="", matcher=None) -> None:
        self.log.debug("Waiting until settled... (%s)", msg)
        start = time.time()
        i = 0
        while True:
            i = i + 1
            if time.time() - start > self.wait_timeout:
                self.log.error("Timeout waiting for Zuul to settle")
                self.log.debug("All schedulers primed: %s",
                               self.__areAllSchedulersPrimed(matcher))
                self.log.debug("All launchers synced: %s",
                               self.__areAllLaunchersSynced())
                self._logQueueStatus(
                    self.log.error, matcher,
                    self.__areZooKeeperEventQueuesEmpty(debug=True),
                    self.__areAllMergeJobsWaiting(),
                    self.__haveAllBuildsReported(),
                    self.__areAllBuildsWaiting(),
                    self.__areAllNodeRequestsComplete(),
                    self.__areAllNodesetRequestsComplete(),
                    self.__areAllImagesUploaded(),
                    all(self.__eventQueuesEmpty(matcher))
                )
                raise Exception("Timeout waiting for Zuul to settle")

            # Make sure no new events show up while we're checking
            self.executor_server.lock.acquire()

            # have all build states propogated to zuul?
            if self.__haveAllBuildsReported():
                # Join ensures that the queue is empty _and_ events have been
                # processed
                self.__eventQueuesJoin(matcher)
                for sched in map(lambda app: app.sched,
                                 self.scheds.filter(matcher)):
                    sched.run_handler_lock.acquire()
                with self.launcher._test_lock:
                    if (self.__areAllSchedulersPrimed(matcher) and
                        self.__areAllLaunchersSynced() and
                        self.__areAllMergeJobsWaiting() and
                        self.__haveAllBuildsReported() and
                        self.__areAllBuildsWaiting() and
                        self.__areAllNodeRequestsComplete() and
                        self.__areAllNodesetRequestsComplete() and
                        self.__areAllImagesUploaded() and
                        self.__areZooKeeperEventQueuesEmpty() and
                        all(self.__eventQueuesEmpty(matcher))):
                        # The queue empty check is placed at the end to
                        # ensure that if a component adds an event between
                        # when locked the run handler and checked that the
                        # components were stable, we don't erroneously
                        # report that we are settled.
                        for sched in map(lambda app: app.sched,
                                         self.scheds.filter(matcher)):
                            if len(self.scheds) > 1:
                                self.refreshPipelines(sched)
                            sched.run_handler_lock.release()
                        self.executor_server.lock.release()
                        self.log.debug("...settled after %.3f ms / "
                                       "%s loops (%s)",
                                       time.time() - start, i, msg)
                        self.logState()
                        return
                for sched in map(lambda app: app.sched,
                                 self.scheds.filter(matcher)):
                    sched.run_handler_lock.release()
            self.executor_server.lock.release()
            for sched in map(lambda app: app.sched,
                             self.scheds.filter(matcher)):
                sched.wake_event.wait(0.1)
            # Let other threads work
            time.sleep(0.1)

    def refreshPipelines(self, sched):
        ctx = None
        for tenant in sched.abide.tenants.values():
            with tenant_read_lock(self.zk_client, tenant.name):
                for manager in tenant.layout.pipeline_managers.values():
                    with (pipeline_lock(self.zk_client, tenant.name,
                                        manager.pipeline.name) as lock,
                          self.createZKContext(lock) as ctx):
                        with manager.currentContext(ctx):
                            manager.state.refresh(ctx)
        # return the context in case the caller wants to examine iops
        return ctx

    def _logQueueStatus(self, logger, matcher, all_zk_queues_empty,
                        all_merge_jobs_waiting, all_builds_reported,
                        all_builds_waiting, all_node_requests_completed,
                        all_nodeset_requests_completed,
                        all_images_uploaded,
                        all_event_queues_empty):
        logger("Queue status:")
        for event_queue in self.__event_queues(matcher):
            is_empty = not event_queue.unfinished_tasks
            self.log.debug("  %s: %s", event_queue, is_empty)
        logger("All ZK event queues empty: %s", all_zk_queues_empty)
        logger("All merge jobs waiting: %s", all_merge_jobs_waiting)
        logger("All builds reported: %s", all_builds_reported)
        logger("All builds waiting: %s", all_builds_waiting)
        logger("All requests completed: %s", all_node_requests_completed)
        logger("All nodeset requests completed: %s",
               all_nodeset_requests_completed)
        logger("All images uploaded: %s", all_images_uploaded)
        logger("All event queues empty: %s", all_event_queues_empty)

    def waitForPoll(self, poller, timeout=30):
        self.log.debug("Wait for poll on %s", poller)
        self.poller_events[poller].clear()
        self.log.debug("Waiting for poll 1 on %s", poller)
        self.poller_events[poller].wait(timeout)
        self.poller_events[poller].clear()
        self.log.debug("Waiting for poll 2 on %s", poller)
        self.poller_events[poller].wait(timeout)
        self.log.debug("Done waiting for poll on %s", poller)

    def logState(self):
        """ Log the current state of the system """
        self.log.info("Begin state dump --------------------")
        for build in self.history:
            self.log.info("Completed build: %s" % build)
        for build in self.builds:
            self.log.info("Running build: %s" % build)
        for tenant in self.scheds.first.sched.abide.tenants.values():
            for manager in tenant.layout.pipeline_managers.values():
                for pipeline_queue in manager.state.queues:
                    if len(pipeline_queue.queue) != 0:
                        status = ''
                        for item in pipeline_queue.queue:
                            status += item.formatStatus()
                        self.log.info(
                            'Tenant %s pipeline %s queue %s contents:' % (
                                tenant.name, manager.pipeline.name,
                                pipeline_queue.name))
                        for l in status.split('\n'):
                            if l.strip():
                                self.log.info(l)
        self.log.info("End state dump --------------------")

    def countJobResults(self, jobs, result):
        jobs = filter(lambda x: x.result == result, jobs)
        return len(list(jobs))

    def getBuildByName(self, name):
        for build in self.builds:
            if build.name == name:
                return build
        raise Exception("Unable to find build %s" % name)

    def assertJobNotInHistory(self, name, project=None):
        for job in self.history:
            if (project is None or
                job.parameters['zuul']['project']['name'] == project):
                self.assertNotEqual(job.name, name,
                                    'Job %s found in history' % name)

    def getJobFromHistory(self, name, project=None, result=None, branch=None):
        for job in self.history:
            if (job.name == name and
                (project is None or
                 job.parameters['zuul']['project']['name'] == project) and
                (result is None or job.result == result) and
                (branch is None or
                 job.parameters['zuul']['branch'] == branch)):
                return job
        raise Exception("Unable to find job %s in history" % name)

    def assertEmptyQueues(self):
        # Make sure there are no orphaned jobs
        for tenant in self.scheds.first.sched.abide.tenants.values():
            for manager in tenant.layout.pipeline_managers.values():
                for pipeline_queue in manager.state.queues:
                    if len(pipeline_queue.queue) != 0:
                        print('pipeline %s queue %s contents %s' % (
                            manager.pipeline.name, pipeline_queue.name,
                            pipeline_queue.queue))
                    self.assertEqual(len(pipeline_queue.queue), 0,
                                     "Pipelines queues should be empty")

    def assertCleanZooKeeper(self):
        # Make sure there are no extraneous ZK nodes
        client = self.merger_api
        self.assertEqual(self.getZKPaths(client.REQUEST_ROOT), [])
        self.assertEqual(self.getZKPaths(client.PARAM_ROOT), [])
        self.assertEqual(self.getZKPaths(client.RESULT_ROOT), [])
        self.assertEqual(self.getZKPaths(client.RESULT_DATA_ROOT), [])
        self.assertEqual(self.getZKPaths(client.WAITER_ROOT), [])
        self.assertEqual(self.getZKPaths(client.LOCK_ROOT), [])

    def assertReportedStat(self, key, value=None, kind=None, timeout=5):
        """Check statsd output

        Check statsd return values.  A ``value`` should specify a
        ``kind``, however a ``kind`` may be specified without a
        ``value`` for a generic match.  Leave both empy to just check
        for key presence.

        :arg str key: The statsd key
        :arg str value: The expected value of the metric ``key``
        :arg str kind: The expected type of the metric ``key``  For example

          - ``c`` counter
          - ``g`` gauge
          - ``ms`` timing
          - ``s`` set

        :arg int timeout: How long to wait for the stat to appear

        :returns: The value
        """

        if value:
            self.assertNotEqual(kind, None)

        start = time.time()
        while time.time() <= (start + timeout):
            # Note our fake statsd just queues up results in a queue.
            # We just keep going through them until we find one that
            # matches, or fail out.  If statsd pipelines are used,
            # large single packets are sent with stats separated by
            # newlines; thus we first flatten the stats out into
            # single entries.
            stats = list(itertools.chain.from_iterable(
                [s.decode('utf-8').split('\n') for s in self.statsd.stats]))

            # Check that we don't have already have a counter value
            # that we then try to extend a sub-key under; this doesn't
            # work on the server.  e.g.
            #  zuul.new.stat            is already a counter
            #  zuul.new.stat.sub.value  will silently not work
            #
            # note only valid for gauges and counters; timers are
            # slightly different because statsd flushes them out but
            # actually writes a bunch of different keys like "mean,
            # std, count", so the "key" isn't so much a key, but a
            # path to the folder where the actual values will be kept.
            # Thus you can extend timer keys OK.
            already_set_keys = set()
            for stat in stats:
                k, v = stat.split(':')
                s_value, s_kind = v.split('|')
                if s_kind == 'c' or s_kind == 'g':
                    already_set_keys.update([k])
            for k in already_set_keys:
                if key != k and key.startswith(k):
                    raise StatException(
                        "Key %s is a gauge/counter and "
                        "we are trying to set subkey %s" % (k, key))

            for stat in stats:
                k, v = stat.split(':')
                s_value, s_kind = v.split('|')

                if key == k:
                    if kind is None:
                        # key with no qualifiers is found
                        return s_value

                    # if no kind match, look for other keys
                    if kind != s_kind:
                        continue

                    if value:
                        # special-case value|ms because statsd can turn
                        # timing results into float of indeterminate
                        # length, hence foiling string matching.
                        if kind == 'ms':
                            if float(value) == float(s_value):
                                return s_value
                        if value == s_value:
                            return s_value
                        # otherwise keep looking for other matches
                        continue

                    # this key matches
                    return s_value
            time.sleep(0.1)

        stats = list(itertools.chain.from_iterable(
            [s.decode('utf-8').split('\n') for s in self.statsd.stats]))
        for stat in stats:
            self.log.debug("Stat: %s", stat)
        raise StatException("Key %s not found in reported stats" % key)

    def assertUnReportedStat(self, key, value=None, kind=None):
        try:
            value = self.assertReportedStat(key, value=value,
                                            kind=kind, timeout=0)
        except StatException:
            return
        raise StatException("Key %s found in reported stats: %s" %
                            (key, value))

    def assertRegexInList(self, regex, items):
        r = re.compile(regex)
        for x in items:
            if r.search(x):
                return
        raise Exception("Regex '%s' not in %s" % (regex, items))

    def assertRegexNotInList(self, regex, items):
        r = re.compile(regex)
        for x in items:
            if r.search(x):
                raise Exception("Regex '%s' in %s" % (regex, items))

    def assertBuilds(self, builds):
        """Assert that the running builds are as described.

        The list of running builds is examined and must match exactly
        the list of builds described by the input.

        :arg list builds: A list of dictionaries.  Each item in the
            list must match the corresponding build in the build
            history, and each element of the dictionary must match the
            corresponding attribute of the build.

        """
        try:
            self.assertEqual(len(self.builds), len(builds))
            for i, d in enumerate(builds):
                for k, v in d.items():
                    self.assertEqual(
                        getattr(self.builds[i], k), v,
                        "Element %i in builds does not match" % (i,))
        except Exception:
            if not self.builds:
                self.log.error("No running builds")
            for build in self.builds:
                self.log.error("Running build: %s" % build)
            raise

    def assertHistory(self, history, ordered=True):
        """Assert that the completed builds are as described.

        The list of completed builds is examined and must match
        exactly the list of builds described by the input.

        :arg list history: A list of dictionaries.  Each item in the
            list must match the corresponding build in the build
            history, and each element of the dictionary must match the
            corresponding attribute of the build.

        :arg bool ordered: If true, the history must match the order
            supplied, if false, the builds are permitted to have
            arrived in any order.

        """
        def matches(history_item, item):
            for k, v in item.items():
                if getattr(history_item, k) != v:
                    return False
            return True
        try:
            self.assertEqual(len(self.history), len(history))
            if ordered:
                for i, d in enumerate(history):
                    if not matches(self.history[i], d):
                        raise Exception(
                            "Element %i in history does not match %s" %
                            (i, self.history[i]))
            else:
                unseen = self.history[:]
                for i, d in enumerate(history):
                    found = False
                    for unseen_item in unseen:
                        if matches(unseen_item, d):
                            found = True
                            unseen.remove(unseen_item)
                            break
                    if not found:
                        raise Exception("No match found for element %i %s "
                                        "in history" % (i, d))
                if unseen:
                    raise Exception("Unexpected items in history")
        except Exception:
            for build in self.history:
                self.log.error("Completed build: %s" % build)
            if not self.history:
                self.log.error("No completed builds")
            raise

    def printHistory(self):
        """Log the build history.

        This can be useful during tests to summarize what jobs have
        completed.

        """
        if not self.history:
            self.log.debug("Build history: no builds ran")
            return

        self.log.debug("Build history:")
        for build in self.history:
            self.log.debug(build)

    def addTagToRepo(self, project, name, sha):
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo(path)
        repo.git.tag(name, sha)

    def delTagFromRepo(self, project, name):
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo(path)
        repo.git.tag('-d', name)

    def addCommitToRepo(self, project, message, files,
                        branch='master', tag=None):
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo(path)
        repo.head.reference = branch
        repo.head.reset(working_tree=True)
        for fn, content in files.items():
            fn = os.path.join(path, fn)
            try:
                os.makedirs(os.path.dirname(fn))
            except OSError:
                pass
            if isinstance(content, SymLink):
                os.symlink(content.target, fn)
            else:
                mode = 'w'
                if isinstance(content, bytes):
                    # the file fixtures are loaded as bytes such that
                    # we also support binary files
                    mode = 'wb'
                with open(fn, mode) as f:
                    f.write(content)
            repo.index.add([fn])
        commit = repo.index.commit(message)
        before = repo.heads[branch].commit
        repo.heads[branch].commit = commit
        repo.head.reference = branch
        repo.git.clean('-x', '-f', '-d')
        repo.heads[branch].checkout()
        if tag:
            repo.create_tag(tag)
        return before

    def commitConfigUpdate(self, project_name, source_name, replace=None):
        """Commit an update to zuul.yaml

        This overwrites the zuul.yaml in the specificed project with
        the contents specified.

        :arg str project_name: The name of the project containing
            zuul.yaml (e.g., common-config)

        :arg str source_name: The path to the file (underneath the
            test fixture directory) whose contents should be used to
            replace zuul.yaml.
        """

        source_path = os.path.join(FIXTURE_DIR, source_name)
        files = {}
        with open(source_path, 'r') as f:
            data = f.read()
            if replace:
                kw = replace(self)
                data = data.format(**kw)
            layout = yaml.safe_load(data)
            files['zuul.yaml'] = data
        for item in layout:
            if 'job' in item:
                jobname = item['job']['name']
                files['playbooks/%s.yaml' % jobname] = ''
        before = self.addCommitToRepo(
            project_name, 'Pulling content from %s' % source_name,
            files)
        return before

    def newTenantConfig(self, source_name):
        """ Use this to update the tenant config file in tests

        This will update self.tenant_config_file to point to a temporary file
        for the duration of this particular test. The content of that file will
        be taken from FIXTURE_DIR/source_name

        After the test the original value of self.tenant_config_file will be
        restored.

        :arg str source_name: The path of the file under
            FIXTURE_DIR that will be used to populate the new tenant
            config file.
        """
        source_path = os.path.join(FIXTURE_DIR, source_name)
        orig_tenant_config_file = self.tenant_config_file
        with tempfile.NamedTemporaryFile(
            delete=False, mode='wb') as new_tenant_config:
            self.tenant_config_file = new_tenant_config.name
            with open(source_path, mode='rb') as source_tenant_config:
                new_tenant_config.write(source_tenant_config.read())
        for app in self.scheds.instances:
            app.config['scheduler']['tenant_config'] = self.tenant_config_file
        self.config['scheduler']['tenant_config'] = self.tenant_config_file
        self.setupAllProjectKeys(self.config)
        self.log.debug(
            'tenant_config_file = {}'.format(self.tenant_config_file))

        def _restoreTenantConfig():
            self.log.debug(
                'restoring tenant_config_file = {}'.format(
                    orig_tenant_config_file))
            os.unlink(self.tenant_config_file)
            self.tenant_config_file = orig_tenant_config_file
            self.config['scheduler']['tenant_config'] = orig_tenant_config_file
        self.addCleanup(_restoreTenantConfig)

    def addEvent(self, connection, event):
        """Inject a Fake (Gerrit) event.

        This method accepts a JSON-encoded event and simulates Zuul
        having received it from Gerrit.  It could (and should)
        eventually apply to any connection type, but is currently only
        used with Gerrit connections.  The name of the connection is
        used to look up the corresponding server, and the event is
        simulated as having been received by all Zuul connections
        attached to that server.  So if two Gerrit connections in Zuul
        are connected to the same Gerrit server, and you invoke this
        method specifying the name of one of them, the event will be
        received by both.

        .. note::

            "self.fake_gerrit.addEvent" calls should be migrated to
            this method.

        :arg str connection: The name of the connection corresponding
            to the gerrit server.
        :arg str event: The JSON-encoded event.

        """
        specified_conn = self.scheds.first.connections.connections[connection]
        for conn in self.scheds.first.connections.connections.values():
            if (isinstance(conn, specified_conn.__class__) and
                specified_conn.server == conn.server):
                conn.addEvent(event)

    def getUpstreamRepos(self, projects):
        """Return upstream git repo objects for the listed projects

        :arg list projects: A list of strings, each the canonical name
                            of a project.

        :returns: A dictionary of {name: repo} for every listed
                  project.
        :rtype: dict

        """

        repos = {}
        for project in projects:
            # FIXME(jeblair): the upstream root does not yet have a
            # hostname component; that needs to be added, and this
            # line removed:
            tmp_project_name = '/'.join(project.split('/')[1:])
            path = os.path.join(self.upstream_root, tmp_project_name)
            repo = git.Repo(path)
            repos[project] = repo
        return repos

    def addAutohold(self, tenant_name, project_name, job_name,
                    ref_filter, reason, count, node_hold_expiration):
        request = HoldRequest()
        request.tenant = tenant_name
        request.project = project_name
        request.job = job_name
        request.ref_filter = ref_filter
        request.reason = reason
        request.max_count = count
        request.node_expiration = node_hold_expiration
        self.sched_zk_nodepool.storeHoldRequest(request)

    def saveChangeDB(self):
        path = os.path.join(self.test_root, "changes.data")
        self.test_config.changes.save(path)

    def loadChangeDB(self):
        path = os.path.join(self.test_root, "changes.data")
        self.test_config.changes.load(path)

    def waitForNodeRequest(self, request, timeout=10):
        with self.createZKContext(None) as ctx:
            for _ in iterate_timeout(
                    timeout, "nodeset request to be fulfilled"):
                request.refresh(ctx)
                if request.state == model.NodesetRequest.State.FULFILLED:
                    return

    def waitForLauncherLayoutSync(self, tenant='tenant-one'):
        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get(tenant) ==
                self.launcher.local_layout_state.get(tenant)):
                break

    def requestNodes(self, labels, tenant="tenant-one", pipeline="check",
                     provider=None, timeout=10):
        result_queue = PipelineResultEventQueue(
            self.zk_client, tenant, pipeline)

        # Don't allow node request cleanup (since these requests
        # will not appear in a pipeline):
        if not hasattr(self, '_node_request_cleanup_lock'):
            self._node_request_cleanup_lock =\
                self.scheds.first.sched.node_request_cleanup_lock.acquire()

        with self.createZKContext(None) as ctx:
            # Lock the pipeline, so we can grab the result event
            with (self.scheds.first.sched.run_handler_lock,
                  pipeline_lock(self.zk_client, tenant, pipeline)):
                request = model.NodesetRequest.new(
                    ctx,
                    tenant_name=tenant,
                    pipeline_name="check",
                    buildset_uuid=uuid.uuid4().hex,
                    job_uuid=uuid.uuid4().hex,
                    job_name="foobar",
                    labels=labels,
                    priority=100,
                    request_time=time.time(),
                    zuul_event_id=uuid.uuid4().hex,
                    span_info=None,
                    preferred_provider=provider,
                )
                if timeout:
                    for _ in iterate_timeout(
                            timeout, "nodeset request to be fulfilled"):
                        result_events = list(result_queue)
                        if result_events:
                            for event in result_events:
                                # Remove event(s) from queue
                                result_queue.ack(event)
                            break
                else:
                    return request

            self.assertEqual(len(result_events), 1)
            for event in result_queue:
                self.assertIsInstance(event, model.NodesProvisionedEvent)
                self.assertEqual(event.request_id, request.uuid)
                self.assertEqual(event.build_set_uuid, request.buildset_uuid)

            request.refresh(ctx)
        return request


class AnsibleZuulTestCase(ZuulTestCase):
    """ZuulTestCase but with an actual ansible executor running"""
    run_ansible = True

    def setUp(self):
        super().setUp()

        py = os.environ.get("ZUUL_ANSIBLE_PYTHON")
        if py:
            # When we actually run Ansible then either Ansible python
            # interpreter discovery must work or we need to set a valid
            # override. Overrides are necessary when Ansible doesn't have the
            # platform in its discovery lookup table as is the case with
            # OpenSUSE Tumbleweed.
            self.fake_nodepool.override_python_path = py

    @contextmanager
    def jobLog(self, build):
        """Print job logs on assertion errors

        This method is a context manager which, if it encounters an
        ecxeption, adds the build log to the debug output.

        :arg Build build: The build that's being asserted.
        """
        try:
            yield
        except Exception:
            path = os.path.join(self.jobdir_root, build.uuid,
                                'work', 'logs', 'job-output.txt')
            with open(path) as f:
                self.log.debug(f.read())
            path = os.path.join(self.jobdir_root, build.uuid,
                                'work', 'logs', 'job-output.json')
            with open(path) as f:
                self.log.debug(f.read())
            raise


class SSLZuulTestCase(ZuulTestCase):
    """ZuulTestCase but using SSL when possible"""
    use_ssl = True


class ZuulGithubAppTestCase(ZuulTestCase):
    def setup_config(self, config_file: str):
        config = super(ZuulGithubAppTestCase, self).setup_config(config_file)
        for section_name in config.sections():
            con_match = re.match(r'^connection ([\'\"]?)(.*)(\1)$',
                                 section_name, re.I)
            if not con_match:
                continue

            if config.get(section_name, 'driver') == 'github':
                if (config.get(section_name, 'app_key',
                               fallback=None) ==
                    '$APP_KEY_FIXTURE$'):
                    config.set(section_name, 'app_key',
                               os.path.join(FIXTURE_DIR, 'app_key'))
        return config
