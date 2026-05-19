# Copyright 2014 OpenStack Foundation
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

import contextlib
import collections
import copy
import datetime
import json
import jwt
import logging
import multiprocessing
import os
import pathlib
import psutil
import re
import shutil
import signal
import shlex
import socket
import subprocess
import tempfile
import threading
import time
import traceback
from concurrent.futures.process import ProcessPoolExecutor, BrokenProcessPool
from functools import partial

from kazoo.exceptions import NoNodeError

import git
from urllib.parse import urlsplit
from opentelemetry import trace

from zuul.exceptions import VariableNameError
from zuul.lib.ansible import AnsibleManager
from zuul.lib.result_data import get_warnings_from_result_data
from zuul.lib import yamlutil as yaml
from zuul.lib.config import get_default
from zuul.lib.logutil import get_annotated_logger
from zuul.lib.monitoring import MonitoringServer
from zuul.lib.statsd import get_statsd
from zuul.lib import tracing
from zuul.lib import filecomments
from zuul.lib.keystorage import KeyStorage
from zuul.lib.varnames import check_varnames

import zuul.lib.repl
import zuul.merger.merger
from zuul.merger.merger import SparsePaths
import zuul.ansible.logconfig
from zuul.executor.sensors.cpu import CPUSensor
from zuul.executor.sensors.hdd import HDDSensor
from zuul.executor.sensors.pause import PauseSensor
from zuul.executor.sensors.process import ProcessSensor
from zuul.executor.sensors.startingbuilds import StartingBuildsSensor
from zuul.executor.sensors.ram import RAMSensor
from zuul.executor.common import zuul_params_from_job
from zuul.launcher.client import LauncherClient
from zuul.lib import commandsocket
from zuul.merger.server import BaseMergeServer, RepoLocks
from zuul.model import (
    BuildCompletedEvent,
    BuildEvent,
    BuildPausedEvent,
    BuildRequest,
    BuildStartedEvent,
    BuildStatusEvent,
    FrozenJob,
    Job,
    RepoState,
)
import zuul.model
from zuul.nodepool import Nodepool
from zuul.version import get_version_string
from zuul.zk.event_queues import (
    PipelineResultEventQueue,
    TenantManagementEventQueue,
)
from zuul.zk.blob_store import BlobStore
from zuul.zk.components import ExecutorComponent, COMPONENT_REGISTRY
from zuul.zk.exceptions import JobRequestNotFound
from zuul.zk.executor import ExecutorApi
from zuul.zk.job_request_queue import JobRequestEvent
from zuul.zk.system import ZuulSystem
from zuul.zk.zkobject import ZKContext
from zuul.zk.semaphore import SemaphoreHandler


BUFFER_LINES_FOR_SYNTAX = 200
OUTPUT_MAX_LINE_BYTES = 51200  # 50 MiB
OUTPUT_MAX_BYTES = 1024 * 1024 * 1024  # 1GiB
DEFAULT_FINGER_PORT = 7900
DEFAULT_STREAM_PORT = 19885
BLACKLISTED_ANSIBLE_CONNECTION_TYPES = [
    'network_cli', 'kubectl', 'project', 'namespace']
BLACKLISTED_VARS = dict(
    ansible_ssh_executable='ssh',
    ansible_ssh_common_args='-o PermitLocalCommand=no',
    ansible_sftp_extra_args='-o PermitLocalCommand=no',
    ansible_scp_extra_args='-o PermitLocalCommand=no',
    ansible_ssh_extra_args='-o PermitLocalCommand=no',
)


@contextlib.contextmanager
def _safe_open(file, *args, **kwargs):
    if (resolved := str(pathlib.Path(file).resolve())) != file:
        raise PathTraversalError(f"Symlink detected {file} -> {resolved}")

    with open(file, *args, **kwargs) as f:
        yield f


def _safe_exists(path):
    if (resolved := str(pathlib.Path(path).resolve())) != path:
        raise PathTraversalError(f"Symlink detected {path} -> {resolved}")
    return os.path.exists(resolved)


def _safe_unlink(path):
    if (resolved := str(pathlib.Path(path).resolve())) != path:
        raise PathTraversalError(f"Symlink detected {path} -> {resolved}")
    return os.unlink(resolved)


class VerboseCommand(commandsocket.Command):
    name = 'verbose'
    help = 'Enable Ansible verbose mode'


class UnVerboseCommand(commandsocket.Command):
    name = 'unverbose'
    help = 'Disable Ansible verbose mode'


class KeepCommand(commandsocket.Command):
    name = 'keep'
    help = 'Keep build directories after completion'


class NoKeepCommand(commandsocket.Command):
    name = 'nokeep'
    help = 'Remove build directories after completion'


COMMANDS = [
    commandsocket.StopCommand,
    commandsocket.PauseCommand,
    commandsocket.UnPauseCommand,
    commandsocket.GracefulCommand,
    VerboseCommand,
    UnVerboseCommand,
    KeepCommand,
    NoKeepCommand,
    commandsocket.ReplCommand,
    commandsocket.NoReplCommand,
]


class NodeRequestError(Exception):
    pass


class StopException(Exception):
    """An exception raised when an inner loop is asked to stop."""
    pass


class ExecutorError(Exception):
    """A non-transient run-time executor error

    This class represents error conditions detected by the executor
    when preparing to run a job which we know are consistently fatal.
    Zuul should not reschedule the build in these cases.
    """
    pass


class RoleNotFoundError(ExecutorError):
    pass


class PathTraversalError(ExecutorError):
    pass


class DiskAccountant(object):
    ''' A single thread to periodically run du and monitor a base directory

    Whenever the accountant notices a dir over limit, it will call the
    given func with an argument of the job directory. That function
    should be used to remediate the problem, generally by killing the
    job producing the disk bloat). The function will be called every
    time the problem is noticed, so it should be handled synchronously
    to avoid stacking up calls.
    '''
    log = logging.getLogger("zuul.ExecutorDiskAccountant")

    def __init__(self, jobs_base, limit, func, cache_dir, usage_func=None):
        '''
        :param str jobs_base: absolute path name of dir to be monitored
        :param int limit: maximum number of MB allowed to be in use in any one
                          subdir
        :param callable func: Function to call with overlimit dirs
        :param str cache_dir: absolute path name of dir to be passed as the
                              first argument to du. This will ensure du does
                              not count any hardlinks to files in this
                              directory against a single job.
        :param callable usage_func: Optional function to call with usage
                                    for every dir _NOT_ over limit
        '''
        # Remove any trailing slash to ensure dirname equality tests work
        cache_dir = cache_dir.rstrip('/')
        jobs_base = jobs_base.rstrip('/')
        # Don't cross the streams
        if cache_dir == jobs_base:
            raise Exception("Cache dir and jobs dir cannot be the same")
        self.thread = threading.Thread(target=self._run,
                                       name='diskaccountant')
        self.thread.daemon = True
        self._running = False
        self.jobs_base = jobs_base
        self.limit = limit
        self.func = func
        self.cache_dir = cache_dir
        self.usage_func = usage_func
        self.stop_event = threading.Event()

    def _run(self):
        while self._running:
            # Walk job base
            before = time.time()
            du = subprocess.Popen(
                ['du', '-m', '--max-depth=1', self.cache_dir, self.jobs_base],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            for line in du.stdout:
                (size, dirname) = line.rstrip().split()
                dirname = dirname.decode('utf8')
                if dirname == self.jobs_base or dirname == self.cache_dir:
                    continue
                if os.path.dirname(dirname) == self.cache_dir:
                    continue
                size = int(size)
                if size > self.limit:
                    self.log.warning(
                        "{job} is using {size}MB (limit={limit})"
                        .format(size=size, job=dirname, limit=self.limit))
                    self.func(dirname)
                elif self.usage_func:
                    self.log.debug(
                        "{job} is using {size}MB (limit={limit})"
                        .format(size=size, job=dirname, limit=self.limit))
                    self.usage_func(dirname, size)
            du.wait()
            du.stdout.close()
            after = time.time()
            # Sleep half as long as that took, or 1s, whichever is longer
            delay_time = max((after - before) / 2, 1.0)
            self.stop_event.wait(delay_time)

    def start(self):
        if self.limit < 0:
            # No need to start if there is no limit.
            return
        self._running = True
        self.thread.start()

    def stop(self):
        if not self.running:
            return
        self._running = False
        self.stop_event.set()
        self.thread.join()

    @property
    def running(self):
        return self._running


class Watchdog(object):
    def __init__(self, timeout, function, args):
        self.timeout = timeout
        self.function = function
        self.args = args
        self.thread = threading.Thread(target=self._run,
                                       name='watchdog')
        self.thread.daemon = True
        self.timed_out = None

        self.end = 0

        self._running = False
        self._stop_event = threading.Event()

    def _run(self):
        while self._running and time.time() < self.end:
            self._stop_event.wait(10)
        if self._running:
            self.timed_out = True
            self.function(*self.args)
        else:
            # Only set timed_out to false if we aren't _running
            # anymore. This means that we stopped running not because
            # of a timeout but because normal execution ended.
            self.timed_out = False

    def start(self):
        self._running = True
        self.end = time.time() + self.timeout
        self.thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()


class SshAgent(object):

    def __init__(self, zuul_event_id=None, build=None):
        self.env = {}
        self.ssh_agent = None
        self.log = get_annotated_logger(
            logging.getLogger("zuul.ExecutorServer"),
            zuul_event_id, build=build)

    def start(self):
        if self.ssh_agent:
            return
        with open('/dev/null', 'r+') as devnull:
            ssh_agent = subprocess.Popen(['ssh-agent'], close_fds=True,
                                         stdout=subprocess.PIPE,
                                         stderr=devnull,
                                         stdin=devnull)
        (output, _) = ssh_agent.communicate()
        output = output.decode('utf8')
        for line in output.split("\n"):
            if '=' in line:
                line = line.split(";", 1)[0]
                (key, value) = line.split('=')
                self.env[key] = value
        self.log.info('Started SSH Agent, {}'.format(self.env))

    def stop(self):
        if 'SSH_AGENT_PID' in self.env:
            try:
                os.kill(int(self.env['SSH_AGENT_PID']), signal.SIGTERM)
            except OSError:
                self.log.exception(
                    'Problem sending SIGTERM to agent {}'.format(self.env))
            self.log.debug('Sent SIGTERM to SSH Agent, {}'.format(self.env))
            self.env = {}

    def __del__(self):
        try:
            self.stop()
        except Exception:
            self.log.exception('Exception in SshAgent destructor')
        try:
            super().__del__(self)
        except AttributeError:
            pass

    def add(self, key_path):
        env = os.environ.copy()
        env.update(self.env)
        key_path = os.path.expanduser(key_path)
        self.log.debug('Adding SSH Key {}'.format(key_path))
        try:
            subprocess.check_output(['ssh-add', key_path], env=env,
                                    stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            self.log.exception('ssh-add failed. stdout: %s, stderr: %s',
                               e.output, e.stderr)
            raise
        self.log.info('Added SSH Key {}'.format(key_path))

    def addData(self, name, key_data):
        env = os.environ.copy()
        env.update(self.env)
        self.log.debug('Adding SSH Key {}'.format(name))
        try:
            subprocess.check_output(['ssh-add', '-'], env=env,
                                    input=key_data.encode('utf8'),
                                    stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            self.log.exception('ssh-add failed. stdout: %s, stderr: %s',
                               e.output, e.stderr)
            raise
        self.log.info('Added SSH Key {}'.format(name))

    def remove(self, key_path):
        env = os.environ.copy()
        env.update(self.env)
        key_path = os.path.expanduser(key_path)
        self.log.debug('Removing SSH Key {}'.format(key_path))
        subprocess.check_output(['ssh-add', '-d', key_path], env=env,
                                stderr=subprocess.PIPE)
        self.log.info('Removed SSH Key {}'.format(key_path))

    def list(self):
        if 'SSH_AUTH_SOCK' not in self.env:
            return None
        env = os.environ.copy()
        env.update(self.env)
        result = []
        for line in subprocess.Popen(['ssh-add', '-L'], env=env,
                                     stdout=subprocess.PIPE).stdout:
            line = line.decode('utf8')
            if line.strip() == 'The agent has no identities.':
                break
            result.append(line.strip())
        return result


class KubeFwd(object):
    kubectl_command = 'kubectl'

    def __init__(self, zuul_event_id, build, kubeconfig, context,
                 namespace, pod):
        self.port1 = None
        self.port2 = None
        self.fwd = None
        self.log = get_annotated_logger(
            logging.getLogger("zuul.ExecutorServer"),
            zuul_event_id, build=build)
        self.kubeconfig = kubeconfig
        self.context = context
        self.namespace = namespace
        self.pod = pod
        self.socket1 = None
        self.socket2 = None

    def _getSocket(self):
        # Reserve a port for each of the possible log streaming ports
        # so that we can restart the forwarder if it exits, which it
        # will if there is any connection problem at all.
        self.socket1 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        self.socket1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket1.bind(('::', 0))
        self.port1 = self.socket1.getsockname()[1]
        self.socket2 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        self.socket2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket2.bind(('::', 0))
        self.port2 = self.socket2.getsockname()[1]

    def start(self):
        if self.fwd:
            return

        if self.socket1 is None or self.socket2 is None:
            self._getSocket()

        cmd = [
            'while', '!',
            self.kubectl_command,
            shlex.quote('--kubeconfig=%s' % self.kubeconfig),
            shlex.quote('--context=%s' % self.context),
            '-n',
            shlex.quote(self.namespace),
            'port-forward',
            shlex.quote('pod/%s' % self.pod),
            '%s:19885' % self.port1,
            '%s:19886' % self.port2,
            ';', 'do', ':;', 'done',
        ]
        cmd = ' '.join(cmd)

        with open('/dev/null', 'r+') as devnull:
            fwd = subprocess.Popen(cmd,
                                   shell=True,
                                   close_fds=True,
                                   start_new_session=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   stdin=devnull)
        # This is a quick check to make sure it started correctly, so
        # we only check the first line and the first port.
        line = fwd.stdout.readline().decode('utf8')
        m = re.match(r'^Forwarding from 127.0.0.1:(\d+) -> 19885', line)
        port = None
        if m:
            port = m.group(1)
        if port != str(self.port1):
            self.log.error("Could not find the forwarded port: %s", line)
            self.stop()
            raise Exception("Unable to start kubectl port forward")
        self.fwd = fwd
        pgid = os.getpgid(self.fwd.pid)
        self.log.info('Started Kubectl port forward on ports %s and %s with '
                      'process group %s', self.port1, self.port2, pgid)

    def stop(self):
        try:
            if self.fwd:
                pgid = os.getpgid(self.fwd.pid)
                os.killpg(pgid, signal.SIGKILL)
                self.fwd.wait()

                # clear stdout buffer before its gone to not miss out on
                # potential connection errors
                fwd_stdout = [line.decode('utf8') for line in self.fwd.stdout]
                self.log.debug(
                    "Rest of kubectl port forward output was: %s",
                    "".join(fwd_stdout)
                )

                self.fwd = None
        except Exception:
            self.log.exception('Unable to stop kubectl port-forward:')
        try:
            if self.socket1:
                self.socket1.close()
                self.socket1 = None
        except Exception:
            self.log.exception('Unable to close port-forward socket:')
        try:
            if self.socket2:
                self.socket2.close()
                self.socket2 = None
        except Exception:
            self.log.exception('Unable to close port-forward socket:')

    def __del__(self):
        try:
            self.stop()
        except Exception:
            self.log.exception('Exception in KubeFwd destructor')
        try:
            super().__del__(self)
        except AttributeError:
            pass


class JobDirPlaybookRole(object):
    def __init__(self, root):
        self.root = root
        self.link_src = None
        self.link_target = None
        self.role_path = None
        self.checkout_description = None
        self.checkout = None

    def toDict(self, jobdir_root=None):
        # This is serialized to the zuul.playbook_context variable
        if jobdir_root:
            strip = len(jobdir_root) + 1
        else:
            strip = 0
        return dict(
            link_name=self.link_name[strip:],
            link_target=self.link_target[strip:],
            role_path=self.role_path[strip:],
            checkout_description=self.checkout_description,
            checkout=self.checkout,
        )


class JobDirPlaybook(object):
    def __init__(self, root):
        self.root = root
        self.trusted = None
        self.project_canonical_name = None
        self.branch = None
        self.canonical_name_and_path = None
        self.path = None
        self.roles = []
        self.roles_path = []
        self.ansible_config = os.path.join(self.root, 'ansible.cfg')
        self.inventory = os.path.join(self.root, 'inventory.yaml')
        self.project_link = os.path.join(self.root, 'project')
        self.secrets_root = os.path.join(self.root, 'group_vars')
        os.makedirs(self.secrets_root)
        self.secrets = os.path.join(self.secrets_root, 'all.yaml')
        self.secrets_content = None
        self.secrets_keys = set()
        self.secrets_oidc = {}
        self.semaphores = []
        self.nesting_level = None
        self.cleanup = False
        self.ignore_result = False

    def addRole(self):
        count = len(self.roles)
        root = os.path.join(self.root, 'role_%i' % (count,))
        os.makedirs(root)
        role_info = JobDirPlaybookRole(root)
        self.roles.append(role_info)
        return role_info


class JobDirProject(object):
    def __init__(self, root):
        self.root = root
        self.canonical_name = None
        self.checkout = None
        self.commit = None

    def toDict(self):
        # This is serialized to the zuul.playbook_context variable
        return dict(
            canonical_name=self.canonical_name,
            checkout=self.checkout,
            commit=self.commit,
        )


class JobDir(object):
    def __init__(self, root, keep, build_uuid):
        '''
        :param str root: Root directory for the individual job directories.
            Can be None to use the default system temp root directory.
        :param bool keep: If True, do not delete the job directory.
        :param str build_uuid: The unique build UUID. If supplied, this will
            be used as the temp job directory name. Using this will help the
            log streaming daemon find job logs.
        '''
        # root
        #   ansible (mounted in bwrap read-only)
        #     logging.json
        #     inventory.yaml
        #     vars_blacklist.yaml
        #     zuul_vars.yaml
        #   .ansible (mounted in bwrap read-write)
        #     fact-cache/localhost
        #     cp
        #   playbook_0 (mounted in bwrap for each playbook read-only)
        #     secrets.yaml
        #     project -> ../trusted/project_0/...
        #     role_0 -> ../trusted/project_0/...
        #   trusted (mounted in bwrap read-only)
        #     project_0
        #       <git.example.com>
        #         <project>
        #   untrusted (mounted in bwrap read-only)
        #     project_0
        #       <git.example.com>
        #         <project>
        #   home (mounted in bwrap read-write)
        #     .ssh
        #       known_hosts
        #   kube (mounted in bwrap read-only)
        #     config
        #   work (mounted in bwrap read-write)
        #     src
        #       <git.example.com>
        #         <project>
        #     logs
        #       job-output.txt
        #     tmp
        #     results.json
        self.keep = keep
        if root:
            tmpdir = root
        else:
            tmpdir = tempfile.gettempdir()
        self.root = os.path.realpath(os.path.join(tmpdir, build_uuid))
        os.mkdir(self.root, 0o700)
        self.home = os.path.join(self.root, 'home')
        os.makedirs(self.home)
        ssh_dir = os.path.join(self.home, '.ssh')
        os.mkdir(ssh_dir, 0o700)
        # A home directory with nothing in it for utility calls.
        self.utility_home = os.path.join(self.root, 'utility-home')
        os.makedirs(self.utility_home)
        self.work_root = os.path.join(self.root, 'work')
        os.makedirs(self.work_root)
        self.src_root = os.path.join(self.work_root, 'src')
        os.makedirs(self.src_root)
        self.log_root = os.path.join(self.work_root, 'logs')
        os.makedirs(self.log_root)
        # Create local tmp directory
        # NOTE(tobiash): This must live within the work root as it can be used
        # by ansible for temporary files which are path checked in untrusted
        # jobs.
        self.local_tmp = os.path.join(self.work_root, 'tmp')
        os.makedirs(self.local_tmp)
        self.ansible_root = os.path.join(self.root, 'ansible')
        os.makedirs(self.ansible_root)
        self.ansible_vars_blacklist = os.path.join(
            self.ansible_root, 'vars_blacklist.yaml')
        with open(self.ansible_vars_blacklist, 'w') as blacklist:
            blacklist.write(json.dumps(BLACKLISTED_VARS))
        self.zuul_vars = os.path.join(self.ansible_root, 'zuul_vars.yaml')
        self.trusted_root = os.path.join(self.root, 'trusted')
        os.makedirs(self.trusted_root)
        self.untrusted_root = os.path.join(self.root, 'untrusted')
        os.makedirs(self.untrusted_root)
        self.kube_dir = os.path.join(self.root, "kube")
        os.makedirs(self.kube_dir)
        self.kubeconfig = os.path.join(self.kube_dir, "config")
        # Create ansible cache directory
        self.ansible_cache_root = os.path.join(self.root, '.ansible')
        self.fact_cache = os.path.join(self.ansible_cache_root, 'fact-cache')
        os.makedirs(self.fact_cache)
        self.control_path = os.path.join(self.ansible_cache_root, 'cp')
        self.job_unreachable_file = os.path.join(self.ansible_cache_root,
                                                 'nodes.unreachable')
        os.makedirs(self.control_path)

        localhost_facts = os.path.join(self.fact_cache, 'localhost')
        jobtime = datetime.datetime.utcnow()
        date_time_facts = {}
        date_time_facts['year'] = jobtime.strftime('%Y')
        date_time_facts['month'] = jobtime.strftime('%m')
        date_time_facts['weekday'] = jobtime.strftime('%A')
        date_time_facts['weekday_number'] = jobtime.strftime('%w')
        date_time_facts['weeknumber'] = jobtime.strftime('%W')
        date_time_facts['day'] = jobtime.strftime('%d')
        date_time_facts['hour'] = jobtime.strftime('%H')
        date_time_facts['minute'] = jobtime.strftime('%M')
        date_time_facts['second'] = jobtime.strftime('%S')
        date_time_facts['epoch'] = jobtime.strftime('%s')
        date_time_facts['date'] = jobtime.strftime('%Y-%m-%d')
        date_time_facts['time'] = jobtime.strftime('%H:%M:%S')
        date_time_facts['iso8601_micro'] = \
            jobtime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        date_time_facts['iso8601'] = \
            jobtime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        date_time_facts['iso8601_basic'] = jobtime.strftime("%Y%m%dT%H%M%S%f")
        date_time_facts['iso8601_basic_short'] = \
            jobtime.strftime("%Y%m%dT%H%M%S")

        # Set the TZ data manually as jobtime is naive.
        date_time_facts['tz'] = 'UTC'
        date_time_facts['tz_offset'] = '+0000'

        executor_facts = {}
        executor_facts['date_time'] = date_time_facts
        executor_facts['module_setup'] = True

        # NOTE(pabelanger): We do not want to leak zuul-executor facts to other
        # playbooks now that smart fact gathering is enabled by default.  We
        # can have ansible skip populating the cache with information by
        # writing a file with the minimum facts we want.
        with open(localhost_facts, 'w') as f:
            json.dump(executor_facts, f)

        self.result_data_file = os.path.join(self.work_root, 'results.json')
        with _safe_open(self.result_data_file, 'w'):
            pass
        self.known_hosts = os.path.join(ssh_dir, 'known_hosts')
        # The ssh config file is not created or used by Zuul, but we
        # forbid its use in jobs.
        self.path_blocklist = [
            os.path.join(self.home, '.ssh', 'config'),
            os.path.join(self.home, '.kube', 'config'),
        ]
        self.inventory = os.path.join(self.ansible_root, 'inventory.yaml')
        self.logging_json = os.path.join(self.ansible_root, 'logging.json')
        self.playbooks = []  # The list of candidate playbooks
        self.pre_playbooks = []
        self.post_playbooks = []
        self.cleanup_playbooks = []
        self.job_output_file = os.path.join(self.log_root, 'job-output.txt')
        # We need to create the job-output.txt upfront in order to close the
        # gap between url reporting and ansible creating the file. Otherwise
        # there is a period of time where the user can click on the live log
        # link on the status page but the log streaming fails because the file
        # is not there yet.
        with _safe_open(self.job_output_file, "w") as job_output:
            job_output.write("{now} | Job console starting\n".format(
                now=datetime.datetime.now()
            ))
        self.trusted_projects = {}
        self.untrusted_projects = {}

        # Create a JobDirPlaybook for the Ansible setup run.  This
        # doesn't use an actual playbook, but it lets us use the same
        # methods to write an ansible.cfg as the rest of the Ansible
        # runs.
        setup_root = os.path.join(self.ansible_root, 'setup_playbook')
        os.makedirs(setup_root)
        self.setup_playbook = JobDirPlaybook(setup_root)
        self.setup_playbook.trusted = True

        # Create a JobDirPlaybook for the Ansible variable freeze run.
        freeze_root = os.path.join(self.ansible_root, 'freeze_playbook')
        os.makedirs(freeze_root)
        self.freeze_playbook = JobDirPlaybook(freeze_root)
        self.freeze_playbook.trusted = False
        self.freeze_playbook.path = os.path.join(self.freeze_playbook.root,
                                                 'freeze_playbook.yaml')

    def addTrustedProject(self, canonical_name, branch):
        # Trusted projects are placed in their own directories so that
        # we can support using different branches of the same project
        # in different playbooks.
        count = len(self.trusted_projects)
        root = os.path.join(self.trusted_root, 'project_%i' % (count,))
        os.makedirs(root)
        project_info = JobDirProject(root)
        project_info.canonical_name = canonical_name
        project_info.checkout = branch
        self.trusted_projects[(canonical_name, branch)] = project_info
        return project_info

    def getTrustedProject(self, canonical_name, branch):
        return self.trusted_projects.get((canonical_name, branch))

    def addUntrustedProject(self, canonical_name, branch):
        # Similar to trusted projects, but these hold checkouts of
        # projects which are allowed to have speculative changes
        # applied.  They might, however, be different branches than
        # what is used in the working dir, so they need their own
        # location.  Moreover, we might avoid mischief if a job alters
        # the contents of the working dir.
        count = len(self.untrusted_projects)
        root = os.path.join(self.untrusted_root, 'project_%i' % (count,))
        os.makedirs(root)
        project_info = JobDirProject(root)
        project_info.canonical_name = canonical_name
        project_info.checkout = branch
        self.untrusted_projects[(canonical_name, branch)] = project_info
        return project_info

    def getUntrustedProject(self, canonical_name, branch):
        return self.untrusted_projects.get((canonical_name, branch))

    def addPrePlaybook(self):
        count = len(self.pre_playbooks)
        root = os.path.join(self.ansible_root, 'pre_playbook_%i' % (count,))
        os.makedirs(root)
        playbook = JobDirPlaybook(root)
        self.pre_playbooks.append(playbook)
        return playbook

    def addPostPlaybook(self):
        count = len(self.post_playbooks)
        root = os.path.join(self.ansible_root, 'post_playbook_%i' % (count,))
        os.makedirs(root)
        playbook = JobDirPlaybook(root)
        self.post_playbooks.append(playbook)
        return playbook

    def addCleanupPlaybook(self):
        count = len(self.cleanup_playbooks)
        root = os.path.join(
            self.ansible_root, 'cleanup_playbook_%i' % (count,))
        os.makedirs(root)
        playbook = JobDirPlaybook(root)
        self.cleanup_playbooks.append(playbook)
        return playbook

    def addPlaybook(self):
        count = len(self.playbooks)
        root = os.path.join(self.ansible_root, 'playbook_%i' % (count,))
        os.makedirs(root)
        playbook = JobDirPlaybook(root)
        self.playbooks.append(playbook)
        return playbook

    def cleanup(self):
        if not self.keep:
            shutil.rmtree(self.root)

    def __enter__(self):
        return self

    def __exit__(self, etype, value, tb):
        self.cleanup()


class UpdateTask(object):
    def __init__(self, connection_name, project_name, repo_state=None,
                 zuul_event_id=None, build=None, span_context=None):
        self.connection_name = connection_name
        self.project_name = project_name
        self.repo_state = repo_state
        self.canonical_name = None
        self.branches = None
        self.refs = None
        self.event = threading.Event()
        self.success = False

        # These variables are used for log annotation
        self.zuul_event_id = zuul_event_id
        self.build = build
        self.span_context = span_context

    def __eq__(self, other):
        if (other and other.connection_name == self.connection_name and
            other.project_name == self.project_name and
            other.repo_state == self.repo_state):
            return True
        return False

    def wait(self):
        self.event.wait()

    def setComplete(self):
        self.event.set()


class DeduplicateQueue(object):
    def __init__(self):
        self.queue = collections.deque()
        self.condition = threading.Condition()

    def qsize(self):
        return len(self.queue)

    def put(self, item):
        # Returns the original item if added, or an equivalent item if
        # already enqueued.
        self.condition.acquire()
        ret = None
        try:
            for x in self.queue:
                if item == x:
                    ret = x
            if ret is None:
                ret = item
                self.queue.append(item)
                self.condition.notify()
        finally:
            self.condition.release()
        return ret

    def get(self):
        self.condition.acquire()
        try:
            while True:
                try:
                    ret = self.queue.popleft()
                    return ret
                except IndexError:
                    pass
                self.condition.wait()
        finally:
            self.condition.release()


def squash_variables(nodes, nodeset, jobvars, groupvars, extravars):
    """Combine the Zuul job variable parameters into a hostvars dictionary.

    This is used by the executor when freezing job variables.  It
    simulates the Ansible variable precedence to arrive at a single
    hostvars dict (ultimately, all variables in ansible are hostvars;
    therefore group vars and extra vars can be combined in such a way
    to present a single hierarchy of variables visible to each host).

    :param list nodes: A list of node dictionaries (as returned by
         getHostList)
    :param Nodeset nodeset: A nodeset (used for group membership).
    :param dict jobvars: A dictionary corresponding to Zuul's job.vars.
    :param dict groupvars: A dictionary keyed by group name with a value of
         a dictionary of variables for that group.
    :param dict extravars: A dictionary corresponding to Zuul's job.extra-vars.

    :returns: A dict keyed by hostname with a value of a dictionary of
         variables for the host.
    """

    # The output dictionary, keyed by hostname.
    ret = {}

    # Zuul runs ansible with the default hash behavior of 'replace';
    # this means we don't need to deep-merge dictionaries.
    groups = sorted(nodeset.getGroups(), key=lambda g: g.name)
    for node in nodes:
        hostname = node['name']
        ret[hostname] = {}
        # group 'all'
        ret[hostname].update(jobvars)
        # group vars
        if 'all' in groupvars:
            ret[hostname].update(groupvars.get('all', {}))
        for group in groups:
            if hostname in group.nodes:
                ret[hostname].update(groupvars.get(group.name, {}))
        # host vars
        ret[hostname].update(node['host_vars'])
        # extra vars
        ret[hostname].update(extravars)

    return ret


def make_setup_inventory_dict(nodes, hostvars):
    hosts = {}
    for node in nodes:
        if (hostvars[node['name']]['ansible_connection'] in
            BLACKLISTED_ANSIBLE_CONNECTION_TYPES):
            continue
        hosts[node['name']] = hostvars[node['name']]

    inventory = {
        'all': {
            'hosts': hosts,
        }
    }

    return inventory


def is_group_var_set(name, host, nodeset, job):
    for group in nodeset.getGroups():
        if host in group.nodes:
            group_vars = job.group_variables.get(group.name, {})
            if name in group_vars:
                return True
    return False


def make_inventory_dict(nodes, nodeset, hostvars, unreachable_nodes,
                        remove_keys=None):
    hosts = {}
    for node in nodes:
        node_hostvars = hostvars[node['name']].copy()
        if remove_keys:
            for k in remove_keys:
                node_hostvars.pop(k, None)
        hosts[node['name']] = node_hostvars

    # localhost has no hostvars, so we'll set what we froze for
    # localhost as the 'all' vars which will in turn be available to
    # localhost plays.
    all_hostvars = hostvars['localhost'].copy()
    if remove_keys:
        for k in remove_keys:
            all_hostvars.pop(k, None)

    inventory = {
        'all': {
            'hosts': hosts,
            'vars': all_hostvars,
            'children': {},
        }
    }

    for group in nodeset.getGroups():
        group_hosts = {}
        for node_name in group.nodes:
            group_hosts[node_name] = None

        inventory['all']['children'].update({
            group.name: {
                'hosts': group_hosts,
            }})

    inventory['all']['children'].update({
        'zuul_unreachable': {
            'hosts': {n: None for n in unreachable_nodes}
        }})

    return inventory


class AnsibleJob(object):
    RESULT_NORMAL = 1
    RESULT_TIMED_OUT = 2
    RESULT_UNREACHABLE = 3
    RESULT_ABORTED = 4
    RESULT_DISK_FULL = 5

    RESULT_MAP = {
        RESULT_NORMAL: 'RESULT_NORMAL',
        RESULT_TIMED_OUT: 'RESULT_TIMED_OUT',
        RESULT_UNREACHABLE: 'RESULT_UNREACHABLE',
        RESULT_ABORTED: 'RESULT_ABORTED',
        RESULT_DISK_FULL: 'RESULT_DISK_FULL',
    }

    semaphore_sleep_time = 30

    def __init__(self, executor_server, build_request, arguments):
        logger = logging.getLogger("zuul.AnsibleJob")
        self.arguments = arguments
        with executor_server.zk_context as ctx:
            self.job = FrozenJob.fromZK(ctx, arguments["job_ref"])
        job_zuul_params = zuul_params_from_job(self.job)
        job_zuul_params["artifacts"] = self.arguments["zuul"].get("artifacts")
        if job_zuul_params["artifacts"] is None:
            del job_zuul_params["artifacts"]
        self.arguments["zuul"].update(job_zuul_params)
        if self.job.failure_output:
            self.failure_output = json.dumps(self.job.failure_output)
        else:
            self.failure_output = '[]'
        self.early_failure = False

        self.zuul_event_id = self.arguments["zuul_event_id"]
        # Record ansible version being used for the cleanup phase
        self.ansible_version = self.job.ansible_version
        self.ansible_split_streams = self.job.ansible_split_streams
        if self.ansible_split_streams is None:
            self.ansible_split_streams = False
        # TODO(corvus): Remove default setting after 4.3.0; this is to handle
        # scheduler/executor version skew.
        self.scheme = self.job.workspace_scheme or zuul.model.SCHEME_GOLANG
        self.workspace_merger = None
        self.log = get_annotated_logger(
            logger, self.zuul_event_id, build=build_request.uuid
        )
        self.executor_server = executor_server
        self.build_request = build_request
        self.nodeset = None
        self.node_request = None
        self.nodeset_request = None
        self.jobdir = None
        self.proc = None
        self.proc_lock = threading.Lock()
        # Is the current process a cleanup playbook?
        self.proc_cleanup = False
        self.running = False
        self.started = False  # Whether playbooks have started running
        self.time_starting_build = None
        self.paused = False
        self.aborted = False
        self.aborted_reason = None
        self._resume_event = threading.Event()
        self.thread = None
        self.project_info = {}
        self.merge_ops = []
        self.private_key_file = get_default(self.executor_server.config,
                                            'executor', 'private_key_file',
                                            '~/.ssh/id_rsa')
        self.winrm_key_file = get_default(self.executor_server.config,
                                          'executor', 'winrm_cert_key_file',
                                          '~/.winrm/winrm_client_cert.key')
        self.winrm_pem_file = get_default(self.executor_server.config,
                                          'executor', 'winrm_cert_pem_file',
                                          '~/.winrm/winrm_client_cert.pem')
        self.winrm_operation_timeout = get_default(
            self.executor_server.config,
            'executor',
            'winrm_operation_timeout_sec')
        self.winrm_read_timeout = get_default(
            self.executor_server.config,
            'executor',
            'winrm_read_timeout_sec')
        self.ssh_agent = SshAgent(zuul_event_id=self.zuul_event_id,
                                  build=self.build_request.uuid)
        self.port_forwards = []
        self.executor_variables_file = None

        self.cpu_times = {'user': 0, 'system': 0,
                          'children_user': 0, 'children_system': 0}

        if self.executor_server.config.has_option('executor', 'variables'):
            self.executor_variables_file = self.executor_server.config.get(
                'executor', 'variables')

        if self.executor_server.config.has_option('executor',
                                                  'output_max_bytes'):
            self.output_max_bytes = self.executor_server.config.get(
                'executor', 'output_max_bytes')
        else:
            self.output_max_bytes = OUTPUT_MAX_BYTES

        plugin_dir = self.executor_server.ansible_manager.getAnsiblePluginDir(
            self.ansible_version)
        self.library_dir = os.path.join(plugin_dir, 'library')
        self.module_utils_dir = os.path.join(self.library_dir, 'module_utils')
        self.action_dir = os.path.join(plugin_dir, 'action')
        self.callback_dir = os.path.join(plugin_dir, 'callback')
        self.lookup_dir = os.path.join(plugin_dir, 'lookup')
        self.filter_dir = os.path.join(plugin_dir, 'filter')
        self.ansible_callbacks = self.executor_server.ansible_callbacks
        # The result of getHostList
        self.host_list = None
        # The supplied job/host/group/extra vars, squashed.  Indexed
        # by hostname.
        self.original_hostvars = {}
        # The same, but frozen
        self.frozen_hostvars = {}
        # The zuul.* vars
        self.debug_zuul_vars = {}
        self.unreachable_nodes = set()
        self.waiting_for_semaphores = False
        try:
            max_attempts = self.arguments["zuul"]["max_attempts"]
        except KeyError:
            # TODO (swestphahl):
            # Remove backward compatibility handling
            max_attempts = self.arguments["max_attempts"]
        self.retry_limit = self.arguments["zuul"]["attempts"] >= max_attempts

        # We don't set normal_vars until we load the include-vars
        # files after preparing repos.
        self.secret_vars = self.arguments["secret_parent_data"]
        if self.job.workspace_checkout == 'auto':
            # If workspace-checkout is auto, we perform a checkout iff
            # we get an empty nodeset; the assumption is that an empty
            # nodeset performs work on the executor and otherwise on a
            # remote worker.
            self.checkout_workspace_repos =\
                not bool(self.job.nodeset.getNodes())
        else:
            self.checkout_workspace_repos = self.job.workspace_checkout
        self.log.info("Checkout workspace repos: %s",
                      self.checkout_workspace_repos)

    def run(self):
        self.running = True
        self.thread = threading.Thread(target=self.execute,
                                       name=f"build-{self.build_request.uuid}")
        self.thread.start()

    def stop(self, reason=None):
        self.aborted = True
        self.aborted_reason = reason

        # if paused we need to resume the job so it can be stopped
        self.resume()
        self.abortRunningProc()

    def pause(self):
        self.log.info(
            "Pausing job %s for ref %s (change %s)" % (
                self.arguments['zuul']['job'],
                self.arguments['zuul']['ref'],
                self.arguments['zuul']['change_url']))
        with _safe_open(self.jobdir.job_output_file, 'a') as job_output:
            job_output.write(
                "{now} |\n"
                "{now} | Job paused\n".format(now=datetime.datetime.now()))

        self.paused = True

        result_data, secret_result_data = self.getResultData()
        data = {'paused': self.paused,
                'data': result_data,
                'secret_data': secret_result_data}
        self.executor_server.pauseBuild(self.build_request, data)
        self._resume_event.wait()

    def resume(self):
        if not self.paused:
            return

        self.log.info(
            "Resuming job %s for ref %s (change %s)" % (
                self.arguments['zuul']['job'],
                self.arguments['zuul']['ref'],
                self.arguments['zuul']['change_url']))
        with _safe_open(self.jobdir.job_output_file, 'a') as job_output:
            job_output.write(
                "{now} | Job resumed\n"
                "{now} |\n".format(now=datetime.datetime.now()))

        self.paused = False
        self.executor_server.resumeBuild(self.build_request)
        self._resume_event.set()

    def wait(self):
        if self.thread:
            self.thread.join()

    def execute(self):
        with tracing.startSpanInContext(
                self.build_request.span_context,
                'JobExecution',
                attributes={'hostname': self.executor_server.hostname}
        ) as span:
            with trace.use_span(span):
                self.do_execute()

    def do_execute(self):
        try:
            self.time_starting_build = time.monotonic()

            # report that job has been taken
            self.executor_server.startBuild(
                self.build_request, self._base_job_data()
            )

            self.setNodeInfo()
            self.loadRepoState()
            self.getSparsePaths()

            self.ssh_agent.start()
            self.ssh_agent.add(self.private_key_file)
            for key in self.arguments.get('ssh_keys', []):
                if 'tenant_name' in key:
                    private_ssh_key, public_ssh_key = \
                        self.executor_server.keystore.getTenantSSHKeys(
                            key['tenant_name'])
                    name = '%s tenant key' % (key['tenant_name'])
                else:
                    private_ssh_key, public_ssh_key = \
                        self.executor_server.keystore.getProjectSSHKeys(
                            key['connection_name'],
                            key['project_name'])
                    name = '%s project key' % (key['project_name'])
                self.ssh_agent.addData(name, private_ssh_key)
            self.jobdir = JobDir(self.executor_server.jobdir_root,
                                 self.executor_server.keep_jobdir,
                                 str(self.build_request.uuid))
            self.lockNodes()
            self._execute()
        except NodeRequestError:
            result_data = dict(
                result="NODE_FAILURE", exception=traceback.format_exc()
            )
            self.executor_server.completeBuild(self.build_request, result_data)
        except BrokenProcessPool:
            # The process pool got broken, re-initialize it and send
            # ABORTED so we re-try the job.
            self.log.exception('Process pool got broken')
            self.executor_server.resetProcessPool()
            self._send_aborted()
        except ExecutorError as e:
            result_data = dict(result='ERROR', error_detail=e.args[0])
            self.log.debug("Sending result: %s", result_data)
            self.executor_server.completeBuild(self.build_request, result_data)
        except Exception:
            self.log.exception("Exception while executing job")
            data = {"exception": traceback.format_exc()}
            self.executor_server.completeBuild(self.build_request, data)
        finally:
            self.running = False
            if self.jobdir:
                try:
                    self.jobdir.cleanup()
                except Exception:
                    self.log.exception("Error cleaning up jobdir:")
            if self.ssh_agent:
                try:
                    self.ssh_agent.stop()
                except Exception:
                    self.log.exception("Error stopping SSH agent:")
            for fwd in self.port_forwards:
                try:
                    fwd.stop()
                except Exception:
                    self.log.exception("Error stopping port forward:")

            # Make sure we return the nodes to nodepool in any case.
            self.unlockNodes()
            try:
                self.executor_server.finishJob(self.build_request.uuid)
            except Exception:
                self.log.exception("Error finalizing job thread:")
            self.log.info("Job execution took: %.3f seconds",
                          self.end_time - self.time_starting_build)

    def setNodeInfo(self):
        try:
            # This shouldn't fail - but theoretically it could. So we handle
            # it similar to a NodeRequestError.
            self.nodeset = self.job.nodeset
        except KeyError:
            self.log.error("Unable to deserialize nodeset")
            raise NodeRequestError

        # Look up the NodeRequest with the provided ID from ZooKeeper. If
        # no ID is provided, this most probably means that the NodeRequest
        # wasn't submitted to ZooKeeper.
        node_request_id = self.arguments.get("noderequest_id")
        if node_request_id:
            zk_nodepool = self.executor_server.nodepool.zk_nodepool
            self.node_request = zk_nodepool.getNodeRequest(
                self.arguments["noderequest_id"])

            if self.node_request is None:
                self.log.error(
                    "Unable to retrieve NodeReqest %s from ZooKeeper",
                    node_request_id,
                )
                raise NodeRequestError
        elif nodeset_request_id := self.arguments.get("nodesetrequest_id"):
            self.nodeset_request = self.executor_server.launcher.getRequest(
                nodeset_request_id)
            if self.nodeset_request is None:
                self.log.error(
                    "Unable to retrieve NodesetReqest %s from ZooKeeper",
                    nodeset_request_id,
                )
                raise NodeRequestError

    def lockNodes(self):
        # If the node_request is not set, this probably means that the
        # NodeRequest didn't contain any nodes and thus was never submitted
        # to ZooKeeper. In that case we don't have anything to lock before
        # executing the build.
        if self.node_request:
            self.log.debug("Locking nodeset")
            try:
                self.executor_server.nodepool.acceptNodes(
                    self.node_request, self.nodeset)
            except Exception:
                self.log.exception(
                    "Error locking nodeset %s", self.nodeset
                )
                raise NodeRequestError
        elif self.nodeset_request:
            self.log.debug("Locking nodeset")
            try:
                self.executor_server.launcher.acceptNodeset(
                    self.nodeset_request, self.nodeset)
            except Exception:
                self.log.exception("Error locking nodeset %s", self.nodeset)
                raise NodeRequestError

    def unlockNodes(self):
        if self.node_request:
            tenant_name = self.arguments["zuul"]["tenant"]
            project_name = self.arguments["zuul"]["project"]["canonical_name"]
            duration = self.end_time - self.time_starting_build
            try:
                self.executor_server.nodepool.returnNodeSet(
                    self.nodeset,
                    self.build_request,
                    tenant_name,
                    project_name,
                    duration,
                    zuul_event_id=self.zuul_event_id,
                )
            except Exception:
                self.log.exception(
                    "Unable to return nodeset %s", self.nodeset
                )
        elif self.nodeset_request:
            tenant_name = self.arguments["zuul"]["tenant"]
            project_name = self.arguments["zuul"]["project"]["canonical_name"]
            duration = self.end_time - self.time_starting_build
            try:
                self.executor_server.launcher.returnNodeset(self.nodeset)
            except Exception:
                self.log.exception("Unable to return nodeset %s", self.nodeset)

    def loadRepoState(self):
        repo_state_keys = self.arguments.get('repo_state_keys')
        repo_state = RepoState()
        with self.executor_server.zk_context as ctx:
            blobstore = BlobStore(ctx)
            for link in repo_state_keys:
                repo_state.load(blobstore, link)
        d = repo_state.state
        # Ensure that we have an origin ref for every local branch.
        # Some of these will be overwritten later as we merge changes,
        # but for starters, we can use the current head of each
        # branch.
        for connection_state in d.values():
            for project_state in connection_state.values():
                for path, hexsha in list(project_state.items()):
                    if path.startswith('refs/heads/'):
                        name = path[11:]
                        remote_name = f'refs/remotes/origin/{name}'
                        if remote_name not in connection_state:
                            project_state[remote_name] = hexsha
        self.repo_state = d

    def _base_job_data(self):
        data = {
            # TODO(mordred) worker_name is needed as a unique name for the
            # client to use for cancelling jobs on an executor. It's
            # defaulting to the hostname for now, but in the future we
            # should allow setting a per-executor override so that one can
            # run more than one executor on a host.
            'worker_name': self.executor_server.hostname,
            'worker_hostname': self.executor_server.hostname,
            'worker_log_port': self.executor_server.log_streaming_port,
        }
        if self.executor_server.zone:
            data['worker_zone'] = self.executor_server.zone
        return data

    def _send_aborted(self):
        result = dict(result='ABORTED')
        self.executor_server.completeBuild(self.build_request, result)

    def _jobOutput(self, job_output, msg):
        job_output.write("{now} | {msg}\n".format(
            now=datetime.datetime.now(),
            msg=msg,
        ))

    def _execute(self):
        tracer = trace.get_tracer("zuul")
        args = self.arguments
        self.log.info(
            "Beginning job %s for ref %s (change %s)" % (
                args['zuul']['job'],
                args['zuul']['ref'],
                args['zuul']['change_url']))
        self.log.debug("Job root: %s" % (self.jobdir.root,))
        tasks = []
        projects = set()

        # Make sure all projects used by the job are updated ...
        for project in args['projects']:
            key = (project['connection'], project['name'])
            projects.add(key)

        # ... as well as all playbook and role projects ...
        for playbook in self.job.all_playbooks:
            key = (playbook['connection'], playbook['project'])
            projects.add(key)
            for role in playbook['roles']:
                key = (role['connection'], role['project'])
                projects.add(key)

        # ... and include-vars projects.
        for iv in self.job.include_vars:
            key = (iv['connection'], iv['project'])
            projects.add(key)

        with _safe_open(self.jobdir.job_output_file, 'a') as job_output:
            self._jobOutput(job_output, "Updating git repos")
            for (connection, project) in projects:
                self.log.debug("Updating project %s %s", connection, project)
                tasks.append(self.executor_server.update(
                    connection, project, repo_state=self.repo_state,
                    zuul_event_id=self.zuul_event_id,
                    build=self.build_request.uuid,
                    span_context=tracing.getSpanContext(
                        trace.get_current_span()),
                ))

            for task in tasks:
                task.wait()

                if not task.success:
                    # On transient error retry the job
                    if (hasattr(task, 'transient_error') and
                        task.transient_error):
                        result = dict(
                            result=None,
                            error_detail=f'Failed to update project '
                                         f'{task.project_name}')
                        self.executor_server.completeBuild(
                            self.build_request, result)
                        return

                    raise ExecutorError(
                        'Failed to update project %s' % task.project_name)

                # Take refs and branches from repo state
                project_repo_state = \
                    self.repo_state[task.connection_name][task.project_name]
                # All branch names
                branches = [
                    ref[11:]  # strip refs/heads/
                    for ref in project_repo_state
                    if ref.startswith('refs/heads/')
                ]
                # All refs without refs/*/ prefix
                refs = []
                for ref in project_repo_state:
                    r = '/'.join(ref.split('/')[2:])
                    if r:
                        refs.append(r)
                self.project_info[task.canonical_name] = {
                    'refs': refs,
                    'branches': branches,
                }

            # Early abort if abort requested
            if self.aborted:
                self._send_aborted()
                return
            self.log.debug("Git updates complete")

            self._jobOutput(job_output, "Cloning repos into workspace")
            self.workspace_merger = self.executor_server._getMerger(
                self.jobdir.src_root,
                self.executor_server.merge_root,
                logger=self.log,
                scheme=self.scheme)
            # Subset of the repo state that is relevant for this job
            relevant_repo_state = collections.defaultdict(dict)
            repos = {}
            for project in args['projects']:
                self.log.debug("Cloning %s/%s" % (project['connection'],
                                                  project['name'],))
                with tracer.start_as_current_span(
                        'BuildCloneRepo',
                        attributes={'connection': project['connection'],
                                    'project': project['name']}):
                    if self.checkout_workspace_repos:
                        sparse_paths = SparsePaths.FULL
                    else:
                        sparse_paths = SparsePaths.EMPTY
                    repo = self.workspace_merger.getRepo(
                        project['connection'],
                        project['name'],
                        sparse_paths=sparse_paths,
                    )
                repos[project['canonical_name']] = repo
                try:
                    project_rs = self.repo_state[project['connection']][
                        project['name']]
                except KeyError:
                    self.log.warning("Project %s/%s not in repo state",
                                     connection, project)
                else:
                    relevant_repo_state[project['connection']][
                        project['name']] = project_rs

            # Early abort if abort requested
            if self.aborted:
                self._send_aborted()
                return

            self._jobOutput(job_output, "Restoring repo states")
            with tracer.start_as_current_span('BuildSetRepoState'):
                recent = self.workspace_merger.setBulkRepoState(
                    relevant_repo_state,
                    self.zuul_event_id,
                    self.executor_server.process_worker,
                )

            # The commit ID of the original item (before merging).  Used
            # later for line mapping.
            item_commit = None
            # Repos that have been checked out during the merge operation.
            merged_repos = set()

            self._jobOutput(job_output, "Merging changes")
            merge_items = [
                i for i in args['items']
                if (i.get('number') and
                    (i['connection'], i['project']) in projects)
            ]
            if merge_items:
                with tracer.start_as_current_span(
                        'BuildMergeChanges'):
                    item_commit = self.doMergeChanges(
                        merge_items, self.repo_state, recent, merged_repos)
                if item_commit is None:
                    # There was a merge conflict and we have already sent
                    # a work complete result, don't run any jobs
                    return

            # Early abort if abort requested
            if self.aborted:
                self._send_aborted()
                return

            self._jobOutput(job_output, "Checking out repos")
            for project in args['projects']:
                repo = repos[project['canonical_name']]
                # If this project is the Zuul project and this is a ref
                # rather than a change, checkout the ref.
                if (project['canonical_name'] ==
                    args['zuul']['project']['canonical_name'] and
                    (not args['zuul'].get('branch')) and
                    args['zuul'].get('ref')):
                    ref = args['zuul']['ref']
                else:
                    ref = None
                selected_ref, selected_desc = self.resolveBranch(
                    project['canonical_name'],
                    ref,
                    args['branch'],
                    self.job.override_branch,
                    self.job.override_checkout,
                    project['override_branch'],
                    project['override_checkout'],
                    project['default_branch'])
                self.log.info("Checking out %s %s %s",
                              project['canonical_name'], selected_desc,
                              selected_ref)
                with tracer.start_as_current_span(
                        'BuildCheckout',
                        attributes={'connection': project['connection'],
                                    'project': project['name']}):
                    if self.checkout_workspace_repos:
                        hexsha = repo.checkout(selected_ref)
                    else:
                        hexsha = repo.getRef(selected_ref)[1]
                        # So that we have a consistent workspace
                        # regardless of whether we merged changes, remove
                        # the working tree if we're not checking out repos
                        # in the workspace.
                        if ((project['connection'], project['name'])
                            in merged_repos):
                            self.log.info("Emptying work tree for %s",
                                          project['canonical_name'])
                            repo.uncheckout()

                # Update the inventory variables to indicate the ref we
                # checked out
                p = args['zuul']['projects'][project['canonical_name']]
                p['checkout'] = selected_ref
                p['checkout_description'] = selected_desc
                p['commit'] = hexsha
                self.merge_ops.append(zuul.model.MergeOp(
                    cmd=['git', 'checkout', selected_ref],
                    path=repo.workspace_project_path))

            # Set the URL of the origin remote for each repo to a bogus
            # value. Keeping the remote allows tools to use it to determine
            # which commits are part of the current change.
            for repo in repos.values():
                repo.setRemoteUrl('file:///dev/null')

            # Early abort if abort requested
            if self.aborted:
                self._send_aborted()
                return

            self.loadIncludeVars()

            # We set the nodes to "in use" as late as possible. So in case
            # the build failed during the checkout phase, the node is
            # still untouched and nodepool can re-allocate it to a
            # different node request / build.  Below this point, we may
            # start to run tasks on nodes (prepareVars in particular uses
            # Ansible to freeze hostvars).
            if self.node_request:
                tenant_name = self.arguments["zuul"]["tenant"]
                project_name = self.arguments["zuul"]["project"][
                    "canonical_name"]
                self.executor_server.nodepool.useNodeSet(
                    self.nodeset, tenant_name, project_name,
                    self.zuul_event_id)
            elif self.nodeset_request:
                self.executor_server.launcher.useNodeset(
                    self.nodeset, self.zuul_event_id)

            self._jobOutput(job_output, "Preparing playbooks")
            # This prepares each playbook and the roles needed for each.
            self.preparePlaybooks(args)
            self.writeLoggingConfig()
            zuul_resources = self.prepareNodes(args)  # set self.host_list
            try:
                # set self.original_hostvars
                self.prepareVars(args, zuul_resources)
            except VariableNameError as e:
                raise ExecutorError(str(e))
            self.writeDebugInventory()
            self.writeRepoStateFile(repos)

            # Early abort if abort requested
            if self.aborted:
                self._send_aborted()
                return

            data = self._base_job_data()
            if self.executor_server.log_streaming_port != DEFAULT_FINGER_PORT:
                data['url'] = "finger://{hostname}:{port}/{uuid}".format(
                    hostname=self.executor_server.hostname,
                    port=self.executor_server.log_streaming_port,
                    uuid=self.build_request.uuid)
            else:
                data['url'] = 'finger://{hostname}/{uuid}'.format(
                    hostname=self.executor_server.hostname,
                    uuid=self.build_request.uuid)

            self.executor_server.updateBuildStatus(self.build_request, data)

        # job_output is out of scope now; playbook methods may open
        # the file again on their own.
        result, unreachable, error_detail = self.runPlaybooks(args)

        # Stop the persistent SSH connections.
        setup_status, setup_code = self.runAnsibleCleanup(
            self.jobdir.setup_playbook)

        if self.aborted_reason == self.RESULT_DISK_FULL:
            result = 'DISK_FULL'
        data, secret_data = self.getResultData()
        warnings = []
        self.mapLines(args, data, item_commit, warnings)
        warnings.extend(get_warnings_from_result_data(data, logger=self.log))

        # If we need to snapshot any nodes, do that here
        if result == 'SUCCESS':
            if not self.handleSnapshots(data):
                result = 'SNAPSHOT_FAILURE'

        result_data = dict(result=result,
                           error_detail=error_detail,
                           warnings=warnings,
                           unreachable=unreachable,
                           data=data,
                           secret_data=secret_data)

        # TODO do we want to log the secret data here?
        self.log.debug("Sending result: %s", result_data)
        self.executor_server.completeBuild(self.build_request, result_data)

    def handleSnapshots(self, result_data):
        # Return a data structure like:
        # zuul:
        #   snapshot_nodes:
        #     - node: <node uuid>
        #       image_name: debian-local
        success = True
        if ((not self.arguments.get('pipeline_builds_images', False)) or
            (not self.nodeset_request)):
            return success

        zuul_data = result_data.get('zuul', {})
        if not (snapshot_nodes := zuul_data.get('snapshot_nodes', [])):
            return success

        node_ids = [x['node'] for x in snapshot_nodes]
        with _safe_open(self.jobdir.job_output_file, 'a') as job_output:
            for status in self.executor_server.launcher.snapshotNodeset(
                    self.nodeset, node_ids, self.zuul_event_id):
                if status.type == status.Type.MESSAGE:
                    job_output.write("{now} | {msg}\n".format(
                        now=datetime.datetime.now(),
                        msg=status.message))
                elif status.type == status.Type.STARTED:
                    self.executor_server.updateBuildStatus(
                        self.build_request,
                        {'snapshot':
                         {'event': BuildEvent.TYPE_SNAPSHOT_STARTED,
                          'time': time.time(),
                          }})
                elif status.type == status.Type.COMPLETED:
                    self.executor_server.updateBuildStatus(
                        self.build_request,
                        {'snapshot':
                         {'event': BuildEvent.TYPE_SNAPSHOT_COMPLETED,
                          'time': time.time(),
                          }})

        artifacts = zuul_data.setdefault('artifacts', [])
        for snapshot_info in snapshot_nodes:
            for node in self.nodeset.getNodes():
                provider_node = getattr(node, "_provider_node", None)
                if provider_node.uuid == snapshot_info['node']:
                    break
            else:
                # Did not find the corresponding node
                success = False
                continue
            external_id = provider_node.snapshot.external_id
            if external_id is None:
                success = False
                continue
            artifact = dict(
                name="Snapshot image",
                url=external_id,
                metadata=dict(
                    type='zuul_image',
                    image_name=snapshot_info['image_name'],
                    format='snapshot',
                )
            )
            artifacts.append(artifact)
        return success

    def writeRepoStateFile(self, repos):
        # Write out the git operation performed up to this point
        repo_state_file = os.path.join(self.jobdir.log_root,
                                       'workspace-repos.json')
        # First make a connection+project_name -> path mapping
        workspace_paths = {}
        for project in self.arguments['projects']:
            repo = repos.get(project['canonical_name'])
            if repo:
                workspace_paths[(project['connection'], project['name'])] =\
                    repo.workspace_project_path
        repo_state = {}
        for connection_name, connection_value in self.repo_state.items():
            for project_name, project_value in connection_value.items():
                # We may have data in self.repo_state for repos that
                # are not present in the work dir (ie, they are used
                # for roles).  To keep things simple for now, we will
                # omit that, but we could add them later.
                workspace_project_path = workspace_paths.get(
                    (connection_name, project_name))
                if workspace_project_path:
                    repo_state[workspace_project_path] = project_value
        repo_state_data = dict(
            repo_state=repo_state,
            merge_ops=[o.toDict() for o in self.merge_ops],
            merge_name=self.executor_server.merge_name,
            merge_email=self.executor_server.merge_email,
        )
        with open(repo_state_file, 'w') as f:
            json.dump(repo_state_data, f, sort_keys=True, indent=2)

    def getResultData(self):
        data = {}
        secret_data = {}
        try:
            with _safe_open(self.jobdir.result_data_file) as f:
                file_data = f.read()
                if file_data:
                    file_data = json.loads(file_data)
                    data = file_data.get('data', {})
                    secret_data = file_data.get('secret_data', {})
            # Check the variable names for safety, but zuul is allowed.
            data_copy = data.copy()
            data_copy.pop('zuul', None)
            check_varnames(data_copy)
            secret_data_copy = data.copy()
            secret_data_copy.pop('zuul', None)
            check_varnames(secret_data_copy)
        except PathTraversalError:
            raise
        except VariableNameError as e:
            self.log.warning("Unable to load result data: %s", str(e))
        except Exception:
            self.log.exception("Unable to load result data:")
        return data, secret_data

    def mapLines(self, args, data, commit, warnings):
        # The data and warnings arguments are mutated in this method.

        # If we received file comments, map the line numbers before
        # we send the result.
        fc = data.get('zuul', {}).get('file_comments')
        if not fc:
            return
        disable = data.get('zuul', {}).get('disable_file_comment_line_mapping')
        if disable:
            return

        try:
            filecomments.validate(fc)
        except Exception as e:
            warnings.append("Job %s: validation error in file comments: %s" %
                            (args['zuul']['job'], str(e)))
            del data['zuul']['file_comments']
            return

        zuul_project = args['zuul']['project']
        # TODO: set checkout on zuul.project so we don't need to look
        # this up in zuul.projects
        selected_ref = args['zuul']['projects'][
            zuul_project['canonical_name']]['checkout']
        lines = filecomments.extractLines(fc)
        new_lines = {}
        mappers = {}
        for (filename, lineno) in lines:
            # Gerrit has several special file names (like /COMMIT_MSG) that
            # start with "/" and should not have mapping done on them
            if filename[0] == "/":
                continue

            try:
                mapper = mappers.get(filename)
                if not mapper:
                    mapper = self.getLineMapper(
                        zuul_project,
                        commit, selected_ref, filename)
                    mappers[filename] = mapper
                new_lineno = mapper.mapLine(lineno)
            except Exception as e:
                # Log at debug level since it's likely a job issue
                self.log.debug("Error mapping line:", exc_info=True)
                if isinstance(e, git.GitCommandError):
                    msg = e.stderr
                else:
                    msg = str(e)
                warnings.append("Job %s: unable to map line "
                                "for file comments: %s" %
                                (args['zuul']['job'], msg))
                new_lineno = None
            if new_lineno is not None:
                new_lines[(filename, lineno)] = new_lineno

        filecomments.updateLines(fc, new_lines)

    def getLineMapper(self, project, commit, head, filename):
        # project is args['zuul']['project']
        env_copy, popen = self.getWrappedUtilityContext()
        repo_dir = os.path.join(self.jobdir.work_root,
                                project['src_dir'], '.git')
        cmd = [
            'git',
            '--git-dir', repo_dir,
            'diff',
            '--no-color',
            f'{commit}..{head}',
            '--',
            filename,
        ]
        proc = popen(
            cmd,
            cwd=self.jobdir.work_root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            env=env_copy,
        )
        diff, err = proc.communicate()
        if err:
            err = err.decode('utf-8')
            self.log.warning("Error from line mapping diff: %s", err.strip())
        diff = diff.decode('utf-8')
        return filecomments.LineMapper(diff)

    def getWrappedUtilityContext(self):
        # Get a wrapped popen suitable for running a utility program
        # inside the sandbox.  Does not include any mounts other than
        # the work directory.
        env_copy = {}
        env_copy['TMP'] = self.jobdir.local_tmp
        env_copy['HOME'] = self.jobdir.utility_home
        ro_paths = []
        rw_paths = []
        secrets = {}
        wrapper = self.executor_server.execution_wrapper
        context = wrapper.getExecutionContext(ro_paths, rw_paths, secrets)
        return env_copy, context.getPopen(
            work_dir=self.jobdir.work_root,
            home_dir=self.jobdir.utility_home,
            share_net=False,
        )

    def doMergeChanges(self, items, repo_state, recent, merged_repos):
        try:
            ret = self.workspace_merger.mergeChanges(
                items, repo_state=repo_state,
                process_worker=self.executor_server.process_worker,
                recent=recent)
        except ValueError:
            # Return ABORTED so that we'll try again. At this point all of
            # the refs we're trying to merge should be valid refs. If we
            # can't fetch them, it should resolve itself.
            self.log.exception("Could not fetch refs to merge from remote")
            result = dict(result='ABORTED')
            self.executor_server.completeBuild(self.build_request, result)
            return None
        if not ret:  # merge conflict
            result = dict(result='MERGE_CONFLICT')
            if self.executor_server.statsd:
                base_key = "zuul.executor.{hostname}.merger"
                self.executor_server.statsd.incr(base_key + ".FAILURE")
            self.executor_server.completeBuild(self.build_request, result)
            return None

        if self.executor_server.statsd:
            base_key = "zuul.executor.{hostname}.merger"
            self.executor_server.statsd.incr(base_key + ".SUCCESS")
        recent = ret[3]
        orig_commit = ret[4]
        self.merge_ops = ret[5] or []
        for key, commit in recent.items():
            (connection, project, branch) = key
            # Compare the commit with the repo state. If it's included in the
            # repo state and it's the same we've set this ref already earlier
            # and don't have to set it again.
            project_repo_state = repo_state.get(
                connection, {}).get(project, {})
            repo_state_commit = project_repo_state.get(
                'refs/heads/%s' % branch)
            if repo_state_commit != commit:
                merged_repos.add((connection, project))
                repo = self.workspace_merger.getRepo(connection, project)
                repo.setRef('refs/heads/' + branch, commit)
        return orig_commit

    def resolveBranch(self, project_canonical_name, ref, zuul_branch,
                      job_override_branch, job_override_checkout,
                      project_override_branch, project_override_checkout,
                      project_default_branch):
        branches = self.project_info[project_canonical_name]['branches']
        refs = self.project_info[project_canonical_name]['refs']
        selected_ref = None
        selected_desc = None
        if project_override_checkout in refs:
            selected_ref = project_override_checkout
            selected_desc = 'project override ref'
        elif project_override_branch in branches:
            selected_ref = project_override_branch
            selected_desc = 'project override branch'
        elif job_override_checkout in refs:
            selected_ref = job_override_checkout
            selected_desc = 'job override ref'
        elif job_override_branch in branches:
            selected_ref = job_override_branch
            selected_desc = 'job override branch'
        elif ref and ref.startswith('refs/heads/'):
            selected_ref = ref[len('refs/heads/'):]
            selected_desc = 'branch ref'
        elif ref and ref.startswith('refs/tags/'):
            selected_ref = ref[len('refs/tags/'):]
            selected_desc = 'tag ref'
        elif zuul_branch and zuul_branch in branches:
            selected_ref = zuul_branch
            selected_desc = 'zuul branch'
        elif project_default_branch in branches:
            selected_ref = project_default_branch
            selected_desc = 'project default branch'
        else:
            raise ExecutorError("Project %s does not have the "
                                "default branch %s" %
                                (project_canonical_name,
                                 project_default_branch))
        return (selected_ref, selected_desc)

    def getAnsibleTimeout(self, start, timeout):
        if timeout is not None:
            now = time.time()
            elapsed = now - start
            timeout = max(0, timeout - elapsed)
        return timeout

    def runPlaybooks(self, args):
        result = None
        error_detail = None
        aborted = False
        unknown_result = False
        unreachable = False

        with _safe_open(self.jobdir.job_output_file, 'a') as job_output:
            job_output.write("{now} | Running Ansible setup\n".format(
                now=datetime.datetime.now()
            ))
        # Run the Ansible 'setup' module on all hosts in the inventory
        # at the start of the job with a 60 second timeout.  If we
        # aren't able to connect to all the hosts and gather facts
        # within that timeout, there is likely a network problem
        # between here and the hosts in the inventory; return them and
        # reschedule the job.

        self.writeSetupInventory()
        setup_status, setup_code = self.runAnsibleSetup(
            self.jobdir.setup_playbook, self.ansible_version)
        if setup_status != self.RESULT_NORMAL or setup_code != 0:
            if setup_status == self.RESULT_TIMED_OUT:
                error_detail = "Ansible setup timeout"
            elif setup_status == self.RESULT_UNREACHABLE:
                error_detail = "Host unreachable"
                unreachable = True
            return result, unreachable, error_detail

        # Freeze the variables so that we have a copy of them without
        # any jinja templates for use in the trusted execution
        # context.
        self.writeInventory(self.jobdir.freeze_playbook,
                            self.original_hostvars)
        freeze_status, freeze_code = self.runAnsibleFreeze(
            self.jobdir.freeze_playbook, self.ansible_version)
        # We ignore the return code because we run this playbook on
        # all hosts, even ones which we didn't run the setup playbook
        # on.  If a host is unreachable, we should still proceed (a
        # later playbook may cause it to become reachable).  We just
        # won't have all of the variables set.
        if freeze_status != self.RESULT_NORMAL:
            if freeze_status == self.RESULT_TIMED_OUT:
                error_detail = "Ansible variable freeze timeout"
            return result, unreachable, error_detail

        self.loadFrozenHostvars()
        pre_failed = False
        nesting_level_achieved = None
        should_retry = False  # We encountered a retryable failure
        will_retry = False  # The above and we have not hit retry_limit
        # Whether we will allow POST_FAILURE to override the result:
        allow_post_result = True
        success = False
        if self.executor_server.statsd:
            key = "zuul.executor.{hostname}.starting_builds"
            self.executor_server.statsd.timing(
                key, (time.monotonic() - self.time_starting_build) * 1000)

        self.started = True
        time_started = time.time()
        # If we have a pre-run playbook timeout we use that. Any pre-run
        # runtime counts against the total timeout for pre-run and run as the
        # timeout value is "total" job timeout which accounts for
        # pre-run and run playbooks. post-run is different because
        # it is used to copy out job logs and we want to do our best
        # to copy logs even when the job has timed out.
        job_timeout = self.job.pre_timeout or self.job.timeout
        for index, playbook in enumerate(self.jobdir.pre_playbooks):
            nesting_level_achieved = playbook.nesting_level
            ansible_timeout = self.getAnsibleTimeout(time_started, job_timeout)
            pre_status, pre_code = self.runAnsiblePlaybook(
                playbook, ansible_timeout, self.ansible_version, phase='pre',
                index=index)
            if pre_status != self.RESULT_NORMAL or pre_code != 0:
                # These should really never fail, so return None and have
                # zuul try again
                pre_failed = True
                should_retry = True
                allow_post_result = False
                if pre_status == self.RESULT_UNREACHABLE:
                    error_detail = "Host unreachable"
                    unreachable = True
                break

        self.log.debug(
            "Overall ansible cpu times: user=%.2f, system=%.2f, "
            "children_user=%.2f, children_system=%.2f" %
            (self.cpu_times['user'], self.cpu_times['system'],
             self.cpu_times['children_user'],
             self.cpu_times['children_system']))

        if not pre_failed:
            # Update job_timeout to reset for longer timeout value if
            # pre-timeout is set
            job_timeout = self.job.timeout
            # At this point, we have gone all the way down.
            nesting_level_achieved = None
            for index, playbook in enumerate(self.jobdir.playbooks):
                # We need all run playbooks to succeed in order to
                # consider the job successful, hence we need to reset
                # the flag here before each playbook execution.
                success = False
                ansible_timeout = self.getAnsibleTimeout(
                    time_started, job_timeout)
                job_status, job_code = self.runAnsiblePlaybook(
                    playbook, ansible_timeout, self.ansible_version,
                    phase='run', index=index)
                if job_status == self.RESULT_ABORTED:
                    aborted = True
                    break
                elif job_status == self.RESULT_TIMED_OUT:
                    # Set the pre-failure flag so this doesn't get
                    # overridden by a post-failure.
                    allow_post_result = False
                    result = 'TIMED_OUT'
                    break
                elif job_status == self.RESULT_UNREACHABLE:
                    # In case we encounter unreachable nodes we need to return
                    # None so the job can be retried. However we still want to
                    # run post playbooks to get a chance to upload logs.
                    allow_post_result = False
                    error_detail = "Host unreachable"
                    unreachable = True
                    if self.early_failure:
                        # Override the result and make sure
                        # we don't retry.
                        self.log.info(
                            "Setting unreachable result as FAILURE "
                            "due to early failure detection")
                        result = 'FAILURE'
                    else:
                        should_retry = True
                    break
                elif job_status == self.RESULT_NORMAL:
                    success = (job_code == 0)
                    if success:
                        if self.early_failure:
                            # Override the result, but proceed as
                            # normal.
                            self.log.info(
                                "Overriding SUCCESS result as FAILURE "
                                "due to early failure detection")
                            result = 'FAILURE'
                        else:
                            result = 'SUCCESS'
                    else:
                        result = 'FAILURE'
                        break
                else:
                    # The result of the job is indeterminate.  Zuul will
                    # run it again.
                    unknown_result = True
                    aborted = True
                    if self.early_failure:
                        result = 'ERROR'
                        error_detail = "Early failure and indeterminate result"
                    else:
                        result = None
                        error_detail = None
                    break

        # check if we need to pause here
        if not aborted:
            result_data, secret_result_data = self.getResultData()
            pause = result_data.get('zuul', {}).get('pause')
            if success and pause:
                self.pause()
            if self.aborted:
                aborted = True

        if not aborted:
            # Report a failure if pre-run failed and the user reported to
            # zuul that the job should not retry.
            if (result_data.get('zuul', {}).get('retry') is False and
                pre_failed):
                result = "FAILURE"
                allow_post_result = False
                should_retry = False

        post_timeout = self.job.post_timeout
        for index, playbook in enumerate(self.jobdir.post_playbooks):
            if aborted and not playbook.cleanup:
                continue
            if (nesting_level_achieved is not None and
                playbook.nesting_level > nesting_level_achieved):
                self.log.info("Skipping post-run playbook %s "
                              "since nesting level %s "
                              "is greater than achieved level of %s",
                              playbook.path, playbook.nesting_level,
                              nesting_level_achieved)
                continue
            will_retry = should_retry and not self.retry_limit
            # Post timeout operates a little differently to the main job
            # timeout. We give each post playbook the full post timeout to
            # do its job because post is where you'll often record job logs
            # which are vital to understanding why timeouts have happened in
            # the first place.
            post_status, post_code = self.runAnsiblePlaybook(
                playbook, post_timeout, self.ansible_version, success,
                phase='post', index=index, will_retry=will_retry)
            if post_status == self.RESULT_ABORTED:
                aborted = True
            if post_status == self.RESULT_UNREACHABLE and not playbook.cleanup:
                # In case we encounter unreachable nodes we need to return None
                # so the job can be retried. However in the case of post
                # playbooks we should still try to run all playbooks to get a
                # chance to upload logs.
                error_detail = "Host unreachable"
                unreachable = True
                if allow_post_result and not self.early_failure:
                    should_retry = True
            if post_status != self.RESULT_NORMAL or post_code != 0:
                # If we encountered a pre-failure, that takes
                # precedence over the post result.
                if not playbook.ignore_result:
                    success = False
                if allow_post_result and not playbook.ignore_result:
                    result = 'POST_FAILURE'
                if (not aborted and
                    (index + 1) == len(self.jobdir.post_playbooks)):
                    self._logFinalPlaybookError()

        if unknown_result:
            return result, unreachable, None

        if aborted:
            return 'ABORTED', unreachable, None

        if should_retry:
            return None, unreachable, error_detail

        return result, unreachable, error_detail

    def _logFinalPlaybookError(self):
        # Failures in the final post playbook can include failures
        # uploading logs, which makes diagnosing issues difficult.
        # Grab the output from the last playbook from the json
        # file and log it.
        json_output = self.jobdir.job_output_file.replace('txt', 'json')
        self.log.debug("Final playbook failed")
        try:
            with _safe_open(json_output, 'r') as f:
                output = json.load(f)
            last_playbook = output[-1]
            # Transform json to yaml - because it's easier to read and given
            # the size of the data it'll be extra-hard to read this as an
            # all on one line stringified nested dict.
            yaml_out = yaml.safe_dump(last_playbook, default_flow_style=False)
            for line in yaml_out.split('\n'):
                self.log.debug(line)
        except PathTraversalError:
            raise
        except FileNotFoundError:
            self.log.debug(f"JSON logfile {json_output} is missing")
            return
        except Exception:
            self.log.exception(
                f"Could not decode json from {json_output}")

    def getHostList(self, args, nodes):
        hosts = []
        for node in nodes:
            # NOTE(mordred): This assumes that the nodepool launcher
            # and the zuul executor both have similar network
            # characteristics, as the launcher will do a test for ipv6
            # viability and if so, and if the node has an ipv6
            # address, it will be the interface_ip.  force-ipv4 can be
            # set to True in the clouds.yaml for a cloud if this
            # results in the wrong thing being in interface_ip
            # TODO(jeblair): Move this notice to the docs.
            for name in node.name:
                connection_type = node.connection_type
                ansible_host = node.interface_ip
                if connection_type == "kubectl":
                    # Nodepool overloaded interface_ip, zuul-launcher
                    # will set this value
                    kubecon = getattr(node, 'kubernetes_connection', None)
                    if kubecon:
                        ansible_host = kubecon['pod']
                port = node.connection_port
                host_vars = self.job.host_variables.get(name, {}).copy()
                check_varnames(host_vars)
                host_vars.update(dict(
                    ansible_host=ansible_host,
                    ansible_user=self.executor_server.default_username,
                    ansible_port=port,
                    nodepool=dict(
                        label=node.label,
                        az=node.az,
                        cloud=node.cloud,
                        provider=node.provider,
                        region=node.region,
                        host_id=node.host_id,
                        external_id=getattr(node, 'external_id', None),
                        slot=node.slot,
                        interface_ip=node.interface_ip,
                        public_ipv4=node.public_ipv4,
                        # This is backwards-compatible behavior for Nodepool;
                        # we should make a new set of variables for Zuul and
                        # avoid this.
                        private_ipv4=node.private_ipv4 or node.public_ipv4,
                        public_ipv6=node.public_ipv6,
                        private_ipv6=node.private_ipv6,
                        node_properties=node.node_properties,
                    ),
                    zuul_node=dict(
                        uuid=getattr(node, 'uuid', None),
                        label=node.label,
                        az=node.az,
                        cloud=node.cloud,
                        provider=node.provider,
                        region=node.region,
                        host_id=node.host_id,
                        external_id=getattr(node, 'external_id', None),
                        slot=node.slot,
                        interface_ip=node.interface_ip,
                        public_ipv4=node.public_ipv4,
                        private_ipv4=node.private_ipv4,
                        public_ipv6=node.public_ipv6,
                        private_ipv6=node.private_ipv6,
                        reuse=node.reuse,
                        node_properties=node.node_properties,
                    ),
                ))

                # Ansible >=2.8 introduced "auto" as an
                # ansible_python_interpreter argument that looks up
                # which python to use on the remote host in an inbuilt
                # table and essentially "does the right thing"
                # (i.e. chooses python3 on 3-only hosts like later
                # Fedoras).
                # If ansible_python_interpreter is set either as a group
                # var or all-var, then don't do anything here; let the
                # user control.
                api = 'ansible_python_interpreter'
                if (api not in self.normal_vars and
                    not is_group_var_set(api, name, self.nodeset, self.job)):
                    python = getattr(node, 'python_path', 'auto')
                    host_vars.setdefault(api, python)

                username = node.username
                if username:
                    host_vars['ansible_user'] = username

                if connection_type:
                    host_vars['ansible_connection'] = connection_type
                    if connection_type == "winrm":
                        host_vars['ansible_winrm_transport'] = 'certificate'
                        host_vars['ansible_winrm_cert_pem'] = \
                            self.winrm_pem_file
                        host_vars['ansible_winrm_cert_key_pem'] = \
                            self.winrm_key_file
                        # NOTE(tobiash): This is necessary when using default
                        # winrm self-signed certificates. This is probably what
                        # most installations want so hard code this here for
                        # now.
                        host_vars['ansible_winrm_server_cert_validation'] = \
                            'ignore'
                        if self.winrm_operation_timeout is not None:
                            host_vars['ansible_winrm_operation_timeout_sec'] =\
                                self.winrm_operation_timeout
                        if self.winrm_read_timeout is not None:
                            host_vars['ansible_winrm_read_timeout_sec'] = \
                                self.winrm_read_timeout
                    elif connection_type == "kubectl":
                        host_vars['ansible_kubectl_context'] = \
                            getattr(node, 'kubectl_context', None)

                shell_type = getattr(node, 'shell_type', None)
                if shell_type:
                    host_vars['ansible_shell_type'] = shell_type

                host_keys = []
                for key in getattr(node, 'host_keys', []):
                    if port != 22:
                        host_keys.append("[%s]:%s %s" % (
                            node.interface_ip, port, key))
                    else:
                        host_keys.append("%s %s" % (
                            node.interface_ip, key))
                if not getattr(node, 'host_keys', None):
                    host_vars['ansible_ssh_common_args'] = \
                        '-o StrictHostKeyChecking=false'

                hosts.append(dict(
                    name=name,
                    host_vars=host_vars,
                    host_keys=host_keys))
        return hosts

    def findPlaybook(self, path, trusted=False):
        if os.path.exists(path):
            return path
        raise ExecutorError("Unable to find playbook %s" % path)

    def getSparsePaths(self):
        # Later we checkout one copy of each context-project-branch
        # combination of repos that are used for playbooks and roles,
        # so we need to find the full set of sparse paths for each
        # repo ahead of time.  Let's not worry about doing that
        # per-branch, we'll just get the superset of all playbook and
        # role paths necessary.
        self.repo_sparse_paths = collections.defaultdict(set)
        for playbook in (self.job.pre_run +
                         self.job.run +
                         self.job.post_run):
            # For every playbook, add the playbook's containing directory
            key = (playbook['connection'], playbook['project'])
            paths = self.repo_sparse_paths[key]
            pbpath = os.path.dirname(playbook['path'])
            # Remove any / at the start and add one at the end.
            pbpath = pbpath.lstrip('/') + '/'
            paths.add(pbpath)
            for role in playbook['roles']:
                # For every role repo, add the roles directory in case
                # the repo is a collection of roles, and also add all
                # of the individual role component directories in case
                # the repo is a bare role.
                key = (role['connection'], role['project'])
                paths = self.repo_sparse_paths[key]
                paths.update({'roles/', 'tasks/', 'handlers/',
                              'templates/', 'files/', 'vars/',
                              'defaults/', 'meta/', 'library/',
                              'module_utils/', 'lookup_plugins/'})
        for iv in self.job.include_vars:
            # For each include-vars, add the contining dir
            key = (iv['connection'], iv['project'])
            paths = self.repo_sparse_paths[key]
            ivpath = os.path.dirname(iv['name'])
            # Remove any / at the start and add one at the end.
            ivpath = ivpath.lstrip('/') + '/'
            paths.add(ivpath)

    def preparePlaybooks(self, args):
        self.writeAnsibleConfig(self.jobdir.setup_playbook)
        self.writeAnsibleConfig(self.jobdir.freeze_playbook)

        for playbook in self.job.pre_run:
            jobdir_playbook = self.jobdir.addPrePlaybook()
            self.preparePlaybook(jobdir_playbook, playbook, args)

        job_playbook = None
        for playbook in self.job.run:
            jobdir_playbook = self.jobdir.addPlaybook()
            self.preparePlaybook(jobdir_playbook, playbook, args)
            if jobdir_playbook.path is not None:
                if job_playbook is None:
                    job_playbook = jobdir_playbook

        if job_playbook is None:
            raise ExecutorError("No playbook specified")

        # If there are cleanup playbooks, then mutate them into
        # post-run playbooks.
        post_run = self.job.post_run
        cleanup_run = self.job.cleanup_run.copy()
        while cleanup_run:
            cleanup_playbook = cleanup_run.pop(0)
            for i in range(len(post_run)):
                post_playbook = post_run[i]
                if (cleanup_playbook.get('nesting_level', 0) >
                    post_playbook.get('nesting_level', 0)):
                    post_run.insert(i, cleanup_playbook)
                    break
            # If we did not insert, then append:
            else:
                post_run.append(cleanup_playbook)

        for playbook in post_run:
            jobdir_playbook = self.jobdir.addPostPlaybook()
            self.preparePlaybook(jobdir_playbook, playbook, args)
            if playbook in self.job.cleanup_run:
                # This backwards compatible handling for MODEL_API < 30
                jobdir_playbook.cleanup = True
                jobdir_playbook.ignore_result = True

    def preparePlaybook(self, jobdir_playbook, playbook, args):
        # Check out the playbook repo if needed and set the path to
        # the playbook that should be run.
        self.log.debug("Prepare playbook repo for %s: %s@%s" %
                       (playbook['trusted'] and 'trusted' or 'untrusted',
                        playbook['project'], playbook['branch']))
        source = self.executor_server.connections.getSource(
            playbook['connection'])
        project = source.getProject(playbook['project'])
        branch = playbook['branch']
        # MODEL_API < 30: if nesting_level not provided, use 0 so
        # everything runs (old behavior)
        jobdir_playbook.nesting_level = playbook.get('nesting_level', 0)
        jobdir_playbook.cleanup = playbook.get('cleanup', False)
        jobdir_playbook.trusted = playbook['trusted']
        jobdir_playbook.branch = branch
        jobdir_playbook.project_canonical_name = project.canonical_name
        jobdir_playbook.canonical_name_and_path = os.path.join(
            project.canonical_name, playbook['path'])
        jobdir_playbook.semaphores = playbook['semaphores']
        path = None

        if not jobdir_playbook.trusted:
            path = self.checkoutUntrustedProject(project, branch, args)
        else:
            path = self.checkoutTrustedProject(project, branch, args)
        path = os.path.join(path, playbook['path'])

        jobdir_playbook.path = self.findPlaybook(
            path,
            trusted=jobdir_playbook.trusted)

        # If this playbook doesn't exist, don't bother preparing
        # roles.
        if not jobdir_playbook.path:
            return

        for role in playbook['roles']:
            self.prepareRole(jobdir_playbook, role, args)

        secrets, jobdir_playbook.secrets_oidc = \
            self.decryptSecrets(playbook['secrets'])
        secrets = self.mergeSecretVars(secrets, jobdir_playbook.secrets_oidc)
        if secrets:
            check_varnames(secrets)
            secrets = yaml.mark_strings_unsafe(secrets)
            jobdir_playbook.secrets_content = yaml.ansible_unsafe_dump(
                secrets, default_flow_style=False)
            jobdir_playbook.secrets_keys = set(secrets.keys())
        if jobdir_playbook.secrets_oidc:
            jobdir_playbook.secrets_keys.update(
                jobdir_playbook.secrets_oidc.keys())

        self.writeAnsibleConfig(jobdir_playbook)

    def decryptSecrets(self, secrets):
        """Decrypt the secrets dictionary provided by the scheduler

        The input dictionary has a frozen secret dictionary as its
        value (with encrypted data and the project name of the key to
        use to decrypt it).

        The output dictionary simply has decrypted data as its value.

        :param dict secrets: The playbook secrets dictionary from the
            scheduler

        :returns: Tuple of decrypted secrets dictionary and oidc dictionary

        """
        ret_secret_data = {}
        ret_secret_oidc = {}
        with self.executor_server.zk_context as ctx:
            blobstore = BlobStore(ctx)
            for secret_name, secret_index in secrets.items():
                if isinstance(secret_index, dict):
                    key = secret_index['blob']
                    data = blobstore.get(key)
                    frozen_secret = json.loads(data.decode('utf-8'))
                else:
                    frozen_secret = self.job.secrets[secret_index]
                secret = zuul.model.Secret(secret_name, None)
                decrypted_dict = yaml.encrypted_load(
                    frozen_secret['encrypted_data'])
                if "secret_oidc" not in decrypted_dict:
                    # MODEL_API < 35
                    secret.secret_data = decrypted_dict
                else:
                    secret.secret_data = decrypted_dict['secret_data']
                    secret.secret_oidc = decrypted_dict['secret_oidc']

                if secret.secret_data:
                    private_secrets_key, public_secrets_key = \
                        self.executor_server.keystore.getProjectSecretsKeys(
                            frozen_secret['connection_name'],
                            frozen_secret['project_name'])
                    secret = secret.decrypt(private_secrets_key)
                    ret_secret_data[secret_name] = secret.secret_data
                else:
                    ret_secret_oidc[secret_name] = secret.secret_oidc

        return ret_secret_data, ret_secret_oidc

    def checkoutTrustedProject(self, project, branch, args):
        pi = self.jobdir.getTrustedProject(project.canonical_name,
                                           branch)
        if not pi:
            pi = self.jobdir.addTrustedProject(project.canonical_name,
                                               branch)
            self.log.debug("Cloning %s@%s into new trusted space %s",
                           project, branch, pi.root)
            # We always use the golang scheme for playbook checkouts
            # (so that the path indicates the canonical repo name for
            # easy debugging; there are no concerns with collisions
            # since we only have one repo in the working dir).
            merger = self.executor_server._getMerger(
                pi.root,
                self.executor_server.merge_root,
                logger=self.log,
                scheme=zuul.model.SCHEME_GOLANG)
            if self.checkout_workspace_repos:
                sparse_paths = SparsePaths.FULL
            else:
                key = (project.connection_name, project.name)
                sparse_paths = list(self.repo_sparse_paths[key])
            hexsha = merger.checkoutBranch(
                project.connection_name, project.name,
                branch,
                repo_state=self.repo_state,
                process_worker=self.executor_server.process_worker,
                sparse_paths=sparse_paths,
                zuul_event_id=self.zuul_event_id)
            pi.commit = hexsha
        else:
            self.log.debug("Using existing repo %s@%s in trusted space %s",
                           project, branch, pi.root)

        path = os.path.join(pi.root,
                            project.canonical_hostname,
                            project.name)
        return path

    def checkoutUntrustedProject(self, project, branch, args):
        pi = self.jobdir.getUntrustedProject(project.canonical_name,
                                             branch)
        if not pi:
            pi = self.jobdir.addUntrustedProject(project.canonical_name,
                                                 branch)
            # If the project is in the dependency chain, clone from
            # there so we pick up any speculative changes, otherwise,
            # clone from the cache.
            #
            # We always use the golang scheme for playbook checkouts
            # (so that the path indicates the canonical repo name for
            # easy debugging; there are no concerns with collisions
            # since we only have one repo in the working dir).
            merger = None
            for p in args['projects']:
                if (p['connection'] == project.connection_name and
                    p['name'] == project.name):
                    # We already have this repo prepared
                    self.log.debug("Found workdir repo for untrusted project")
                    merger = self.executor_server._getMerger(
                        pi.root,
                        self.jobdir.src_root,
                        logger=self.log,
                        scheme=zuul.model.SCHEME_GOLANG,
                        cache_scheme=self.scheme)
                    # Since we're not restoring the repo state and
                    # we're skipping the ref setup after cloning, we
                    # do need to at least ensure the branch we're
                    # going to check out exists.
                    repo = self.workspace_merger.getRepo(p['connection'],
                                                         p['name'],
                                                         keep_remote_url=True)
                    # We call it a branch, but it can actually be any
                    # ref including a tag.  Get the ref object so we
                    # can duplicate the full path.
                    ref_path, ref_sha = repo.getRef(branch)
                    repo_state = {
                        p['connection']: {
                            p['name']: {
                                ref_path: ref_sha,
                            }
                        }
                    }
                    break

            if merger is None:
                merger = self.executor_server._getMerger(
                    pi.root,
                    self.executor_server.merge_root,
                    logger=self.log,
                    scheme=zuul.model.SCHEME_GOLANG)
                # If we don't have this repo yet prepared we need to
                # restore the repo state. Otherwise we have
                # speculative merges in the repo and must not restore
                # the repo state again.
                repo_state = self.repo_state

            self.log.debug("Cloning %s@%s into new untrusted space %s",
                           project, branch, pi.root)
            if self.checkout_workspace_repos:
                sparse_paths = SparsePaths.FULL
            else:
                key = (project.connection_name, project.name)
                sparse_paths = list(self.repo_sparse_paths[key])
            hexsha = merger.checkoutBranch(
                project.connection_name, project.name,
                branch, repo_state=repo_state,
                process_worker=self.executor_server.process_worker,
                sparse_paths=sparse_paths,
                zuul_event_id=self.zuul_event_id)
            pi.commit = hexsha
        else:
            self.log.debug("Using existing repo %s@%s in trusted space %s",
                           project, branch, pi.root)

        path = os.path.join(pi.root,
                            project.canonical_hostname,
                            project.name)
        return path

    def mergeSecretVars(self, secrets, secrets_oidc):
        '''
        Merge secret return data with secrets.

        :arg secrets dict: Actual Zuul secrets.
        :arg secrets_oidc dict: Actual Zuul oidc secrets.
        '''

        secret_vars = self.secret_vars

        # We need to handle secret vars specially.  We want to pass
        # them to Ansible as we do secrets, but we want them to have
        # the lowest priority.  In order to accomplish that, we will
        # simply remove any top-level secret var with the same name as
        # anything above it in precedence.

        other_vars = set()
        other_vars.update(self.normal_vars.keys())
        for group_vars in self.job.group_variables.values():
            other_vars.update(group_vars.keys())
        for host_vars in self.job.host_variables.values():
            other_vars.update(host_vars.keys())
        other_vars.update(self.job.extra_variables.keys())
        other_vars.update(secrets.keys())
        other_vars.update(secrets_oidc.keys())

        ret = secret_vars.copy()
        for key in other_vars:
            ret.pop(key, None)

        # Add in the actual secrets
        if secrets:
            ret.update(secrets)

        return ret

    def prepareRole(self, jobdir_playbook, role, args):
        if role['type'] == 'zuul':
            role_info = jobdir_playbook.addRole()
            self.prepareZuulRole(jobdir_playbook, role, args, role_info)

    def findRole(self, path, trusted=False):
        d = os.path.join(path, 'tasks')
        if os.path.isdir(d):
            # None signifies that the repo is a bare role
            return None
        d = os.path.join(path, 'roles')
        if os.path.isdir(d):
            # This repo has a collection of roles
            return d
        # It is neither a bare role, nor a collection of roles
        raise RoleNotFoundError("Unable to find role in %s" % (path,))

    def selectBranchForProject(self, project, project_default_branch,
                               alternative_branch=None,
                               consider_ref=None):
        # Find if the project is one of the job-specified projects.
        # If it is, we can honor the project checkout-override options.
        args = self.arguments
        args_project = {}
        for p in args['projects']:
            if (p['canonical_name'] == project.canonical_name):
                args_project = p
                break

        ref = None
        if consider_ref:
            # If this project is the Zuul project and this is a ref
            # rather than a change, checkout the ref.
            if (project.canonical_name ==
                args['zuul']['project']['canonical_name'] and
                (not args['zuul'].get('branch')) and
                args['zuul'].get('ref')):
                ref = args['zuul']['ref']

        return self.resolveBranch(
            project.canonical_name,
            ref,
            args['branch'] or alternative_branch,
            self.job.override_branch,
            self.job.override_checkout,
            args_project.get('override_branch'),
            args_project.get('override_checkout'),
            project_default_branch)

    def prepareZuulRole(self, jobdir_playbook, role, args, role_info):
        self.log.debug("Prepare zuul role for %s" % (role,))
        # Check out the role repo if needed
        source = self.executor_server.connections.getSource(
            role['connection'])
        project = source.getProject(role['project'])
        name = role['target_name']
        path = None

        # Find the branch to use for this role.  We should generally
        # follow the normal fallback procedure, unless this role's
        # project is the playbook's project, in which case we should
        # use the playbook branch.
        if jobdir_playbook.project_canonical_name == project.canonical_name:
            branch = jobdir_playbook.branch
            self.log.debug("Role project is playbook project, "
                           "using playbook branch %s", branch)
            role_info.checkout_description = 'playbook branch'
            role_info.checkout = branch
        else:
            branch, selected_desc = self.selectBranchForProject(
                project, role['project_default_branch'],
                alternative_branch=jobdir_playbook.branch)
            self.log.debug("Role using %s %s", selected_desc, branch)
            role_info.checkout_description = selected_desc
            role_info.checkout = branch

        if not jobdir_playbook.trusted:
            path = self.checkoutUntrustedProject(project, branch, args)
        else:
            path = self.checkoutTrustedProject(project, branch, args)

        # The name of the symlink is the requested name of the role
        # (which may be the repo name or may be something else; this
        # can come into play if this is a bare role).
        link = os.path.join(role_info.root, name)
        link = os.path.realpath(link)
        if not link.startswith(os.path.realpath(role_info.root)):
            raise ExecutorError("Invalid role name %s" % name)
        os.symlink(path, link)

        role_info.link_name = link
        role_info.link_target = path
        try:
            role_path = self.findRole(link, trusted=jobdir_playbook.trusted)
        except RoleNotFoundError:
            if role['implicit']:
                self.log.debug("Implicit role not found in %s", link)
                return
            raise
        if role_path is None:
            # In the case of a bare role, add the containing directory
            role_path = role_info.root
        role_info.role_path = role_path
        self.log.debug("Adding role path %s", role_path)
        jobdir_playbook.roles_path.append(role_path)

    def prepareKubeConfig(self, jobdir, data):
        kube_cfg_path = jobdir.kubeconfig
        if os.path.exists(kube_cfg_path):
            with open(kube_cfg_path) as f:
                kube_cfg = yaml.safe_load(f)
        else:
            kube_cfg = {
                'apiVersion': 'v1',
                'kind': 'Config',
                'preferences': {},
                'users': [],
                'clusters': [],
                'contexts': [],
                'current-context': None,
            }
        # Add cluster
        cluster_name = urlsplit(data['host']).netloc.replace('.', '-')

        # Do not add a cluster/server that already exists in the kubeconfig
        # because that leads to 'duplicate name' errors on multi-node builds.
        # Also, as the cluster name directly corresponds to a server, there
        # is no need to add it twice.
        if cluster_name not in [c['name'] for c in kube_cfg['clusters']]:
            cluster = {
                'server': data['host'],
            }
            if data.get('ca_crt'):
                cluster['certificate-authority-data'] = data['ca_crt']
            if data['skiptls']:
                cluster['insecure-skip-tls-verify'] = True
            kube_cfg['clusters'].append({
                'name': cluster_name,
                'cluster': cluster,
            })

        # Add user
        user_name = "%s:%s" % (data['namespace'], data['user'])
        kube_cfg['users'].append({
            'name': user_name,
            'user': {
                'token': data['token'],
            },
        })

        # Add context
        data['context_name'] = "%s/%s" % (user_name, cluster_name)
        kube_cfg['contexts'].append({
            'name': data['context_name'],
            'context': {
                'user': user_name,
                'cluster': cluster_name,
                'namespace': data['namespace']
            }
        })
        if not kube_cfg['current-context']:
            kube_cfg['current-context'] = data['context_name']

        with open(kube_cfg_path, "w") as of:
            of.write(yaml.safe_dump(kube_cfg, default_flow_style=False))

    def prepareNodes(self, args):
        # Returns the zuul.resources ansible variable for later user

        # The (non-resource) nodes we want to keep in the inventory
        inventory_nodes = []
        # The zuul.resources ansible variable
        zuul_resources = {}
        for node in self.nodeset.getNodes():
            if node.connection_type in (
                    'namespace', 'project', 'kubectl'):
                # TODO: decrypt resource data using scheduler key
                data = node.connection_port or node.kubernetes_connection
                # Setup kube/config file
                self.prepareKubeConfig(self.jobdir, data)
                # Convert connection_port in kubectl connection parameters
                node.connection_port = None
                node.kubectl_namespace = data['namespace']
                node.kubectl_context = data['context_name']
                # Add node information to zuul.resources
                zuul_resources[node.name[0]] = {
                    'namespace': data['namespace'],
                    'context': data['context_name'],
                }
                if node.connection_type in ('project', 'namespace'):
                    # Project are special nodes that are not the inventory
                    pass
                else:
                    inventory_nodes.append(node)
                    # Add the real pod name to the resources_var
                    zuul_resources[node.name[0]]['pod'] = data['pod']
                    # Add the resources (cpu, mem) of the pod to the inventory
                    # since Ansible might report values of the host machine in
                    # its ansible_* host facts (ie. its not cgroup aware)
                    zuul_resources[node.name[0]]['resources'] = node.resources

                    fwd = KubeFwd(zuul_event_id=self.zuul_event_id,
                                  build=self.build_request.uuid,
                                  kubeconfig=self.jobdir.kubeconfig,
                                  context=data['context_name'],
                                  namespace=data['namespace'],
                                  pod=data['pod'])
                    try:
                        fwd.start()
                        self.port_forwards.append(fwd)
                        zuul_resources[node.name[0]]['stream_port1'] = \
                            fwd.port1
                        zuul_resources[node.name[0]]['stream_port2'] = \
                            fwd.port2
                    except Exception:
                        self.log.exception("Unable to start port forward:")
                        self.log.error("Kubectl and socat are required for "
                                       "streaming logs")
            else:
                # A normal node to include in inventory
                inventory_nodes.append(node)

        self.host_list = self.getHostList(args, inventory_nodes)

        with open(self.jobdir.known_hosts, 'w') as known_hosts:
            for node in self.host_list:
                for key in node['host_keys']:
                    known_hosts.write('%s\n' % key)
        return zuul_resources

    def loadIncludeVars(self):
        parent_data = self.arguments["parent_data"]

        normal_vars = parent_data.copy()
        for iv in self.job.include_vars:
            source = self.executor_server.connections.getSource(
                iv['connection'])
            project = source.getProject(iv['project'])

            branch, selected_desc = self.selectBranchForProject(
                project, iv['project_default_branch'],
                consider_ref=iv.get('use_ref', True))
            self.log.debug("Include-vars project %s using %s %s",
                           project.canonical_name, selected_desc, branch)
            if not iv['trusted']:
                path = self.checkoutUntrustedProject(project, branch,
                                                     self.arguments)
            else:
                path = self.checkoutTrustedProject(project, branch,
                                                   self.arguments)
            path = os.path.join(path, iv['name'])
            try:
                with open(path) as f:
                    self.log.debug("Loading vars from %s", path)
                    new_vars = yaml.safe_load(f)
                    normal_vars = Job._deepUpdate(normal_vars, new_vars)
            except FileNotFoundError:
                self.log.info("Vars file %s not found", path)
                if iv['required']:
                    raise ExecutorError(
                        f"Required vars file {iv['name']} not found")

        self.normal_vars = Job._deepUpdate(normal_vars,
                                           self.job.variables)

    def prepareVars(self, args, zuul_resources):
        normal_vars = self.normal_vars.copy()
        check_varnames(normal_vars)

        # Check the group and extra var names for safety; they'll get
        # merged later
        for group in self.nodeset.getGroups():
            group_vars = self.job.group_variables.get(group.name, {})
            check_varnames(group_vars)

        check_varnames(self.job.extra_variables)

        # Start with what the client supplied
        zuul_vars = args['zuul'].copy()
        # Overlay the zuul.resources we set in prepareNodes
        zuul_vars['resources'] = zuul_resources

        # Add in executor info
        zuul_vars['executor'] = dict(
            hostname=self.executor_server.hostname,
            src_root=self.jobdir.src_root,
            log_root=self.jobdir.log_root,
            work_root=self.jobdir.work_root,
            result_data_file=self.jobdir.result_data_file,
            inventory_file=self.jobdir.inventory)
        zuul_vars['ansible_version'] = self.ansible_version

        # Add playbook_context info
        zuul_vars['playbook_context'] = dict(
            playbook_projects={},
            pre_playbooks=[],
            playbooks=[],
            post_playbooks=[],
        )
        strip = len(self.jobdir.root) + 1
        for pi in self.jobdir.trusted_projects.values():
            root = os.path.join(pi.root[strip:], pi.canonical_name)
            zuul_vars['playbook_context']['playbook_projects'][
                root] = pi.toDict()
        for pi in self.jobdir.untrusted_projects.values():
            root = os.path.join(pi.root[strip:], pi.canonical_name)
            zuul_vars['playbook_context']['playbook_projects'][
                root] = pi.toDict()

        for pb_phase in ("pre_playbooks", "playbooks", "post_playbooks"):
            for pb in getattr(self.jobdir, pb_phase):
                zuul_vars['playbook_context'][pb_phase].append(dict(
                    path=pb.path[strip:],
                    roles=[ri.toDict(self.jobdir.root) for ri in pb.roles
                           if ri.role_path is not None],
                ))

        # The zuul vars in the debug inventory.yaml file should not
        # have any !unsafe tags, so save those before we update the
        # execution version of those.
        self.debug_zuul_vars = copy.deepcopy(zuul_vars)
        if 'change_message' in zuul_vars:
            zuul_vars['change_message'] = yaml.mark_strings_unsafe(
                zuul_vars['change_message'])
        for item in zuul_vars['items']:
            if 'change_message' in item:
                item['change_message'] = yaml.mark_strings_unsafe(
                    item['change_message'])
        for ref in zuul_vars.get('buildset_refs', []):
            if 'change_message' in ref:
                ref['change_message'] = yaml.mark_strings_unsafe(
                    ref['change_message'])
        for ref in zuul_vars.get('build_refs', []):
            if 'change_message' in ref:
                ref['change_message'] = yaml.mark_strings_unsafe(
                    ref['change_message'])

        out_vars = {
            'zuul': zuul_vars,
            'zuul_user_dir': (
                "{{ inventory_hostname == 'localhost' and "
                "zuul.executor.work_root or ansible_user_dir }}"
            ),
        }

        with open(self.jobdir.zuul_vars, 'w') as zuul_vars_yaml:
            zuul_vars_yaml.write(
                yaml.ansible_unsafe_dump(out_vars,
                                         default_flow_style=False))

        # Squash all and extra vars into localhost (it's not
        # explicitly listed).
        localhost = {
            'name': 'localhost',
            'host_vars': {},
        }
        host_list = self.host_list + [localhost]
        self.original_hostvars = squash_variables(
            host_list, self.nodeset, normal_vars,
            self.job.group_variables, self.job.extra_variables)

    def loadFrozenHostvars(self):
        # Read in the frozen hostvars, and remove the frozen variable
        # from the fact cache.

        # localhost hold our "all" vars.
        localhost = {
            'name': 'localhost',
        }
        host_list = self.host_list + [localhost]
        for host in host_list:
            self.log.debug("Loading frozen vars for %s", host['name'])
            path = os.path.join(self.jobdir.fact_cache, host['name'])
            facts = {}
            if os.path.exists(path):
                with open(path) as f:
                    facts = json.loads(f.read())
            self.frozen_hostvars[host['name']] = facts.pop('_zuul_frozen', {})
            # Always include the nodepool vars, even if we didn't run
            # the playbook for this host.
            if 'host_vars' in host:
                self.frozen_hostvars[host['name']]['nodepool'] =\
                    host['host_vars']['nodepool']
                self.frozen_hostvars[host['name']]['zuul_node'] =\
                    host['host_vars']['zuul_node']
            with open(path, 'w') as f:
                f.write(json.dumps(facts))

            # While we're here, update both hostvars dicts with
            # an !unsafe copy of the original input as well.
            unsafe = yaml.mark_strings_unsafe(
                self.original_hostvars[host['name']])
            self.frozen_hostvars[host['name']]['unsafe_vars'] = unsafe

            unsafe = yaml.mark_strings_unsafe(
                self.original_hostvars[host['name']])
            self.original_hostvars[host['name']]['unsafe_vars'] = unsafe

    def updateUnreachableHosts(self):
        # Load the unreachable file and update our running scoreboard
        # of unreachable hosts.
        try:
            with _safe_open(self.jobdir.job_unreachable_file, 'r') as f:
                for line in f.readlines():
                    node = line.strip()
                    self.log.debug("Noting %s as unreachable", node)
                    self.unreachable_nodes.add(node)
        except PathTraversalError:
            raise
        except Exception:
            self.log.exception("Error updating unreachable hosts:")
        try:
            os.unlink(self.jobdir.job_unreachable_file)
        except Exception:
            self.log.exception("Error unlinking unreachable host file")

    def writeDebugInventory(self):
        # This file is unused by Zuul, but the base jobs copy it to logs
        # for debugging, so let's continue to put something there.
        inventory = make_inventory_dict(
            self.host_list, self.nodeset, self.original_hostvars,
            self.unreachable_nodes)

        inventory['all']['vars']['zuul'] = self.debug_zuul_vars
        with open(self.jobdir.inventory, 'w') as inventory_yaml:
            inventory_yaml.write(
                yaml.ansible_unsafe_dump(
                    inventory,
                    ignore_aliases=True,
                    default_flow_style=False))

    def writeSetupInventory(self):
        jobdir_playbook = self.jobdir.setup_playbook
        setup_inventory = make_setup_inventory_dict(
            self.host_list, self.original_hostvars)
        setup_inventory = yaml.mark_strings_unsafe(setup_inventory)

        with open(jobdir_playbook.inventory, 'w') as inventory_yaml:
            # Write this inventory with !unsafe tags to avoid mischief
            # since we're running without bwrap.
            inventory_yaml.write(
                yaml.ansible_unsafe_dump(setup_inventory,
                                         default_flow_style=False))

    def writeInventory(self, jobdir_playbook, hostvars):
        inventory = make_inventory_dict(
            self.host_list, self.nodeset, hostvars,
            self.unreachable_nodes,
            remove_keys=jobdir_playbook.secrets_keys)

        with open(jobdir_playbook.inventory, 'w') as inventory_yaml:
            inventory_yaml.write(
                yaml.ansible_unsafe_dump(inventory, default_flow_style=False))

    def writeLoggingConfig(self):
        self.log.debug("Writing logging config for job %s %s",
                       self.jobdir.job_output_file,
                       self.jobdir.logging_json)
        logging_config = zuul.ansible.logconfig.JobLoggingConfig(
            job_output_file=self.jobdir.job_output_file)
        logging_config.writeJson(self.jobdir.logging_json)

    def writeAnsibleConfig(self, jobdir_playbook):
        callback_path = self.callback_dir
        with open(jobdir_playbook.ansible_config, 'w') as config:
            config.write('[defaults]\n')
            config.write('inventory = %s\n' % jobdir_playbook.inventory)
            config.write('local_tmp = %s\n' % self.jobdir.local_tmp)
            config.write('retry_files_enabled = False\n')
            config.write('gathering = smart\n')
            config.write('fact_caching = jsonfile\n')
            config.write('fact_caching_connection = %s\n' %
                         self.jobdir.fact_cache)
            config.write('library = %s\n' % self.library_dir)
            config.write('module_utils = %s\n' % self.module_utils_dir)
            config.write('command_warnings = False\n')
            # Disable the Zuul callback plugins for the freeze playbooks
            # as that output is verbose and would be confusing for users.
            if jobdir_playbook != self.jobdir.freeze_playbook:
                config.write('callback_plugins = %s\n' % callback_path)
                config.write('stdout_callback = zuul_stream\n')
            config.write('filter_plugins = %s\n'
                         % self.filter_dir)
            config.write('nocows = True\n')  # save useless stat() calls
            # bump the timeout because busy nodes may take more than
            # 10s to respond
            config.write('timeout = 30\n')

            # We need the action dir to make the zuul_return plugin
            # available to every job, and a customized command plugin
            # to inject zuul_log_id to make log streaming work.
            config.write('action_plugins = %s\n'
                         % self.action_dir)

            if jobdir_playbook.roles_path:
                config.write('roles_path = %s\n' % ':'.join(
                    jobdir_playbook.roles_path))

            # On playbooks with secrets we want to prevent the
            # printing of args since they may be passed to a task or a
            # role. Otherwise, printing the args could be useful for
            # debugging.
            config.write('display_args_to_stdout = %s\n' %
                         str(not jobdir_playbook.secrets_content))

            # Increase the internal poll interval of ansible.
            # The default interval of 0.001s is optimized for interactive
            # ui at the expense of CPU load. As we have a non-interactive
            # automation use case a longer poll interval is more suitable
            # and reduces CPU load of the ansible process.
            config.write('internal_poll_interval = 0.01\n')

            if self.ansible_callbacks:
                config.write('callbacks_enabled =\n')
                for callback in self.ansible_callbacks.keys():
                    config.write('    %s,\n' % callback)

            config.write('[ssh_connection]\n')
            # NOTE(pabelanger): Try up to 3 times to run a task on a host, this
            # helps to mitigate UNREACHABLE host errors with SSH.
            config.write('retries = 3\n')
            # NB: when setting pipelining = True, keep_remote_files
            # must be False (the default).  Otherwise it apparently
            # will override the pipelining option and effectively
            # disable it.  Pipelining has a side effect of running the
            # command without a tty (ie, without the -tt argument to
            # ssh).  We require this behavior so that if a job runs a
            # command which expects interactive input on a tty (such
            # as sudo) it does not hang.
            config.write('pipelining = True\n')
            config.write('control_path_dir = %s\n' % self.jobdir.control_path)
            ssh_args = "-o ControlMaster=auto -o ControlPersist=60s " \
                "-o ServerAliveInterval=60 " \
                "-o UserKnownHostsFile=%s" % self.jobdir.known_hosts
            config.write('ssh_args = %s\n' % ssh_args)

            if self.ansible_callbacks:
                for cb_name, cb_config in self.ansible_callbacks.items():
                    config.write("[callback_%s]\n" % cb_name)
                    for k, n in cb_config.items():
                        config.write("%s = %s\n" % (k, n))

    def _ansibleTimeout(self, msg):
        self.log.warning(msg)
        self.abortRunningProc(timed_out=True)

    def abortRunningProc(self, timed_out=False):
        with self.proc_lock:
            if not self.proc:
                self.log.debug("Abort: no process is running")
                return
            elif self.proc_cleanup and not timed_out:
                self.log.debug("Abort: cleanup is in progress")
                return

            self.log.debug("Abort: sending kill signal to job process group")
            try:
                pgid = os.getpgid(self.proc.pid)
                os.killpg(pgid, signal.SIGKILL)
            except Exception:
                self.log.exception("Exception while killing ansible process:")

    def runAnsible(self, cmd, timeout, playbook, ansible_version,
                   allow_pre_fail, wrapped=True, cleanup=False,
                   phase=None, index=None):
        # Cleanup any forbidden files in the home directory.
        # TODO: add a job-based allowlist of files to explicitly copy
        # between playbook runs.

        for path in self.jobdir.path_blocklist:
            try:
                _safe_unlink(path)
            except FileNotFoundError:
                pass

        config_file = playbook.ansible_config
        env_copy = {key: value
                    for key, value in os.environ.copy().items()
                    if not key.startswith("ZUUL_")}
        env_copy.update(self.ssh_agent.env)
        env_copy['ZUUL_OUTPUT_MAX_BYTES'] = str(self.output_max_bytes)
        env_copy['ZUUL_JOB_LOG_CONFIG'] = self.jobdir.logging_json
        env_copy['ZUUL_JOB_FAILURE_OUTPUT'] = self.failure_output
        env_copy['ZUUL_JOBDIR'] = self.jobdir.root
        if self.executor_server.log_console_port != DEFAULT_STREAM_PORT:
            env_copy['ZUUL_CONSOLE_PORT'] = str(
                self.executor_server.log_console_port)
        env_copy['TMP'] = self.jobdir.local_tmp
        env_copy['KUBECONFIG'] = self.jobdir.kubeconfig
        env_copy['ZUUL_ANSIBLE_SPLIT_STREAMS'] = str(
            self.ansible_split_streams)
        pythonpath = env_copy.get('PYTHONPATH')
        if pythonpath:
            pythonpath = [pythonpath]
        else:
            pythonpath = []

        ansible_dir = self.executor_server.ansible_manager.getAnsibleDir(
            ansible_version)
        pythonpath = [ansible_dir] + pythonpath
        env_copy['PYTHONPATH'] = os.path.pathsep.join(pythonpath)

        if playbook.trusted:
            opt_prefix = 'trusted'
        else:
            opt_prefix = 'untrusted'
        ro_paths = get_default(self.executor_server.config, 'executor',
                               '%s_ro_paths' % opt_prefix)
        rw_paths = get_default(self.executor_server.config, 'executor',
                               '%s_rw_paths' % opt_prefix)
        ro_paths = ro_paths.split(":") if ro_paths else []
        rw_paths = rw_paths.split(":") if rw_paths else []

        ro_paths.append(ansible_dir)
        ro_paths.append(
            self.executor_server.ansible_manager.getAnsibleInstallDir(
                ansible_version))
        ro_paths.append(self.jobdir.ansible_root)
        ro_paths.append(self.jobdir.kube_dir)
        ro_paths.append(self.jobdir.trusted_root)
        ro_paths.append(self.jobdir.untrusted_root)
        ro_paths.append(playbook.root)

        rw_paths.append(self.jobdir.ansible_cache_root)

        if self.executor_variables_file:
            ro_paths.append(self.executor_variables_file)

        secrets = {}
        if playbook.secrets_content:
            secrets[playbook.secrets] = playbook.secrets_content

        if wrapped:
            wrapper = self.executor_server.execution_wrapper
        else:
            wrapper = self.executor_server.connections.drivers['nullwrap']

        context = wrapper.getExecutionContext(ro_paths, rw_paths, secrets)

        popen = context.getPopen(
            work_dir=self.jobdir.work_root,
            home_dir=self.jobdir.home,
            ssh_auth_sock=env_copy.get('SSH_AUTH_SOCK'))

        env_copy['ANSIBLE_CONFIG'] = config_file
        env_copy['HOME'] = self.jobdir.home

        with self.proc_lock:
            self.proc_cleanup = playbook.cleanup
            if self.aborted and not playbook.cleanup:
                return (self.RESULT_ABORTED, None)
            self.log.debug("Ansible command: ANSIBLE_CONFIG=%s ZUUL_JOBDIR=%s "
                           "ZUUL_JOB_LOG_CONFIG=%s PYTHONPATH=%s TMP=%s %s",
                           env_copy['ANSIBLE_CONFIG'],
                           env_copy['ZUUL_JOBDIR'],
                           env_copy['ZUUL_JOB_LOG_CONFIG'],
                           env_copy['PYTHONPATH'],
                           env_copy['TMP'],
                           " ".join(shlex.quote(c) for c in cmd))
            self.proc = popen(
                cmd,
                cwd=self.jobdir.work_root,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                # Either this must be present, or we need the
                # --new-session argument for bwrap.
                start_new_session=True,
                env=env_copy,
            )

        syntax_buffer = collections.deque(maxlen=BUFFER_LINES_FOR_SYNTAX)
        ret = None
        if timeout:
            watchdog = Watchdog(timeout, self._ansibleTimeout,
                                ("Ansible timeout exceeded: %s" % timeout,))
            watchdog.start()
        try:
            ansible_log = get_annotated_logger(
                logging.getLogger("zuul.AnsibleJob.output"),
                self.zuul_event_id, build=self.build_request.uuid)

            first = True
            for line in iter(
                    partial(self.proc.stdout.readline, OUTPUT_MAX_LINE_BYTES),
                    b''):
                if line and line[-1:] != b'\n':
                    self.log.warning(
                        "Ansible output exceeds max. line size of %s KiB",
                        OUTPUT_MAX_LINE_BYTES / 1024)
                if first:
                    # When we receive our first log line, bwrap should
                    # have started Ansible and it should still be
                    # running.  This is our best opportunity to list
                    # the process ids in the namespace.
                    try:
                        ns, pids = context.getNamespacePids(self.proc)
                        if ns is not None:
                            self.log.debug("Process ids in namespace %s: %s",
                                           ns, pids)
                    except Exception:
                        self.log.exception("Unable to list namespace pids")
                    first = False
                if b'FATAL ERROR DURING FILE TRANSFER' in line:
                    # This can end up being an unreachable host (see
                    # below), so don't pre-fail in this case.
                    allow_pre_fail = False
                result_line = None
                if line.startswith(b'RESULT'):
                    result_line = line[len('RESULT'):].strip()
                    if result_line == b'unreachable':
                        self.log.info("Early unreachable host")
                        allow_pre_fail = False
                    if allow_pre_fail and result_line == b'failure':
                        self.log.info("Early failure in job")
                        self.early_failure = True
                        self.executor_server.updateBuildStatus(
                            self.build_request, {'pre_fail': True})
                        # No need to pre-fail again
                        allow_pre_fail = False
                syntax_buffer.append(line)

                if line.startswith(b'fatal'):
                    line = line[:8192].rstrip()
                else:
                    line = line[:1024].rstrip()

                if result_line:
                    ansible_log.debug("Ansible result output: %s" % (line,))
                else:
                    ansible_log.debug("Ansible output: %s" % (line,))
            self.log.debug("Ansible output terminated")
            try:
                cpu_times = self.proc.cpu_times()
                self.log.debug("Ansible cpu times: user=%.2f, system=%.2f, "
                               "children_user=%.2f, "
                               "children_system=%.2f" %
                               (cpu_times.user, cpu_times.system,
                                cpu_times.children_user,
                                cpu_times.children_system))
                self.cpu_times['user'] += cpu_times.user
                self.cpu_times['system'] += cpu_times.system
                self.cpu_times['children_user'] += cpu_times.children_user
                self.cpu_times['children_system'] += cpu_times.children_system
            except psutil.NoSuchProcess:
                self.log.warn("Cannot get cpu_times for process %d. Is your"
                              "/proc mounted with hidepid=2"
                              " on an old linux kernel?", self.proc.pid)
            ret = self.proc.wait()
            self.log.debug("Ansible exit code: %s" % (ret,))
        finally:
            if timeout:
                watchdog.stop()
                self.log.debug("Stopped watchdog")
            self.log.debug("Stopped disk job killer")

        with self.proc_lock:
            self.proc.stdout.close()
            self.proc = None
            self.proc_cleanup = False

        if timeout and watchdog.timed_out:
            try:
                message = 'This playbook timed out.'
                start = None
                end = None
                if watchdog.end != 0:
                    start = watchdog.end - timeout
                    end = time.time()
                self.addPlaybookToJson(playbook, phase, index,
                                       message, start, end)
            except Exception:
                # This is a best effort attempt at adding additional logging
                # info when things have gone wrong. Simply ignore errors if
                # things are so wrong we can't add more debug info.
                self.log.exception('Unable to append timeout json data:')
            return (self.RESULT_TIMED_OUT, None)

        ansible_error_lines = []
        # Note: Unlike documented ansible currently wrongly returns 4 on
        # unreachable so we have the zuul_unreachable callback module that
        # creates the file nodes.unreachable in case there were
        # unreachable nodes. This can be removed once ansible returns a
        # distinct value for unreachable.
        # TODO: Investigate whether the unreachable callback can be
        # removed in favor of the ansible result log stream (see above
        # in pre-fail)
        if ret == 3 or _safe_exists(self.jobdir.job_unreachable_file):
            # AnsibleHostUnreachable: We had a network issue connecting to
            # our zuul-worker.
            self.updateUnreachableHosts()
            return (self.RESULT_UNREACHABLE, None)
        elif ret == -9:
            # Received abort request.
            return (self.RESULT_ABORTED, None)
        elif ret == 1:
            found_marker = False
            for line in syntax_buffer:
                if line.startswith(b'ERROR!'):
                    found_marker = True
                if not found_marker:
                    continue
                line = line.decode('utf-8').rstrip()
                ansible_error_lines.append(line)
        elif ret == 4:
            # Ansible could not parse the yaml.
            self.log.debug("Ansible parse error")
            # TODO(mordred) If/when we rework use of logger in ansible-playbook
            # we'll want to change how this works to use that as well. For now,
            # this is what we need to do.
            # TODO(mordred) We probably want to put this into the json output
            # as well.
            ansible_error_lines.append('ANSIBLE PARSE ERROR')
            for line in syntax_buffer:
                line = line.decode('utf-8').rstrip()
                ansible_error_lines.append(line)
        elif ret == 250:
            # Unexpected error from ansible
            ansible_error_lines.append('UNEXPECTED ANSIBLE ERROR')
            found_marker = False
            for line in syntax_buffer:
                if line.startswith(b'ERROR! Unexpected Exception'):
                    found_marker = True
                if not found_marker:
                    continue
                line = line.decode('utf-8').rstrip()
                ansible_error_lines.append(line)
        elif ret == 2:
            found_marker = False
            for line in syntax_buffer:
                # This is a workaround to detect winrm connection failures
                # that are not detected by ansible. These can be detected
                # if the string 'FATAL ERROR DURING FILE TRANSFER' is in
                # the ansible output. In this case we should treat the
                # host as unreachable and retry the job.
                if b'FATAL ERROR DURING FILE TRANSFER' in line:
                    return self.RESULT_UNREACHABLE, None

                # Extract errors for special cases that are treated like
                # task errors by Ansible (e.g. missing role when using
                # 'include_role').
                if line.startswith(b'ERROR!'):
                    found_marker = True
                if not found_marker:
                    continue
                line = line.decode('utf-8').rstrip()
                ansible_error_lines.append(line)
        if ansible_error_lines:
            with _safe_open(self.jobdir.job_output_file, 'a') as job_output:
                for line in ansible_error_lines:
                    job_output.write("{now} | {line}\n".format(
                        now=datetime.datetime.now(),
                        line=line))
        if ansible_error_lines and phase and index is not None:
            try:
                message = ' '.join(ansible_error_lines)
                start = None
                end = None
                if timeout and watchdog.end != 0:
                    start = watchdog.end - timeout
                    end = time.time()
                self.addPlaybookToJson(playbook, phase, index,
                                       message, start, end)
            except Exception:
                # This is a best effort attempt at adding additional logging
                # info when things have gone wrong. Simply ignore errors if
                # things are so wrong we can't add more debug info.
                self.log.exception('Unable to append Ansible error json data:')

        if self.aborted:
            return (self.RESULT_ABORTED, None)

        return (self.RESULT_NORMAL, ret)

    def runAnsibleSetup(self, playbook, ansible_version):
        if self.executor_server.verbose:
            verbose = '-vvv'
        else:
            verbose = '-v'

        # TODO: select correct ansible version from job
        ansible = self.executor_server.ansible_manager.getAnsibleCommand(
            ansible_version,
            command='ansible')
        cmd = [ansible, '*', verbose, '-m', 'setup',
               '-i', playbook.inventory,
               '-a', 'gather_subset=!all']
        if self.executor_variables_file is not None:
            cmd.extend(['-e@%s' % self.executor_variables_file])

        result, code = self.runAnsible(
            cmd=cmd, timeout=self.executor_server.setup_timeout,
            playbook=playbook, ansible_version=ansible_version,
            allow_pre_fail=False, wrapped=False)
        self.log.debug("Ansible complete, result %s code %s" % (
            self.RESULT_MAP[result], code))
        if self.executor_server.statsd:
            base_key = "zuul.executor.{hostname}.phase.setup"
            self.executor_server.statsd.incr(base_key + ".%s" %
                                             self.RESULT_MAP[result])
        return result, code

    def runAnsibleFreeze(self, playbook, ansible_version):
        if self.executor_server.verbose:
            verbose = '-vvv'
        else:
            verbose = '-v'

        # Create a play for each host with set_fact, and every
        # top-level variable.
        plays = []
        localhost = {
            'name': 'localhost',
        }
        for host in self.host_list + [localhost]:
            tasks = [{
                'zuul_freeze': {
                    '_zuul_freeze_vars': list(
                        self.original_hostvars[host['name']].keys()),
                },
            }]
            play = {
                'hosts': host['name'],
                'tasks': tasks,
            }
            if host['name'] == 'localhost':
                play['gather_facts'] = False
            plays.append(play)

        self.log.debug("Freeze playbook: %s", repr(plays))
        with open(self.jobdir.freeze_playbook.path, 'w') as f:
            f.write(yaml.safe_dump(plays, default_flow_style=False))

        cmd = [self.executor_server.ansible_manager.getAnsibleCommand(
            ansible_version), verbose, playbook.path]

        if self.executor_variables_file is not None:
            cmd.extend(['-e@%s' % self.executor_variables_file])

        cmd.extend(['-e', '@' + self.jobdir.ansible_vars_blacklist])
        cmd.extend(['-e', '@' + self.jobdir.zuul_vars])

        result, code = self.runAnsible(
            cmd=cmd, timeout=self.executor_server.setup_timeout,
            playbook=playbook, ansible_version=ansible_version,
            allow_pre_fail=False)
        self.log.debug("Ansible freeze complete, result %s code %s" % (
            self.RESULT_MAP[result], code))

        return result, code

    def runAnsibleCleanup(self, playbook):
        # TODO(jeblair): This requires a bugfix in Ansible 2.4
        # Once this is used, increase the controlpersist timeout.
        return (self.RESULT_NORMAL, 0)

        if self.executor_server.verbose:
            verbose = '-vvv'
        else:
            verbose = '-v'

        cmd = ['ansible', '*', verbose, '-m', 'meta',
               '-a', 'reset_connection']

        result, code = self.runAnsible(
            cmd=cmd, timeout=60, playbook=playbook,
            wrapped=False)
        self.log.debug("Ansible complete, result %s code %s" % (
            self.RESULT_MAP[result], code))
        if self.executor_server.statsd:
            base_key = "zuul.executor.{hostname}.phase.cleanup"
            self.executor_server.statsd.incr(base_key + ".%s" %
                                             self.RESULT_MAP[result])
        return result, code

    def emitPlaybookBanner(self, playbook, step, phase, result=None):
        # This is used to print a header and a footer, respectively at the
        # beginning and the end of each playbook execution.
        # We are doing it from the executor rather than from a callback because
        # the parameters are not made available to the callback until it's too
        # late.
        phase = phase or ''
        trusted = playbook.trusted
        trusted = 'trusted' if trusted else 'untrusted'
        branch = playbook.branch
        playbook = playbook.canonical_name_and_path

        if phase and phase != 'run':
            phase = '{phase}-run'.format(phase=phase)
        phase = phase.upper()

        if result is not None:
            result = self.RESULT_MAP[result]
            msg = "{phase} {step} {result}: [{trusted} : {playbook}@{branch}]"
            msg = msg.format(phase=phase, step=step, result=result,
                             trusted=trusted, playbook=playbook, branch=branch)
        else:
            msg = "{phase} {step}: [{trusted} : {playbook}@{branch}]"
            msg = msg.format(phase=phase, step=step, trusted=trusted,
                             playbook=playbook, branch=branch)

        with _safe_open(self.jobdir.job_output_file, 'a') as job_output:
            job_output.write("{now} | {msg}\n".format(
                now=datetime.datetime.now(),
                msg=msg))

    def _generateOidcTokens(self, playbook):
        # In case web_root is not configured in zuul and tenant,
        # 'zuul_root_url' would not be available in the arguments,
        # just log a warning and skip generating oidc tokens
        if 'zuul_root_url' not in self.arguments:
            self.log.warning(
                "web_root is not configured in zuul, "
                "skipping oidc token generation")
            return
        oidc_tokens = {}
        for oidc_name, oidc_config in playbook.secrets_oidc.items():
            algorithm = oidc_config['algorithm']
            private_secrets_key, _, version = \
                self.executor_server.keystore.getLatestOidcSigningKeys(
                    algorithm=algorithm)
            iat = int(time.time())
            ttl = oidc_config['ttl']
            if 'name' in oidc_config:
                # MODEL_API >= 35
                secret_name = oidc_config['name']
            else:
                secret_name = oidc_name
            exp = iat + ttl
            tenant = self.arguments['zuul']['tenant']
            canonical_project_name = oidc_config.get(
                '_source_project_canonical_name')
            if canonical_project_name is None:
                # MODEL_API >= 36
                # Remove this fallback; this should only happen during
                # the Zuul upgrade where we added the new attr
                self.log.warning("Using backwards-compatible project "
                                 "cname for OIDC secret")
                canonical_project_name = \
                    self.arguments['zuul']['project']['canonical_name']
            sub = f'secret:{tenant}/{canonical_project_name}/{secret_name}'
            iss = oidc_config.get('iss', self.arguments["zuul_root_url"])

            payload = {
                "iss": iss,
                'sub': sub,
                'build-uuid': self.arguments["zuul"]["build"],
                'job-name': self.arguments['zuul']['job'],
                'playbook': playbook.canonical_name_and_path,
                'pipeline': self.arguments['zuul']['pipeline'],
                'tenant': tenant,
                'iat': iat,
                'exp': exp,
            }
            custom_claims = oidc_config.get('claims', {})
            for key, value in custom_claims.items():
                # custom claims should not overwrite the default claims
                if key not in payload:
                    payload[key] = value

            token = jwt.encode(
                payload, private_secrets_key, algorithm=algorithm,
                headers={"kid": f"{algorithm}-{version}"})
            oidc_tokens[oidc_name] = token

        oidc_tokens_content = yaml.ansible_unsafe_dump(
            yaml.mark_strings_unsafe(oidc_tokens), default_flow_style=False)

        if playbook.secrets_content:
            playbook.secrets_content += oidc_tokens_content
        else:
            playbook.secrets_content = oidc_tokens_content

    def runAnsiblePlaybook(self, playbook, timeout, ansible_version,
                           success=None, phase=None, index=None,
                           will_retry=None):
        if playbook.secrets_oidc:
            self._generateOidcTokens(playbook)

        if playbook.trusted or playbook.secrets_content:
            self.writeInventory(playbook, self.frozen_hostvars)
        else:
            self.writeInventory(playbook, self.original_hostvars)

        if self.executor_server.verbose:
            verbose = '-vvv'
        else:
            verbose = '-v'

        cmd = [self.executor_server.ansible_manager.getAnsibleCommand(
            ansible_version), verbose, playbook.path]

        if success is not None:
            cmd.extend(['-e', 'zuul_success=%s' % str(bool(success))])

        if will_retry is not None:
            cmd.extend(['-e', f'zuul_will_retry={bool(will_retry)}'])

        if phase:
            cmd.extend(['-e', 'zuul_execution_phase=%s' % phase])

        if index is not None:
            cmd.extend(['-e', 'zuul_execution_phase_index=%s' % index])

        cmd.extend(['-e', 'zuul_execution_trusted=%s' % str(playbook.trusted)])
        cmd.extend([
            '-e',
            'zuul_execution_canonical_name_and_path=%s'
            % playbook.canonical_name_and_path])
        cmd.extend(['-e', 'zuul_execution_branch=%s' % str(playbook.branch)])

        if self.executor_variables_file is not None:
            cmd.extend(['-e@%s' % self.executor_variables_file])

        if not playbook.trusted:
            cmd.extend(['-e', '@' + self.jobdir.ansible_vars_blacklist])
        cmd.extend(['-e', '@' + self.jobdir.zuul_vars])

        self.emitPlaybookBanner(playbook, 'START', phase)

        semaphore_handle = self.arguments['semaphore_handle']
        semaphore_wait_start = time.time()
        acquired_semaphores = True
        while not self.executor_server.semaphore_handler.acquireFromInfo(
                self.log, playbook.semaphores, semaphore_handle):
            self.log.debug("Unable to acquire playbook semaphores, waiting")
            if not self.waiting_for_semaphores:
                self.waiting_for_semaphores = True
            remain = self.getAnsibleTimeout(semaphore_wait_start, timeout)
            if remain is not None and remain <= 0:
                self.log.info("Timed out waiting for semaphore")
                acquired_semaphores = False
                result = self.RESULT_TIMED_OUT
                code = 0
                break
            if self.aborted and not playbook.cleanup:
                acquired_semaphores = False
                result = self.RESULT_ABORTED
                code = 0
                break
            time.sleep(self.semaphore_sleep_time)
        if self.waiting_for_semaphores:
            self.waiting_for_semaphores = False
            timeout = self.getAnsibleTimeout(semaphore_wait_start, timeout)

        if acquired_semaphores:
            result, code = self.runAnsible(
                cmd, timeout, playbook, ansible_version,
                # Don't allow pre fail to reset things if the job will be
                # retried anyway.
                allow_pre_fail=phase in ('run', 'post') and not will_retry,
                cleanup=phase == 'cleanup',
                phase=phase,
                index=index)
        self.log.debug("Ansible complete, result %s code %s" % (
            self.RESULT_MAP[result], code))

        if acquired_semaphores:
            if COMPONENT_REGISTRY.model_api >= 33:
                event_queue = self.executor_server.management_events[
                    self.build_request.tenant_name]
            else:
                event_queue = self.executor_server.result_events[
                    self.build_request.tenant_name][
                        self.build_request.pipeline_name]
            self.executor_server.semaphore_handler.releaseFromInfo(
                self.log, event_queue, playbook.semaphores, semaphore_handle)

        if self.executor_server.statsd:
            base_key = "zuul.executor.{hostname}.phase.{phase}"
            self.executor_server.statsd.incr(
                base_key + ".{result}",
                result=self.RESULT_MAP[result],
                phase=phase or 'unknown')

        self.emitPlaybookBanner(playbook, 'END', phase, result=result)
        return result, code

    def addPlaybookToJson(self, playbook, phase, index, message, start, end):
        # Append debug info to the job-output.json file. Note this needs to
        # work in conjunction with the Ansible zuul_json callback module.
        output_filename = self.jobdir.job_output_file.replace(
            'txt', 'json')

        def _write(mode):
            with _safe_open(output_filename, mode) as f:
                self.writePlaybookToJson(
                    f, playbook, phase, index, message, start, end)

        try:
            _write('r+')
        except FileNotFoundError:
            _write('w')

    def writePlaybookToJson(self, outfile, playbook, phase, index, message,
                            start, end):
        file_len = outfile.seek(0, os.SEEK_END)
        if file_len == 0:
            # This is a new file without any content
            outfile.write('[\n')
        else:
            # Existing file: Remove three bytes to eat the trailing
            # '\n]\n' and add the ',' on the end of the last line.
            outfile.seek(file_len - 3)
            outfile.write(',\n')

        playbook_info = {}
        playbook_info['playbook'] = playbook.canonical_name_and_path
        playbook_info['branch'] = playbook.branch
        playbook_info['phase'] = phase
        # Index is a string for some reason
        playbook_info['index'] = str(index)
        playbook_info['stats'] = {}
        playbook_info['trusted'] = playbook.trusted
        playbook_info['plays'] = [{
            'play': {
                'name': message
            },
            'tasks': []
        }]
        if start and end:
            playbook_info['plays'][0]['play']['duration'] = {
                'start': datetime.datetime.fromtimestamp(
                    start, tz=datetime.timezone.utc).isoformat(),
                'end': datetime.datetime.fromtimestamp(
                    end, tz=datetime.timezone.utc).isoformat(),
            }
        json.dump(playbook_info, outfile,
                  indent=4, sort_keys=True, separators=(',', ': '))
        outfile.write('\n]\n')


class ExecutorServer(BaseMergeServer):
    log = logging.getLogger("zuul.ExecutorServer")
    _ansible_manager_class = AnsibleManager
    _job_class = AnsibleJob
    _repo_locks_class = RepoLocks

    # Number of seconds past node expiration a hold request will remain
    EXPIRED_HOLD_REQUEST_TTL = 24 * 60 * 60

    def __init__(
        self,
        config,
        connections=None,
        jobdir_root=None,
        keep_jobdir=False,
        log_streaming_port=DEFAULT_FINGER_PORT,
        log_console_port=DEFAULT_STREAM_PORT,
    ):
        super().__init__(config, 'executor', connections)

        self.keep_jobdir = keep_jobdir
        self.jobdir_root = jobdir_root
        self.keystore = KeyStorage(
            self.zk_client,
            password=self._get_key_store_password())
        self._running = False
        self._command_running = False
        # TODOv3(mordred): make the executor name more unique --
        # perhaps hostname+pid.
        self.hostname = get_default(self.config, 'executor', 'hostname',
                                    socket.getfqdn())
        self.component_info = ExecutorComponent(
            self.zk_client, self.hostname, version=get_version_string())
        self.component_info.register()
        COMPONENT_REGISTRY.create(self.zk_client)
        self.zk_context = ZKContext(self.zk_client, None, None, self.log)
        self.monitoring_server = MonitoringServer(self.config, 'executor',
                                                  self.component_info)
        self.monitoring_server.start()
        self.tracing = tracing.Tracing(self.config)
        self.log_streaming_port = log_streaming_port
        self.governor_lock = threading.Lock()
        self.run_lock = threading.Lock()
        self.verbose = False
        self.command_map = {
            commandsocket.StopCommand.name: self.stop,
            commandsocket.PauseCommand.name: self.pause,
            commandsocket.UnPauseCommand.name: self.unpause,
            commandsocket.GracefulCommand.name: self.graceful,
            VerboseCommand.name: self.verboseOn,
            UnVerboseCommand.name: self.verboseOff,
            KeepCommand.name: self.keep,
            NoKeepCommand.name: self.nokeep,
            commandsocket.ReplCommand.name: self.startRepl,
            commandsocket.NoReplCommand.name: self.stopRepl,
        }
        self.log_console_port = log_console_port
        self.repl = None

        statsd_extra_keys = {'hostname': self.hostname}
        self.statsd = get_statsd(config, statsd_extra_keys)
        self.default_username = get_default(self.config, 'executor',
                                            'default_username', 'zuul')
        self.disk_limit_per_job = int(get_default(self.config, 'executor',
                                                  'disk_limit_per_job', 250))
        self.setup_timeout = int(get_default(self.config, 'executor',
                                             'ansible_setup_timeout', 60))
        self.zone = get_default(self.config, 'executor', 'zone')
        self.allow_unzoned = get_default(self.config, 'executor',
                                         'allow_unzoned', False)

        # If this executor has no zone configured it is implicitly unzoned
        if self.zone is None:
            self.allow_unzoned = True

        # Those attributes won't change, so it's enough to set them once on the
        # component info.
        self.component_info.zone = self.zone
        self.component_info.allow_unzoned = self.allow_unzoned

        self.ansible_callbacks = {}
        for section_name in self.config.sections():
            cb_match = re.match(r'^ansible_callback ([\'\"]?)(.*)(\1)$',
                                section_name, re.I)
            if not cb_match:
                continue
            cb_name = cb_match.group(2)
            self.ansible_callbacks[cb_name] = dict(
                self.config.items(section_name)
            )

        # TODO(tobiash): Take cgroups into account
        self.update_workers = multiprocessing.cpu_count()
        self.update_threads = []
        # If the execution driver ever becomes configurable again,
        # this is where it would happen.
        execution_wrapper_name = 'bubblewrap'
        self.accepting_work = True
        self.execution_wrapper = connections.drivers[execution_wrapper_name]

        self.update_queue = DeduplicateQueue()

        command_socket = get_default(
            self.config, 'executor', 'command_socket',
            '/var/lib/zuul/executor.socket')
        self.command_socket = commandsocket.CommandSocket(command_socket)

        state_dir = get_default(self.config, 'executor', 'state_dir',
                                '/var/lib/zuul', expand_user=True)

        # If keep is not set, ensure the job dir is empty on startup,
        # in case we were uncleanly shut down.
        if not self.keep_jobdir:
            for fn in os.listdir(self.jobdir_root):
                fn = os.path.join(self.jobdir_root, fn)
                if not os.path.isdir(fn):
                    continue
                self.log.info("Deleting stale jobdir %s", fn)
                # We use rm here instead of shutil because of
                # https://bugs.python.org/issue22040
                jobdir = os.path.join(self.jobdir_root, fn)
                # First we need to ensure all directories are
                # writable to avoid permission denied error
                subprocess.Popen([
                    "find", jobdir,
                    # Filter non writable perms
                    "-type", "d", "!", "-perm", "/u+w",
                    # Replace by writable perms
                    "-exec", "chmod", "0700", "{}", "+"]).wait()
                if subprocess.Popen(["rm", "-Rf", jobdir]).wait():
                    raise RuntimeError("Couldn't delete: " + jobdir)

        self.job_workers = {}
        self.disk_accountant = DiskAccountant(self.jobdir_root,
                                              self.disk_limit_per_job,
                                              self.stopJobDiskFull,
                                              self.merge_root)

        if self.statsd:
            base_key = 'zuul.executor.{hostname}'
        else:
            base_key = None
        self.pause_sensor = PauseSensor(self.statsd, base_key,
                                        get_default(self.config, 'executor',
                                                    'paused_on_start', False))
        self.log.info("Starting executor (hostname: %s) in %spaused mode" % (
            self.hostname, "" if self.pause_sensor.pause else "un"))

        cpu_sensor = CPUSensor(self.statsd, base_key, config)
        self.sensors = [
            cpu_sensor,
            HDDSensor(self.statsd, base_key, config),
            self.pause_sensor,
            ProcessSensor(self.statsd, base_key, config),
            RAMSensor(self.statsd, base_key, config),
            StartingBuildsSensor(self.statsd, base_key,
                                 self, cpu_sensor.max_load_avg, config),
        ]

        manage_ansible = get_default(
            self.config, 'executor', 'manage_ansible', True)
        ansible_dir = os.path.join(state_dir, 'ansible')
        ansible_install_root = get_default(
            self.config, 'executor', 'ansible_root', None)
        if not ansible_install_root:
            # NOTE: Even though we set this value the zuul installation
            # adjacent virtualenv location is still checked by the ansible
            # manager. ansible_install_root's value is only used if those
            # default locations do not have venvs preinstalled.
            ansible_install_root = os.path.join(state_dir, 'ansible-bin')
        self.ansible_manager = self._ansible_manager_class(
            ansible_dir, runtime_install_root=ansible_install_root)
        if not self.ansible_manager.validate():
            if not manage_ansible:
                raise Exception('Error while validating ansible '
                                'installations. Please run '
                                'zuul-manage-ansible to install all supported '
                                'ansible versions.')
            else:
                self.ansible_manager.install()
        self.ansible_manager.copyAnsibleFiles()

        self.process_merge_jobs = get_default(self.config, 'executor',
                                              'merge_jobs', True)
        self.component_info.process_merge_jobs = self.process_merge_jobs

        self.system = ZuulSystem(self.zk_client)
        self.nodepool = Nodepool(self.zk_client, self.system.system_id,
                                 self.statsd)
        self.launcher = LauncherClient(self.zk_client, None,
                                       component_info=self.component_info)

        self.management_events = TenantManagementEventQueue.createRegistry(
            self.zk_client)
        self.result_events = PipelineResultEventQueue.createRegistry(
            self.zk_client)
        self.build_worker = threading.Thread(
            target=self.runBuildWorker,
            name="ExecutorServerBuildWorkerThread",
        )

        self.build_loop_wake_event = threading.Event()

        zone_filter = [self.zone]
        if self.allow_unzoned:
            # In case we are allowed to execute unzoned jobs, make sure, we are
            # subscribed to the default zone.
            zone_filter.append(None)

        self.executor_api = ExecutorApi(
            self.zk_client,
            zone_filter=zone_filter,
            build_request_callback=self.build_loop_wake_event.set,
            build_event_callback=self._handleBuildEvent,
        )

        # Used to offload expensive operations to different processes
        self.process_worker = None

        self.semaphore_handler = SemaphoreHandler(
            self.zk_client, self.statsd, None, None, None)

    def _get_key_store_password(self):
        try:
            return self.config["keystore"]["password"]
        except KeyError:
            raise RuntimeError("No key store password configured!")

    def _repoLock(self, connection_name, project_name):
        return self.repo_locks.getRepoLock(connection_name, project_name)

    # We use a property to reflect the accepting_work state on the component
    # since it might change quite often.
    @property
    def accepting_work(self):
        return self.component_info.accepting_work

    @accepting_work.setter
    def accepting_work(self, work):
        self.component_info.accepting_work = work

    def start(self):
        # Start merger worker only if we process merge jobs
        if self.process_merge_jobs:
            super().start()

        self._running = True
        self._command_running = True

        try:
            multiprocessing.set_start_method('spawn')
        except RuntimeError:
            # Note: During tests this can be called multiple times which
            # results in a runtime error. This is ok here as we've set this
            # already correctly.
            self.log.warning('Multiprocessing context has already been set')
        self.process_worker = ProcessPoolExecutor()

        self.build_worker.start()

        self.log.debug("Starting command processor")
        self.command_socket.start()
        self.command_thread = threading.Thread(target=self.runCommand,
                                               name='command')
        self.command_thread.daemon = True
        self.command_thread.start()

        self.log.debug("Starting %s update workers" % self.update_workers)
        for i in range(self.update_workers):
            update_thread = threading.Thread(target=self._updateLoop,
                                             name='update')
            update_thread.daemon = True
            update_thread.start()
            self.update_threads.append(update_thread)

        self.governor_stop_event = threading.Event()
        self.governor_thread = threading.Thread(target=self.run_governor,
                                                name='governor')
        self.governor_thread.daemon = True
        self.governor_thread.start()
        self.disk_accountant.start()
        self.component_info.state = self.component_info.RUNNING

    def register_work(self):
        if self._running:
            self.accepting_work = True
            self.build_loop_wake_event.set()

    def unregister_work(self):
        self.accepting_work = False

    def stop(self):
        self.log.debug("Stopping executor")
        self.component_info.state = self.component_info.STOPPED
        self.keystore.stop()
        self.connections.stop()
        self.disk_accountant.stop()
        # The governor can change function registration, so make sure
        # it has stopped.
        self.governor_stop_event.set()
        self.governor_thread.join()
        # Tell the executor worker to abort any jobs it just accepted,
        # and grab the list of currently running job workers.
        with self.run_lock:
            self._running = False
            self._command_running = False
            workers = list(self.job_workers.values())

        for job_worker in workers:
            try:
                job_worker.stop()
            except Exception:
                self.log.exception("Exception sending stop command "
                                   "to worker:")
        for job_worker in workers:
            try:
                job_worker.wait()
            except Exception:
                self.log.exception("Exception waiting for worker "
                                   "to stop:")

        # Now that we aren't accepting any new jobs, and all of the
        # running jobs have stopped, tell the update processor to
        # stop.
        for _ in self.update_threads:
            self.update_queue.put(None)

        self.command_socket.stop()

        # All job results should have been sent by now, shutdown the
        # build and merger workers.
        self.build_loop_wake_event.set()
        self.build_worker.join()

        if self.process_worker is not None:
            self.process_worker.shutdown()

        if self.statsd:
            base_key = 'zuul.executor.{hostname}'
            self.statsd.gauge(base_key + '.load_average', 0)
            self.statsd.gauge(base_key + '.pct_used_ram', 0)
            self.statsd.gauge(base_key + '.running_builds', 0)
            self.statsd.close()
            self.statsd = None

        # Use the BaseMergeServer's stop method to disconnect from
        # ZooKeeper.  We do this as one of the last steps to ensure
        # that all ZK related components can be stopped first.
        super().stop()
        self.stopRepl()
        self.monitoring_server.stop()
        self.tracing.stop()
        self.executor_api.stop()
        self.log.debug("Stopped executor")

    def join(self):
        self.log.debug("Joining executor")
        self.governor_thread.join()
        for update_thread in self.update_threads:
            update_thread.join()
        self.build_loop_wake_event.set()
        self.build_worker.join()
        # The BaseMergeServer's join method also disconnects the
        # Zookeeper client, so this must happend after we've stopped
        # all other threads that interact with Zookeeper.
        if self.process_merge_jobs:
            super().join()
        self.command_thread.join()
        self.monitoring_server.join()
        self.log.debug("Joined executor")

    def pause(self):
        self.log.debug('Pausing')
        self.component_info.state = self.component_info.PAUSED
        self.pause_sensor.pause = True
        self.manageLoad()
        if self.process_merge_jobs:
            super().pause()

    def unpause(self):
        self.log.debug('Resuming')
        self.component_info.state = self.component_info.RUNNING
        self.pause_sensor.pause = False
        self.manageLoad()
        if self.process_merge_jobs:
            super().unpause()

    def graceful(self):
        # This pauses the executor end shuts it down when there is no running
        # build left anymore
        self.log.info('Stopping graceful')
        self.pause()
        while self.job_workers:
            self.log.debug('Waiting for %s jobs to end', len(self.job_workers))
            time.sleep(30)
        try:
            self.stop()
        except Exception:
            self.log.exception('Error while stopping')

    def isComponentRunning(self):
        return self._running

    def verboseOn(self):
        self.verbose = True

    def verboseOff(self):
        self.verbose = False

    def keep(self):
        self.keep_jobdir = True

    def nokeep(self):
        self.keep_jobdir = False

    def startRepl(self):
        if self.repl:
            return
        self.repl = zuul.lib.repl.REPLServer(self)
        self.repl.start()

    def stopRepl(self):
        if not self.repl:
            # not running
            return
        self.repl.stop()
        self.repl = None

    def runCommand(self):
        while self._command_running:
            try:
                command, args = self.command_socket.get()
                if command != '_stop':
                    self.command_map[command](*args)
            except Exception:
                self.log.exception("Exception while processing command")

    def _updateLoop(self):
        while True:
            try:
                self._innerUpdateLoop()
            except StopException:
                return
            except Exception:
                self.log.exception("Exception in update thread:")

    def resetProcessPool(self):
        """
        This is called in order to re-initialize a broken process pool if it
        got broken e.g. by an oom killed child process
        """
        if self.process_worker:
            try:
                self.process_worker.shutdown()
            except Exception:
                self.log.exception('Failed to shutdown broken process worker')
            self.process_worker = ProcessPoolExecutor()

    def _innerUpdateLoop(self):
        # Inside of a loop that keeps the main repositories up to date
        task = self.update_queue.get()
        if task is None:
            # We are asked to stop
            raise StopException()
        log = get_annotated_logger(
            self.log, task.zuul_event_id, build=task.build)
        try:
            if task.span_context:
                # We're inside of a TPE so we have to restore the
                # parent span (we can't just "start_span").
                lock_span = tracing.startSpanInContext(
                    task.span_context,
                    'BuildRepoUpdateLock',
                    attributes={'connection': task.connection_name,
                                'project': task.project_name})
                update_span = tracing.startSpanInContext(
                    task.span_context,
                    'BuildRepoUpdate',
                    attributes={'connection': task.connection_name,
                                'project': task.project_name})
            else:
                lock_span = contextlib.nullcontext()
                update_span = contextlib.nullcontext()
            lock = self.repo_locks.getRepoLock(
                task.connection_name, task.project_name)
            with lock_span, lock, update_span:
                log.info("Updating repo %s/%s",
                         task.connection_name, task.project_name)
                self.merger.updateRepo(
                    task.connection_name, task.project_name,
                    repo_state=task.repo_state,
                    zuul_event_id=task.zuul_event_id, build=task.build)
                source = self.connections.getSource(task.connection_name)
                project = source.getProject(task.project_name)
                task.canonical_name = project.canonical_name
                log.debug("Finished updating repo %s/%s",
                          task.connection_name, task.project_name)
                task.success = True
        except BrokenProcessPool:
            # The process pool got broken. Reset it to unbreak it for further
            # requests.
            log.exception('Process pool got broken')
            self.resetProcessPool()
            task.transient_error = True
        except IOError:
            log.exception('Got I/O error while updating repo %s/%s',
                          task.connection_name, task.project_name)
            task.transient_error = True
        except Exception:
            log.exception('Got exception while updating repo %s/%s',
                          task.connection_name, task.project_name)
        finally:
            task.setComplete()

    def update(self, connection_name, project_name, repo_state=None,
               zuul_event_id=None, build=None, span_context=None):
        # Update a repository in the main merger

        task = UpdateTask(connection_name, project_name, repo_state=repo_state,
                          zuul_event_id=zuul_event_id, build=build,
                          span_context=span_context)
        task = self.update_queue.put(task)
        return task

    def _update(self, connection_name, project_name, zuul_event_id=None):
        """
        The executor overrides _update so it can do the update asynchronously.
        """
        log = get_annotated_logger(self.log, zuul_event_id)
        task = self.update(connection_name, project_name,
                           zuul_event_id=zuul_event_id)
        task.wait()
        if not task.success:
            msg = "Update of '{}' failed".format(project_name)
            log.error(msg)
            raise Exception(msg)

    def executeJob(self, build_request, params):
        zuul_event_id = params['zuul_event_id']
        log = get_annotated_logger(self.log, zuul_event_id)
        log.debug(
            "Got %s job: %s",
            params["zuul"]["job"],
            build_request.uuid,
        )
        if self.statsd:
            base_key = 'zuul.executor.{hostname}'
            self.statsd.incr(base_key + '.builds')
        self.job_workers[build_request.uuid] = self._job_class(
            self, build_request, params
        )
        # Run manageLoad before starting the thread mostly for the
        # benefit of the unit tests to make the calculation of the
        # number of starting jobs more deterministic.
        self.manageLoad()
        self.job_workers[build_request.uuid].run()

    def _handleBuildEvent(self, build_request, build_event):
        log = get_annotated_logger(
            self.log, build_request.event_id, build=build_request.uuid)
        log.debug(
            "Received %s event for build %s", build_event.name, build_request)
        # Fulfill the resume/cancel requests after our internal calls
        # to aid the test suite in avoiding races.
        if build_event == JobRequestEvent.CANCELED:
            if self.stopJob(build_request):
                try:
                    self.executor_api.fulfillCancel(build_request)
                except NoNodeError:
                    self.log.warning("Unable to fulfill cancel request, "
                                     "build request may have been removed")
        elif build_event == JobRequestEvent.RESUMED:
            if self.resumeJob(build_request):
                self.executor_api.fulfillResume(build_request)
        elif build_event == JobRequestEvent.DELETED:
            self.stopJob(build_request)

    def runBuildWorker(self):
        while self._running:
            self.build_loop_wake_event.wait()
            self.build_loop_wake_event.clear()
            try:
                # Always delay the response to the first build request
                delay_response = True
                for build_request in self.executor_api.next():
                    # Check the sensors again as they might have changed in the
                    # meantime. E.g. the last build started within the next()
                    # generator could have fulfilled the StartingBuildSensor.
                    if not self.accepting_work:
                        break
                    if not self._running:
                        break

                    # Delay our response to running a new job based on
                    # the number of jobs we're currently running, in
                    # an attempt to spread load evenly among
                    # executors.
                    if delay_response:
                        workers = len(self.job_workers)
                        delay = (workers ** 2) / 1000.0
                        time.sleep(delay)

                    delay_response = self._runBuildWorker(build_request)
            except Exception:
                self.log.exception("Error in build loop:")
                time.sleep(5)

    def _runBuildWorker(self, build_request: BuildRequest):
        log = get_annotated_logger(
            self.log, event=build_request.event_id, build=build_request.uuid)
        # Lock and update the build request
        if not self.executor_api.lock(build_request, blocking=False):
            return False

        # Ensure that the request is still in state requested. This method is
        # called based on cached data and there might be a mismatch between the
        # cached state and the real state of the request. The lock might
        # have been successful because the request is already completed and
        # thus unlocked.
        if build_request.state != BuildRequest.REQUESTED:
            self._retry(build_request.lock, self.executor_api.unlock,
                        build_request)
            return False

        try:
            params = self.executor_api.getParams(build_request)
            # Directly update the build in ZooKeeper, so we don't loop
            # over and try to lock it again and again.  Do this before
            # clearing the params so if we fail, no one tries to
            # re-run the job.
            build_request.state = BuildRequest.RUNNING
            # Set the hostname on the build request so it can be used by
            # zuul-web for the live log streaming.
            build_request.worker_info = {
                "hostname": self.hostname,
                "log_port": self.log_streaming_port,
                "zone": self.zone,
            }
            self.executor_api.update(build_request)
        except Exception:
            log.exception("Exception while preparing to start worker")
            # If we failed at this point, we have not written anything
            # to ZK yet; the only thing we need to do is to ensure
            # that we release the lock, and another executor will be
            # able to grab the build.
            self._retry(build_request.lock, self.executor_api.unlock,
                        build_request)
            return False

        try:
            self.executor_api.clearParams(build_request)
            log.debug("Next executed job: %s", build_request)
            self.executeJob(build_request, params)
        except Exception:
            # Note, this is not a finally clause, because if we
            # sucessfuly start executing the job, it's the
            # AnsibleJob's responsibility to call completeBuild and
            # unlock the request.
            log.exception("Exception while starting worker")
            result = {
                "result": "ERROR",
                "exception": traceback.format_exc(),
            }
            self.completeBuild(build_request, result)
            return False
        return True

    def run_governor(self):
        while not self.governor_stop_event.wait(10):
            try:
                self.manageLoad()
            except Exception:
                self.log.exception("Exception in governor thread:")

    def manageLoad(self):
        ''' Apply some heuristics to decide whether or not we should
            be asking for more jobs '''
        with self.governor_lock:
            return self._manageLoad()

    def _manageLoad(self):
        if self.accepting_work:
            unregister = False
            for sensor in self.sensors:
                ok, message = sensor.isOk()
                if not ok:
                    # Continue to check all sensors to emit stats
                    unregister = True
                    self.log.info(
                        "Unregistering due to {}".format(message))
            if unregister:
                self.unregister_work()
        else:
            reregister = True
            limits = []
            for sensor in self.sensors:
                ok, message = sensor.isOk()
                limits.append(message)
                if not ok:
                    # Continue to check all sensors to emit stats
                    reregister = False
            if reregister:
                self.log.info("Re-registering as job is within its limits "
                              "{}".format(", ".join(limits)))
                self.register_work()

    def finishJob(self, unique):
        del self.job_workers[unique]
        self.log.debug(
            "Finishing Job: %s, queue(%d): %s",
            unique,
            len(self.job_workers),
            self.job_workers,
        )

    def stopJobDiskFull(self, jobdir):
        unique = os.path.basename(jobdir)
        self.stopJobByUnique(unique, reason=AnsibleJob.RESULT_DISK_FULL)

    def resumeJob(self, build_request):
        log = get_annotated_logger(
            self.log, build_request.event_id, build=build_request.uuid)
        log.debug("Resume job")
        return self.resumeJobByUnique(
            build_request.uuid, build_request.event_id
        )

    def stopJob(self, build_request):
        log = get_annotated_logger(
            self.log, build_request.event_id, build=build_request.uuid)
        log.debug("Stop job")
        return self.stopJobByUnique(build_request.uuid, build_request.event_id)

    def resumeJobByUnique(self, unique, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        job_worker = self.job_workers.get(unique)
        if not job_worker:
            log.debug("Unable to find worker for job %s", unique)
            return False
        try:
            job_worker.resume()
        except Exception:
            log.exception("Exception sending resume command to worker:")
        return True

    def stopJobByUnique(self, unique, reason=None, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        job_worker = self.job_workers.get(unique)
        if not job_worker:
            log.debug("Unable to find worker for job %s", unique)
            return False
        try:
            job_worker.stop(reason)
        except Exception:
            log.exception("Exception sending stop command to worker:")
        return True

    def _handleExpiredHoldRequest(self, request):
        '''
        Check if a hold request is expired and delete it if it is.

        The 'expiration' attribute will be set to the clock time when the
        hold request was used for the last time. If this is NOT set, then
        the request is still active.

        If a node expiration time is set on the request, and the request is
        expired, *and* we've waited for a defined period past the node
        expiration (EXPIRED_HOLD_REQUEST_TTL), then we will delete the hold
        request.

        :param: request Hold request
        :returns: True if it is expired, False otherwise.
        '''
        if not request.expired:
            return False

        if not request.node_expiration:
            # Request has been used up but there is no node expiration, so
            # we don't auto-delete it.
            return True

        elapsed = time.time() - request.expired
        if elapsed < self.EXPIRED_HOLD_REQUEST_TTL + request.node_expiration:
            # Haven't reached our defined expiration lifetime, so don't
            # auto-delete it yet.
            return True

        try:
            self.nodepool.zk_nodepool.lockHoldRequest(request)
            self.log.info("Removing expired hold request %s", request)
            self.nodepool.zk_nodepool.deleteHoldRequest(
                request, self.launcher)
        except Exception:
            self.log.exception(
                "Failed to delete expired hold request %s", request
            )
        finally:
            try:
                self.nodepool.zk_nodepool.unlockHoldRequest(request)
            except Exception:
                pass

        return True

    def _getAutoholdRequest(self, args):
        autohold_key_base = (
            args["zuul"]["tenant"],
            args["zuul"]["project"]["canonical_name"],
            args["zuul"]["job"],
        )

        class Scope(object):
            """Enum defining a precedence/priority of autohold requests.

            Autohold requests for specific refs should be fulfilled first,
            before those for changes, and generic jobs.

            Matching algorithm goes over all existing autohold requests, and
            returns one with the highest number (in case of duplicated
            requests the last one wins).
            """
            NONE = 0
            JOB = 1
            CHANGE = 2
            REF = 3

        # Do a partial match of the autohold key against all autohold
        # requests, ignoring the last element of the key (ref filter),
        # and finally do a regex match between ref filter from
        # the autohold request and the build's change ref to check
        # if it matches. Lastly, make sure that we match the most
        # specific autohold request by comparing "scopes"
        # of requests - the most specific is selected.
        autohold = None
        scope = Scope.NONE
        self.log.debug("Checking build autohold key %s", autohold_key_base)
        for request_id in self.nodepool.zk_nodepool.getHoldRequests():
            request = self.nodepool.zk_nodepool.getHoldRequest(request_id)
            if not request:
                continue

            if self._handleExpiredHoldRequest(request):
                continue

            ref_filter = request.ref_filter

            if request.current_count >= request.max_count:
                # This request has been used the max number of times
                continue
            elif not (
                request.tenant == autohold_key_base[0]
                and request.project == autohold_key_base[1]
                and request.job == autohold_key_base[2]
            ):
                continue
            elif not re.match(ref_filter, args["zuul"]["ref"]):
                continue

            if ref_filter == ".*":
                candidate_scope = Scope.JOB
            elif ref_filter.endswith(".*"):
                candidate_scope = Scope.CHANGE
            else:
                candidate_scope = Scope.REF

            self.log.debug(
                "Build autohold key %s matched scope %s",
                autohold_key_base,
                candidate_scope,
            )
            if candidate_scope > scope:
                scope = candidate_scope
                autohold = request

        return autohold

    def _processAutohold(self, ansible_job, duration, result):
        # We explicitly only want to hold nodes for jobs if they have
        # failed / retry_limit / post_failure and have an autohold request.
        hold_list = ["FAILURE", "RETRY_LIMIT", "POST_FAILURE", "TIMED_OUT"]
        if result not in hold_list:
            return False

        request = self._getAutoholdRequest(ansible_job.arguments)
        if request is not None:
            self.log.debug("Got autohold %s", request)

            if ansible_job.node_request:
                self.nodepool.holdNodeSet(
                    ansible_job.nodeset, request, ansible_job.build_request,
                    duration, ansible_job.zuul_event_id)
                return True
            elif ansible_job.nodeset_request:
                self.launcher.holdNodeSet(
                    self.nodepool.zk_nodepool,
                    ansible_job.nodeset, request, ansible_job.build_request,
                    duration, ansible_job.zuul_event_id)
                return True
        return False

    def startBuild(self, build_request, data):
        data["start_time"] = time.time()

        event = BuildStartedEvent(
            build_request.uuid, build_request.build_set_uuid,
            build_request.job_uuid,
            build_request.path, data, build_request.event_id)
        self.result_events[build_request.tenant_name][
            build_request.pipeline_name].put(event)

    def updateBuildStatus(self, build_request, data):
        event = BuildStatusEvent(
            build_request.uuid, build_request.build_set_uuid,
            build_request.job_uuid,
            build_request.path, data, build_request.event_id)
        self.result_events[build_request.tenant_name][
            build_request.pipeline_name].put(event)

    def pauseBuild(self, build_request, data):
        build_request.state = BuildRequest.PAUSED
        try:
            self.executor_api.update(build_request)
        except JobRequestNotFound as e:
            self.log.warning("Could not pause build: %s", str(e))
            return

        event = BuildPausedEvent(
            build_request.uuid, build_request.build_set_uuid,
            build_request.job_uuid,
            build_request.path, data, build_request.event_id)
        self.result_events[build_request.tenant_name][
            build_request.pipeline_name].put(event)

    def resumeBuild(self, build_request):
        build_request.state = BuildRequest.RUNNING
        try:
            self.executor_api.update(build_request)
        except JobRequestNotFound as e:
            self.log.warning("Could not resume build: %s", str(e))
            return

    def completeBuild(self, build_request, result):
        result["end_time"] = time.time()

        log = get_annotated_logger(self.log, build_request.event_id,
                                   build=build_request.uuid)

        # NOTE (felix): We store the end_time on the ansible job to calculate
        # the in-use duration of locked nodes when the nodeset is returned.
        # NOTE: this method may be called before we create a job worker.
        ansible_job = self.job_workers.get(build_request.uuid)
        if ansible_job:
            ansible_job.end_time = time.monotonic()
            duration = ansible_job.end_time - ansible_job.time_starting_build

            # If the result is None, check if the build has reached
            # its max attempts and if so set the result to
            # RETRY_LIMIT.  This must be done in order to correctly
            # process the autohold in the next step. Since we only
            # want to hold the node if the build has reached a final
            # result.
            if result.get("result") is None and ansible_job.retry_limit:
                result["result"] = "RETRY_LIMIT"

            # Provide the hold information back to the scheduler via the build
            # result.
            try:
                held = self._processAutohold(ansible_job, duration,
                                             result.get("result"))
                result["held"] = held
                log.info("Held status set to %s", held)
            except Exception:
                log.exception("Unable to process autohold for %s",
                              build_request)

        if not build_request.lock.is_still_valid():
            # If we lost the lock at any point before updating the
            # state to COMPLETED, then the scheduler may have (or
            # will) detect it as an incomplete build and generate an
            # error event for us.  We don't need to submit a
            # completion event in that case.
            #
            # TODO: If we make the scheduler robust against receiving
            # duplicate completion events for the same build, we could
            # choose continue here and submit the completion event in
            # the hopes that we would win the race against the cleanup
            # thread.  That might (in some narrow circumstances)
            # rescue an otherwise acceptable build from being
            # discarded.
            return

        updater = self.executor_api.getRequestUpdater(build_request)
        event = BuildCompletedEvent(
            build_request.uuid, build_request.build_set_uuid,
            build_request.job_uuid,
            build_request.path, result, build_request.event_id)
        build_request.state = BuildRequest.COMPLETED
        updated = False
        put_method = self.result_events[build_request.tenant_name][
            build_request.pipeline_name].put
        try:
            self._retry(build_request.lock,
                        put_method, event, updater=updater)
            updated = True
        except JobRequestNotFound as e:
            log.warning("Could not find build: %s", str(e))
            return
        except NoNodeError:
            log.warning("Pipeline was removed: %s",
                        build_request.pipeline_name)

        if not updated:
            # If the pipeline was removed but the request remains, we
            # should still update the build request just in case, in
            # order to prevent another executor from starting an
            # unecessary build.
            try:
                self._retry(build_request.lock, self.executor_api.update,
                            build_request)
            except JobRequestNotFound as e:
                self.log.warning("Build was removed: %s", str(e))

        self._retry(build_request.lock, self.executor_api.unlock,
                    build_request)
