# Copyright 2017 Red Hat, Inc.
# Copyright 2024-2025 Acme Gating, LLC
#
# Zuul is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Zuul is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

# This is not needed in python3 - but it is needed in python2 because there
# is a json module in ansible.plugins.callback and python2 gets confused.
# Easy local testing with ansible-playbook is handy when hacking on zuul_stream
# so just put in the __future__ statement.
from __future__ import absolute_import

DOCUMENTATION = '''
    callback: zuul_stream
    short_description: This is the Zuul streaming callback
    version_added: "2.4"
    description:
        - This callback is necessary for Zuul to properly stream logs from the
          remote test node.
    type: stdout
    extends_documentation_fragment:
      - default_callback
    requirements:
      - Set as stdout in config
'''

import datetime
import logging
import logging.config
import json
import os
import re2
import socket
import threading
import time

from ansible.playbook.block import Block
from ansible.plugins.callback import default
from ansible.module_utils._text import to_text
from ansible.module_utils.parsing.convert_bool import boolean
from zuul.ansible import paths

from zuul.ansible import logconfig

LOG_STREAM_VERSION = 0

DEFAULT_POSIX_PORT = 19885
DEFAULT_WINDOWS_PORT = 19886

POSIX_ACTIONS = (
    'command',
    'shell',
    'ansible.builtin.command',
    'ansible.builtin.shell',
)

WINDOWS_ACTIONS = (
    'win_command',
    'win_shell',
    'ansible.windows.win_command',
    'ansible.windows.win_shell',
)

ACTION_LOG_STREAM_PORT = {
    **{a: DEFAULT_POSIX_PORT for a in POSIX_ACTIONS},
    **{a: DEFAULT_WINDOWS_PORT for a in WINDOWS_ACTIONS},
}

# Actions that support live log streaming
STREAMING_ACTIONS = POSIX_ACTIONS + WINDOWS_ACTIONS

# Actions that produce stdout/err that should go in the log from JSON
OUTPUT_ACTIONS = ('raw',)

# Actions that produce stdout/err that should go in the log (whether
# from streaming or JSON).
ALL_ACTIONS = STREAMING_ACTIONS + OUTPUT_ACTIONS


def zuul_filter_result(result):
    """Remove keys from shell/command output.

    Zuul streams stdout/stderr into the log above, so including stdout and
    stderr in the result dict that ansible displays in the logs is duplicate
    noise. We keep stdout/stderr in the result dict so that other callback
    plugins like ARA could also have access to it. But drop them here.

    Remove changed so that we don't show a bunch of "changed" titles
    on successful shell tasks, since that doesn't make sense from a Zuul
    POV. The super class treats missing "changed" key as False.

    Remove cmd because most of the script content where people want to
    see the script run is run with -x. It's possible we may want to revist
    this to be smarter about when we remove it - like, only remove it
    if it has an embedded newline - so that for normal 'simple' uses
    of cmd it'll echo what the command was for folks.
    """

    stdout = result.pop('stdout', '')
    stdout_lines = result.pop('stdout_lines', [])
    if not stdout_lines and stdout:
        stdout_lines = stdout.split('\n')

    stderr = result.pop('stderr', '')
    stderr_lines = result.pop('stderr_lines', [])
    if not stderr_lines and stderr:
        stderr_lines = stderr.split('\n')

    for key in ('changed', 'cmd', 'zuul_log_id', 'invocation'):
        result.pop(key, None)

    # Combine stdout / stderr
    return stdout_lines + stderr_lines


def is_rescuable(task):
    if task._parent is None:
        return False
    if isinstance(task._parent, Block):
        if task._parent.rescue:
            return True
    return is_rescuable(task._parent)


class Streamer:
    def __init__(self, callback, host, ip, port, log_id):
        self.callback = callback
        self.host = host
        self.ip = ip
        self.port = port
        self.log_id = log_id
        self._zuul_console_version = 0
        self.stopped = False
        self.connected = False

    def start(self):
        self.thread = threading.Thread(target=self._read_log)
        self.thread.daemon = True
        self.thread.start()

    def stop(self, grace):
        self.stop_ts = time.monotonic()
        self.stop_grace = grace
        self.stopped = True

    def join(self, timeout):
        self.thread.join(timeout)

    def is_alive(self):
        return self.thread.is_alive()

    def _read_log_connect(self):
        logger_retries = 0
        # We don't check stopped here because if we ever succeed in
        # connecting, then we should always try again; that's because
        # we may need to reconnect in order to send another command to
        # delete the log file.
        while True:
            try:
                s = socket.create_connection((self.ip, self.port), 5)
                # Disable the socket timeout after we have successfully
                # connected to accomodate the fact that jobs may not be writing
                # logs continously. Without this we can easily trip the 5
                # second timeout.
                s.settimeout(None)
                # Some remote commands can run for some time without
                # producing output.  In case there are network
                # components that might drop an idle TCP connection,
                # enable keepalives so that we can hopefully maintain
                # the connection, or at the least, be notified if it
                # is terminated.  Ping every 30 seconds after 30
                # seconds of idle activity.  Allow 3 minutes of lost
                # pings before we fail.
                s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 9)
                self.connected = True
                return s
            except socket.timeout:
                if not self.stopped:
                    self.callback._log_streamline(
                        "localhost",
                        "Timeout exception waiting for the logger. "
                        "Please check connectivity to [%s:%s]"
                        % (self.ip, self.port),
                        stopped=True)
                return None
            except Exception:
                if self.stopped:
                    return None
                if logger_retries % 10 == 0:
                    self.callback._log("[%s] Waiting on logger" % self.host)
                logger_retries += 1
                time.sleep(0.1)
                continue

    def _read_log(self):
        s = self._read_log_connect()
        if s is None:
            # Can't connect; _read_log_connect() already logged an
            # error for us, just bail
            return

        # If we were asked to stop during the connection attempt, and
        # the grace time has expired return now.  If we are stopped
        # for a skipped task, then there will be no grace time set, so
        # this should return immediately.
        if self.stopped:
            if time.monotonic() >= (self.stop_ts + self.stop_grace):
                return

        # Find out what version we are running against
        s.send(f'v:{LOG_STREAM_VERSION}\n'.encode('utf-8'))
        buff = s.recv(1024).decode('utf-8').strip()

        # NOTE(ianw) 2022-07-21 : zuul_console from < 6.3.0 do not
        # understand this protocol.  They will assume the send
        # above is a log request and send back the not found
        # message in a loop.  So to handle this we disconnect and
        # reconnect.  When we decide to remove this, we can remove
        # anything in the "version 0" path.
        if buff == '[Zuul] Log not found':
            s.shutdown(socket.SHUT_RDWR)
            s.close()
            s = self._read_log_connect()
            if s is None:
                return
        else:
            self._zuul_console_version = int(buff)

        if self._zuul_console_version >= 1:
            msg = 's:%s\n' % self.log_id
        else:
            msg = '%s\n' % self.log_id

        s.send(msg.encode("utf-8"))
        buff = s.recv(4096)
        buffering = True
        while buffering:
            if b'\n' in buff:
                (line, buff) = buff.split(b'\n', 1)

                stopped = False
                if self.stopped:
                    if time.monotonic() >= (self.stop_ts + self.stop_grace):
                        # The task is finished, but we're still
                        # reading the log.  We might just be reading
                        # more data, or we might be reading "Log not
                        # found" repeatedly.  That might be due to
                        # contention on a busy node and the log will
                        # show up soon, or it may be because we
                        # skipped the task and nothing ran.  In the
                        # latter case, the grace period will be 0, but
                        # in the former, we will allow an extra 10
                        # seconds for the log to show up.
                        stopped = True

                # We can potentially get binary data here. In order to
                # being able to handle that use the backslashreplace
                # error handling method. This decodes unknown utf-8
                # code points to escape sequences which exactly represent
                # the correct data without throwing a decoding exception.
                done = self.callback._log_streamline(
                    self.host, line.decode("utf-8", "backslashreplace"),
                    stopped=stopped)
                if done:
                    if self._zuul_console_version > 0:
                        try:
                            # reestablish connection and tell console to
                            # clean up
                            s.shutdown(socket.SHUT_RDWR)
                            s.close()
                            s = self._read_log_connect()
                            s.send(f'f:{self.log_id}\n'.encode('utf-8'))
                            s.shutdown(socket.SHUT_RDWR)
                            s.close()
                        except Exception:
                            # Don't worry if this fails
                            pass
                    return
            else:
                try:
                    more = s.recv(4096)
                except TimeoutError:
                    self.callback._log_streamline(
                        self.host,
                        "[Zuul] Lost log stream connection to [%s:%s]"
                        % (self.ip, self.port),
                        stopped=True)
                    raise
                if not more:
                    buffering = False
                else:
                    buff += more
        if buff:
            self.callback._log_streamline(
                self.host, buff.decode("utf-8", "backslashreplace"),
                stopped=True)


class CallbackModule(default.CallbackModule):

    '''
    This is the Zuul streaming callback. It's based on the default
    callback plugin, but streams results from shell commands.
    '''

    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'stdout'
    CALLBACK_NAME = 'zuul_stream'

    def __init__(self):

        super(CallbackModule, self).__init__()
        self._task = None
        self._daemon_running = False
        self._play = None
        self._streamers = {}  # task uuid -> streamer
        self.sent_failure_result = False
        self.configure_logger()
        self.configure_regexes()
        self._items_done = False
        self._deferred_result = None
        self._playbook_name = None

    def configure_logger(self):
        # ansible appends timestamp, user and pid to the log lines emitted
        # to the log file. We're doing other things though, so we don't want
        # this.
        logging_config = logconfig.load_job_config(
            os.environ['ZUUL_JOB_LOG_CONFIG'])

        if self._display.verbosity > 2:
            logging_config.setDebug()

        logging_config.apply()

        self._logger = logging.getLogger('zuul.executor.ansible')
        self._result_logger = logging.getLogger(
            'zuul.executor.ansible.result')

    def configure_regexes(self):
        try:
            serialized = os.environ.get('ZUUL_JOB_FAILURE_OUTPUT', '[]')
            regexes = json.loads(serialized)
            self.failure_output = [re2.compile(x) for x in regexes]
        except Exception as e:
            self._log("Error compiling failure output regexes: %s"
                      % (str(e),), job=False, executor=True)
            self.failure_output = []

    def _log(self, msg, ts=None, job=True, executor=False, debug=False):
        # With the default "linear" strategy (and likely others),
        # Ansible will send the on_task_start callback, and then fork
        # a worker process to execute that task.  Since we spawn a
        # thread in the on_task_start callback, we can end up emitting
        # a log message in this method while Ansible is forking.  If a
        # forked process inherits a Python file object (i.e., stdout)
        # that is locked by a thread that doesn't exist in the fork
        # (i.e., this one), it can deadlock when trying to flush the
        # file object.  To minimize the chances of that happening, we
        # should avoid using _display outside the main thread.
        # Therefore:

        # Do not set executor=True from any calls from a thread
        # spawned in this callback.

        msg = msg.rstrip()
        if job:
            now = ts or datetime.datetime.now()
            self._logger.info("{now} | {msg}".format(now=now, msg=msg))
        if executor:
            if debug:
                self._display.vvv(msg)
            else:
                self._display.display(msg)

    def _log_streamline(self, host, line, stopped):
        if "[Zuul] Task exit code" in line:
            return True
        elif "[Zuul] Log not found" in line:
            # don't output this line
            if stopped:
                return True
            else:
                return False
        elif " | " in line:
            ts, ln = line.split(' | ', 1)

            self._log("%s | %s " % (host, ln), ts=ts)
            self._check_failure_output(host, ln)
            return False
        else:
            self._log("%s | %s " % (host, line))
            self._check_failure_output(host, line)
            return False

    def _check_failure_output(self, host, line):
        if self.sent_failure_result:
            return
        for fail_re in self.failure_output:
            if fail_re.match(line):
                self._log(
                    '%s | Early failure in job, matched regex "%s"' % (
                        host, fail_re.pattern,))
                self._result_logger.info("failure")
                self.sent_failure_result = True

    def _log_module_failure(self, result, result_dict):
        if 'module_stdout' in result_dict and result_dict['module_stdout']:
            self._log_message(
                result, status='MODULE FAILURE',
                msg=result_dict['module_stdout'])
        elif 'exception' in result_dict and result_dict['exception']:
            self._log_message(
                result, status='MODULE FAILURE',
                msg=result_dict['exception'])
        elif 'module_stderr' in result_dict:
            self._log_message(
                result, status='MODULE FAILURE',
                msg=result_dict['module_stderr'])

    def v2_playbook_on_start(self, playbook):
        self._playbook_name = os.path.splitext(playbook._file_name)[0]

    def v2_playbook_on_include(self, included_file):
        for host in included_file._hosts:
            self._log("{host} | included: {filename}".format(
                host=host.name,
                filename=included_file._filename))

    def v2_playbook_on_play_start(self, play):
        self._play = play
        # Log an extra blank line to get space before each play
        self._log("")

        # the name of a play defaults to the hosts string
        name = play.get_name().strip()
        msg = u"PLAY [{name}]".format(name=name)

        self._log(msg)

    def v2_playbook_on_task_start(self, task, is_conditional):
        # Log an extra blank line to get space before each task
        self._log("")

        self._task = task

        if self._play.strategy != 'free':
            task_name = self._print_task_banner(task)
        else:
            task_name = task.get_name().strip()

        if task.loop is not None:
            # Don't try to stream from loops
            return
        if task.async_val:
            # Don't try to stream from async tasks
            return
        if task.action in STREAMING_ACTIONS:
            play_vars = self._play._variable_manager._hostvars

            hosts = self._get_task_hosts(task)
            for host, inventory_hostname in hosts:
                default_port = ACTION_LOG_STREAM_PORT.get(task.action)
                # This is intended to be only used for testing where
                # we change the port so we can run another instance
                # that doesn't conflict with one setup by the test
                # environment
                port = int(os.environ.get("ZUUL_CONSOLE_PORT", default_port))
                if port is None:
                    continue

                if (host in ('localhost', '127.0.0.1')):
                    # Don't try to stream from localhost
                    continue
                ip = play_vars[host].get(
                    'ansible_host', play_vars[host].get(
                        'ansible_inventory_host'))
                if (ip in ('localhost', '127.0.0.1')):
                    # Don't try to stream from localhost
                    continue
                # Note: this checks hostvars, but not task vars.
                if boolean(play_vars[host].get(
                        'zuul_console_disabled', False)):
                    # The user has told us not to even try
                    continue
                if play_vars[host].get('ansible_connection') in ('winrm',):
                    # The winrm connections don't support streaming for now
                    continue
                if play_vars[host].get('ansible_connection') in ('kubectl', ):
                    # Stream from the forwarded port on kubectl conns
                    if task.action in WINDOWS_ACTIONS:
                        port_id = 'stream_port2'
                    else:
                        port_id = 'stream_port1'
                    port = play_vars[host]['zuul']['resources'][
                        inventory_hostname].get(port_id)
                    if port is None:
                        self._log("[Zuul] Kubectl and socat must be installed "
                                  "on the Zuul executor for streaming output "
                                  "from pods")
                        continue
                    ip = '127.0.0.1'

                # Get a unique key for ZUUL_LOG_ID_MAP.  Use it to add
                # a counter to the log id so that if we run the same
                # task more than once, we get a unique log file.  See
                # comments in paths.py for details.
                log_host = paths._sanitize_filename(inventory_hostname)
                key = "%s-%s" % (self._task._uuid, log_host)
                count = paths.ZUUL_LOG_ID_MAP.get(key, 0) + 1
                paths.ZUUL_LOG_ID_MAP[key] = count
                log_id = "%s-%s-%s" % (
                    self._task._uuid, count, log_host)

                self._log("[%s] Starting to log %s for task %s"
                          % (host, log_id, task_name),
                          job=False, executor=True)
                streamer = Streamer(self, host, ip, port, log_id)
                streamer.start()
                self._streamers[self._task._uuid] = streamer

    def v2_playbook_on_handler_task_start(self, task):
        self.v2_playbook_on_task_start(task, False)

    def _stop_streamers(self):
        while True:
            keys = list(self._streamers.keys())
            if not keys:
                # No streamers, so all are stopped
                break
            streamer = self._streamers.pop(keys[0], None)
            if streamer is None:
                # Perhaps we raced; restart the loop and try again
                continue
            # Give 10 seconds of grace for the log to appear
            streamer.stop(10)
            # And 30 seconds to finish streaming after that
            streamer.join(30)
            if streamer.is_alive():
                msg = "[Zuul] Log Stream did not terminate"
                self._log(msg)

    def _stop_skipped_task_streamer(self, task):
        # Immediately stop a streamer for a skipped task.  We don't
        # expect a logfile in that situation.
        streamer = self._streamers.pop(task._uuid, None)
        if streamer is None:
            return
        # Give no grace since we don't expect the log to exist
        streamer.stop(0)
        # If the streamer connected, then we need to wait to join the
        # thread.  If not, then we can skip it because even if the
        # streamer is still attempting to connect, it will exit before
        # doing anything else.
        if streamer.connected:
            streamer.join(30)
            if streamer.is_alive():
                msg = "[Zuul] Log Stream did not terminate"
                self._log(msg)

    def _process_result_for_localhost(self, result, is_task=True):
        result_dict = dict(result._result)
        localhost_names = ('localhost', '127.0.0.1', '::1')
        is_localhost = False
        task_host = result._host.get_name()
        delegated_vars = result_dict.get('_ansible_delegated_vars', None)
        if delegated_vars:
            delegated_host = delegated_vars['ansible_host']
            if delegated_host in localhost_names:
                is_localhost = True
        elif result._task._variable_manager is None:
            # Handle fact gathering which doens't have a variable manager
            if task_host == 'localhost':
                is_localhost = True
        else:
            task_hostvars = result._task._variable_manager._hostvars[task_host]
            # Normally hosts in the inventory will have ansible_host
            # or ansible_inventory host defined.  The implied
            # inventory record for 'localhost' will have neither, so
            # default to that if none are supplied.
            if task_hostvars.get('ansible_host', task_hostvars.get(
                    'ansible_inventory_host', 'localhost')) in localhost_names:
                is_localhost = True

        if not is_localhost and is_task:
            self._stop_streamers()
        if result._task.action in ALL_ACTIONS:
            stdout_lines = zuul_filter_result(result_dict)
            # We don't have streaming for localhost, raw, or disabled
            # consoles, so get standard out after the fact.
            play_vars = self._play._variable_manager._hostvars
            # Note: this checks hostvars, but not task vars.
            is_disabled = boolean(play_vars[result._host.get_name()].get(
                'zuul_console_disabled', False))
            if (is_localhost or
                is_disabled or
                result._task.action in OUTPUT_ACTIONS):
                for line in stdout_lines:
                    hostname = self._get_hostname(result)
                    self._log("%s | %s " % (hostname, line))
                    self._check_failure_output(hostname, line)

    def _v2_runner_on_failed(self, result, ignore_errors=False):
        result_dict = dict(result._result)

        self._handle_exception(result_dict)

        if result_dict.get('msg') == 'All items completed':
            result_dict['status'] = 'ERROR'
            self._deferred_result = result_dict
            return

        self._process_result_for_localhost(result)

        def is_module_failure(msg):
            return isinstance(msg, str) and msg.startswith('MODULE FAILURE')

        if result._task.loop and 'results' in result_dict:
            # items have their own events
            pass
        elif is_module_failure(result_dict.get('msg', '')):
            self._log_module_failure(result, result_dict)
        else:
            self._log_message(
                result=result, status='ERROR', result_dict=result_dict)
        if ignore_errors:
            self._log_message(result, "Ignoring Errors", status="ERROR")

    def v2_runner_on_failed(self, result, ignore_errors=False):
        ret = self._v2_runner_on_failed(result, ignore_errors)
        if (not ignore_errors and
            not self.sent_failure_result and
            not is_rescuable(result._task)):
            self._result_logger.info("failure")
            self.sent_failure_result = True
        return ret

    def v2_runner_on_unreachable(self, result):
        ret = self._v2_runner_on_failed(result)
        self._result_logger.info("unreachable")
        return ret

    def v2_runner_on_skipped(self, result):
        self._stop_skipped_task_streamer(result._task)
        if result._task.loop:
            self._items_done = False
            self._deferred_result = dict(result._result)
        else:
            reason = result._result.get('skip_reason')
            if reason:
                # No reason means it's an item, which we'll log differently
                self._log_message(result, status='skipping', msg=reason)

    def v2_runner_item_on_skipped(self, result):
        reason = result._result.get('skip_reason')
        if reason:
            self._log_message(result, status='skipping', msg=reason)
        else:
            self._log_message(result, status='skipping')

        if self._deferred_result:
            self._process_deferred(result)

    def v2_runner_on_ok(self, result):
        result_dict = dict(result._result)

        if result._task.action in ('command', 'shell'):
            # The command module has a small set of msgs it returns;
            # we can use that to detect if decided not to execute the
            # command:
            # "Did/Would not run command since 'path' exists/does not exist"
            # is the message we're looking for.
            if 'not run command since' in result_dict.get('msg', ''):
                self._stop_skipped_task_streamer(result._task)

        if result._task.action in ('win_command', 'win_shell'):
            # The win_command module has a small set of msgs it returns;
            # we can use that to detect if decided not to execute the
            # command:
            # "skipped, since $creates exists" and "skipped, since
            # $removes does not exist" are the messages we're looking
            # for.
            m = result_dict.get('msg', '')
            if 'skipped, since' in m and 'exist' in m:
                self._stop_skipped_task_streamer(result._task)

        if (self._play.strategy == 'free'
                and self._last_task_banner != result._task._uuid):
            self._print_task_banner(result._task)

        self._clean_results(result_dict, result._task.action)
        if '_zuul_nolog_return' in result_dict:
            # We have a custom zuul module that doesn't want the parameters
            # from its returned splatted to stdout. This is typically for
            # modules that are collecting data to be displayed some other way.
            for key in list(result_dict.keys()):
                if key != 'changed':
                    result_dict.pop(key)

        if result_dict.get('changed', False):
            status = 'changed'
        else:
            status = 'ok'

        if (result_dict.get('msg') == 'All items completed'
                and not self._items_done):
            result_dict['status'] = status
            self._deferred_result = result_dict
            return

        if not result._task.loop:
            self._process_result_for_localhost(result)
        else:
            self._items_done = False

        self._handle_warnings(result_dict)

        if result._task.loop and 'results' in result_dict:
            # items have their own events
            pass
        elif to_text(result_dict.get('msg', '')).startswith('MODULE FAILURE'):
            self._log_module_failure(result, result_dict)
        elif result._task.action == 'debug':
            # this is a debug statement, handle it special
            for key in [k for k in result_dict
                        if k.startswith('_ansible')]:
                del result_dict[key]
            if 'changed' in result_dict:
                del result_dict['changed']
            keyname = next(iter(result_dict.keys()))
            # If it has msg, that means it was like:
            #
            #  debug:
            #    msg: Some debug text the user was looking for
            #
            # So we log it with self._log to get just the raw string the
            # user provided. Note that msg may be a multi line block quote
            # so we handle that here as well.
            if keyname == 'msg':
                msg_lines = to_text(result_dict['msg']).rstrip().split('\n')
                for msg_line in msg_lines:
                    self._log(msg=msg_line)
            else:
                self._log_message(
                    msg=json.dumps(result_dict, indent=2, sort_keys=True),
                    status=status, result=result)
        elif result._task.action not in STREAMING_ACTIONS:
            if 'msg' in result_dict:
                self._log_message(msg=result_dict['msg'],
                                  result=result, status=status)
            else:
                self._log_message(
                    result=result,
                    status=status)
        elif 'results' in result_dict:
            for res in result_dict['results']:
                self._log_message(
                    result,
                    "Runtime: {delta}".format(**res))
        elif result_dict.get('msg') == 'All items completed':
            self._log_message(result, result_dict['msg'])
        else:
            if 'delta' in result_dict:
                self._log_message(
                    result,
                    "Runtime: {delta}".format(
                        **result_dict))
            else:
                # NOTE(ianw) 2022-08-24 : *Fairly* sure that you only
                # fall into here when the call actually fails (and has
                # not start/end time), but it is ignored by
                # failed_when matching.
                self._log_message(result, msg='ERROR (ignored)',
                                  result_dict=result_dict)

    def v2_runner_item_on_ok(self, result):
        result_dict = dict(result._result)
        self._process_result_for_localhost(result, is_task=False)

        if result_dict.get('changed', False):
            status = 'changed'
        else:
            status = 'ok'

        # This fallback may not be strictly necessary. 'item' is the
        # default and we'll avoid problems in the common case if ansible
        # changes.
        loop_var = result_dict.get('ansible_loop_var', 'item')

        if to_text(result_dict.get('msg', '')).startswith('MODULE FAILURE'):
            self._log_module_failure(result, result_dict)
        elif result._task.action not in ALL_ACTIONS:
            if 'msg' in result_dict:
                self._log_message(
                    result=result, msg=result_dict['msg'], status=status)
            else:
                self._log_message(
                    result=result,
                    msg=json.dumps(result_dict[loop_var],
                                   indent=2, sort_keys=True),
                    status=status)
        else:
            stdout_lines = zuul_filter_result(result_dict)
            for line in stdout_lines:
                hostname = self._get_hostname(result)
                self._log("%s | %s " % (hostname, line))

            if isinstance(result_dict[loop_var], str):
                self._log_message(
                    result,
                    "Item: {loop_var} Runtime: {delta}".format(
                        loop_var=result_dict[loop_var],
                        delta=result_dict['delta']))
            else:
                self._log_message(
                    result,
                    "Item: Runtime: {delta}".format(
                        **result_dict))

        if self._deferred_result:
            self._process_deferred(result)

    def v2_runner_item_on_failed(self, result):
        result_dict = dict(result._result)
        self._process_result_for_localhost(result, is_task=False)

        # This fallback may not be strictly necessary. 'item' is the
        # default and we'll avoid problems in the common case if ansible
        # changes.
        loop_var = result_dict.get('ansible_loop_var', 'item')

        if to_text(result_dict.get('msg', '')).startswith('MODULE FAILURE'):
            self._log_module_failure(result, result_dict)
        elif result._task.action not in ALL_ACTIONS:
            self._log_message(
                result=result,
                msg="Item: {loop_var}".format(loop_var=result_dict[loop_var]),
                status='ERROR',
                result_dict=result_dict)
        else:
            stdout_lines = zuul_filter_result(result_dict)
            for line in stdout_lines:
                hostname = self._get_hostname(result)
                self._log("%s | %s " % (hostname, line))

            # self._log("Result: %s" % (result_dict))
            self._log_message(
                result, "Item: {loop_var} Result: {rc}".format(
                    loop_var=result_dict[loop_var], rc=result_dict['rc']))

        if self._deferred_result:
            self._process_deferred(result)

    def v2_playbook_on_stats(self, stats):
        # Add a spacer line before the stats so that there will be a line
        # between the last task and the recap
        self._log("")

        self._log("PLAY RECAP")

        hosts = sorted(stats.processed.keys())
        for host in hosts:
            t = stats.summarize(host)
            msg = (
                "{host} |"
                " ok: {ok}"
                " changed: {changed}"
                " unreachable: {unreachable}"
                " failed: {failures}".format(host=host, **t))

            # NOTE(pabelanger) Ansible 2.8 added rescued support
            if 'rescued' in t:
                # Even though skipped was in stable-2.7 and lower, only
                # stable-2.8 started rendering it. So just lump into rescued
                # check.
                msg += " skipped: {skipped} rescued: {rescued}".format(**t)
            # NOTE(pabelanger) Ansible 2.8 added ignored support
            if 'ignored' in t:
                msg += " ignored: {ignored}".format(**t)
            self._log(msg)

        # Add a spacer line after the stats so that there will be a line
        # between each playbook
        self._log("")

    def _process_deferred(self, result):
        self._items_done = True
        result_dict = self._deferred_result
        self._deferred_result = None
        status = result_dict.get('status')

        if status:
            self._log_message(result, "All items complete", status=status)

        # Log an extra blank line to get space after each task
        self._log("")

    def _print_task_banner(self, task):

        task_name = task.get_name().strip()

        if task.loop:
            task_type = 'LOOP'
        else:
            task_type = 'TASK'

        # TODO(mordred) With the removal of printing task args, do we really
        # want to keep doing this section?
        task_args = task.args.copy()
        is_shell = task_args.pop('_uses_shell', False)
        if is_shell and task_name == 'command':
            task_name = 'shell'
        # win_shell doesn't use _uses_shell.
        raw_params = task_args.pop('_raw_params', '').split('\n')
        # If there's just a single line, go ahead and print it
        if len(raw_params) == 1 and task_name in STREAMING_ACTIONS:
            task_name = '{name}: {command}'.format(
                name=task_name, command=raw_params[0])

        msg = "{task_type} [{task}]".format(
            task_type=task_type,
            task=task_name)
        self._log(msg)
        return task

    def _get_task_hosts(self, task):
        result = []

        # _restriction returns the parsed/compiled list of hosts after
        # applying subsets/limits
        hosts = self._play._variable_manager._inventory._restriction
        for inventory_host in hosts:
            # If this task has as delegate to, we don't care about the play
            # hosts, we care about the task's delegate target.
            if task.delegate_to:
                host = task.delegate_to
            else:
                host = inventory_host
            result.append((host, inventory_host))

        return result

    def _dump_result_dict(self, result_dict):
        result_dict = result_dict.copy()
        for key in list(result_dict.keys()):
            if key.startswith('_ansible'):
                del result_dict[key]
        zuul_filter_result(result_dict)
        return result_dict

    def _log_message(self, result, msg=None, status="ok", result_dict=None):
        hostname = self._get_hostname(result)
        if result_dict:
            result_dict = self._dump_result_dict(result_dict)
        if result._task.no_log:
            self._log("{host} | {msg}".format(
                host=hostname,
                msg="Output suppressed because no_log was given"))
            return
        if (not msg and result_dict
                and set(result_dict.keys()) == set(['msg', 'failed'])):
            msg = result_dict['msg']
            result_dict = None
        if msg:
            # ensure msg is a string; e.g.
            #
            # debug:
            #   msg: '{{ var }}'
            #
            # may not be!
            msg_lines = to_text(msg).rstrip().split('\n')
            if len(msg_lines) > 1:
                self._log("{host} | {status}:".format(
                    host=hostname, status=status))
                for msg_line in msg_lines:
                    self._log("{host} | {msg_line}".format(
                        host=hostname, msg_line=msg_line))
            else:
                self._log("{host} | {status}: {msg}".format(
                    host=hostname, status=status, msg=msg))
        else:
            self._log("{host} | {status}".format(
                host=hostname, status=status))
        if result_dict:
            result_string = json.dumps(result_dict, indent=2, sort_keys=True)
            for line in result_string.split('\n'):
                self._log("{host} | {line}".format(host=hostname, line=line))

    def _get_hostname(self, result):
        delegated_vars = result._result.get('_ansible_delegated_vars', None)
        if delegated_vars:
            return "{host} -> {delegated_host}".format(
                host=result._host.get_name(),
                delegated_host=delegated_vars['ansible_host'])
        else:
            return result._host.get_name()
