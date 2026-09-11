#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2012, Michael DeHaan <michael.dehaan@gmail.com>, and others
# Copyright: (c) 2016, Toshio Kuratomi <tkuratomi@ansible.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import annotations

import json
import sys

# flake8: noqa
# This file shares a significant chunk of code with an upstream ansible
# function, run_command. The goal is to not have to fork quite so much
# of that function, and discussing that design with upstream means we
# should keep the changes to substantive ones only. For that reason, this
# file is purposely not enforcing pep8, as making the function pep8 clean
# would remove our ability to easily have a discussion with our friends
# upstream

DOCUMENTATION = r'''
---
module: command
short_description: Execute commands on targets
version_added: historical
description:
     - The C(ansible.builtin.command) module takes the command name followed by a list of space-delimited arguments.
     - The given command will be executed on all selected nodes.
     - The command(s) will not be processed through the shell, so operations like C("*"), C("<"), C(">"), C("|"), C(";") and C("&") will not work.
       Also, environment variables are resolved via Python, not shell, see O(expand_argument_vars) and are left unchanged if not matched.
       Use the M(ansible.builtin.shell) module if you need these features.
     - To create C(command) tasks that are easier to read than the ones using space-delimited arguments,
       pass parameters using the C(args) L(task keyword,https://docs.ansible.com/ansible/latest/reference_appendices/playbooks_keywords.html#task)
       or use O(cmd) parameter.
     - Either a free form command or O(cmd) parameter is required, see the examples.
     - For Windows targets, use the M(ansible.windows.win_command) module instead.
extends_documentation_fragment:
    - action_common_attributes
    - action_common_attributes.raw
attributes:
    check_mode:
        details: while the command itself is arbitrary and cannot be subject to the check mode semantics it adds O(creates)/O(removes) options as a workaround
        support: partial
    diff_mode:
        support: none
    platform:
      support: full
      platforms: posix
    raw:
      support: full
options:
  expand_argument_vars:
    description:
      - Expands the arguments that are variables, for example C($HOME) will be expanded before being passed to the command to run.
      - If a variable is not matched, it is left unchanged, unlike shell substitution which would remove it.
      - Set to V(false) to disable expansion and treat the value as a literal argument.
    type: bool
    default: true
    version_added: "2.16"
  free_form:
    description:
      - The command module takes a free form string as a command to run.
      - There is no actual parameter named C(free_form).
  cmd:
    type: str
    description:
      - The command to run.
  argv:
    type: list
    elements: str
    description:
      - Passes the command as a list rather than a string.
      - Use O(argv) to avoid quoting values that would otherwise be interpreted incorrectly (for example "user name").
      - Only the string (free form) or the list (argv) form can be provided, not both.  One or the other must be provided.
    version_added: "2.6"
  creates:
    type: path
    description:
      - A filename or (since 2.0) glob pattern. If a matching file already exists, this step B(will not) be run.
      - This is checked before O(removes) is checked.
  removes:
    type: path
    description:
      - A filename or (since 2.0) glob pattern. If a matching file exists, this step B(will) be run.
      - This is checked after O(creates) is checked.
    version_added: "0.8"
  chdir:
    type: path
    description:
      - Change into this directory before running the command.
    version_added: "0.6"
  stdin:
    description:
      - Set the stdin of the command directly to the specified value.
    type: str
    version_added: "2.4"
  stdin_add_newline:
    type: bool
    default: yes
    description:
      - If set to V(true), append a newline to stdin data.
    version_added: "2.8"
  strip_empty_ends:
    description:
      - Strip empty lines from the end of stdout/stderr in result.
    version_added: "2.8"
    type: bool
    default: yes
notes:
    -  If you want to run a command through the shell (say you are using C(<), C(>), C(|), and so on),
       you actually want the M(ansible.builtin.shell) module instead.
       Parsing shell metacharacters can lead to unexpected commands being executed if quoting is not done correctly so it is more secure to
       use the M(ansible.builtin.command) module when possible.
    -  O(creates), O(removes), and O(chdir) can be specified after the command.
       For instance, if you only want to run a command if a certain file does not exist, use this.
    -  Check mode is supported when passing O(creates) or O(removes). If running in check mode and either of these are specified, the module will
       check for the existence of the file and report the correct changed status. If these are not supplied, the task will be skipped.
    -  The O(ignore:executable) parameter is removed since version 2.4. If you have a need for this parameter, use the M(ansible.builtin.shell) module instead.
    -  For Windows targets, use the M(ansible.windows.win_command) module instead.
    -  For rebooting systems, use the M(ansible.builtin.reboot) or M(ansible.windows.win_reboot) module.
    -  If the command returns non UTF-8 data, it must be encoded to avoid issues. This may necessitate using M(ansible.builtin.shell) so the output
       can be piped through C(base64).
seealso:
- module: ansible.builtin.raw
- module: ansible.builtin.script
- module: ansible.builtin.shell
- module: ansible.windows.win_command
author:
    - Ansible Core Team
    - Michael DeHaan
'''

EXAMPLES = r'''
- name: Return motd to registered var
  ansible.builtin.command: cat /etc/motd
  register: mymotd

# free-form (string) arguments, all arguments on one line
- name: Run command if /path/to/database does not exist (without 'args')
  ansible.builtin.command: /usr/bin/make_database.sh db_user db_name creates=/path/to/database

# free-form (string) arguments, some arguments on separate lines with the 'args' keyword
# 'args' is a task keyword, passed at the same level as the module
- name: Run command if /path/to/database does not exist (with 'args' keyword)
  ansible.builtin.command: /usr/bin/make_database.sh db_user db_name
  args:
    creates: /path/to/database

# 'cmd' is module parameter
- name: Run command if /path/to/database does not exist (with 'cmd' parameter)
  ansible.builtin.command:
    cmd: /usr/bin/make_database.sh db_user db_name
    creates: /path/to/database

- name: Change the working directory to somedir/ and run the command as db_owner if /path/to/database does not exist
  ansible.builtin.command: /usr/bin/make_database.sh db_user db_name
  become: yes
  become_user: db_owner
  args:
    chdir: somedir/
    creates: /path/to/database

- name: Run command using argv with mixed argument formats
  ansible.builtin.command:
    argv:
      - /path/to/binary
      - -v
      - --debug
      - --longopt
      - value for longopt
      - --other-longopt=value for other longopt
      - positional

# argv (list) arguments, each argument on a separate line, 'args' keyword not necessary
# 'argv' is a parameter, indented one level from the module
- name: Use 'argv' to send a command as a list - leave 'command' empty
  ansible.builtin.command:
    argv:
      - /usr/bin/make_database.sh
      - Username with whitespace
      - dbname with whitespace
    creates: /path/to/database

- name: Safely use templated variable to run command. Always use the quote filter to avoid injection issues
  ansible.builtin.command: cat {{ myfile|quote }}
  register: myoutput
'''

RETURN = r'''
msg:
  description: changed
  returned: always
  type: bool
  sample: True
start:
  description: The command execution start time.
  returned: always
  type: str
  sample: '2017-09-29 22:03:48.083128'
end:
  description: The command execution end time.
  returned: always
  type: str
  sample: '2017-09-29 22:03:48.084657'
delta:
  description: The command execution delta time.
  returned: always
  type: str
  sample: '0:00:00.001529'
stdout:
  description: The command standard output.
  returned: always
  type: str
  sample: 'Clustering node rabbit@slave1 with rabbit@master …'
stderr:
  description: The command standard error.
  returned: always
  type: str
  sample: 'ls cannot access foo: No such file or directory'
cmd:
  description: The command executed by the task.
  returned: always
  type: list
  sample:
  - echo
  - hello
rc:
  description: The command return code (0 means success).
  returned: always
  type: int
  sample: 0
stdout_lines:
  description: The command standard output split in lines.
  returned: always
  type: list
  sample: [u'Clustering node rabbit@slave1 with rabbit@master …']
stderr_lines:
  description: The command standard error split in lines.
  returned: always
  type: list
  sample: [u'ls cannot access foo: No such file or directory', u'ls …']
'''

import datetime
import glob
import os
import shlex
import select

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common.text.converters import to_native, to_bytes, to_text
from ansible.module_utils.common.collections import is_iterable

# Imports needed for Zuul things
import io
import re
import subprocess
import traceback
import threading
from ansible.module_utils.basic import heuristic_log_sanitize
from ansible.module_utils.six import (
    PY2,
    PY3,
    b,
    binary_type,
    string_types,
    text_type,
)
from ansible.module_utils.six.moves import shlex_quote


LOG_STREAM_FILE = '/tmp/console-{log_uuid}.log'
PASSWD_ARG_RE = re.compile(r'^[-]{0,2}pass[-]?(word|wd)?')


class Console:
    def __init__(self, log_uuid):
        # The streamer currently will not ask us for output from
        # loops.  This flag uuid was set in the action plugin if this
        # call was part of a loop.  This avoids us leaving behind
        # files that will never be read, but also means no other
        # special-casing for any of this path.
        if log_uuid == 'in-loop-ignore':
            self.logfile_name = os.devnull
        elif log_uuid == 'skip':
            self.logfile_name = os.devnull
        else:
            self.logfile_name = LOG_STREAM_FILE.format(log_uuid=log_uuid)

    def open(self):
        self.logfile = open(self.logfile_name, 'ab', buffering=0)

    def __enter__(self):
        self.open()
        return self

    def close(self):
        self.logfile.close()

    def __exit__(self, etype, value, tb):
        self.close()

    def addLine(self, ln):
        # Note this format with deliminator is "inspired" by the old
        # Jenkins format but with microsecond resolution instead of
        # millisecond.  It is kept so log parsing/formatting remains
        # consistent.
        ts = str(datetime.datetime.now()).encode('utf-8')
        if not isinstance(ln, bytes):
            try:
                ln = ln.encode('utf-8')
            except Exception:
                ln = repr(ln).encode('utf-8') + b'\n'
        outln = b'%s | %s' % (ts, ln)
        self.logfile.write(outln)


class StreamFollower:
    def __init__(self, cmd, log_uuid, output_max_bytes, no_log):
        self.cmd = cmd
        self.log_uuid = log_uuid
        self.exception = None
        self.output_max_bytes = output_max_bytes
        # Lists to save stdout/stderr log lines in as we collect them
        self.log_bytes = io.BytesIO()
        self.stderr_log_bytes = io.BytesIO()
        self.stdout_thread = None
        self.stderr_thread = None
        # Total size in bytes of all log and stderr_log lines
        self.log_size = 0
        self.no_log = no_log

    def join(self):
        if self.exception:
            try:
                self.console.close()
            except Exception:
                pass
            raise self.exception
        for t in (self.stdout_thread, self.stderr_thread):
            if t is None:
                continue
            t.join(10)
            if t.is_alive():
                with Console(self.log_uuid) as console:
                    console.addLine("[Zuul] standard output/error still open "
                                    "after child exited")
        self.console.close()

    def follow(self):
        self.console = Console(self.log_uuid)
        self.console.open()
        if self.cmd.stdout:
            self.stdout_thread = threading.Thread(
                target=self.follow_main,
                args=(self.cmd.stdout, self.log_bytes))
            self.stdout_thread.daemon = True
            self.stdout_thread.start()
        if self.cmd.stderr:
            self.stderr_thread = threading.Thread(
                target=self.follow_main,
                args=(self.cmd.stderr, self.stderr_log_bytes))
            self.stderr_thread.daemon = True
            self.stderr_thread.start()

    def follow_main(self, fd, log_bytes):
        try:
            self.follow_inner(fd, log_bytes)
        except Exception as e:
            self.exception = e

    def follow_inner(self, fd, log_bytes):
        newline_warning = False
        while True:
            line = fd.readline()
            if not line:
                break
            if self.no_log:
                continue
            self.log_size += len(line)
            if self.log_size > self.output_max_bytes:
                msg = (
                    '[Zuul] Log output exceeded max of %s, '
                    'terminating\n' % (self.output_max_bytes,))
                self.console.addLine(msg)
                try:
                    pgid = os.getpgid(self.cmd.pid)
                    os.killpg(pgid, signal.SIGKILL)
                except Exception:
                    pass
                raise Exception(msg)
            log_bytes.write(line)
            if not line[-1] != b'\n':
                line += b'\n'
                newline_warning = True
            self.console.addLine(line)
        if newline_warning:
            self.console.addLine('[Zuul] No trailing newline\n')


# Taken from ansible/module_utils/basic.py ... forking the method for now
# so that we can dive in and figure out how to make appropriate hook points
def zuul_run_command(self, args, zuul_log_id, zuul_ansible_split_streams, zuul_output_max_bytes, check_rc=False, close_fds=True, executable=None, data=None, binary_data=False, path_prefix=None, cwd=None,
                use_unsafe_shell=False, prompt_regex=None, environ_update=None, umask=None, encoding='utf-8', errors='surrogate_or_strict',
                expand_user_and_vars=True, pass_fds=None, before_communicate_callback=None, ignore_invalid_cwd=True, handle_exceptions=True):

    '''
    Execute a command, returns rc, stdout, and stderr.

    :arg args: is the command to run
        * If args is a list, the command will be run with shell=False.
        * If args is a string and use_unsafe_shell=False it will split args to a list and run with shell=False
        * If args is a string and use_unsafe_shell=True it runs with shell=True.
    :kw check_rc: Whether to call fail_json in case of non zero RC.
        Default False
    :kw close_fds: See documentation for subprocess.Popen(). Default True
    :kw executable: See documentation for subprocess.Popen(). Default None
    :kw data: If given, information to write to the stdin of the command
    :kw binary_data: If False, append a newline to the data.  Default False
    :kw path_prefix: If given, additional path to find the command in.
        This adds to the PATH environment variable so helper commands in
        the same directory can also be found
    :kw cwd: If given, working directory to run the command inside
    :kw use_unsafe_shell: See `args` parameter.  Default False
    :kw prompt_regex: Regex string (not a compiled regex) which can be
        used to detect prompts in the stdout which would otherwise cause
        the execution to hang (especially if no input data is specified)
    :kw environ_update: dictionary to *update* environ variables with
    :kw umask: Umask to be used when running the command. Default None
    :kw encoding: Since we return native strings, on python3 we need to
        know the encoding to use to transform from bytes to text.  If you
        want to always get bytes back, use encoding=None.  The default is
        "utf-8".  This does not affect transformation of strings given as
        args.
    :kw errors: Since we return native strings, on python3 we need to
        transform stdout and stderr from bytes to text.  If the bytes are
        undecodable in the ``encoding`` specified, then use this error
        handler to deal with them.  The default is ``surrogate_or_strict``
        which means that the bytes will be decoded using the
        surrogateescape error handler if available (available on all
        python3 versions we support) otherwise a UnicodeError traceback
        will be raised.  This does not affect transformations of strings
        given as args.
    :kw expand_user_and_vars: When ``use_unsafe_shell=False`` this argument
        dictates whether ``~`` is expanded in paths and environment variables
        are expanded before running the command. When ``True`` a string such as
        ``$SHELL`` will be expanded regardless of escaping. When ``False`` and
        ``use_unsafe_shell=False`` no path or variable expansion will be done.
    :kw pass_fds: When running on Python 3 this argument
        dictates which file descriptors should be passed
        to an underlying ``Popen`` constructor. On Python 2, this will
        set ``close_fds`` to False.
    :kw before_communicate_callback: This function will be called
        after ``Popen`` object will be created
        but before communicating to the process.
        (``Popen`` object will be passed to callback as a first argument)
    :kw ignore_invalid_cwd: This flag indicates whether an invalid ``cwd``
        (non-existent or not a directory) should be ignored or should raise
        an exception.
    :kw handle_exceptions: This flag indicates whether an exception will
        be handled inline and issue a failed_json or if the caller should
        handle it.
    :returns: A 3-tuple of return code (integer), stdout (native string),
        and stderr (native string).  On python2, stdout and stderr are both
        byte strings.  On python3, stdout and stderr are text strings converted
        according to the encoding and errors parameters.  If you want byte
        strings on python3, use encoding=None to turn decoding to text off.
    '''
    # used by clean args later on
    self._clean = None

    if not isinstance(args, (list, bytes, str)):
        msg = "Argument 'args' to run_command must be list or string"
        self.fail_json(rc=257, cmd=args, msg=msg)

    shell = False
    if use_unsafe_shell:

        # stringify args for unsafe/direct shell usage
        if isinstance(args, list):
            args = b" ".join([to_bytes(shlex.quote(x), errors='surrogate_or_strict') for x in args])
        else:
            args = to_bytes(args, errors='surrogate_or_strict')

        # not set explicitly, check if set by controller
        if executable:
            executable = to_bytes(executable, errors='surrogate_or_strict')
            args = [executable, b'-c', args]
        elif self._shell not in (None, '/bin/sh'):
            args = [to_bytes(self._shell, errors='surrogate_or_strict'), b'-c', args]
        else:
            shell = True
    else:
        # ensure args are a list
        if isinstance(args, (bytes, str)):
            args = shlex.split(to_text(args, errors='surrogateescape'))

        # expand ``~`` in paths, and all environment vars
        if expand_user_and_vars:
            args = [to_bytes(os.path.expanduser(os.path.expandvars(x)), errors='surrogate_or_strict') for x in args if x is not None]
        else:
            args = [to_bytes(x, errors='surrogate_or_strict') for x in args if x is not None]

    prompt_re = None
    if prompt_regex:
        if isinstance(prompt_regex, str):
            prompt_regex = to_bytes(prompt_regex, errors='surrogateescape')
        try:
            prompt_re = re.compile(prompt_regex, re.MULTILINE)
        except re.error:
            self.fail_json(msg="invalid prompt regular expression given to run_command")

    rc = 0
    msg = None
    st_in = None

    env = os.environ.copy()
    # We can set this from both an attribute and per call
    env.update(self.run_command_environ_update or {})
    env.update(environ_update or {})
    if path_prefix:
        path = env.get('PATH', '')
        if path:
            env['PATH'] = "%s:%s" % (path_prefix, path)
        else:
            env['PATH'] = path_prefix

    # If using test-module.py and explode, the remote lib path will resemble:
    #   /tmp/test_module_scratch/debug_dir/ansible/module_utils/basic.py
    # If using ansible or ansible-playbook with a remote system:
    #   /tmp/ansible_vmweLQ/ansible_modlib.zip/ansible/module_utils/basic.py

    # Clean out python paths set by ansiballz
    if 'PYTHONPATH' in env:
        pypaths = [x for x in env['PYTHONPATH'].split(':')
                   if x and
                   not x.endswith('/ansible_modlib.zip') and
                   not x.endswith('/debug_dir')]
        if pypaths and any(pypaths):
            env['PYTHONPATH'] = ':'.join(pypaths)

    if data:
        st_in = subprocess.PIPE

    def preexec():
        if umask:
            os.umask(umask)

    # ZUUL: merge stdout/stderr depending on config
    stderr = subprocess.PIPE if zuul_ansible_split_streams else subprocess.STDOUT
    kwargs = dict(
        executable=executable,
        shell=shell,
        close_fds=close_fds,
        stdin=st_in,
        stdout=subprocess.PIPE,
        stderr=stderr,
        preexec_fn=preexec,
        env=env,
    )
    if pass_fds:
        kwargs["pass_fds"] = pass_fds

    # make sure we're in the right working directory
    if cwd:
        cwd = to_bytes(os.path.abspath(os.path.expanduser(cwd)), errors='surrogate_or_strict')
        if os.path.isdir(cwd):
            kwargs['cwd'] = cwd
        elif not ignore_invalid_cwd:
            self.fail_json(msg="Provided cwd is not a valid directory: %s" % cwd)

    t = None
    fail_json_kwargs = None

    try:
        if self._debug:
            self.log('Executing <%s>: %s',
                     zuul_log_id, self._clean_args(args))

        # ZUUL: Replaced the execution loop with the zuul_runner run function

        cmd = subprocess.Popen(args, **kwargs)
        if before_communicate_callback:
            before_communicate_callback(cmd)

        follower = StreamFollower(cmd, zuul_log_id, zuul_output_max_bytes, self.no_log)
        follower.follow()

        # ZUUL: Our log thread will catch the output so don't do that here.

        if data:
            if not binary_data:
                data += '\n'
            if isinstance(data, text_type):
                data = to_bytes(data)
            cmd.stdin.write(data)
            cmd.stdin.close()

        # ZUUL: If the console log follow thread *is* stuck in readline,
        # we can't close stdout (attempting to do so raises an
        # exception) , so this is disabled.
        # cmd.stdout.close()
        # cmd.stderr.close()

        rc = cmd.wait()

        # ZUUL: Give the thread that is writing the console log up to
        # 10 seconds to catch up and exit.  If it hasn't done so by
        # then, it is very likely stuck in readline() because it
        # spawed a child that is holding stdout or stderr open.
        follower.join()
        # ZUUL: stdout and stderr are in the console log file
        # ZUUL: return the saved log lines so we can ship them back
        stdout = follower.log_bytes.getvalue()
        stderr = follower.stderr_log_bytes.getvalue()

    except (OSError, IOError) as e:
        self.log("Error Executing CMD:%s Exception:%s" % (self._clean_args(args), to_native(e)))
        # ZUUL: store fail_json_kwargs and fail later in finally
        fail_json_kwargs = dict(rc=e.errno, stdout=b'', stderr=b'', msg=to_native(e), cmd=self._clean_args(args))
    except Exception as e:
        self.log("Error Executing CMD:%s Exception:%s" % (self._clean_args(args), to_native(traceback.format_exc())))
        # ZUUL: store fail_json_kwargs and fail later in finally
        fail_json_kwargs = dict(rc=257, stdout=b'', stderr=b'', msg=to_native(e), exception=traceback.format_exc(), cmd=self._clean_args(args))
    finally:
        with Console(zuul_log_id) as console:
            if fail_json_kwargs:
                # we hit an exception and need to use the rc from
                # fail_json_kwargs
                rc = fail_json_kwargs['rc']

            console.addLine("[Zuul] Task exit code: %s\n" % rc)

        if fail_json_kwargs:
            self.fail_json(**fail_json_kwargs)

    if rc != 0 and check_rc:
        msg = heuristic_log_sanitize(stderr.rstrip(), self.no_log_values)
        self.fail_json(cmd=self._clean_args(args), rc=rc, stdout=stdout, stderr=stderr, msg=msg)

    if encoding is not None:
        return (rc, to_native(stdout, encoding=encoding, errors=errors),
                to_native(stderr, encoding=encoding, errors=errors))

    return (rc, stdout, stderr)


def main():

    # the command module is the one ansible module that does not take key=value args
    # hence don't copy this one if you are looking to build others!
    # NOTE: ensure splitter.py is kept in sync for exceptions
    module = AnsibleModule(
        argument_spec=dict(
            _raw_params=dict(),
            _uses_shell=dict(type='bool', default=False),
            argv=dict(type='list', elements='str'),
            chdir=dict(type='path'),
            executable=dict(),
            expand_argument_vars=dict(type='bool', default=True),
            creates=dict(type='path'),
            removes=dict(type='path'),
            # The default for this really comes from the action plugin
            stdin=dict(required=False),
            stdin_add_newline=dict(type='bool', default=True),
            strip_empty_ends=dict(type='bool', default=True),
            zuul_log_id=dict(type='str'),
            zuul_ansible_split_streams=dict(type='bool'),
            zuul_output_max_bytes=dict(type='int'),
            zuul_no_log=dict(type='bool'),
        ),
        supports_check_mode=True,
    )
    shell = module.params['_uses_shell']
    chdir = module.params['chdir']
    executable = module.params['executable']
    args = module.params['_raw_params']
    argv = module.params['argv']
    creates = module.params['creates']
    removes = module.params['removes']
    stdin = module.params['stdin']
    stdin_add_newline = module.params['stdin_add_newline']
    strip = module.params['strip_empty_ends']
    expand_argument_vars = module.params['expand_argument_vars']
    zuul_log_id = module.params['zuul_log_id']
    zuul_ansible_split_streams = module.params["zuul_ansible_split_streams"]
    zuul_output_max_bytes = module.params['zuul_output_max_bytes']

    # we promised these in 'always' ( _lines get auto-added on action plugin)
    r = {'changed': False, 'stdout': '', 'stderr': '', 'rc': None, 'cmd': None, 'start': None, 'end': None, 'delta': None, 'msg': ''}

    if not shell and executable:
        module.warn("As of Ansible 2.4, the parameter 'executable' is no longer supported with the 'command' module. Not using '%s'." % executable)
        executable = None

    if not zuul_log_id:
        module.fail_json(rc=256, msg="zuul_log_id missing: %s" % module.params)

    if (not args or args.strip() == '') and not argv:
        r['rc'] = 256
        r['msg'] = "no command given"
        module.fail_json(**r)

    if args and argv:
        r['rc'] = 256
        r['msg'] = "only command or argv can be given, not both"
        module.fail_json(**r)

    if not shell and args:
        args = shlex.split(args)

    args = args or argv
    # All args must be strings
    if is_iterable(args, include_strings=False):
        args = [to_native(arg, errors='surrogate_or_strict', nonstring='simplerepr') for arg in args]

    r['cmd'] = args

    if chdir:
        chdir = to_bytes(chdir, errors='surrogate_or_strict')

        try:
            os.chdir(chdir)
        except (IOError, OSError) as e:
            r['msg'] = 'Unable to change directory before execution: %s' % to_text(e)
            module.fail_json(**r)

    # check_mode partial support, since it only really works in checking creates/removes
    if module.check_mode:
        shoulda = "Would"
    else:
        shoulda = "Did"

    # special skips for idempotence if file exists (assumes command creates)
    if creates:
        if glob.glob(creates):
            r['msg'] = "%s not run command since '%s' exists" % (shoulda, creates)
            r['stdout'] = "skipped, since %s exists" % creates  # TODO: deprecate

            r['rc'] = 0

    # special skips for idempotence if file does not exist (assumes command removes)
    if not r['msg'] and removes:
        if not glob.glob(removes):
            r['msg'] = "%s not run command since '%s' does not exist" % (shoulda, removes)
            r['stdout'] = "skipped, since %s does not exist" % removes  # TODO: deprecate
            r['rc'] = 0

    if r['msg']:
        module.exit_json(**r)

    # actually executes command (or not ...)
    if not module.check_mode:
        r['start'] = datetime.datetime.now()
        r['rc'], r['stdout'], r['stderr'] = zuul_run_command(module, args, zuul_log_id, zuul_ansible_split_streams, zuul_output_max_bytes, executable=executable, use_unsafe_shell=shell, encoding=None,
                                                               data=stdin, binary_data=(not stdin_add_newline),
                                                               expand_user_and_vars=expand_argument_vars)
        r['end'] = datetime.datetime.now()
    else:
        # this is partial check_mode support, since we end up skipping if we get here
        r['rc'] = 0
        r['msg'] = "Command would have run if not in check mode"
        r['skipped'] = True

    r['changed'] = True
    r['zuul_log_id'] = zuul_log_id

    # convert to text for jsonization and usability
    if r['start'] is not None and r['end'] is not None:
        # these are datetime objects, but need them as strings to pass back
        r['delta'] = to_text(r['end'] - r['start'])
        r['end'] = to_text(r['end'])
        r['start'] = to_text(r['start'])

    if strip:
        r['stdout'] = to_text(r['stdout']).rstrip("\r\n")
        r['stderr'] = to_text(r['stderr']).rstrip("\r\n")

    if r['rc'] != 0:
        r['msg'] = 'non-zero return code'
        module.fail_json(**r)

    module.exit_json(**r)


if __name__ == '__main__':
    main()
