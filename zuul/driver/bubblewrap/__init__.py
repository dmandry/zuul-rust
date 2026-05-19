# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2013 OpenStack Foundation
# Copyright 2016 Red Hat, Inc.
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

import argparse
import fcntl
import grp
import logging
import os
import os.path
import psutil
import pwd
import shlex
import subprocess
import threading
import re
import struct

from typing import Dict, List  # noqa

from zuul.driver import (Driver, WrapperInterface)
from zuul.execution_context import BaseExecutionContext

# Increase the OOM badness score by about 20% of the available RAM.
# This should be sufficient to make child processes the target of the
# oom killer.
OOM_SCORE_ADJ = 200


class CgroupFile:
    def __init__(self, path):
        self.path = path
        self.fd = None

    def __enter__(self):
        self.fd = os.open(self.path, os.O_WRONLY)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.close(self.fd)
        self.fd = None

    def write(self, s):
        os.write(self.fd, s.encode('ascii'))


class CgroupManager:
    log = logging.getLogger("zuul.CgroupManager")

    def __init__(self, user, memory_limit):
        # This is called before logging is configured, so we print to
        # stdout.
        self.enabled = bool(memory_limit)
        self.user = user
        self.memory_limit = memory_limit
        if self.enabled:
            self.setup()
        else:
            print("cgroup management disabled")

    def setup(self):
        print("Setting up cgroups")
        if self.user:
            pw = pwd.getpwnam(self.user)
        else:
            pw = None
        with open('/proc/self/cgroup') as f:
            cgroup = f.read().strip()
        if cgroup[:3] != "0::":
            raise Exception("cgroup v2 required")
        orig_group = f'/sys/fs/cgroup{cgroup[3:]}'
        parts = orig_group.split('/')
        if (len(parts) >= 2 and
            parts[-1] == 'zuul-main' and
            parts[-2] == 'zuul-executor'):
            # We appear to be re-running the initialization, perhaps
            # because we were restarted within our own context.
            orig_group = '/'.join(parts[:-2])
        parent_group = f'{orig_group}/zuul-executor'
        main_group = f'{parent_group}/zuul-main'
        build_group = f'{parent_group}/zuul-build'
        print(f"Using cgroup {parent_group}")
        # Hierarchy:
        # root/
        #  zuul-executor/
        #    zuul-main/   (zuul-executor daemon in this group)
        #    zuul-build/  (bwrap subprocs in this group)
        # In order to migrate from main to build, we need to be able
        # to write to the common parent's procs file; in order to have
        # the least impact on the root cgroup, we create our own
        # intermediate parent.
        if not os.path.exists(parent_group):
            os.mkdir(parent_group)
            os.chmod(parent_group, 0o755)
            if pw:
                os.chown(f'{parent_group}/cgroup.procs', pw.pw_uid, pw.pw_gid)
        if not os.path.exists(main_group):
            os.mkdir(main_group)
            os.chmod(main_group, 0o755)
        if not os.path.exists(build_group):
            os.mkdir(build_group)
            os.chmod(build_group, 0o755)
            if pw:
                os.chown(f'{build_group}/cgroup.procs', pw.pw_uid, pw.pw_gid)
        # Migrate any proceses in the original group to our new main
        # group
        with open(f'{orig_group}/cgroup.procs') as orig:
            pids = [l.strip() for l in orig]
        with CgroupFile(f'{main_group}/cgroup.procs') as main:
            for pid in pids:
                main.write(pid + '\n')
        # Now we can set the controllers
        with CgroupFile(f'{orig_group}/cgroup.subtree_control') as orig:
            orig.write('+memory\n')
        with CgroupFile(f'{parent_group}/cgroup.subtree_control') as parent:
            parent.write('+memory\n')
        with CgroupFile(f'{build_group}/memory.max') as mem:
            mem.write(f'{self.memory_limit}\n')
        self.build_group = build_group

    def moveToBuildGroup(self, pid):
        if not self.enabled:
            return
        self.log.debug("Moving PID %s to cgroup %s", pid, self.build_group)
        with CgroupFile(f'{self.build_group}/cgroup.procs') as build:
            build.write(str(pid) + '\n')


class WrappedPopen(object):
    def __init__(self, command, fds, cgroup_manager):
        self.command = command
        self.fds = fds
        self.cgroup_manager = cgroup_manager

    def __call__(self, args, *sub_args, **kwargs):
        try:
            args = self.command + args
            pass_fds = list(kwargs.get('pass_fds', []))
            for fd in self.fds:
                if fd not in pass_fds:
                    pass_fds.append(fd)
            kwargs['pass_fds'] = pass_fds
            proc = psutil.Popen(args, *sub_args, **kwargs,
                                preexec_fn=self._bwrap_pre_exec)
        finally:
            self.__del__()
        return proc

    def _bwrap_pre_exec(self):
        if self.cgroup_manager:
            # We are the child now
            pid = os.getpid()
            # Move process into build cgroup
            self.cgroup_manager.moveToBuildGroup(pid)

    def __del__(self):
        for fd in self.fds:
            try:
                os.close(fd)
            except OSError:
                pass
        self.fds = []


class BubblewrapExecutionContext(BaseExecutionContext):
    log = logging.getLogger("zuul.BubblewrapExecutionContext")

    def __init__(self, bwrap_command, ro_paths, rw_paths, secrets,
                 cgroup_manager):
        self.bwrap_command = bwrap_command
        self.mounts_map = {'ro': ro_paths, 'rw': rw_paths}
        self.secrets = secrets
        self.cgroup_manager = cgroup_manager

    def getNamespacePids(self, proc):
        # Given a Popen object (proc), return the namespace and a list
        # of host-side process ids in proc's namespace.
        ps = subprocess.Popen(
            ['ps', '-axo', 'pidns,pid,ppid'],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        pid_to_child_list = {}
        ns_to_pid_list = {}
        pid_to_ns_map = {}
        for line in ps.stdout:
            try:
                (pidns, pid, ppid) = map(int, line.rstrip().split())
            except ValueError:
                continue
            pid_to_child_list.setdefault(ppid, []).append(pid)
            ns_to_pid_list.setdefault(pidns, []).append(pid)
            pid_to_ns_map[pid] = pidns
        for child in pid_to_child_list.get(proc.pid, []):
            ns = pid_to_ns_map.get(child)
            if ns is not None:
                return ns, ns_to_pid_list.get(ns)
        return None, []

    def startPipeWriter(self, pipe, data):
        # In case we have a large amount of data to write through a
        # pipe, spawn a thread to handle the writes.
        t = threading.Thread(target=self._writer, args=(pipe, data))
        t.daemon = True
        t.start()

    def _writer(self, pipe, data):
        os.write(pipe, data)
        os.close(pipe)

    def setpag(self):
        # If we are on a system with AFS, ensure that each playbook
        # invocation ends up in its own PAG.
        # http://asa.scripts.mit.edu/trac/attachment/ticket/145/setpag.txt#L315
        if os.path.exists("/proc/fs/openafs/afs_ioctl"):
            f = os.open("/proc/fs/openafs/afs_ioctl", os.O_RDONLY)
            # 0x40084301 is the result of _IOW('C', 1, void *) which originates
            # from OpenAFS' VIOC_SYSCALL defined at:
            # https://github.com/openafs/openafs/blob/
            # 630d423897e5fffed1873aa9d12c4e74a8481041/
            # src/config/afs_args.h#L258
            # The 21 at the end of the struct is AFSCALL_SETPAG which
            # is defined at:
            # https://github.com/openafs/openafs/blob/
            # 630d423897e5fffed1873aa9d12c4e74a8481041/
            # src/config/afs_args.h#L97
            fcntl.ioctl(f, 0x40084301, struct.pack("lllll", 0, 0, 0, 0, 21))
            os.close(f)

    def getPopen(self, **kwargs):
        self.setpag()
        bwrap_command = list(self.bwrap_command)
        for mount_type in ('ro', 'rw'):
            bind_arg = '--ro-bind' if mount_type == 'ro' else '--bind'
            for bind in self.mounts_map[mount_type]:
                bwrap_command.extend([bind_arg, bind, bind])

        # A list of file descriptors which must be held open so that
        # bwrap may read from them.
        read_fds = []
        # Need users and groups
        uid = os.getuid()
        passwd = list(pwd.getpwuid(uid))
        # Replace our user's actual home directory with the build home dir.
        passwd = passwd[:5] + [kwargs['home_dir']] + passwd[6:]
        passwd_bytes = b':'.join(
            ['{}'.format(x).encode('utf8') for x in passwd])
        (passwd_r, passwd_w) = os.pipe()
        os.write(passwd_w, passwd_bytes)
        os.write(passwd_w, b'\n')
        os.close(passwd_w)
        read_fds.append(passwd_r)

        gid = os.getgid()
        group = grp.getgrgid(gid)
        group_bytes = b':'.join(
            ['{}'.format(x).encode('utf8') for x in group])
        group_r, group_w = os.pipe()
        os.write(group_w, group_bytes)
        os.write(group_w, b'\n')
        os.close(group_w)
        read_fds.append(group_r)

        # Create a tmpfs for each directory which holds secrets, and
        # tell bubblewrap to write the contents to a file therein.
        secret_dirs = set()
        for fn, content in self.secrets.items():
            secret_dir = os.path.dirname(fn)
            if secret_dir not in secret_dirs:
                bwrap_command.extend(['--tmpfs', secret_dir])
                secret_dirs.add(secret_dir)
            secret_r, secret_w = os.pipe()
            self.startPipeWriter(secret_w, content.encode('utf8'))
            bwrap_command.extend(['--file', str(secret_r), fn])
            read_fds.append(secret_r)

        kwargs = dict(kwargs)  # Don't update passed in dict
        kwargs['uid'] = uid
        kwargs['gid'] = gid
        kwargs['uid_fd'] = passwd_r
        kwargs['gid_fd'] = group_r
        if kwargs.get('share_net', True):
            bwrap_command.append('--share-net')
        if ssh_sock := kwargs.get('ssh_auth_sock', None):
            bwrap_command.extend([
                '--ro-bind', ssh_sock, ssh_sock,
            ])

        command = [x.format(**kwargs) for x in bwrap_command]

        self.log.debug("Bubblewrap command: %s",
                       " ".join(shlex.quote(c) for c in command))

        wrapped_popen = WrappedPopen(command, read_fds, self.cgroup_manager)

        return wrapped_popen


class BubblewrapDriver(Driver, WrapperInterface):
    log = logging.getLogger("zuul.BubblewrapDriver")
    name = 'bubblewrap'

    release_file_re = re.compile(r'^\W+-release$')
    bwrap_version_re = re.compile(r'^(\d+\.\d+\.\d+).*')

    def __init__(self):
        self.cgroup_manager = None
        pid = os.getpid()
        with open(f"/proc/{pid}/oom_score_adj") as f:
            starting_score_adj = int(f.read().strip())

        self.oom_score_adj = min(1000, (starting_score_adj) + OOM_SCORE_ADJ)
        self.log.debug("Initializing bubblewrap with oom_score_adj "
                       "starting: %s, final: %s",
                       starting_score_adj, self.oom_score_adj)
        self.userns_enabled = self._is_userns_enabled()
        self.bwrap_version = self._parse_bwrap_version()
        self.bwrap_command = self._bwrap_command()

    def check(self):
        # Validate basic bwrap functionality before we attempt to run
        # workloads under bwrap.
        context = self.getExecutionContext()
        popen = context.getPopen(work_dir='/tmp',
                                 home_dir='/tmp')
        p = popen(['id'],
                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        p.communicate()
        if p.returncode != 0:
            self.log.error("Non zero return code executing: %s",
                           " ".join(shlex.quote(c)
                                    for c in popen.command + ['id']))
            raise Exception('bwrap execution validation failed. You can '
                            'use `zuul-bwrap /tmp /tmp id` to investigate '
                            'manually.')

    def setCgroupManager(self, cgroup_manager):
        self.cgroup_manager = cgroup_manager

    def _is_userns_enabled(self):
        # This is based on the bwrap checks found here:
        # https://github.com/containers/bubblewrap/blob/
        # ad76c2d6ba8091a7afa95568e46af2261b362439/bubblewrap.c#L2735
        return_val = False
        if os.path.exists('/proc/self/ns/user'):
            # Rhel 7 specific case
            if os.path.exists('/sys/module/user_namespace/parameters/enable'):
                with open('/sys/module/user_namespace/parameters/enable') as f:
                    s = f.read()
                    if not s or s[0] != 'Y':
                        return return_val
            if os.path.exists('/proc/sys/user/max_user_namespaces'):
                with open('/proc/sys/user/max_user_namespaces') as f:
                    s = f.read()
                    try:
                        i = int(s.strip())
                        if i < 1:
                            return return_val
                    except ValueError:
                        # If we can't determine the max namespace count but
                        # namespaces are generally enabled then we should
                        # treat them as enabled.
                        return_val = True
            return_val = True
        return return_val

    def _parse_bwrap_version(self):
        p = subprocess.run(['bwrap', '--version'], text=True,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # We don't know for sure what version schema bwrap may end up using.
        # Match what they have done historically and be a bit forgiving of
        # alpha, beta, rc, etc annotations.
        r = self.bwrap_version_re.match(p.stdout.split()[-1])
        if p.returncode == 0 and r:
            return tuple(map(int, r.group(1).split('.')))
        else:
            if p.returncode == 0:
                self.log.warning('Unable to determine bwrap version, from '
                                 '"%s". Using 0.0.0' % p.stdout.strip())
            else:
                self.log.warning('Unable to determine bwrap version, got '
                                 'returncode "%s". Using 0.0.0' % p.returncode)
            return (0, 0, 0)

    def reconfigure(self, tenant):
        pass

    def stop(self):
        pass

    def _bwrap_command(self):
        if self.bwrap_version >= (0, 8, 0) and self.userns_enabled:
            userns_flags = ['--unshare-user', '--disable-userns']
        else:
            userns_flags = []
        bwrap_command = [
            'setpriv',
            '--ambient-caps',
            '-all',
            'choom',
            '-n', str(self.oom_score_adj),
            '--',
            'bwrap',
            '--tmpfs', '/tmp',
            '--dir', '/var',
            '--dir', '/var/tmp',
            '--dir', '/run/user/{uid}',
            '--ro-bind', '/usr', '/usr',
            '--ro-bind', '/lib', '/lib',
            '--ro-bind', '/bin', '/bin',
            '--ro-bind', '/sbin', '/sbin',
            '--ro-bind', '/etc/ld.so.cache', '/etc/ld.so.cache',
            '--ro-bind', '/etc/resolv.conf', '/etc/resolv.conf',
            '--ro-bind', '/etc/hosts', '/etc/hosts',
            '--ro-bind', '/etc/localtime', '/etc/localtime',
            '--bind', '{work_dir}', '{work_dir}',
            '--tmpfs', '{work_dir}/tmp',
            '--bind', '{home_dir}', '{home_dir}',
            '--proc', '/proc',
            '--dev', '/dev',
            '--chdir', '{work_dir}',
            '--unshare-all',
            '--die-with-parent',
            '--uid', '{uid}',
            '--gid', '{gid}',
            '--file', '{uid_fd}', '/etc/passwd',
            '--file', '{gid_fd}', '/etc/group',
        ] + userns_flags

        for path in ['/lib64',
                     '/etc/nsswitch.conf',
                     '/etc/lsb-release.d',
                     '/etc/alternatives',
                     '/etc/ssl/certs',
                     '/etc/subuid',
                     '/etc/containers',
                     ]:
            if os.path.exists(path):
                bwrap_command.extend(['--ro-bind', path, path])
        for fn in os.listdir('/etc'):
            if self.release_file_re.match(fn):
                path = os.path.join('/etc', fn)
                bwrap_command.extend(['--ro-bind', path, path])

        return bwrap_command

    def getExecutionContext(self, ro_paths=None, rw_paths=None, secrets=None):
        if not ro_paths:
            ro_paths = []
        if not rw_paths:
            rw_paths = []
        if not secrets:
            secrets = {}
        return BubblewrapExecutionContext(
            self.bwrap_command,
            ro_paths, rw_paths,
            secrets, self.cgroup_manager)


def main(args=None):
    logging.basicConfig(level=logging.DEBUG)

    driver = BubblewrapDriver()
    driver.check()

    parser = argparse.ArgumentParser()
    parser.add_argument('--ro-paths', nargs='+')
    parser.add_argument('--rw-paths', nargs='+')
    parser.add_argument('--secret', nargs='+')
    parser.add_argument('work_dir')
    parser.add_argument('home_dir')
    parser.add_argument('run_args', nargs='+')
    cli_args = parser.parse_args()

    # The zuul-bwrap command is often run for debugging purposes. An SSH
    # agent may not be necessary or present in that situation.
    ssh_auth_sock = os.environ.get('SSH_AUTH_SOCK', None)

    secrets = {}
    if cli_args.secret:
        for secret in cli_args.secret:
            fn, content = secret.split('=', 1)
            secrets[fn] = content

    context = driver.getExecutionContext(
        cli_args.ro_paths, cli_args.rw_paths,
        secrets)

    popen = context.getPopen(work_dir=cli_args.work_dir,
                             home_dir=cli_args.home_dir,
                             ssh_auth_sock=ssh_auth_sock)
    x = popen(cli_args.run_args)
    x.wait()


if __name__ == '__main__':
    main()
