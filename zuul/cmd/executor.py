# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2013-2014 OpenStack Foundation
# Copyright 2021-2022 Acme Gating, LLC
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

import logging
import os
import pwd
import sys
import signal

import zuul.cmd
import zuul.executor.server
from zuul.driver.bubblewrap import CgroupManager
from zuul.lib.config import get_default


class Executor(zuul.cmd.ZuulDaemonApp):
    app_name = 'executor'
    app_description = 'A standalone Zuul executor.'

    def createParser(self):
        parser = super(Executor, self).createParser()
        parser.add_argument('--keep-jobdir', dest='keep_jobdir',
                            action='store_true',
                            help='keep local jobdirs after run completes')
        self.addSubCommands(parser, zuul.executor.server.COMMANDS)
        return parser

    def parseArguments(self, args=None):
        super(Executor, self).parseArguments()

    def exit_handler(self, signum, frame):
        if self.config.has_option('executor', 'sigterm_method'):
            graceful = self.config.get('executor', 'sigterm_method')
        else:
            graceful = 'graceful'
        if graceful.lower() == 'graceful':
            self.executor.graceful()
        elif graceful.lower() == 'stop':
            self.executor.stop()
        else:
            self.log.error("Unknown value for executor.sigterm_method:"
                           f"'{graceful}'. Expected 'graceful' or 'stop'")
            self.executor.graceful()

    def start_log_streamer(self):
        pipe_read, pipe_write = os.pipe()
        child_pid = os.fork()
        if child_pid == 0:
            os.close(pipe_write)
            import zuul.lib.log_streamer

            self.log.info("Starting log streamer")
            streamer = zuul.lib.log_streamer.LogStreamer(
                '::', self.finger_port, self.job_dir)

            # Keep running until the parent dies:
            pipe_read = os.fdopen(pipe_read)
            try:
                pipe_read.read()
            except KeyboardInterrupt:
                pass
            self.log.info("Stopping log streamer")
            streamer.stop()
            os._exit(0)
        else:
            os.close(pipe_read)
            self.log_streamer_pipe = pipe_write
            self.log_streamer_pid = child_pid

    def dropPrivs(self, user):
        if os.getuid() != 0:
            return

        self.log.info("Changing to user %s", user)
        pw = pwd.getpwnam(user)

        # Change owner on our pid file so it can be removed by us after
        # dropping privileges. May not exist if not a daemon.
        pid_file = self.getPidFile()
        if pid_file and os.path.exists(pid_file):
            os.chown(pid_file, pw.pw_uid, pw.pw_gid)

        os.setgroups([])
        os.setgid(pw.pw_gid)
        os.setuid(pw.pw_uid)
        os.umask(0o022)

    def run(self):
        self.handleCommands()

        # Cgroup configuration requires root; do it here and then drop
        # privileges, before we initialize anything else.
        user = get_default(self.config, 'executor', 'user')
        memory_limit = get_default(
            self.config, 'executor', 'cgroup_memory_limit')
        cgroup_manager = CgroupManager(user, memory_limit)
        if user:
            self.dropPrivs(user)

        self.setup_logging('executor', 'log_config')
        self.log = logging.getLogger("zuul.Executor")

        self.configure_connections(sources=True)

        # Check bwrap after dropping privileges
        self.connections.drivers['bubblewrap'].setCgroupManager(
            cgroup_manager)
        self.connections.drivers['bubblewrap'].check()

        if self.config.has_option('executor', 'job_dir'):
            self.job_dir = os.path.expanduser(
                self.config.get('executor', 'job_dir'))
            if not os.path.isdir(self.job_dir):
                print("Invalid job_dir: {job_dir}".format(
                    job_dir=self.job_dir))
                sys.exit(1)
        else:
            self.job_dir = '/var/lib/zuul/builds'
            if not os.path.exists(self.job_dir):
                os.mkdir(self.job_dir)

        self.finger_port = int(
            get_default(self.config, 'executor', 'finger_port',
                        zuul.executor.server.DEFAULT_FINGER_PORT)
        )

        self.start_log_streamer()

        ExecutorServer = zuul.executor.server.ExecutorServer
        self.executor = ExecutorServer(self.config,
                                       self.connections,
                                       jobdir_root=self.job_dir,
                                       keep_jobdir=self.args.keep_jobdir,
                                       log_streaming_port=self.finger_port)
        self.executor.start()

        if self.args.nodaemon:
            signal.signal(signal.SIGTERM, self.exit_handler)

        while True:
            try:
                self.executor.join()
                break
            except KeyboardInterrupt:
                print("Ctrl + C: asking executor to exit nicely...\n")
                self.exit_handler(signal.SIGINT, None)

        os.close(self.log_streamer_pipe)
        os.waitpid(self.log_streamer_pid, 0)


def main():
    Executor().main()
