# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2013 OpenStack Foundation
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

import abc
import argparse
import configparser
import daemon
import extras
import io
import json
import logging
import logging.config
import os
import signal
import socket
import sys
import traceback
import threading

yappi = extras.try_import('yappi')
objgraph = extras.try_import('objgraph')

# as of python-daemon 1.6 it doesn't bundle pidlockfile anymore
# instead it depends on lockfile-0.9.1 which uses pidfile.
pid_file_module = extras.try_imports(['daemon.pidlockfile', 'daemon.pidfile'])

from zuul.ansible import logconfig
import zuul.lib.connections
from zuul.lib.config import get_default


def stack_dump_handler(signum, frame):
    signal.signal(signal.SIGUSR2, signal.SIG_IGN)
    log = logging.getLogger("zuul.stack_dump")
    log.debug("Beginning debug handler")
    try:
        if yappi:
            if yappi.is_running():
                log.debug("Stopping Yappi")
                yappi.stop()
                yappi_out = io.StringIO()
                yappi.get_func_stats().print_all(out=yappi_out)
                yappi.get_thread_stats().print_all(out=yappi_out)
                log.debug(yappi_out.getvalue())
                yappi_out.close()
                yappi.clear_stats()
    except Exception:
        log.exception("Yappi error:")
    try:
        threads = {}
        for t in threading.enumerate():
            threads[t.ident] = t
        log_str = ""
        for thread_id, stack_frame in sys._current_frames().items():
            thread = threads.get(thread_id)
            if thread:
                thread_name = thread.name
                thread_is_daemon = str(thread.daemon)
            else:
                thread_name = '(Unknown)'
                thread_is_daemon = '(Unknown)'
            log_str += "Thread: %s %s d: %s\n"\
                       % (thread_id, thread_name, thread_is_daemon)
            log_str += "".join(traceback.format_stack(stack_frame))
        log.debug(log_str)
    except Exception:
        log.exception("Thread dump error:")
    try:
        if objgraph:
            log.debug("Most common types:")
            objgraph_out = io.StringIO()
            objgraph.show_growth(limit=100, file=objgraph_out)
            log.debug(objgraph_out.getvalue())
            objgraph_out.close()
    except Exception:
        log.exception("Objgraph error:")
    try:
        if yappi:
            if not yappi.is_running():
                log.debug("Starting Yappi")
                yappi.start()
    except Exception:
        log.exception("Yappi error:")
    log.debug("End debug handler")
    signal.signal(signal.SIGUSR2, stack_dump_handler)


class ZuulApp(object):
    app_name = None  # type: str
    app_description = None  # type: str

    def __init__(self):
        self.args = None
        self.config = None
        self.connections = {}
        self.commands = {}

    def _get_version(self):
        from zuul.version import release_string
        return "Zuul version: %s" % release_string

    def createParser(self):
        parser = argparse.ArgumentParser(
            description=self.app_description,
            formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('-c', dest='config',
                            help='specify the config file')
        parser.add_argument('--version', dest='version', action='version',
                            version=self._get_version(),
                            help='show zuul version')
        return parser

    def addSubCommands(self, parser, commands):
        # Add a list of commandsocket.Command items to the parser
        subparsers = parser.add_subparsers(
            title='Online commands',
            description=('The following commands may be used to affect '
                         'the running process.'),
        )
        for command in commands:
            self.commands[command.name] = command
            cmd = subparsers.add_parser(
                command.name, help=command.help)
            cmd.set_defaults(command=command.name)
            for arg in command.args:
                cmd.add_argument(arg.name,
                                 help=arg.help,
                                 default=arg.default)

    def handleCommands(self):
        command_name = getattr(self.args, 'command', None)
        if command_name in self.commands:
            command = self.commands[self.args.command]
            command_str = command.name
            command_args = [getattr(self.args, arg.name)
                            for arg in command.args]
            if command_args:
                command_str += ' ' + json.dumps(command_args)
            self.sendCommand(command_str)
            sys.exit(0)

    def parseArguments(self, args=None):
        parser = self.createParser()
        self.args = parser.parse_args(args)

        if getattr(self.args, 'foreground', False):
            self.args.nodaemon = True
        else:
            self.args.nodaemon = False

        if getattr(self.args, 'command', None):
            self.args.nodaemon = True

        return parser

    def envBool(self, name, default=None):
        """Get the named variable from the environment and
        convert to boolean"""
        val = os.getenv(name)
        if val is None:
            return default
        if val.lower() in ['false', '0']:
            return False
        return True

    def readConfig(self):
        safe_env = {
            k: v for k, v in os.environ.items()
            if k.startswith('ZUUL_')
        }
        self.config = configparser.ConfigParser(safe_env)
        if self.args.config:
            locations = [self.args.config]
        else:
            locations = ['/etc/zuul/zuul.conf',
                         '~/zuul.conf']
        for fp in locations:
            if os.path.exists(os.path.expanduser(fp)):
                self.config.read(os.path.expanduser(fp))
                return
        raise Exception("Unable to locate config file in %s" % locations)

    def setup_logging(self, section, parameter):
        if self.config.has_option(section, parameter):
            fp = os.path.expanduser(self.config.get(section, parameter))
            logging_config = logconfig.load_config(fp)
        else:
            # If someone runs in the foreground and doesn't give a logging
            # config, leave the config set to emit to stdout.
            if hasattr(self.args, 'nodaemon') and self.args.nodaemon:
                logging_config = logconfig.ServerLoggingConfig()
            else:
                # Setting a server value updates the defaults to use
                # WatchedFileHandler on /var/log/zuul/{server}-debug.log
                # and /var/log/zuul/{server}.log
                logging_config = logconfig.ServerLoggingConfig(server=section)
            if hasattr(self.args, 'debug') and self.args.debug:
                logging_config.setDebug()
        logging_config.apply()

    def configure_connections(self, database=False, sources=False,
                              triggers=False, reporters=False,
                              providers=False):
        self.connections = zuul.lib.connections.ConnectionRegistry()
        self.connections.configure(self.config, database=database,
                                   sources=sources, triggers=triggers,
                                   reporters=reporters, providers=providers)


class ZuulDaemonApp(ZuulApp, metaclass=abc.ABCMeta):
    def createParser(self):
        parser = super(ZuulDaemonApp, self).createParser()
        parser.add_argument('-d', dest='debug', action='store_true',
                            help='enable debug log')
        parser.add_argument('-f', dest='foreground', action='store_true',
                            help='run in foreground')
        return parser

    def getPidFile(self):
        pid_fn = get_default(self.config, self.app_name, 'pidfile',
                             '/var/run/zuul/%s.pid' % self.app_name,
                             expand_user=True)
        return pid_fn

    @abc.abstractmethod
    def run(self):
        """
        This is the main run method of the application.
        """
        pass

    def setup_logging(self, section, parameter):
        super(ZuulDaemonApp, self).setup_logging(section, parameter)
        from zuul.version import release_string
        log = logging.getLogger(
            "zuul.{section}".format(section=section.title()))
        log.debug(
            "Configured logging: {version}".format(
                version=release_string))

    def main(self):
        self.parseArguments()
        self.readConfig()

        pid_fn = self.getPidFile()
        pid = pid_file_module.TimeoutPIDLockFile(pid_fn, 10)

        # Early register the stack dump handler for all zuul apps. This makes
        # it possible to also gather stack dumps during startup hangs.
        signal.signal(signal.SIGUSR2, stack_dump_handler)

        if self.args.nodaemon:
            self.run()
        else:
            # Exercise the pidfile before we do anything else (including
            # logging or daemonizing)
            with pid:
                pass

            with daemon.DaemonContext(pidfile=pid, umask=0o022):
                self.run()

    def sendCommand(self, cmd):
        command_socket = get_default(
            self.config, self.app_name, 'command_socket',
            '/var/lib/zuul/%s.socket' % self.app_name)
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(command_socket)
        cmd = '%s\n' % cmd
        s.sendall(cmd.encode('utf8'))
