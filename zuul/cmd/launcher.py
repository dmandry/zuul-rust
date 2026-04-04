# Copyright 2024 BMW Group
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
import signal

import zuul.cmd
from zuul.launcher.server import COMMANDS, Launcher


class LauncherApp(zuul.cmd.ZuulDaemonApp):
    app_name = 'launcher'
    app_description = 'The Zuul launcher.'
    launcher = None

    def __init__(self):
        super().__init__()

    def createParser(self):
        parser = super().createParser()
        self.addSubCommands(parser, COMMANDS)
        return parser

    def exit_handler(self, signum, frame):
        self.log.debug('SIGTERM received running stop handler')
        if self.launcher:
            self.launcher.stop()
            self.launcher.join()

    def run(self):
        self.handleCommands()

        self.setup_logging('launcher', 'log_config')
        self.log = logging.getLogger('zuul.launcher')

        self.configure_connections(providers=True)
        self.launcher = Launcher(self.config, self.connections)
        self.launcher.start()

        if self.args.nodaemon:
            signal.signal(signal.SIGTERM, self.exit_handler)

        while True:
            try:
                self.launcher.join()
                break
            except KeyboardInterrupt:
                print("Ctrl + C: asking launcher to exit nicely...\n")
                self.exit_handler(signal.SIGINT, None)


def main():
    LauncherApp().main()
