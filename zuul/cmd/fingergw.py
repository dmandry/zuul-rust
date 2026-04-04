# Copyright 2017 Red Hat, Inc.
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
import signal

import zuul.cmd
from zuul.lib.config import get_default
from zuul.lib import fingergw


class FingerGatewayApp(zuul.cmd.ZuulDaemonApp):
    '''
    Class for the daemon that will distribute any finger requests to the
    appropriate Zuul executor handling the specified build UUID.
    '''
    app_name = 'fingergw'
    app_description = 'The Zuul finger gateway.'

    def __init__(self):
        super(FingerGatewayApp, self).__init__()
        self.gateway = None

    def createParser(self):
        parser = super(FingerGatewayApp, self).createParser()
        self.addSubCommands(parser, fingergw.COMMANDS)
        return parser

    def exit_handler(self, signum, frame):
        self.log.debug('SIGTERM received running stop handler')
        if self.gateway:
            self.gateway.stop()

    def run(self):
        '''
        Main entry point for the FingerGatewayApp.

        Called by the main() method of the parent class.
        '''
        self.handleCommands()

        self.setup_logging('fingergw', 'log_config')
        self.log = logging.getLogger('zuul.fingergw')

        cmdsock = get_default(
            self.config, 'fingergw', 'command_socket',
            '/var/lib/zuul/%s.socket' % self.app_name)

        self.gateway = fingergw.FingerGateway(
            self.config,
            cmdsock,
            self.getPidFile(),
        )

        self.gateway.start()

        if self.args.nodaemon:
            signal.signal(signal.SIGTERM, self.exit_handler)

        while True:
            try:
                self.gateway.join()
                break
            except KeyboardInterrupt:
                print("Ctrl + C: asking gateway to exit nicely...\n")
                self.exit_handler(signal.SIGINT, None)


def main():
    FingerGatewayApp().main()
