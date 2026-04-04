#!/usr/bin/env python
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

import signal

import zuul.cmd
import zuul.merger.server


class Merger(zuul.cmd.ZuulDaemonApp):
    app_name = 'merger'
    app_description = 'A standalone Zuul merger.'

    def createParser(self):
        parser = super(Merger, self).createParser()
        self.addSubCommands(parser, zuul.merger.server.COMMANDS)
        return parser

    def exit_handler(self, signum, frame):
        self.log.debug('SIGTERM received running stop handler')
        self.merger.stop()
        self.merger.join()

    def run(self):
        self.handleCommands()

        self.configure_connections(sources=True)

        self.setup_logging('merger', 'log_config')

        self.merger = zuul.merger.server.MergeServer(
            self.config, self.connections)
        self.merger.start()

        if self.args.nodaemon:
            signal.signal(signal.SIGTERM, self.exit_handler)

        while True:
            try:
                self.merger.join()
                break
            except KeyboardInterrupt:
                print("Ctrl + C: asking merger to exit nicely...\n")
                self.exit_handler(signal.SIGINT, None)


def main():
    Merger().main()
