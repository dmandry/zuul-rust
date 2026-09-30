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
import sys

import zuul.cmd
import zuul.model
import zuul.web
import zuul.driver.sql
import zuul.driver.github
import zuul.lib.auth


class WebServer(zuul.cmd.ZuulDaemonApp):
    app_name = 'web'
    app_description = 'A standalone Zuul web server.'

    def createParser(self):
        parser = super().createParser()
        self.addSubCommands(parser, zuul.web.COMMANDS)
        return parser

    def exit_handler(self, signum, frame):
        if signum == signal.SIGUSR1:
            str_sig = 'SIGUSR1'
        elif signum == signal.SIGTERM:
            str_sig = 'SIGTERM'
        else:
            str_sig = 'Signal: %s' % str(signum)
        self.log.debug('%s received running stop handler', str_sig)
        self.web.stop()

    def _run(self):
        info = zuul.model.WebInfo.fromConfig(self.config)

        # Validate config here before we spin up the ZuulWeb object
        for conn_name, connection in self.connections.connections.items():
            try:
                connection.validateWebConfig(self.config, self.connections)
            except Exception:
                self.log.exception("Error validating config")
                sys.exit(1)

        try:
            self.web = zuul.web.ZuulWeb(
                config=self.config,
                info=info,
                connections=self.connections,
                authenticators=self.authenticators,
            )
            self.connections.load(self.web.zk_client,
                                  self.web.component_registry)
        except Exception:
            self.log.exception("Error creating ZuulWeb:")
            sys.exit(1)

        signal.signal(signal.SIGUSR1, self.exit_handler)
        signal.signal(signal.SIGTERM, self.exit_handler)

        self.log.info('Zuul Web Server starting')
        self.web.start()

        try:
            self.web.join()
        except KeyboardInterrupt:
            print("Ctrl + C: asking process to exit nicely...\n")
            self.exit_handler(signal.SIGINT, None)
            self.web.join()

        self.log.info("Zuul Web Server stopped")

    def configure_authenticators(self):
        self.authenticators = zuul.lib.auth.AuthenticatorRegistry()
        self.authenticators.configure(self.config)

    def run(self):
        self.handleCommands()

        self.setup_logging('web', 'log_config')
        self.log = logging.getLogger("zuul.WebServer")

        try:
            self.configure_connections(database=True, sources=True,
                                       triggers=True, reporters=True,
                                       providers=True)
            self.configure_authenticators()
            self._run()
        except Exception:
            self.log.exception("Exception from WebServer:")


def main():
    WebServer().main()
