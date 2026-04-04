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

import logging
import sys
import signal

import zuul.cmd
import zuul.scheduler


class Scheduler(zuul.cmd.ZuulDaemonApp):
    app_name = 'scheduler'
    app_description = 'The main zuul process.'

    def __init__(self):
        super(Scheduler, self).__init__()

    def createParser(self):
        parser = super(Scheduler, self).createParser()
        parser.add_argument('--validate-tenants', dest='validate_tenants',
                            metavar='TENANT', nargs='*',
                            help='Load configuration of the listed tenants and'
                                 ' exit afterwards, indicating success or '
                                 'failure via the exit code. If no tenant is '
                                 'listed, all tenants will be validated. '
                                 'Note: this requires ZooKeeper and '
                                 'will distribute work to mergers.')
        parser.add_argument('--wait-for-init', dest='wait_for_init',
                            action='store_true',
                            default=self.envBool('ZUUL_WAIT_FOR_INIT'),
                            help='Wait until all tenants are fully loaded '
                                 'before beginning to process events. '
                            '(also available as ZUUL_WAIT_FOR_INIT).')
        parser.add_argument('--disable-pipelines', dest='disable_pipelines',
                            action='store_true',
                            default=self.envBool('ZUUL_DISABLE_PIPELINES'),
                            help='Discard all pipeline related events '
                            '(also available as ZUUL_DISABLE_PIPELINES).')
        self.addSubCommands(parser, zuul.scheduler.COMMANDS)
        return parser

    def fullReconfigure(self):
        self.log.debug("Reconfiguration triggered")
        self.readConfig()
        self.setup_logging('scheduler', 'log_config')
        try:
            self.sched.reconfigure(self.config)
        except Exception:
            self.log.exception("Reconfiguration failed:")

    def smartReconfigure(self):
        self.log.debug("Smart reconfiguration triggered")
        self.readConfig()
        self.setup_logging('scheduler', 'log_config')
        try:
            self.sched.reconfigure(self.config, smart=True)
        except Exception:
            self.log.exception("Reconfiguration failed:")

    def tenantReconfigure(self, tenants):
        self.log.debug("Tenant reconfiguration triggered")
        self.readConfig()
        self.setup_logging('scheduler', 'log_config')
        try:
            self.sched.reconfigure(self.config, smart=False, tenants=tenants)
        except Exception:
            self.log.exception("Reconfiguration failed:")

    def exit_handler(self, signum, frame):
        self.log.debug('SIGTERM received running stop handler')
        self.sched.stop()
        self.sched.join()

    def run(self):
        self.handleCommands()

        self.setup_logging('scheduler', 'log_config')
        self.log = logging.getLogger("zuul.Scheduler")

        self.configure_connections(database=True, sources=True, triggers=True,
                                   reporters=True, providers=True)
        self.sched = zuul.scheduler.Scheduler(self.config,
                                              self.connections, self,
                                              self.args.wait_for_init,
                                              self.args.disable_pipelines)
        if self.args.validate_tenants is None:
            self.connections.registerScheduler(self.sched)
            self.connections.load(self.sched.zk_client,
                                  self.sched.component_registry)

        self.log.info('Starting scheduler')
        try:
            self.sched.start()
            if self.args.validate_tenants is not None:
                self.sched.validateTenants(
                    self.config, self.args.validate_tenants)
            else:
                self.sched.prime(self.config)
        except Exception:
            self.log.exception("Error starting Zuul:")
            # TODO(jeblair): If we had all threads marked as daemon,
            # we might be able to have a nicer way of exiting here.
            self.sched.stop()
            sys.exit(1)

        if self.args.validate_tenants is not None:
            self.sched.stop()
            self.sched.stopConnections()
            sys.exit(0)

        if self.args.nodaemon:
            signal.signal(signal.SIGTERM, self.exit_handler)

        while True:
            try:
                self.sched.join()
                break
            except KeyboardInterrupt:
                print("Ctrl + C: asking scheduler to exit nicely...\n")
                self.exit_handler(signal.SIGINT, None)


def main():
    Scheduler().main()
