# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2013 OpenStack Foundation
# Copyright 2022 Acme Gating, LLC
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
import babel.dates
import datetime
import dateutil.parser
import dateutil.tz
import hashlib
import json
import jwt
import logging
import prettytable
import os
import re
import sys
import time
import textwrap
import requests
import urllib.parse

import zuul.cmd
from zuul.lib.config import get_default
from zuul.model import (
    Abide,
    ImageBuildArtifact,
    ImageUpload,
    Pipeline,
    PipelineChangeList,
    PipelineState,
    SystemAttributes,
)
from zuul.zk import ZooKeeperClient
from zuul.lib.keystorage import KeyStorage
from zuul.manager.independent import IndependentPipelineManager
from zuul.zk.locks import tenant_read_lock, pipeline_lock
from zuul.zk.zkobject import ZKContext
from zuul.zk.components import COMPONENT_REGISTRY
from zuul.zk.image_registry import (
    ImageBuildRegistry,
    ImageUploadRegistry,
)

from kazoo.exceptions import NoNodeError, NodeExistsError


def parse_cutoff(now, before, older_than):
    if before and not older_than:
        cutoff = dateutil.parser.parse(before)
        if cutoff.tzinfo and cutoff.tzinfo != dateutil.tz.tzutc():
            raise RuntimeError("Timestamp must be specified as UTC")
        cutoff = cutoff.replace(tzinfo=dateutil.tz.tzutc())
        return cutoff
    elif older_than and not before:
        value = older_than[:-1]
        suffix = older_than[-1]
        if suffix == 'd':
            delta = datetime.timedelta(days=int(value))
        elif suffix == 'h':
            delta = datetime.timedelta(hours=int(value))
        else:
            raise RuntimeError("Unsupported relative time")
        return now - delta
    else:
        raise RuntimeError(
            "Either --before or --older-than must be supplied")


# todo This should probably live somewhere else
class ZuulRESTClient(object):
    """Basic client for Zuul's REST API"""
    def __init__(self, url, verify=False, auth_token=None):
        self.url = url
        if not self.url.endswith('/'):
            self.url += '/'
        self.auth_token = auth_token
        self.verify = verify
        self.base_url = urllib.parse.urljoin(self.url, 'api/')
        self.session = requests.Session()
        self.session.verify = self.verify
        self.session.headers = dict(
            Authorization='Bearer %s' % self.auth_token)

    def _check_status(self, req):
        try:
            req.raise_for_status()
        except Exception as e:
            if req.status_code == 401:
                print('Unauthorized - your token might be invalid or expired.')
            elif req.status_code == 403:
                print('Insufficient privileges to perform the action.')
            else:
                print('Unknown error: "%e"' % e)

    def autohold(self, tenant, project, job, change, ref,
                 reason, count, node_hold_expiration):
        if not self.auth_token:
            raise Exception('Auth Token required')
        args = {"reason": reason,
                "count": count,
                "job": job,
                "change": change,
                "ref": ref,
                "node_hold_expiration": node_hold_expiration}
        url = urllib.parse.urljoin(
            self.base_url,
            'tenant/%s/project/%s/autohold' % (tenant, project))
        req = self.session.post(url, json=args)
        self._check_status(req)
        return req.json()

    def autohold_list(self, tenant):
        if not tenant:
            raise Exception('"--tenant" argument required')
        url = urllib.parse.urljoin(
            self.base_url,
            'tenant/%s/autohold' % tenant)
        req = requests.get(url, verify=self.verify)
        self._check_status(req)
        return req.json()

    def enqueue(self, tenant, pipeline, project, trigger, change):
        if not self.auth_token:
            raise Exception('Auth Token required')
        args = {"trigger": trigger,
                "change": change,
                "pipeline": pipeline}
        url = urllib.parse.urljoin(
            self.base_url,
            'tenant/%s/project/%s/enqueue' % (tenant, project))
        req = self.session.post(url, json=args)
        self._check_status(req)
        return req.json()

    def enqueue_ref(self, tenant, pipeline, project,
                    trigger, ref, oldrev, newrev):
        if not self.auth_token:
            raise Exception('Auth Token required')
        args = {"trigger": trigger,
                "ref": ref,
                "oldrev": oldrev,
                "newrev": newrev,
                "pipeline": pipeline}
        url = urllib.parse.urljoin(
            self.base_url,
            'tenant/%s/project/%s/enqueue' % (tenant, project))
        req = self.session.post(url, json=args)
        self._check_status(req)
        return req.json()

    def dequeue(self, tenant, pipeline, project, change=None, ref=None):
        if not self.auth_token:
            raise Exception('Auth Token required')
        args = {"pipeline": pipeline}
        if change and not ref:
            args['change'] = change
        elif ref and not change:
            args['ref'] = ref
        else:
            raise Exception('need change OR ref')
        url = urllib.parse.urljoin(
            self.base_url,
            'tenant/%s/project/%s/dequeue' % (tenant, project))
        req = self.session.post(url, json=args)
        self._check_status(req)
        return req.json()

    def promote(self, tenant, pipeline, change_ids):
        if not self.auth_token:
            raise Exception('Auth Token required')
        args = {
            "pipeline": pipeline,
            "changes": change_ids,
        }
        url = urllib.parse.urljoin(
            self.base_url,
            'tenant/%s/promote' % tenant)
        req = self.session.post(url, json=args)
        self._check_status(req)
        return req.json()

    def get_running_jobs(self, *args, **kwargs):
        raise NotImplementedError(
            'This action is unsupported by the REST API')


class Client(zuul.cmd.ZuulApp):
    app_name = 'zuul'
    app_description = 'Zuul CLI client.'
    log = logging.getLogger("zuul.Client")

    def createParser(self):
        parser = super(Client, self).createParser()
        parser.add_argument('-v', dest='verbose', action='store_true',
                            help='verbose output')
        parser.add_argument('--auth-token', dest='auth_token',
                            required=False,
                            default=None,
                            help='Authentication Token, needed if using the'
                                 'REST API')
        parser.add_argument('--zuul-url', dest='zuul_url',
                            required=False,
                            default=None,
                            help='Zuul API URL, needed if using the '
                                 'REST API without a configuration file')
        parser.add_argument('--insecure', dest='insecure_ssl',
                            required=False,
                            action='store_false',
                            help='Do not verify SSL connection to Zuul, '
                                 'when using the REST API (Defaults to False)')

        subparsers = parser.add_subparsers(title='commands',
                                           description='valid commands',
                                           help='additional help')

        # Autohold
        cmd_autohold = subparsers.add_parser(
            'autohold', help='[DEPRECATED - use zuul-client] '
                             'hold nodes for failed job')
        cmd_autohold.add_argument('--tenant', help='tenant name',
                                  required=True)
        cmd_autohold.add_argument('--project', help='project name',
                                  required=True)
        cmd_autohold.add_argument('--job', help='job name',
                                  required=True)
        cmd_autohold.add_argument('--change',
                                  help='specific change to hold nodes for',
                                  required=False, default='')
        cmd_autohold.add_argument('--ref', help='git ref to hold nodes for',
                                  required=False, default='')
        cmd_autohold.add_argument('--reason', help='reason for the hold',
                                  required=True)
        cmd_autohold.add_argument('--count',
                                  help='number of job runs (default: 1)',
                                  required=False, type=int, default=1)
        cmd_autohold.add_argument(
            '--node-hold-expiration',
            help=('how long in seconds should the node set be in HOLD status '
                  '(default: scheduler\'s default_hold_expiration value)'),
            required=False, type=int)
        cmd_autohold.set_defaults(func=self.autohold)

        cmd_autohold_delete = subparsers.add_parser(
            'autohold-delete', help='[DEPRECATED - use zuul-client] '
                                    'delete autohold request')
        cmd_autohold_delete.set_defaults(func=self.autohold_delete)
        cmd_autohold_delete.add_argument('id', metavar='REQUEST_ID',
                                         help='the hold request ID')

        cmd_autohold_info = subparsers.add_parser(
            'autohold-info', help='[DEPRECATED - use zuul-client] '
                                  'retrieve autohold request detailed info')
        cmd_autohold_info.set_defaults(func=self.autohold_info)
        cmd_autohold_info.add_argument('id', metavar='REQUEST_ID',
                                       help='the hold request ID')

        cmd_autohold_list = subparsers.add_parser(
            'autohold-list', help='[DEPRECATED - use zuul-client] '
                                  'list autohold requests')
        cmd_autohold_list.add_argument('--tenant', help='tenant name',
                                       required=True)
        cmd_autohold_list.set_defaults(func=self.autohold_list)

        # Enqueue/Dequeue
        cmd_enqueue = subparsers.add_parser(
            'enqueue',
            help='[DEPRECATED - use zuul-client] enqueue a change')
        cmd_enqueue.add_argument('--tenant', help='tenant name',
                                 required=True)
        # TODO(mhu) remove in a few releases
        cmd_enqueue.add_argument('--trigger',
                                 help='trigger name (deprecated and ignored. '
                                      'Kept only for backward compatibility)',
                                 required=False, default=None)
        cmd_enqueue.add_argument('--pipeline', help='pipeline name',
                                 required=True)
        cmd_enqueue.add_argument('--project', help='project name',
                                 required=True)
        cmd_enqueue.add_argument('--change', help='change id',
                                 required=True)
        cmd_enqueue.set_defaults(func=self.enqueue)

        cmd_enqueue = subparsers.add_parser(
            'enqueue-ref',
            help='[DEPRECATED - use zuul-client] enqueue a ref',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Submit a trigger event

            Directly enqueue a trigger event.  This is usually used
            to manually "replay" a trigger received from an external
            source such as gerrit.'''))
        cmd_enqueue.add_argument('--tenant', help='tenant name',
                                 required=True)
        cmd_enqueue.add_argument('--trigger', help='trigger name',
                                 required=False, default=None)
        cmd_enqueue.add_argument('--pipeline', help='pipeline name',
                                 required=True)
        cmd_enqueue.add_argument('--project', help='project name',
                                 required=True)
        cmd_enqueue.add_argument('--ref', help='ref name',
                                 required=True)
        cmd_enqueue.add_argument(
            '--oldrev', help='old revision', default=None)
        cmd_enqueue.add_argument(
            '--newrev', help='new revision', default=None)
        cmd_enqueue.set_defaults(func=self.enqueue_ref)

        cmd_dequeue = subparsers.add_parser(
            'dequeue',
            help='[DEPRECATED - use zuul-client] '
                 'dequeue a buildset by its '
                 'change or ref')
        cmd_dequeue.add_argument('--tenant', help='tenant name',
                                 required=True)
        cmd_dequeue.add_argument('--pipeline', help='pipeline name',
                                 required=True)
        cmd_dequeue.add_argument('--project', help='project name',
                                 required=True)
        cmd_dequeue.add_argument('--change', help='change id',
                                 default=None)
        cmd_dequeue.add_argument('--ref', help='ref name',
                                 default=None)
        cmd_dequeue.set_defaults(func=self.dequeue)

        # Promote
        cmd_promote = subparsers.add_parser(
            'promote',
            help='[DEPRECATED - use zuul-client] '
                 'promote one or more changes')
        cmd_promote.add_argument('--tenant', help='tenant name',
                                 required=True)
        cmd_promote.add_argument('--pipeline', help='pipeline name',
                                 required=True)
        cmd_promote.add_argument('--changes', help='change ids',
                                 required=True, nargs='+')
        cmd_promote.set_defaults(func=self.promote)

        # Show
        cmd_show = subparsers.add_parser(
            'show',
            help='[DEPRECATED - use zuul-client] '
                 'show current statuses')
        cmd_show.set_defaults(func=self.show_running_jobs)
        show_subparsers = cmd_show.add_subparsers(title='show')
        show_running_jobs = show_subparsers.add_parser(
            'running-jobs',
            help='show the running jobs'
        )
        running_jobs_columns = list(self._show_running_jobs_columns().keys())
        show_running_jobs.add_argument(
            '--columns',
            help="comma separated list of columns to display (or 'ALL')",
            choices=running_jobs_columns.append('ALL'),
            default='name, worker.name, start_time, result'
        )

        # TODO: add filters such as queue, project, changeid etc
        show_running_jobs.set_defaults(func=self.show_running_jobs)

        # Conf check
        cmd_conf_check = subparsers.add_parser(
            'tenant-conf-check',
            help='validate the tenant configuration')
        cmd_conf_check.set_defaults(func=self.validate)

        # Auth token
        cmd_create_auth_token = subparsers.add_parser(
            'create-auth-token',
            help='create an Authentication Token for the web API',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Create an Authentication Token for the administration web API

            Create a bearer token that can be used to access Zuul's
            administration web API. This is typically used to delegate
            privileged actions such as enqueueing and autoholding to
            third parties, scoped to a single tenant.
            At least one authenticator must be configured with a secret
            that can be used to sign the token.'''))
        cmd_create_auth_token.add_argument(
            '--auth-config',
            help=('The authenticator to use. '
                  'Must match an authenticator defined in zuul\'s '
                  'configuration file.'),
            default='zuul_operator',
            required=True)
        cmd_create_auth_token.add_argument(
            '--tenant',
            help=('When specified, zuul.admin claim with the value '
                  'of the tenant will be added to the token.'),
            required=False)
        cmd_create_auth_token.add_argument(
            '--user',
            help=("The user's name. Used for traceability in logs."),
            default=None,
            required=True)
        cmd_create_auth_token.add_argument(
            '--expires-in',
            help=('Token validity duration in seconds '
                  '(default: %i)' % 600),
            type=int,
            default=600,
            required=False)
        cmd_create_auth_token.add_argument(
            '--claim',
            help=('Custom claim in format claim:value. It can '
                  'be used multiple times to add multiple claims.'),
            action="extend",
            type=lambda x: tuple(x.split(':')),
            nargs='*',
            default=[])
        cmd_create_auth_token.add_argument(
            '--print-meta-info',
            action='store_true',
            help="When specified, the meta info of the token will be printed")
        cmd_create_auth_token.set_defaults(func=self.create_auth_token)

        # Key storage
        cmd_import_keys = subparsers.add_parser(
            'import-keys',
            help='import project keys to ZooKeeper',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Import previously exported project secret keys to ZooKeeper

            Given a file with previously exported project keys, this
            command will import them into ZooKeeper.  Existing keys
            will not be overwritten; to overwrite keys, add the
            --force flag.'''))
        cmd_import_keys.set_defaults(command='import-keys')
        cmd_import_keys.add_argument('path', type=str,
                                     help='key export file path')
        cmd_import_keys.add_argument('--force', action='store_true',
                                     help='overwrite existing keys')
        cmd_import_keys.set_defaults(func=self.import_keys)

        cmd_export_keys = subparsers.add_parser(
            'export-keys',
            help='export project keys from ZooKeeper',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Export project secret keys from ZooKeeper

            This command exports project secret keys from ZooKeeper
            and writes them to a file which is suitable for backing
            up and later use with the import-keys command.

            The key contents are still protected by the keystore
            password and can not be used or decrypted without it.'''))
        cmd_export_keys.set_defaults(command='export-keys')
        cmd_export_keys.add_argument('path', type=str,
                                     help='key export file path')
        cmd_export_keys.set_defaults(func=self.export_keys)

        cmd_copy_keys = subparsers.add_parser(
            'copy-keys',
            help='copy keys from one project to another',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Copy secret keys from one project to another

            When projects are renamed, this command may be used to
            copy the secret keys from the current name to the new name
            in order to avoid service interruption.'''))
        cmd_copy_keys.set_defaults(command='copy-keys')
        cmd_copy_keys.add_argument('src_connection', type=str,
                                   help='original connection name')
        cmd_copy_keys.add_argument('src_project', type=str,
                                   help='original project name')
        cmd_copy_keys.add_argument('dest_connection', type=str,
                                   help='new connection name')
        cmd_copy_keys.add_argument('dest_project', type=str,
                                   help='new project name')
        cmd_copy_keys.set_defaults(func=self.copy_keys)

        cmd_delete_keys = subparsers.add_parser(
            'delete-keys',
            help='delete project keys',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Delete the ssh and secrets keys for a project
            '''))
        cmd_delete_keys.set_defaults(command='delete-keys')
        cmd_delete_keys.add_argument('connection', type=str,
                                     help='connection name')
        cmd_delete_keys.add_argument('project', type=str,
                                     help='project name')
        cmd_delete_keys.set_defaults(func=self.delete_keys)

        cmd_delete_oidc_signing_keys = subparsers.add_parser(
            'delete-oidc-signing-keys',
            help='delete OIDC signing keys',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Delete the OIDC signing keys of an algorithm
            '''))
        cmd_delete_oidc_signing_keys.set_defaults(
            command='delete-oidc-signing-keys')
        cmd_delete_oidc_signing_keys.add_argument(
            'algorithm', type=str, help='algorithm name')
        cmd_delete_oidc_signing_keys.set_defaults(
            func=self.delete_oidc_signing_keys)

        # image storage
        cmd_import_images = subparsers.add_parser(
            'import-images',
            help='import images to ZooKeeper',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Import previously exported images to ZooKeeper

            Given a file with previously exported images, this
            command will import them into ZooKeeper.  Existing images
            will not be overwritten.'''))
        cmd_import_images.set_defaults(command='import-images')
        cmd_import_images.add_argument('path', type=str,
                                       help='image export file path')
        cmd_import_images.set_defaults(func=self.import_images)

        cmd_export_images = subparsers.add_parser(
            'export-images',
            help='export images from ZooKeeper',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Export images from ZooKeeper

            This command exports images from ZooKeeper
            and writes them to a file which is suitable for backing
            up and later use with the import-images command.'''))
        cmd_export_images.set_defaults(command='export-images')
        cmd_export_images.add_argument('path', type=str,
                                       help='image export file path')
        cmd_export_images.set_defaults(func=self.export_images)

        # ZK Maintenance
        cmd_delete_state = subparsers.add_parser(
            'delete-state',
            help='delete ephemeral ZooKeeper state',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Delete all ephemeral state stored in ZooKeeper

            Zuul stores a considerable amount of ephemeral state
            information in ZooKeeper.  Generally it should be able to
            detect and correct any errors, but if the state becomes
            corrupted and it is unable to recover, this command may be
            used to delete all ephemeral data from ZooKeeper and start
            anew.

            Do not run this command while any Zuul component is
            running (perform a complete shutdown first).

            This command will only remove ephemeral Zuul data from
            ZooKeeper; it will not remove private keys or Nodepool
            data.'''))
        cmd_delete_state.set_defaults(command='delete-state')
        cmd_delete_state.add_argument(
            '--keep-config-cache', action='store_true',
            help='keep config cache')
        cmd_delete_state.set_defaults(func=self.delete_state)

        cmd_delete_pipeline_state = subparsers.add_parser(
            'delete-pipeline-state',
            help='delete single pipeline ZooKeeper state',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Delete a single pipeline state stored in ZooKeeper

            In the unlikely event that a bug in Zuul or ZooKeeper data
            corruption occurs in such a way that it only affects a
            single pipeline, this command might be useful in
            attempting to recover.

            The circumstances under which this command will be able to
            effect a recovery are very rare and even so it may not be
            sufficient.  In general, if an error occurs it is better
            to shut Zuul down and run "zuul delete-state".

            This command will lock the specified tenant and
            then completely delete the pipeline state.'''))
        cmd_delete_pipeline_state.set_defaults(command='delete-pipeline-state')
        cmd_delete_pipeline_state.set_defaults(func=self.delete_pipeline_state)
        cmd_delete_pipeline_state.add_argument('tenant', type=str,
                                               help='tenant name')
        cmd_delete_pipeline_state.add_argument('pipeline', type=str,
                                               help='pipeline name')

        # DB Maintenance
        cmd_prune_database = subparsers.add_parser(
            'prune-database',
            help='prune old database entries',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
            Prune old database entries

            This command will delete database entries older than the
            specified cutoff (which can be specified as either an
            absolute or relative time).'''))
        cmd_prune_database.set_defaults(command='prune-database')
        cmd_prune_database.add_argument(
            '--before',
            help='absolute timestamp (e.g., "2022-01-31 12:00:00")')
        cmd_prune_database.add_argument(
            '--older-than',
            help='relative time (e.g., "24h" or "180d")')
        cmd_prune_database.add_argument(
            '--batch-size',
            default=10000,
            help='transaction batch size')
        cmd_prune_database.set_defaults(func=self.prune_database)

        return parser

    def parseArguments(self, args=None):
        parser = super(Client, self).parseArguments()
        if not getattr(self.args, 'func', None):
            parser.print_help()
            sys.exit(1)
        if self.args.func == self.enqueue_ref:
            # if oldrev or newrev is set, ensure they're not the same
            if (self.args.oldrev is not None) or \
               (self.args.newrev is not None):
                if self.args.oldrev == self.args.newrev:
                    parser.error(
                        "The old and new revisions must not be the same.")
        if self.args.func == self.dequeue:
            if self.args.change is None and self.args.ref is None:
                parser.error("Change or ref needed.")
            if self.args.change is not None and self.args.ref is not None:
                parser.error(
                    "The 'change' and 'ref' arguments are mutually exclusive.")

    def setup_logging(self):
        """Client logging does not rely on conf file"""
        if self.args.verbose:
            logging.basicConfig(level=logging.DEBUG)

    def main(self):
        self.parseArguments()
        if not self.args.zuul_url:
            self.readConfig()
        self.setup_logging()
        if self.args.func in [self.autohold, self.autohold_delete,
                              self.enqueue, self.enqueue_ref,
                              self.dequeue, self.promote]:
            print(
                "Warning: this command is deprecated with zuul-admin, "
                "please use `zuul-client` instead",
                file=sys.stderr)
        if self.args.func():
            sys.exit(0)
        else:
            sys.exit(1)

    def get_client(self):
        if self.args.zuul_url:
            self.log.debug('Zuul URL provided as argument, using REST client')
            client = ZuulRESTClient(self.args.zuul_url,
                                    self.args.insecure_ssl,
                                    self.args.auth_token)
            return client
        conf_sections = self.config.sections()
        if 'webclient' in conf_sections:
            self.log.debug('web section found in config, using REST client')
            server = get_default(self.config, 'webclient', 'url', None)
            verify = get_default(self.config, 'webclient', 'verify_ssl',
                                 self.args.insecure_ssl)
            client = ZuulRESTClient(server, verify,
                                    self.args.auth_token)
        else:
            print('Unable to find a way to connect to Zuul, add a '
                  '"webclient" section to your configuration file')
            sys.exit(1)
        if server is None:
            print('Missing "server" configuration value')
            sys.exit(1)
        return client

    def autohold(self):
        if self.args.change and self.args.ref:
            print("Change and ref can't be both used for the same request")
            return False
        if "," in self.args.change:
            print("Error: change argument can not contain any ','")
            return False

        node_hold_expiration = self.args.node_hold_expiration
        client = self.get_client()
        r = client.autohold(
            tenant=self.args.tenant,
            project=self.args.project,
            job=self.args.job,
            change=self.args.change,
            ref=self.args.ref,
            reason=self.args.reason,
            count=self.args.count,
            node_hold_expiration=node_hold_expiration)
        return r

    def autohold_delete(self):
        client = self.get_client()
        return client.autohold_delete(self.args.id)

    def autohold_info(self):
        client = self.get_client()
        request = client.autohold_info(self.args.id)

        if not request:
            print("Autohold request not found")
            return True

        print("ID: %s" % request['id'])
        print("Tenant: %s" % request['tenant'])
        print("Project: %s" % request['project'])
        print("Job: %s" % request['job'])
        print("Ref Filter: %s" % request['ref_filter'])
        print("Max Count: %s" % request['max_count'])
        print("Current Count: %s" % request['current_count'])
        print("Node Expiration: %s" % request['node_expiration'])
        print("Request Expiration: %s" % time.ctime(request['expired']))
        print("Reason: %s" % request['reason'])
        print("Held Nodes: %s" % request['nodes'])

        return True

    def autohold_list(self):
        client = self.get_client()
        autohold_requests = client.autohold_list(tenant=self.args.tenant)

        if not autohold_requests:
            print("No autohold requests found")
            return True

        table = prettytable.PrettyTable(
            field_names=[
                'ID', 'Tenant', 'Project', 'Job', 'Ref Filter',
                'Current Count', 'Max Count', 'Reason'
            ])

        for request in autohold_requests:
            table.add_row([
                request['id'],
                request['tenant'],
                request['project'],
                request['job'],
                request['ref_filter'],
                request['current_count'],
                request['max_count'],
                request['reason'],
            ])

        print(table)
        return True

    def enqueue(self):
        client = self.get_client()
        r = client.enqueue(
            tenant=self.args.tenant,
            pipeline=self.args.pipeline,
            project=self.args.project,
            trigger=self.args.trigger,
            change=self.args.change)
        return r

    def enqueue_ref(self):
        client = self.get_client()
        r = client.enqueue_ref(
            tenant=self.args.tenant,
            pipeline=self.args.pipeline,
            project=self.args.project,
            trigger=self.args.trigger,
            ref=self.args.ref,
            oldrev=self.args.oldrev,
            newrev=self.args.newrev)
        return r

    def dequeue(self):
        client = self.get_client()
        r = client.dequeue(
            tenant=self.args.tenant,
            pipeline=self.args.pipeline,
            project=self.args.project,
            change=self.args.change,
            ref=self.args.ref)
        return r

    def create_auth_token(self):
        auth_section = ''
        for section_name in self.config.sections():
            if re.match(r'^auth ([\'\"]?)%s(\1)$' % self.args.auth_config,
                        section_name, re.I):
                auth_section = section_name
                break
        if auth_section == '':
            print('"%s" authenticator configuration not found.'
                  % self.args.auth_config)
            sys.exit(1)
        now = int(time.time())
        exp = now + self.args.expires_in
        token = {'iat': now,
                 'exp': exp,
                 'iss': get_default(self.config, auth_section, 'issuer_id'),
                 'aud': get_default(self.config, auth_section, 'client_id'),
                 'sub': self.args.user,
                }
        # Add zuul.admin claim only when tenant is specified
        if self.args.tenant:
            token['zuul'] = {'admin': [self.args.tenant, ]}

        # Add custom claims, allow to overwrite default claims
        # when it is required.
        for k, v in self.args.claim:
            token[k] = v

        driver = get_default(
            self.config, auth_section, 'driver')
        if driver == 'HS256':
            key = get_default(self.config, auth_section, 'secret')
        elif driver == 'RS256':
            private_key = get_default(self.config, auth_section, 'private_key')
            try:
                with open(private_key, 'r') as pk:
                    key = pk.read()
            except Exception as e:
                print('Could not read private key at "%s": %s' % (private_key,
                                                                  e))
                sys.exit(1)
        else:
            print('Unknown or unsupported authenticator driver "%s"' % driver)
            sys.exit(1)
        try:
            auth_token = jwt.encode(token,
                                    key=key,
                                    algorithm=driver)
            print("Bearer %s" % auth_token)
            if self.args.print_meta_info:
                print("---------------- Meta Info ----------------")
                if self.args.tenant:
                    print("Tenant:\t\t%s" % self.args.tenant)
                print("User:\t\t%s" % self.args.user)
                print("Generated At:\t%s" % time.strftime(
                    "%Y-%m-%d %H:%M:%S UTC", time.gmtime(now)))
                print("Expired At:\t%s" % time.strftime(
                    "%Y-%m-%d %H:%M:%S UTC", time.gmtime(exp)))
                print("SHA1 Checksum:\t%s" % hashlib.sha1(
                    auth_token.encode("utf-8")).hexdigest())
                if self.args.claim:
                    print("Custom Claims:\t%s" % self.args.claim)
                print("-------------------------------------------")
            err_code = 0
        except Exception as e:
            print("Error when generating Auth Token")
            print(e)
            err_code = 1
        finally:
            sys.exit(err_code)

    def promote(self):
        client = self.get_client()
        r = client.promote(
            tenant=self.args.tenant,
            pipeline=self.args.pipeline,
            change_ids=self.args.changes)
        return r

    def show_running_jobs(self):
        client = self.get_client()
        running_items = client.get_running_jobs()

        if len(running_items) == 0:
            print("No jobs currently running")
            return True

        all_fields = self._show_running_jobs_columns()
        fields = all_fields.keys()

        table = prettytable.PrettyTable(
            field_names=[all_fields[f]['title'] for f in fields])
        for item in running_items:
            for job in item['jobs']:
                values = []
                for f in fields:
                    v = job
                    for part in f.split('.'):
                        if hasattr(v, 'get'):
                            v = v.get(part, '')
                    if ('transform' in all_fields[f]
                        and callable(all_fields[f]['transform'])):
                        v = all_fields[f]['transform'](v)
                    if 'append' in all_fields[f]:
                        v += all_fields[f]['append']
                    values.append(v)
                table.add_row(values)
        print(table)
        return True

    def _epoch_to_relative_time(self, epoch):
        if epoch:
            delta = datetime.timedelta(seconds=(time.time() - int(epoch)))
            return babel.dates.format_timedelta(delta, locale='en_US')
        else:
            return "Unknown"

    def _boolean_to_yes_no(self, value):
        return 'Yes' if value else 'No'

    def _boolean_to_pass_fail(self, value):
        return 'Pass' if value else 'Fail'

    def _format_list(self, l):
        return ', '.join(l) if isinstance(l, list) else ''

    def _show_running_jobs_columns(self):
        """A helper function to get the list of available columns for
        `zuul show running-jobs`. Also describes how to convert particular
        values (for example epoch to time string)"""

        return {
            'name': {
                'title': 'Job Name',
            },
            'elapsed_time': {
                'title': 'Elapsed Time',
                'transform': self._epoch_to_relative_time
            },
            'remaining_time': {
                'title': 'Remaining Time',
                'transform': self._epoch_to_relative_time
            },
            'url': {
                'title': 'URL'
            },
            'result': {
                'title': 'Result'
            },
            'voting': {
                'title': 'Voting',
                'transform': self._boolean_to_yes_no
            },
            'uuid': {
                'title': 'UUID'
            },
            'execute_time': {
                'title': 'Execute Time',
                'transform': self._epoch_to_relative_time,
                'append': ' ago'
            },
            'start_time': {
                'title': 'Start Time',
                'transform': self._epoch_to_relative_time,
                'append': ' ago'
            },
            'end_time': {
                'title': 'End Time',
                'transform': self._epoch_to_relative_time,
                'append': ' ago'
            },
            'estimated_time': {
                'title': 'Estimated Time',
                'transform': self._epoch_to_relative_time,
                'append': ' to go'
            },
            'pipeline': {
                'title': 'Pipeline'
            },
            'canceled': {
                'title': 'Canceled',
                'transform': self._boolean_to_yes_no
            },
            'retry': {
                'title': 'Retry'
            },
            'number': {
                'title': 'Number'
            },
            'nodeset': {
                'title': 'Nodeset'
            },
            'worker.name': {
                'title': 'Worker'
            },
            'worker.hostname': {
                'title': 'Worker Hostname'
            },
        }

    def validate(self):
        from zuul import scheduler
        from zuul import configloader
        from zuul.configloader import as_list
        self.configure_connections(sources=True, triggers=True, reporters=True)

        class SchedulerConfig(scheduler.Scheduler):
            # A custom scheduler constructor adapted for config check
            # to avoid loading runtime clients.
            def __init__(self, config, connections):
                self.config = config
                self.connections = connections
                self.unparsed_config_cache = None

        def _normalize_role_mappings(mappings):
            new_mappings = {}
            for k, v in mappings.items():
                new_mappings[k] = as_list(v)
            return new_mappings

        zuul_globals = SystemAttributes.fromConfig(self.config)
        sched = SchedulerConfig(self.config, self.connections)
        loader = configloader.ConfigLoader(
            self.connections, None, None, zuul_globals, None)
        tenant_config, script = sched._checkTenantSourceConf(self.config)
        try:
            abide = Abide()
            unparsed_abide = loader.readConfig(
                tenant_config, from_script=script)
            loader.loadAuthzRules(abide, unparsed_abide)
            loader.loadSemaphores(abide, unparsed_abide)
            # Do not load TPCs since that requires ZK
            for conf_tenant in unparsed_abide.tenants.values():
                loader.tenant_parser.getSchema()(conf_tenant)
                global_semaphores = set(as_list(conf_tenant.get(
                    'semaphores', [])))
                loader.tenant_parser.validateGlobalSemaphores(
                    abide, global_semaphores)
                role_mappings = _normalize_role_mappings(conf_tenant.get(
                    'role-mappings', {}))
                loader.tenant_parser.validateRoleMappings(
                    abide, role_mappings)
            if unparsed_abide.api_roots:
                api_root = unparsed_abide.api_roots[0]
                role_mappings = _normalize_role_mappings(api_root.get(
                    'role-mappings', {}))
                loader.api_root_parser.validateRoleMappings(
                    abide, role_mappings)

            print("Tenants config validated with success")
            err_code = 0
        except Exception as e:
            print("Error when validating tenants config")
            print(e)
            err_code = 1
        finally:
            sys.exit(err_code)

    def export_keys(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()
        try:
            password = self.config["keystore"]["password"]
        except KeyError:
            raise RuntimeError("No key store password configured!")
        keystore = KeyStorage(zk_client, password=password)
        export = keystore.exportKeys()
        with open(os.open(self.args.path,
                          os.O_CREAT | os.O_WRONLY, 0o600), 'w') as f:
            json.dump(export, f)
        sys.exit(0)

    def import_keys(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()
        try:
            password = self.config["keystore"]["password"]
        except KeyError:
            raise RuntimeError("No key store password configured!")
        keystore = KeyStorage(zk_client, password=password)
        with open(self.args.path, 'r') as f:
            import_data = json.load(f)
        keystore.importKeys(import_data, self.args.force)
        sys.exit(0)

    def copy_keys(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()
        try:
            password = self.config["keystore"]["password"]
        except KeyError:
            raise RuntimeError("No key store password configured!")
        keystore = KeyStorage(zk_client, password=password)
        args = self.args
        # Load
        ssh = keystore.loadProjectSSHKeys(args.src_connection,
                                          args.src_project)
        secrets = keystore.loadProjectsSecretsKeys(args.src_connection,
                                                   args.src_project)
        # Save
        keystore.saveProjectSSHKeys(args.dest_connection,
                                    args.dest_project, ssh)
        keystore.saveProjectsSecretsKeys(args.dest_connection,
                                         args.dest_project, secrets)
        self.log.info("Copied keys from %s %s to %s %s",
                      args.src_connection, args.src_project,
                      args.dest_connection, args.dest_project)
        sys.exit(0)

    def delete_keys(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()
        try:
            password = self.config["keystore"]["password"]
        except KeyError:
            raise RuntimeError("No key store password configured!")
        keystore = KeyStorage(zk_client, password=password)
        args = self.args
        keystore.deleteProjectSSHKeys(args.connection, args.project)
        keystore.deleteProjectsSecretsKeys(args.connection, args.project)
        keystore.deleteProjectDir(args.connection, args.project)
        self.log.info("Delete keys from %s %s",
                      args.connection, args.project)
        sys.exit(0)

    def delete_oidc_signing_keys(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()
        try:
            password = self.config["keystore"]["password"]
        except KeyError:
            raise RuntimeError("No key store password configured!")
        keystore = KeyStorage(zk_client, password=password)
        algorithm = self.args.algorithm
        keystore.deleteOidcSigningKeys(algorithm)
        self.log.info("Delete OIDC signing keys for %s", algorithm)
        sys.exit(0)

    def export_images(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()
        image_build_registry = ImageBuildRegistry(zk_client)
        image_build_registry.waitForSync()
        image_upload_registry = ImageUploadRegistry(zk_client)
        image_upload_registry.waitForSync()
        artifacts = [
            x.toDict() for x in image_build_registry.getAllArtifacts()]
        uploads = [
            x.toDict() for x in image_upload_registry.getAllUploads()]
        export = dict(
            artifacts=artifacts,
            uploads=uploads,
        )

        with open(os.open(self.args.path,
                          os.O_CREAT | os.O_WRONLY, 0o600), 'w') as f:
            json.dump(export, f)
        sys.exit(0)

    def import_images(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()
        with open(self.args.path, 'r') as f:
            import_data = json.load(f)
        with ZKContext(zk_client, None, None, self.log) as context:
            for upload in import_data['uploads']:
                try:
                    ImageUpload.new(context, **upload)
                except NodeExistsError:
                    self.log.debug("Upload already exists %s", upload['uuid'])
            for artifact in import_data['artifacts']:
                try:
                    ImageBuildArtifact.new(context, **artifact)
                except NodeExistsError:
                    self.log.debug("Artifact already exists %s",
                                   artifact['uuid'])

        sys.exit(0)

    def delete_state(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()
        confirm = input("Are you sure you want to delete "
                        "all ephemeral data from ZooKeeper? (yes/no) ")
        if confirm.strip().lower() != 'yes':
            print("Aborting")
            sys.exit(1)
        if self.args.keep_config_cache:
            try:
                children = zk_client.client.get_children('/zuul')
            except NoNodeError:
                children = []
            for child in children:
                if child == 'config':
                    continue
                path = f'/zuul/{child}'
                self.log.debug("Deleting %s", path)
                zk_client.fastRecursiveDelete(path)
        else:
            zk_client.fastRecursiveDelete('/zuul')
        sys.exit(0)

    def delete_pipeline_state(self):
        logging.basicConfig(level=logging.INFO)

        zk_client = ZooKeeperClient.fromConfig(self.config)
        zk_client.connect()

        args = self.args
        safe_tenant = urllib.parse.quote_plus(args.tenant)
        safe_pipeline = urllib.parse.quote_plus(args.pipeline)
        COMPONENT_REGISTRY.create(zk_client)

        class FakeTenant:
            pass

        tenant = FakeTenant()
        tenant.name = args.tenant

        with tenant_read_lock(zk_client, args.tenant, self.log):
            path = f'/zuul/tenant/{safe_tenant}/pipeline/{safe_pipeline}'
            pipeline = Pipeline(args.pipeline)
            with pipeline_lock(
                    zk_client, args.tenant, args.pipeline
            ) as plock:
                zk_client.fastRecursiveDelete(path)
                with ZKContext(zk_client, plock, None, self.log) as context:
                    manager = IndependentPipelineManager(
                        None, pipeline, tenant)
                    manager.state = PipelineState.new(
                        context, _path=path, layout_uuid=None)
                    PipelineChangeList.new(context, manager=manager)

        sys.exit(0)

    def prune_database(self):
        logging.basicConfig(level=logging.INFO)
        args = self.args
        now = datetime.datetime.now(dateutil.tz.tzutc())
        cutoff = parse_cutoff(now, args.before, args.older_than)
        self.configure_connections(database=True)
        connection = self.connections.getSqlConnection()
        connection.deleteBuildsets(cutoff, args.batch_size)
        connection.deleteSystemEvents(cutoff)
        sys.exit(0)


def main():
    if sys.argv[0].endswith('zuul'):
        print(
            "Warning: this command name is deprecated, "
            "use `zuul-admin` instead",
            file=sys.stderr)
    Client().main()
