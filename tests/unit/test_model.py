# Copyright 2015 Red Hat, Inc.
# Copyright 2023 Acme Gating, LLC
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


import configparser
import os
import textwrap
import uuid
from unittest import mock

import fixtures
import testtools
import voluptuous

from zuul import model
from zuul import configloader
from zuul.lib import encryption
from zuul.lib import yamlutil as yaml
import zuul.lib.connections

from tests.base import BaseTestCase, FIXTURE_DIR
from zuul.lib.ansible import AnsibleManager
from zuul.lib import tracing
from zuul.lib.re2util import ZuulRegex
from zuul.model_api import MODEL_API
from zuul.zk.zkobject import LocalZKContext
from zuul.zk.components import COMPONENT_REGISTRY
from zuul import change_matcher


class Dummy(object):
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class TestJob(BaseTestCase):
    def setUp(self):
        COMPONENT_REGISTRY.registry = Dummy()
        COMPONENT_REGISTRY.registry.model_api = MODEL_API
        self._env_fixture = self.useFixture(
            fixtures.EnvironmentVariable('HISTTIMEFORMAT', '%Y-%m-%dT%T%z '))
        super(TestJob, self).setUp()
        # Toss in % in env vars to trigger the configparser issue
        self.connections = zuul.lib.connections.ConnectionRegistry()
        self.addCleanup(self.connections.stop)
        self.connection = Dummy(connection_name='dummy_connection')
        self.source = Dummy(canonical_hostname='git.example.com',
                            connection=self.connection)
        self.abide = model.Abide()
        self.tenant = model.Tenant('tenant')
        self.tenant.allowed_labels = None
        self.tenant.disallowed_labels = None
        self.tenant.default_ansible_version = AnsibleManager().default_version
        self.tenant.semaphore_handler = Dummy(abide=self.abide)
        self.layout = model.Layout(self.tenant)
        self.tenant.layout = self.layout
        self.project = model.Project('project', self.source)
        self.context = model.SourceContext(
            self.project.canonical_name, self.project.name,
            self.project.connection_name, 'master', 'test')
        self.tpc = model.TenantProjectConfig(self.project)
        self.tpc.trusted = True
        self.tenant.addTPC(self.tpc)
        self.pipeline = model.Pipeline('gate')
        self.pipeline.source_context = self.context
        self.manager = mock.Mock()
        self.manager.pipeline = self.pipeline
        self.manager.tenant = self.tenant
        self.zk_context = LocalZKContext(self.log)
        self.manager.current_context = self.zk_context
        self.manager.state = model.PipelineState()
        self.manager.state._set(manager=self.manager)
        self.layout.addPipelineManager(self.manager)
        with self.zk_context as ctx:
            self.queue = model.ChangeQueue.new(
                ctx, manager=self.manager)
        self.pcontext = configloader.ParseContext(
            self.connections, None, None, AnsibleManager())

        private_key_file = os.path.join(FIXTURE_DIR, 'private.pem')
        with open(private_key_file, "rb") as f:
            priv, pub = encryption.deserialize_rsa_keypair(f.read())
            self.project.private_secrets_key = priv
            self.project.public_secrets_key = pub
        m = yaml.Mark('name', 0, 0, 0, '', 0)
        self.start_mark = model.ZuulMark(m, m, '')
        config = configparser.ConfigParser()
        self.tracing = tracing.Tracing(config)

    @property
    def job(self):
        job = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'job',
            'parent': None,
            'irrelevant-files': [
                '^docs/.*$'
            ]}, None)
        return job

    def test_change_matches_returns_false_for_matched_skip_if(self):
        change = model.Change('project')
        change.files = ['/COMMIT_MSG', 'docs/foo']
        self.assertFalse(self.job.changeMatchesFiles(change))

    def test_change_matches_returns_false_for_single_matched_skip_if(self):
        change = model.Change('project')
        change.files = ['docs/foo']
        self.assertFalse(self.job.changeMatchesFiles(change))

    def test_change_matches_returns_true_for_unmatched_skip_if(self):
        change = model.Change('project')
        change.files = ['/COMMIT_MSG', 'foo']
        self.assertTrue(self.job.changeMatchesFiles(change))

    def test_change_matches_returns_true_for_single_unmatched_skip_if(self):
        change = model.Change('project')
        change.files = ['foo']
        self.assertTrue(self.job.changeMatchesFiles(change))

    def test_job_sets_defaults_for_boolean_attributes(self):
        self.assertIsNotNone(self.job.voting)

    def test_job_variants(self):
        # This simulates freezing a job.

        secret = model.Secret('foo', self.context)
        self.layout.addSecret(secret)

        secrets = [model.SecretUse('foo', 'foo')]
        py27_pre = model.PlaybookContext(
            self.context, 'py27-pre', [], secrets, [])
        py27_run = model.PlaybookContext(
            self.context, 'py27-run', [], secrets, [])
        py27_post = model.PlaybookContext(
            self.context, 'py27-post', [], secrets, [])

        py27 = model.Job('py27')
        py27.timeout = 30
        py27.pre_run = (py27_pre,)
        py27.run = (py27_run,)
        py27.post_run = (py27_post,)

        job = py27.copy()
        job.setBase(self.layout, None)
        self.assertEqual(30, job.timeout)

        # Apply the diablo variant
        diablo = model.Job('py27')
        diablo.timeout = 40
        job.applyVariant(diablo, self.layout, None)

        self.assertEqual(40, job.timeout)
        self.assertEqual(['py27-pre'],
                         [x.path for x in job.pre_run])
        self.assertEqual(['py27-run'],
                         [x.path for x in job.run])
        self.assertEqual(['py27-post'],
                         [x.path for x in job.post_run])
        self.assertEqual(secrets, job.pre_run[0].secrets)
        self.assertEqual(secrets, job.run[0].secrets)
        self.assertEqual(secrets, job.post_run[0].secrets)

        # Set the job to final for the following checks
        job.final = True
        self.assertTrue(job.voting)

        good_final = model.Job('py27')
        good_final.voting = False
        job.applyVariant(good_final, self.layout, None)
        self.assertFalse(job.voting)

        bad_final = model.Job('py27')
        bad_final.timeout = 600
        with testtools.ExpectedException(
                model.JobConfigurationError,
                "Unable to modify final job"):
            job.applyVariant(bad_final, self.layout, None)

    @mock.patch("zuul.model.zkobject.ZKObject._save")
    def test_job_inheritance_job_tree(self, save_mock):
        base = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'base',
            'parent': None,
            'timeout': 30,
        }, None)
        self.layout.addJob(base)
        python27 = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'python27',
            'parent': 'base',
            'timeout': 40,
        }, None)
        self.layout.addJob(python27)
        python27diablo = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'python27',
            'branches': [
                'stable/diablo'
            ],
            'timeout': 50,
        }, None)
        self.layout.addJob(python27diablo)

        project_config = self.pcontext.project_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'project',
            'gate': {
                'jobs': [
                    {'python27': {'timeout': 70,
                                  'run': 'playbooks/python27.yaml'}}
                ]
            }
        })
        self.layout.addProjectConfig(self.project.canonical_name,
                                     project_config)

        change = model.Change(self.project)
        change.branch = 'master'
        change.cache_stat = Dummy(key=Dummy(reference=uuid.uuid4().hex))
        item = self.queue.enqueueChanges([change], None)

        self.assertTrue(base.changeMatchesBranch(self.tenant, change))
        self.assertTrue(python27.changeMatchesBranch(self.tenant, change))
        self.assertFalse(python27diablo.changeMatchesBranch(
            self.tenant, change))

        with self.zk_context as ctx:
            item.freezeJobGraph(self.layout, ctx,
                                skip_file_matcher=False,
                                redact_secrets_and_keys=False)
        self.assertEqual(len(item.getJobs()), 1)
        job = item.getJobs()[0]
        self.assertEqual(job.name, 'python27')
        self.assertEqual(job.timeout, 70)

        change.branch = 'stable/diablo'
        change.cache_stat = Dummy(key=Dummy(reference=uuid.uuid4().hex))
        item = self.queue.enqueueChanges([change], None)

        self.assertTrue(base.changeMatchesBranch(self.tenant, change))
        self.assertTrue(python27.changeMatchesBranch(self.tenant, change))
        self.assertTrue(python27diablo.changeMatchesBranch(
            self.tenant, change))

        with self.zk_context as ctx:
            item.freezeJobGraph(self.layout, ctx,
                                skip_file_matcher=False,
                                redact_secrets_and_keys=False)
        self.assertEqual(len(item.getJobs()), 1)
        job = item.getJobs()[0]
        self.assertEqual(job.name, 'python27')
        self.assertEqual(job.timeout, 70)

    @mock.patch("zuul.model.zkobject.ZKObject._save")
    def test_inheritance_keeps_matchers(self, save_mock):
        base = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'base',
            'parent': None,
            'timeout': 30,
        }, None)
        self.layout.addJob(base)
        python27 = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'python27',
            'parent': 'base',
            'timeout': 40,
            'irrelevant-files': ['^ignored-file$'],
        }, None)
        self.layout.addJob(python27)

        project_config = self.pcontext.project_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'project',
            'gate': {
                'jobs': [
                    'python27',
                ]
            }
        })
        self.layout.addProjectConfig(self.project.canonical_name,
                                     project_config)

        change = model.Change(self.project)
        change.branch = 'master'
        change.cache_stat = Dummy(key=Dummy(reference=uuid.uuid4().hex))
        change.files = ['/COMMIT_MSG', 'ignored-file']
        item = self.queue.enqueueChanges([change], None)

        self.assertTrue(base.changeMatchesFiles(change))
        self.assertFalse(python27.changeMatchesFiles(change))

        self.manager.getFallbackLayout = mock.Mock(return_value=None)
        with self.zk_context as ctx:
            item.freezeJobGraph(self.layout, ctx,
                                skip_file_matcher=False,
                                redact_secrets_and_keys=False)
        self.assertEqual([], item.getJobs())

    def test_job_source_project(self):
        base_project = model.Project('base_project', self.source)
        base_context = model.SourceContext(
            base_project.canonical_name, base_project.name,
            base_project.connection_name, 'master', 'test')
        tpc = model.TenantProjectConfig(base_project)
        tpc.trusted = True
        self.tenant.addTPC(tpc)

        base = self.pcontext.job_parser.fromYaml({
            '_source_context': base_context,
            '_start_mark': self.start_mark,
            'parent': None,
            'name': 'base',
        }, None)
        self.layout.addJob(base)

        other_project = model.Project('other_project', self.source)
        other_context = model.SourceContext(
            other_project.canonical_name, other_project.name,
            other_project.connection_name, 'master', 'test')
        tpc = model.TenantProjectConfig(other_project)
        tpc.trusted = True
        self.tenant.addTPC(tpc)
        base2 = self.pcontext.job_parser.fromYaml({
            '_source_context': other_context,
            '_start_mark': self.start_mark,
            'name': 'base',
        }, None)
        with testtools.ExpectedException(
                Exception,
                "Job base in other_project is not permitted "
                "to shadow job base in base_project"):
            self.layout.addJob(base2)

    @mock.patch("zuul.model.zkobject.ZKObject._save")
    def test_job_pipeline_allow_untrusted_secrets(self, save_mock):
        self.pipeline.post_review = False
        job = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'job',
            'parent': None,
            'post-review': True
        }, None)

        self.layout.addJob(job)

        project_config = self.pcontext.project_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'project',
            'gate': {
                'jobs': [
                    'job'
                ]
            }
        })

        self.layout.addProjectConfig(self.project.canonical_name,
                                     project_config)

        change = model.Change(self.project)
        # Test master
        change.branch = 'master'
        change.cache_stat = Dummy(key=Dummy(reference=uuid.uuid4().hex))
        item = self.queue.enqueueChanges([change], None)
        with testtools.ExpectedException(
                model.JobConfigurationError,
                "Pre-review pipeline gate does not allow post-review job"):
            with self.zk_context as ctx:
                item.freezeJobGraph(self.layout, ctx,
                                    skip_file_matcher=False,
                                    redact_secrets_and_keys=False)

    def test_job_deduplicate_secrets(self):
        # Verify that in a job with two secrets with the same name on
        # different playbooks (achieved by inheritance) the secrets
        # are not deduplicated.  Also verify that the same secret used
        # twice is deduplicated.

        secret1_data = {'text': 'secret1 data'}
        secret2_data = {'text': 'secret2 data'}
        secret1 = self.pcontext.secret_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'secret1',
            'data': secret1_data,
        })
        self.layout.addSecret(secret1)

        secret2 = self.pcontext.secret_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'secret2',
            'data': secret2_data,
        })
        self.layout.addSecret(secret2)

        # In the first job, we test deduplication.
        base = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'base',
            'parent': None,
            'secrets': [
                {'name': 'mysecret',
                 'secret': 'secret1'},
                {'name': 'othersecret',
                 'secret': 'secret1'},
            ],
            'pre-run': 'playbooks/pre.yaml',
        }, None)
        self.layout.addJob(base)

        # The second job should have a secret with the same name as
        # the first job, but with a different value, to make sure it
        # is not deduplicated.
        python27 = self.pcontext.job_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'python27',
            'parent': 'base',
            'secrets': [
                {'name': 'mysecret',
                 'secret': 'secret2'},
            ],
            'run': 'playbooks/python27.yaml',
        }, None)
        self.layout.addJob(python27)

        project_config = self.pcontext.project_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'project',
            'gate': {
                'jobs': ['python27'],
            }
        })
        self.layout.addProjectConfig(self.project.canonical_name,
                                     project_config)

        change = model.Change(self.project)
        change.branch = 'master'
        change.cache_stat = Dummy(key=Dummy(reference=uuid.uuid4().hex))
        item = self.queue.enqueueChanges([change], None)

        self.assertTrue(base.changeMatchesBranch(self.tenant, change))
        self.assertTrue(python27.changeMatchesBranch(self.tenant, change))

        with self.zk_context as ctx:
            item.freezeJobGraph(self.layout, ctx,
                                skip_file_matcher=False,
                                redact_secrets_and_keys=False)
        self.assertEqual(len(item.getJobs()), 1)
        job = item.getJobs()[0]
        self.assertEqual(job.name, 'python27')

        pre_idx = job.pre_run[0]['secrets']['mysecret']
        pre_secret = yaml.encrypted_load(
            job.secrets[pre_idx]['encrypted_data'])
        expected = {
            'secret_data': secret1_data,
            'secret_oidc': {},
        }
        self.assertEqual(expected, pre_secret)

        # Verify that they were deduplicated
        pre2_idx = job.pre_run[0]['secrets']['othersecret']
        self.assertEqual(pre_idx, pre2_idx)

        # Verify that the second secret is distinct
        run_idx = job.run[0]['secrets']['mysecret']
        run_secret = yaml.encrypted_load(
            job.secrets[run_idx]['encrypted_data'])
        expected = {
            'secret_data': secret2_data,
            'secret_oidc': {},
        }
        self.assertEqual(expected, run_secret)

    def _test_job_override_control(self, attr, job_attr,
                                   default, default_value,
                                   inherit, inherit_value,
                                   override, override_value,
                                   errors,
                                   ):
        # Default behavior
        data = configloader.safe_load_yaml(default, self.context)
        parent = self.pcontext.job_parser.fromYaml(data[0]['job'])
        child = self.pcontext.job_parser.fromYaml(data[1]['job'])
        job = parent.copy()
        job.setBase(self.layout, None)
        job.applyVariant(child, self.layout, None)
        self.assertEqual(default_value, getattr(job, job_attr))

        # Explicit inherit
        data = configloader.safe_load_yaml(inherit, self.context)
        parent = self.pcontext.job_parser.fromYaml(data[0]['job'])
        child = self.pcontext.job_parser.fromYaml(data[1]['job'])
        job = parent.copy()
        job.setBase(self.layout, None)
        job.applyVariant(child, self.layout, None)
        self.assertEqual(inherit_value, getattr(job, job_attr))

        # Explicit override
        data = configloader.safe_load_yaml(override, self.context)
        parent = self.pcontext.job_parser.fromYaml(data[0]['job'])
        child = self.pcontext.job_parser.fromYaml(data[1]['job'])
        job = parent.copy()
        job.setBase(self.layout, None)
        job.applyVariant(child, self.layout, None)
        self.assertEqual(override_value, getattr(job, job_attr))

        # Make sure we can't put the override in the wrong place
        for conf, exc in errors:
            with testtools.ExpectedException(exc):
                data = configloader.safe_load_yaml(conf, self.context)
                child = self.pcontext.job_parser.fromYaml(data[0]['job'])

    def _test_job_override_control_set(
            self, attr, job_attr=None,
            default_override=False,
            value_factory=lambda values: {v for v in values}):
        if job_attr is None:
            job_attr = attr
        default = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}: parent-{attr}
            - job:
                name: child
                {attr}: child-{attr}
            """)

        inherit = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}: parent-{attr}
            - job:
                name: child
                {attr}: !inherit child-{attr}
            """)
        inherit_value = value_factory([f'parent-{attr}', f'child-{attr}'])

        override = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}: parent-{attr}
            - job:
                name: child
                {attr}: !override [child-{attr}]
            """)
        override_value = value_factory([f'child-{attr}'])

        if default_override:
            default_value = override_value
        else:
            default_value = inherit_value

        errors = [(textwrap.dedent(
            f"""
            - job:
                name: child
                {attr}: [!override child-{attr}]
            """), voluptuous.error.MultipleInvalid)]
        self._test_job_override_control(attr, job_attr,
                                        default, default_value,
                                        inherit, inherit_value,
                                        override, override_value,
                                        errors)

    def test_job_override_control_tags(self):
        self._test_job_override_control_set('tags')

    def test_job_override_control_requires(self):
        self._test_job_override_control_set('requires')

    def test_job_override_control_provides(self):
        self._test_job_override_control_set('provides')

    def test_job_override_control_include_vars(self):
        self._test_job_override_control_set(
            'include-vars',
            job_attr='include_vars',
            value_factory=lambda values:
                [model.JobIncludeVars(v, 'git.example.com/project', True, True)
                 for v in values]
        )

    def test_job_override_control_dependencies(self):
        self._test_job_override_control_set(
            'dependencies',
            default_override=True,
            value_factory=lambda values:
            {model.JobDependency(v) for v in values})

    def test_job_override_control_failure_output(self):
        self._test_job_override_control_set(
            'failure-output',
            job_attr='failure_output',
            value_factory=lambda values: tuple(v for v in sorted(values)))

    def test_job_override_control_files(self):
        self._test_job_override_control_set(
            'files',
            job_attr='file_matcher',
            default_override=True,
            value_factory=lambda values: change_matcher.MatchAnyFiles(
                [change_matcher.FileMatcher(ZuulRegex(v))
                 for v in sorted(values)]))

    def test_job_override_control_irrelevant_files(self):
        self._test_job_override_control_set(
            'irrelevant-files',
            job_attr='irrelevant_file_matcher',
            default_override=True,
            value_factory=lambda values: change_matcher.MatchAllFiles(
                [change_matcher.FileMatcher(ZuulRegex(v))
                 for v in sorted(values)]))

    def _test_job_override_control_dict(
            self, attr, job_attr=None,
            default_override=False):
        if job_attr is None:
            job_attr = attr
        default = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                  parent: 1
            - job:
                name: child
                {attr}:
                  child: 2
            """)

        inherit = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                   parent: 1
            - job:
                name: child
                {attr}: !inherit
                   child: 2
            """)
        inherit_value = {'parent': 1, 'child': 2}

        override = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                   parent: 1
            - job:
                name: child
                {attr}: !override
                   child: 2
            """)
        override_value = {'child': 2}

        if default_override:
            default_value = override_value
        else:
            default_value = inherit_value

        errors = [
            (textwrap.dedent(
                f"""
                - job:
                    name: child
                    {attr}:
                      !override child: 2
                """), voluptuous.error.MultipleInvalid),
            (textwrap.dedent(
                f"""
                - job:
                    name: child
                    {attr}:
                      child: !override 2
                """), Exception),
        ]
        self._test_job_override_control(attr, job_attr,
                                        default, default_value,
                                        inherit, inherit_value,
                                        override, override_value,
                                        errors)

    def test_job_override_control_vars(self):
        self._test_job_override_control_dict(
            'vars', job_attr='variables')

    def test_job_override_control_extra_vars(self):
        self._test_job_override_control_dict(
            'extra-vars', job_attr='extra_variables')

    def _test_job_override_control_host_dict(
            self, attr, job_attr=None,
            default_override=False):
        if job_attr is None:
            job_attr = attr
        default = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                  host:
                    parent: 1
            - job:
                name: child
                {attr}:
                  host:
                    child: 2
            """)

        inherit = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                   host:
                     parent: 1
            - job:
                name: child
                {attr}: !inherit
                   host:
                     child: 2
            """)
        inherit_value = {'host': {'parent': 1, 'child': 2}}

        override = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                   host:
                     parent: 1
            - job:
                name: child
                {attr}: !override
                   host:
                     child: 2
            """)
        override_value = {'host': {'child': 2}}

        if default_override:
            default_value = override_value
        else:
            default_value = inherit_value

        errors = [
            (textwrap.dedent(
                f"""
                - job:
                    name: child
                    {attr}:
                      !override host:
                        child: 2
                """), voluptuous.error.MultipleInvalid),
            (textwrap.dedent(
                f"""
                - job:
                    name: child
                    {attr}:
                      host: !override
                        child: 2
                """), voluptuous.error.MultipleInvalid),
            (textwrap.dedent(
                f"""
                - job:
                    name: child
                    {attr}:
                      host:
                        child: !override 2
            """), Exception),
        ]
        self._test_job_override_control(attr, job_attr,
                                        default, default_value,
                                        inherit, inherit_value,
                                        override, override_value,
                                        errors)

    def test_job_override_control_host_vars(self):
        self._test_job_override_control_host_dict(
            'host-vars', job_attr='host_variables')

    def test_job_override_control_group_vars(self):
        self._test_job_override_control_host_dict(
            'group-vars', job_attr='group_variables')

    def test_job_override_control_required_projects(self):
        parent = model.Project('parent-project', self.source)
        child = model.Project('child-project', self.source)
        parent_tpc = model.TenantProjectConfig(parent)
        child_tpc = model.TenantProjectConfig(child)
        self.tenant.addTPC(parent_tpc)
        self.tenant.addTPC(child_tpc)

        default = textwrap.dedent(
            """
            - job:
                name: parent
                required-projects: parent-project
            - job:
                name: child
                required-projects: child-project
            """)

        inherit = textwrap.dedent(
            """
            - job:
                name: parent
                required-projects: parent-project
            - job:
                name: child
                required-projects: !inherit child-project
            """)
        inherit_value = {
            'git.example.com/parent-project': model.JobProject(
                'git.example.com/parent-project'),
            'git.example.com/child-project': model.JobProject(
                'git.example.com/child-project'),
        }

        override = textwrap.dedent(
            """
            - job:
                name: parent
                required-projects: parent-project
            - job:
                name: child
                required-projects: !override [child-project]
            """)
        override_value = {
            'git.example.com/child-project': model.JobProject(
                'git.example.com/child-project'),
        }

        default_value = inherit_value

        errors = [(textwrap.dedent(
            """
            - job:
                name: child
                required-projects: [!override child-project]
            """), voluptuous.error.MultipleInvalid)]
        self._test_job_override_control('required-projects',
                                        'required_projects',
                                        default, default_value,
                                        inherit, inherit_value,
                                        override, override_value,
                                        errors)

    def _test_job_final_control(self, attr, job_attr,
                                default, default_value,
                                final, mutate_value=None):
        # Default behavior
        data = configloader.safe_load_yaml(default, self.context)
        parent = self.pcontext.job_parser.fromYaml(data[0]['job'])
        child = self.pcontext.job_parser.fromYaml(data[1]['job'])
        job = parent.copy()
        job.setBase(self.layout, None)
        job.applyVariant(child, self.layout, None)
        value = getattr(job, job_attr)
        if mutate_value:
            value = mutate_value(value)
        self.assertEqual(default_value, value)

        # Verify final attr exception
        data = configloader.safe_load_yaml(final, self.context)
        parent = self.pcontext.job_parser.fromYaml(data[0]['job'])
        child = self.pcontext.job_parser.fromYaml(data[1]['job'])
        job = parent.copy()
        job.setBase(self.layout, None)
        with testtools.ExpectedException(model.JobConfigurationError,
                                         ".* final attribute"):
            job.applyVariant(child, self.layout, None)

    def _test_job_final_control_set(
            self, attr, job_attr=None,
            default_override=False,
            value_factory=lambda values: {v for v in values}):
        if job_attr is None:
            job_attr = attr
        default = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}: parent-{attr}
            - job:
                name: child
                {attr}: child-{attr}
            """)
        inherit_value = value_factory([f'parent-{attr}', f'child-{attr}'])
        override_value = value_factory([f'child-{attr}'])
        if default_override:
            default_value = override_value
        else:
            default_value = inherit_value

        final = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}: parent-{attr}
                attribute-control:
                  {attr}:
                    final: true
            - job:
                name: child
                {attr}: child-{attr}
            """)

        self._test_job_final_control(attr, job_attr,
                                     default, default_value,
                                     final)

    def test_job_final_control_tags(self):
        self._test_job_final_control_set('tags')

    def test_job_final_control_requires(self):
        self._test_job_final_control_set('requires')

    def test_job_final_control_provides(self):
        self._test_job_final_control_set('provides')

    def test_job_final_control_dependencies(self):
        self._test_job_final_control_set(
            'dependencies',
            default_override=True,
            value_factory=lambda values:
            {model.JobDependency(v) for v in values})

    def test_job_final_control_failure_output(self):
        self._test_job_final_control_set(
            'failure-output',
            job_attr='failure_output',
            value_factory=lambda values: tuple(v for v in sorted(values)))

    def test_job_final_control_files(self):
        self._test_job_final_control_set(
            'files',
            job_attr='file_matcher',
            default_override=True,
            value_factory=lambda values: change_matcher.MatchAnyFiles(
                [change_matcher.FileMatcher(ZuulRegex(v))
                 for v in sorted(values)]))

    def test_job_final_control_irrelevant_files(self):
        self._test_job_final_control_set(
            'irrelevant-files',
            job_attr='irrelevant_file_matcher',
            default_override=True,
            value_factory=lambda values: change_matcher.MatchAllFiles(
                [change_matcher.FileMatcher(ZuulRegex(v))
                 for v in sorted(values)]))

    def _test_job_final_control_dict(
            self, attr, job_attr=None,
            default_override=False):
        if job_attr is None:
            job_attr = attr
        default = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                  parent: 1
            - job:
                name: child
                {attr}:
                  child: 2
            """)

        final = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                   parent: 1
                attribute-control:
                  {attr}:
                    final: true
            - job:
                name: child
                {attr}:
                   child: 2
            """)
        inherit_value = {'parent': 1, 'child': 2}
        default_value = {'child': 2}

        if default_override:
            default_value = default_value
        else:
            default_value = inherit_value

        self._test_job_final_control(attr, job_attr,
                                     default, default_value,
                                     final)

    def test_job_final_control_vars(self):
        self._test_job_final_control_dict(
            'vars', job_attr='variables')

    def test_job_final_control_extra_vars(self):
        self._test_job_final_control_dict(
            'extra-vars', job_attr='extra_variables')

    def _test_job_final_control_host_dict(
            self, attr, job_attr=None,
            default_override=False):
        if job_attr is None:
            job_attr = attr
        default = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                  host:
                    parent: 1
            - job:
                name: child
                {attr}:
                  host:
                    child: 2
            """)

        final = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}:
                   host:
                     parent: 1
                attribute-control:
                  {attr}:
                    final: true
            - job:
                name: child
                {attr}:
                   host:
                     child: 2
            """)
        inherit_value = {'host': {'parent': 1, 'child': 2}}
        default_value = {'host': {'child': 2}}

        if default_override:
            default_value = default_value
        else:
            default_value = inherit_value

        self._test_job_final_control(attr, job_attr,
                                     default, default_value,
                                     final)

    def test_job_final_control_host_vars(self):
        self._test_job_final_control_host_dict(
            'host-vars', job_attr='host_variables')

    def test_job_final_control_group_vars(self):
        self._test_job_final_control_host_dict(
            'group-vars', job_attr='group_variables')

    def test_job_final_control_required_projects(self):
        parent = model.Project('parent-project', self.source)
        child = model.Project('child-project', self.source)
        parent_tpc = model.TenantProjectConfig(parent)
        child_tpc = model.TenantProjectConfig(child)
        self.tenant.addTPC(parent_tpc)
        self.tenant.addTPC(child_tpc)

        default = textwrap.dedent(
            """
            - job:
                name: parent
                required-projects: parent-project
            - job:
                name: child
                required-projects: child-project
            """)

        final = textwrap.dedent(
            """
            - job:
                name: parent
                required-projects: parent-project
                attribute-control:
                  required-projects:
                    final: true
            - job:
                name: child
                required-projects: child-project
            """)

        default_value = {
            'git.example.com/parent-project': model.JobProject(
                'git.example.com/parent-project'),
            'git.example.com/child-project': model.JobProject(
                'git.example.com/child-project'),
        }

        self._test_job_final_control('required-projects',
                                     'required_projects',
                                     default, default_value,
                                     final)

    def test_job_final_control_type(self):
        default = textwrap.dedent(
            """
            - job:
                name: parent
                type: initializer
            - job:
                name: child
                type: regular
            """)

        final = textwrap.dedent(
            """
            - job:
                name: parent
                type: initializer
                attribute-control:
                  type:
                    final: true
            - job:
                name: child
                type: regular
            """)

        default_value = 'regular'

        self._test_job_final_control('type',
                                     'type',
                                     default, default_value,
                                     final)

    def test_job_final_control_voting(self):
        default = textwrap.dedent(
            """
            - job:
                name: parent
                voting: true
            - job:
                name: child
                voting: false
            """)

        final = textwrap.dedent(
            """
            - job:
                name: parent
                voting: true
                attribute-control:
                  voting:
                    final: true
            - job:
                name: child
                voting: false
            """)

        default_value = False

        self._test_job_final_control('voting',
                                     'voting',
                                     default, default_value,
                                     final)

    def _test_job_final_control_playbooks(self, attr, job_attr,
                                          default_value):
        default = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}: parent-playbook
            - job:
                name: child
                {attr}: child-playbook
            """)

        final = textwrap.dedent(
            f"""
            - job:
                name: parent
                {attr}: parent-playbook
                attribute-control:
                  {attr}:
                    final: true
            - job:
                name: child
                {attr}: child-playbook
            """)

        def mutate_value(playbooks):
            return [
                playbook_context.path for playbook_context in playbooks
            ]

        self._test_job_final_control(attr, job_attr,
                                     default, default_value,
                                     final, mutate_value=mutate_value)

    def test_job_final_control_pre_run(self):
        default_value = ['parent-playbook', 'child-playbook']
        self._test_job_final_control_playbooks('pre-run', 'pre_run',
                                               default_value)

    def test_job_final_control_run(self):
        default_value = ['child-playbook']
        self._test_job_final_control_playbooks('run', 'run', default_value)

    def test_job_final_control_post_run(self):
        default_value = ['child-playbook', 'parent-playbook']
        self._test_job_final_control_playbooks('post-run', 'post_run',
                                               default_value)

    @mock.patch("zuul.model.zkobject.ZKObject._save")
    def test_image_permissions(self, save_mock):
        self.pipeline.post_review = False
        image = self.pcontext.image_parser.fromYaml({
            '_source_context': self.context,
            '_start_mark': self.start_mark,
            'name': 'image',
            'type': 'zuul',
        })
        self.layout.addImage(image)

        job_project = model.Project('other-project', self.source)
        job_context = model.SourceContext(
            job_project.canonical_name, job_project.name,
            job_project.connection_name, 'master', 'test')
        tpc = model.TenantProjectConfig(job_project)
        tpc.trusted = True
        self.tenant.addTPC(tpc)

        job = self.pcontext.job_parser.fromYaml({
            '_source_context': job_context,
            '_start_mark': self.start_mark,
            'name': 'job',
            'parent': None,
            'post-review': True,
            'image-build-name': 'image',
        }, None)

        self.layout.addJob(job)

        project_config = self.pcontext.project_parser.fromYaml({
            '_source_context': job_context,
            '_start_mark': self.start_mark,
            'name': 'other-project',
            'gate': {
                'jobs': [
                    'job'
                ]
            }
        })

        self.layout.addProjectConfig(job_project.canonical_name,
                                     project_config)

        change = model.Change(job_project)
        # Test master
        change.branch = 'master'
        change.cache_stat = Dummy(key=Dummy(reference=uuid.uuid4().hex))
        item = self.queue.enqueueChanges([change], None)
        with testtools.ExpectedException(
                model.JobConfigurationError,
                "The image build job"):
            with self.zk_context as ctx:
                item.freezeJobGraph(self.layout, ctx,
                                    skip_file_matcher=False,
                                    redact_secrets_and_keys=False)


class FakeFrozenJob(model.Job):

    def __init__(self, name):
        super().__init__(name)
        self.uuid = uuid.uuid4().hex
        self.ref = 'fake reference'
        self.all_refs = [self.ref]
        self.matches_change = True

    def _set(self, **kw):
        self.__dict__.update(kw)


class TestGraph(BaseTestCase):
    def setUp(self):
        COMPONENT_REGISTRY.registry = Dummy()
        COMPONENT_REGISTRY.registry.model_api = MODEL_API
        super().setUp()

    def test_job_graph_disallows_circular_dependencies(self):
        jobs = [FakeFrozenJob('job%d' % i) for i in range(0, 10)]

        def setup_graph():
            graph = model.JobGraph({})
            prevjob = None
            for j in jobs[:3]:
                if prevjob:
                    j.dependencies = frozenset([
                        model.JobDependency(prevjob.name)])
                graph.addJob(j)
                prevjob = j
            # 0 triggers 1 triggers 2 triggers 3...
            return graph

        # Cannot depend on itself
        graph = setup_graph()
        j = FakeFrozenJob('jobX')
        j.dependencies = frozenset([model.JobDependency(j.name)])
        graph.addJob(j)
        with testtools.ExpectedException(
                Exception,
                "Dependency cycle detected in job jobX"):
            graph.freezeDependencies(self.log)

        # Disallow circular dependencies
        graph = setup_graph()
        jobs[3].dependencies = frozenset([model.JobDependency(jobs[4].name)])
        graph.addJob(jobs[3])
        jobs[4].dependencies = frozenset([model.JobDependency(jobs[3].name)])
        graph.addJob(jobs[4])
        with testtools.ExpectedException(
                Exception,
                "Dependency cycle detected in job job3"):
            graph.freezeDependencies(self.log)

        graph = setup_graph()
        jobs[3].dependencies = frozenset([model.JobDependency(jobs[5].name)])
        graph.addJob(jobs[3])
        jobs[4].dependencies = frozenset([model.JobDependency(jobs[3].name)])
        graph.addJob(jobs[4])
        jobs[5].dependencies = frozenset([model.JobDependency(jobs[4].name)])
        graph.addJob(jobs[5])

        with testtools.ExpectedException(
                Exception,
                "Dependency cycle detected in job job3"):
            graph.freezeDependencies(self.log)

        graph = setup_graph()
        jobs[3].dependencies = frozenset([model.JobDependency(jobs[2].name)])
        graph.addJob(jobs[3])
        jobs[4].dependencies = frozenset([model.JobDependency(jobs[3].name)])
        graph.addJob(jobs[4])
        jobs[5].dependencies = frozenset([model.JobDependency(jobs[4].name)])
        graph.addJob(jobs[5])
        jobs[6].dependencies = frozenset([model.JobDependency(jobs[2].name)])
        graph.addJob(jobs[6])
        graph.freezeDependencies(self.log)

    def test_job_graph_allows_soft_dependencies(self):
        parent = FakeFrozenJob('parent')
        child = FakeFrozenJob('child')
        child.dependencies = frozenset([
            model.JobDependency(parent.name, True)])

        # With the parent
        graph = model.JobGraph({})
        graph.addJob(parent)
        graph.addJob(child)
        graph.freezeDependencies(self.log)
        self.assertEqual(graph.getParentJobsRecursively(child),
                         [parent])

        # Skip the parent
        graph = model.JobGraph({})
        graph.addJob(child)
        graph.freezeDependencies(self.log)
        self.assertEqual(graph.getParentJobsRecursively(child), [])

    def test_job_graph_allows_soft_dependencies4(self):
        # A more complex scenario with multiple parents at each level
        parents = [FakeFrozenJob('parent%i' % i) for i in range(6)]
        child = FakeFrozenJob('child')
        child.dependencies = frozenset([
            model.JobDependency(parents[0].name, True),
            model.JobDependency(parents[1].name)])
        parents[0].dependencies = frozenset([
            model.JobDependency(parents[2].name),
            model.JobDependency(parents[3].name, True)])
        parents[1].dependencies = frozenset([
            model.JobDependency(parents[4].name),
            model.JobDependency(parents[5].name)])
        # Run them all
        graph = model.JobGraph({})
        for j in parents:
            graph.addJob(j)
        graph.addJob(child)
        graph.freezeDependencies(self.log)
        self.assertEqual(set(graph.getParentJobsRecursively(child)),
                         set(parents))

        # Skip first parent, therefore its recursive dependencies don't appear
        graph = model.JobGraph({})
        for j in parents:
            if j is not parents[0]:
                graph.addJob(j)
        graph.addJob(child)
        graph.freezeDependencies(self.log)
        self.assertEqual(set(graph.getParentJobsRecursively(child)),
                         set(parents) -
                         set([parents[0], parents[2], parents[3]]))

        # Skip a leaf node
        graph = model.JobGraph({})
        for j in parents:
            if j is not parents[3]:
                graph.addJob(j)
        graph.addJob(child)
        graph.freezeDependencies(self.log)
        self.assertEqual(set(graph.getParentJobsRecursively(child)),
                         set(parents) - set([parents[3]]))

    def test_soft_dependency_mixed_cycle(self):
        # This is a regression test to make sure we are not processing
        # jobs twice, when mixing soft/hard dependencies to a job.
        graph = model.JobGraph({})
        parent = FakeFrozenJob("parent")
        graph.addJob(parent)

        intermediate = FakeFrozenJob("intermediate")
        intermediate.dependencies = frozenset([
            # Hard dependency to parent
            model.JobDependency(parent.name)
        ])
        graph.addJob(intermediate)

        child = FakeFrozenJob("child")
        child.dependencies = frozenset([
            # Soft dependency to parent
            model.JobDependency(parent.name, soft=True),
            # Dependency to intermediate, which has a hard dependency
            # to parent.
            model.JobDependency(intermediate.name),
        ])
        graph.addJob(child)

        # We don't expect this to raise an exception
        graph.freezeDependencies(self.log)


class TestTenant(BaseTestCase):
    def test_add_project(self):
        tenant = model.Tenant('tenant')
        connection1 = Dummy(connection_name='dummy_connection1')
        source1 = Dummy(canonical_hostname='git1.example.com',
                        name='dummy',  # TODOv3(jeblair): remove
                        connection=connection1)

        source1_project1 = model.Project('project1', source1)
        source1_project1_tpc = model.TenantProjectConfig(source1_project1)
        source1_project1_tpc.trusted = True
        tenant.addTPC(source1_project1_tpc)
        d = {'project1':
             {'git1.example.com': source1_project1}}
        self.assertEqual(d, tenant.projects)
        self.assertEqual((True, source1_project1),
                         tenant.getProject('project1'))
        self.assertEqual((True, source1_project1),
                         tenant.getProject('git1.example.com/project1'))

        source1_project2 = model.Project('project2', source1)
        tpc = model.TenantProjectConfig(source1_project2)
        tenant.addTPC(tpc)
        d = {'project1':
             {'git1.example.com': source1_project1},
             'project2':
             {'git1.example.com': source1_project2}}
        self.assertEqual(d, tenant.projects)
        self.assertEqual((False, source1_project2),
                         tenant.getProject('project2'))
        self.assertEqual((False, source1_project2),
                         tenant.getProject('git1.example.com/project2'))

        connection2 = Dummy(connection_name='dummy_connection2')
        source2 = Dummy(canonical_hostname='git2.example.com',
                        name='dummy',  # TODOv3(jeblair): remove
                        connection=connection2)

        source2_project1 = model.Project('project1', source2)
        tpc = model.TenantProjectConfig(source2_project1)
        tenant.addTPC(tpc)
        d = {'project1':
             {'git1.example.com': source1_project1,
              'git2.example.com': source2_project1},
             'project2':
             {'git1.example.com': source1_project2}}
        self.assertEqual(d, tenant.projects)
        with testtools.ExpectedException(
                Exception,
                "Project name 'project1' is ambiguous"):
            tenant.getProject('project1')
        self.assertEqual((False, source1_project2),
                         tenant.getProject('project2'))
        self.assertEqual((True, source1_project1),
                         tenant.getProject('git1.example.com/project1'))
        self.assertEqual((False, source2_project1),
                         tenant.getProject('git2.example.com/project1'))

        source2_project2 = model.Project('project2', source2)
        tpc = model.TenantProjectConfig(source2_project2)
        tpc.trusted = True
        tenant.addTPC(tpc)
        d = {'project1':
             {'git1.example.com': source1_project1,
              'git2.example.com': source2_project1},
             'project2':
             {'git1.example.com': source1_project2,
              'git2.example.com': source2_project2}}
        self.assertEqual(d, tenant.projects)
        with testtools.ExpectedException(
                Exception,
                "Project name 'project1' is ambiguous"):
            tenant.getProject('project1')
        with testtools.ExpectedException(
                Exception,
                "Project name 'project2' is ambiguous"):
            tenant.getProject('project2')
        self.assertEqual((True, source1_project1),
                         tenant.getProject('git1.example.com/project1'))
        self.assertEqual((False, source2_project1),
                         tenant.getProject('git2.example.com/project1'))
        self.assertEqual((False, source1_project2),
                         tenant.getProject('git1.example.com/project2'))
        self.assertEqual((True, source2_project2),
                         tenant.getProject('git2.example.com/project2'))

        source1_project2b = model.Project('subpath/project2', source1)
        tpc = model.TenantProjectConfig(source1_project2b)
        tpc.trusted = True
        tenant.addTPC(tpc)
        d = {'project1':
             {'git1.example.com': source1_project1,
              'git2.example.com': source2_project1},
             'project2':
             {'git1.example.com': source1_project2,
              'git2.example.com': source2_project2},
             'subpath/project2':
             {'git1.example.com': source1_project2b}}
        self.assertEqual(d, tenant.projects)
        self.assertEqual((False, source1_project2),
                         tenant.getProject('git1.example.com/project2'))
        self.assertEqual((True, source2_project2),
                         tenant.getProject('git2.example.com/project2'))
        self.assertEqual((True, source1_project2b),
                         tenant.getProject('subpath/project2'))
        self.assertEqual(
            (True, source1_project2b),
            tenant.getProject('git1.example.com/subpath/project2'))

        source2_project2b = model.Project('subpath/project2', source2)
        tpc = model.TenantProjectConfig(source2_project2b)
        tpc.trusted = True
        tenant.addTPC(tpc)
        d = {'project1':
             {'git1.example.com': source1_project1,
              'git2.example.com': source2_project1},
             'project2':
             {'git1.example.com': source1_project2,
              'git2.example.com': source2_project2},
             'subpath/project2':
             {'git1.example.com': source1_project2b,
              'git2.example.com': source2_project2b}}
        self.assertEqual(d, tenant.projects)
        self.assertEqual((False, source1_project2),
                         tenant.getProject('git1.example.com/project2'))
        self.assertEqual((True, source2_project2),
                         tenant.getProject('git2.example.com/project2'))
        with testtools.ExpectedException(
                Exception,
                "Project name 'subpath/project2' is ambiguous"):
            tenant.getProject('subpath/project2')
        self.assertEqual(
            (True, source1_project2b),
            tenant.getProject('git1.example.com/subpath/project2'))
        self.assertEqual(
            (True, source2_project2b),
            tenant.getProject('git2.example.com/subpath/project2'))

        with testtools.ExpectedException(
                Exception,
                "Project project1 is already in project index"):
            tenant._addProject(source1_project1_tpc)


class TestRef(BaseTestCase):
    def test_ref_equality(self):
        change1 = model.Change('project1')
        change1.ref = '/change1'
        change1b = model.Change('project1')
        change1b.ref = '/change1'
        change2 = model.Change('project2')
        change2.ref = '/change2'
        self.assertFalse(change1.equals(change2))
        self.assertTrue(change1.equals(change1b))

        tag1 = model.Tag('project1')
        tag1.ref = '/tag1'
        tag1b = model.Tag('project1')
        tag1b.ref = '/tag1'
        tag2 = model.Tag('project2')
        tag2.ref = '/tag2'
        self.assertFalse(tag1.equals(tag2))
        self.assertTrue(tag1.equals(tag1b))

        self.assertFalse(tag1.equals(change1))

        branch1 = model.Branch('project1')
        branch1.ref = '/branch1'
        branch1b = model.Branch('project1')
        branch1b.ref = '/branch1'
        branch2 = model.Branch('project2')
        branch2.ref = '/branch2'
        self.assertFalse(branch1.equals(branch2))
        self.assertTrue(branch1.equals(branch1b))

        self.assertFalse(branch1.equals(change1))
        self.assertFalse(branch1.equals(tag1))


class TestSourceContext(BaseTestCase):
    def setUp(self):
        super().setUp()
        COMPONENT_REGISTRY.registry = Dummy()
        COMPONENT_REGISTRY.registry.model_api = MODEL_API
        self.connection = Dummy(connection_name='dummy_connection')
        self.source = Dummy(canonical_hostname='git.example.com',
                            connection=self.connection)
        self.project = model.Project('project', self.source)
        self.context = model.SourceContext(
            self.project.canonical_name, self.project.name,
            self.project.connection_name, 'master', 'test')
        self.context.implied_branches = [
            change_matcher.BranchMatcher(ZuulRegex('foo')),
            change_matcher.ImpliedBranchMatcher(ZuulRegex('foo')),
        ]

    def test_serialize(self):
        self.context.deserialize(self.context.serialize())
