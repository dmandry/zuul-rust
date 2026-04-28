# Copyright 2017 Red Hat, Inc.
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
import fixtures
import logging
import textwrap
import testtools
import voluptuous as vs
from collections import defaultdict
from configparser import ConfigParser

from zuul import model
from zuul.lib.ansible import AnsibleManager
from zuul.configloader import (
    AuthorizationRuleParser, AuthorizationRoleParser,
    ConfigLoader, safe_load_yaml
)
from zuul.exceptions import AuthZRoleNotFoundError, AuthZRuleNotFoundError
from zuul.model import Abide, MergeRequest, SourceContext
from zuul.zk.locks import tenant_read_lock

from tests.base import (
    ZuulTestCase,
    iterate_timeout,
    okay_tracebacks,
    simple_layout,
)
from tests.unit.test_launcher import LauncherBaseTestCase


class TestConfigLoader(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_update_system_config(self):
        """Test if the system config can be updated without a scheduler."""
        sched = self.scheds.first.sched

        # Get the current system config before instantiating a ConfigLoader.
        unparsed_abide, zuul_globals = sched.system_config_cache.get()
        ansible_manager = AnsibleManager(
            default_version=zuul_globals.default_ansible_version)
        loader = ConfigLoader(
            sched.connections, sched.system, self.zk_client, zuul_globals,
            sched.unparsed_config_cache, sched.statsd,
            keystorage=sched.keystore)
        abide = Abide()
        loader.loadTPCs(abide, unparsed_abide)
        loader.loadAuthzRules(abide, unparsed_abide)

        for tenant_name in unparsed_abide.tenants:
            tlock = tenant_read_lock(self.zk_client, tenant_name, self.log)
            # Consider all caches valid (min. ltime -1)
            min_ltimes = defaultdict(lambda: defaultdict(lambda: -1))
            with tlock:
                tenant = loader.loadTenant(
                    abide, tenant_name, ansible_manager, unparsed_abide,
                    min_ltimes=min_ltimes)

                self.assertEqual(tenant.name, tenant_name)

    @okay_tracebacks(' _processCatJob')
    def test_merge_job_cleanup(self):
        # When issuing a full reconfiguration or a tenant validation, we
        # cannot hold the merge jobs in queue as the configloader is
        # explicitly waiting for the cat jobs to complete. Thus, instead
        # of holding the merge jobs we directly fail those cat jobs to
        # trigger the merge request cleanup.
        self.merger_api.failJobs(MergeRequest.CAT)
        self.executor_server.merger_api.failJobs(MergeRequest.CAT)
        self.assertRaises(
            Exception,
            lambda: self.scheds.execute(lambda app: app.sched.validateTenants(
                app.config, {'tenant-one'})),
        )

        self.waitUntilSettled()

        # Even if the merge jobs are canceled, there could be a race
        # condition where an executor picks up a merge request and
        # re-creates the result znode in ZK after the request got
        # deleted. To prevent assertCleanZooKeeper to fail this test
        # even if the cancelation was successful, we have to manually
        # clean up lost merge requests.
        self.merger_api.cleanup()

        # As we are explicitly failing cat jobs we are expecting
        # validateTenants() to fail with an exception.
        # As this catches other exceptions that might be raised when
        # canceling the merge jobs, there is no proper way to validate
        # this apart from checking the test/exception logs for specific
        # messages.
        for record in self._exception_logs:
            if "Unable to cancel job" in record.msg:
                self.fail(
                    f"Exception while canceling merge jobs: {record.msg}")


class TenantParserTestCase(ZuulTestCase):
    create_project_keys = True

    JOB_SET = set([
        'pipeline', 'job', 'semaphore', 'project',
        'project-template', 'nodeset', 'secret', 'queue',
    ])
    PROVIDER_SET = set([
        'image', 'flavor', 'label', 'section', 'provider',
    ])
    UNTRUSTED_SET = JOB_SET
    CONFIG_SET = UNTRUSTED_SET | PROVIDER_SET

    def setupAllProjectKeys(self, config: ConfigParser):
        for project in ['common-config', 'org/project1', 'org/project2']:
            self.setupProjectKeys('gerrit', project)


class TestTenantSimple(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/simple.yaml'

    def test_tenant_simple(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2'],
                         [x.name for x in tenant.untrusted_projects])

        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[1]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET, tpc.load_classes)
        self.assertTrue('common-config-job' in tenant.layout.jobs)
        self.assertTrue('project1-job' in tenant.layout.jobs)
        self.assertTrue('project2-job' in tenant.layout.jobs)
        project1_config = tenant.layout.project_configs.get(
            'review.example.com/org/project1')
        self.assertTrue('common-config-job' in
                        project1_config[0].pipelines['check'].job_list.jobs)
        self.assertTrue('project1-job' in
                        project1_config[1].pipelines['check'].job_list.jobs)
        project2_config = tenant.layout.project_configs.get(
            'review.example.com/org/project2')
        self.assertTrue('common-config-job' in
                        project2_config[0].pipelines['check'].job_list.jobs)
        self.assertTrue('project2-job' in
                        project2_config[1].pipelines['check'].job_list.jobs)

    def test_cache(self):
        # A full reconfiguration should issue cat jobs for all repos
        with self.assertLogs('zuul.TenantParser', level='DEBUG') as full_logs:
            self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
            self.waitUntilSettled()
            self.log.debug("Full reconfigure logs:")
            for x in full_logs.output:
                self.log.debug(x)
            self.assertRegexInList(
                r'Submitting cat job (.*?) for gerrit common-config master',
                full_logs.output)
            self.assertRegexInList(
                r'Submitting cat job (.*?) for gerrit org/project1 master',
                full_logs.output)
            self.assertRegexInList(
                r'Submitting cat job (.*?) for gerrit org/project2 master',
                full_logs.output)
            self.assertRegexNotInList(
                r'Using files from cache',
                full_logs.output)

        first = self.scheds.first
        second = self.createScheduler()
        second.start()
        self.assertEqual(len(self.scheds), 2)
        for _ in iterate_timeout(10, "until priming is complete"):
            state_one = first.sched.local_layout_state.get("tenant-one")
            if state_one:
                break

        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (second.sched.local_layout_state.get(
                    "tenant-one") == state_one):
                break

        self.log.debug("Freeze scheduler-1")
        # Start the log context manager for the update test below now,
        # so that it's already in place when we release the second
        # scheduler lock.
        with self.assertLogs('zuul.TenantParser', level='DEBUG'
                             ) as update_logs:
            lock1 = second.sched.layout_update_lock
            lock2 = second.sched.run_handler_lock
            with lock1, lock2:
                # A tenant reconfiguration should use the cache except for the
                # updated project.
                file_dict = {'zuul.d/test.yaml': ''}

                # Now start a second log context manager just for the
                # tenant reconfig test
                with self.assertLogs('zuul.TenantParser', level='DEBUG') \
                     as tenant_logs:
                    A = self.fake_gerrit.addFakeChange(
                        'org/project1', 'master', 'A',
                        files=file_dict)
                    A.setMerged()
                    self.fake_gerrit.addEvent(A.getChangeMergedEvent())
                    self.waitUntilSettled(matcher=[first])
                    self.log.debug("Tenant reconfigure logs:")
                    for x in tenant_logs.output:
                        self.log.debug(x)

                    self.assertRegexNotInList(
                        r'Submitting cat job (.*?) for '
                        r'gerrit common-config master',
                        tenant_logs.output)
                    self.assertRegexInList(
                        r'Submitting cat job (.*?) for '
                        r'gerrit org/project1 master',
                        tenant_logs.output)
                    self.assertRegexNotInList(
                        r'Submitting cat job (.*?) for '
                        r'gerrit org/project2 master',
                        tenant_logs.output)
                    self.assertRegexNotInList(
                        r'Using files from cache',
                        tenant_logs.output)

            # A layout update should use the unparsed config cache
            # except for what needs to be refreshed from the files
            # cache in ZK.
            self.log.debug("Thaw scheduler-1")
            state_one = first.sched.local_layout_state.get("tenant-one")
            for _ in iterate_timeout(
                    10, "all schedulers to have the same layout state"):
                if (second.sched.local_layout_state.get(
                        "tenant-one") == state_one):
                    break
            self.log.debug("Layout update logs:")
            for x in update_logs.output:
                self.log.debug(x)

            self.assertRegexNotInList(
                r'Submitting cat job',
                update_logs.output)
            self.assertRegexNotInList(
                r'Using files from cache for project '
                r'review.example.com/common-config @master.*',
                update_logs.output)
            self.assertRegexInList(
                r'Using files from cache for project '
                r'review.example.com/org/project1 @master.*',
                update_logs.output)
            self.assertRegexNotInList(
                r'Using files from cache for project '
                r'review.example.com/org/project2 @master.*',
                update_logs.output)

    @okay_tracebacks('_cacheTenantYAMLBranch')
    def test_cache_new_branch(self):
        # This tests scheduler startup right after a new branch is
        # created.
        first = self.scheds.first
        lock1 = first.sched.layout_update_lock
        lock2 = first.sched.run_handler_lock
        with lock1, lock2:
            self.create_branch('org/project1', 'stable')
            self.fake_gerrit.addEvent(
                self.fake_gerrit.getFakeBranchCreatedEvent(
                    'org/project1', 'stable'))

            # Start another scheduler and prime it while the existing
            # scheduler is paused and has not processed the new branch
            # creation event.  The new scheduler will encounter a
            # recoverable error with the branch min_ltimes.
            second = self.createScheduler()
            second.start()
            self.assertEqual(len(self.scheds), 2)
            for _ in iterate_timeout(10, "until priming is complete"):
                state_one = first.sched.local_layout_state.get("tenant-one")
                if state_one:
                    break

            for _ in iterate_timeout(
                    10, "all schedulers to have the same layout state"):
                if (second.sched.local_layout_state.get(
                        "tenant-one") == state_one):
                    break
        self.waitUntilSettled()

    def test_variant_description(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        job = tenant.layout.jobs.get("project2-job")
        self.assertEqual(job[0].variant_description, "")
        self.assertEqual(job[1].variant_description, "stable")

    def test_merge_anchor(self):
        to_parse = textwrap.dedent(
            """
            - job:
                name: job1
                vars: &docker_vars
                  registry: 'registry.example.org'

            - job:
                name: job2
                vars:
                  <<: &buildenv_vars
                    image_name: foo
                  <<: *docker_vars

            - job:
                name: job3
                vars:
                  <<: *buildenv_vars
                  <<: *docker_vars
            """)
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        project = list(tenant.config_projects)[0]
        source_context = SourceContext(
            project.canonical_name, project.name, project.connection_name,
            'master', 'zuul.yaml')

        data = safe_load_yaml(to_parse, source_context)
        self.assertEqual(len(data), 3)
        job_vars = [i['job']['vars'] for i in data]
        # Test that merging worked
        self.assertEqual(job_vars, [
            {'registry': 'registry.example.org'},
            {'registry': 'registry.example.org', 'image_name': 'foo'},
            {'registry': 'registry.example.org', 'image_name': 'foo'},
        ])

    def test_deny_localhost_nodeset(self):
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: localhost
                nodes:
                  - name: localhost
                    label: ubuntu
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # No job should have run due to the change introducing a config error
        self.assertHistory([])
        self.assertTrue(A.reported)
        self.assertTrue("Nodes named 'localhost' are not allowed."
                        in A.messages[0])

        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: localhost-group
                nodes:
                  - name: ubuntu
                    label: ubuntu
                groups:
                  - name: localhost
                    nodes: ubuntu
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # No job should have run due to the change introducing a config error
        self.assertHistory([])
        self.assertTrue(B.reported)
        self.assertTrue("Groups named 'localhost' are not allowed."
                        in B.messages[0])


class TestTenantOverride(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/override.yaml'

    def test_tenant_override(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2', 'org/project4'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET - set(['project']),
                         tpc.load_classes)
        project = list(tenant.untrusted_projects)[1]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(set(['job']), tpc.load_classes)
        self.assertTrue('common-config-job' in tenant.layout.jobs)
        self.assertTrue('project1-job' in tenant.layout.jobs)
        self.assertTrue('project2-job' in tenant.layout.jobs)
        project1_config = tenant.layout.project_configs.get(
            'review.example.com/org/project1')
        self.assertTrue('common-config-job' in
                        project1_config[0].pipelines['check'].job_list.jobs)
        self.assertFalse('project1-job' in
                         project1_config[0].pipelines['check'].job_list.jobs)
        project2_config = tenant.layout.project_configs.get(
            'review.example.com/org/project2')
        self.assertTrue('common-config-job' in
                        project2_config[0].pipelines['check'].job_list.jobs)
        self.assertFalse('project2-job' in
                         project2_config[0].pipelines['check'].job_list.jobs)


class TestTenantGroups(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/groups.yaml'

    def test_tenant_groups(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET - set(['project']),
                         tpc.load_classes)
        project = list(tenant.untrusted_projects)[1]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET - set(['project']),
                         tpc.load_classes)
        self.assertTrue('common-config-job' in tenant.layout.jobs)
        self.assertTrue('project1-job' in tenant.layout.jobs)
        self.assertTrue('project2-job' in tenant.layout.jobs)
        project1_config = tenant.layout.project_configs.get(
            'review.example.com/org/project1')
        self.assertTrue('common-config-job' in
                        project1_config[0].pipelines['check'].job_list.jobs)
        self.assertFalse('project1-job' in
                         project1_config[0].pipelines['check'].job_list.jobs)
        project2_config = tenant.layout.project_configs.get(
            'review.example.com/org/project2')
        self.assertTrue('common-config-job' in
                        project2_config[0].pipelines['check'].job_list.jobs)
        self.assertFalse('project2-job' in
                         project2_config[0].pipelines['check'].job_list.jobs)


class TestTenantGroups2(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/groups2.yaml'

    def test_tenant_groups2(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2', 'org/project3'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET - set(['project']),
                         tpc.load_classes)
        project = list(tenant.untrusted_projects)[1]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET - set(['project', 'job']),
                         tpc.load_classes)
        self.assertTrue('common-config-job' in tenant.layout.jobs)
        self.assertTrue('project1-job' in tenant.layout.jobs)
        self.assertFalse('project2-job' in tenant.layout.jobs)
        project1_config = tenant.layout.project_configs.get(
            'review.example.com/org/project1')
        self.assertTrue('common-config-job' in
                        project1_config[0].pipelines['check'].job_list.jobs)
        self.assertFalse('project1-job' in
                         project1_config[0].pipelines['check'].job_list.jobs)
        project2_config = tenant.layout.project_configs.get(
            'review.example.com/org/project2')
        self.assertTrue('common-config-job' in
                        project2_config[0].pipelines['check'].job_list.jobs)
        self.assertFalse('project2-job' in
                         project2_config[0].pipelines['check'].job_list.jobs)


class TestTenantGroups3(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/groups3.yaml'

    def test_tenant_groups3(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(False, tenant.exclude_unprotected_branches)
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(set(['job']), tpc.load_classes)
        project = list(tenant.untrusted_projects)[1]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(set(['project', 'job']), tpc.load_classes)
        self.assertTrue('common-config-job' in tenant.layout.jobs)
        self.assertTrue('project1-job' in tenant.layout.jobs)
        self.assertTrue('project2-job' in tenant.layout.jobs)
        project1_config = tenant.layout.project_configs.get(
            'review.example.com/org/project1')
        self.assertTrue('common-config-job' in
                        project1_config[0].pipelines['check'].job_list.jobs)
        self.assertFalse('project1-job' in
                         project1_config[0].pipelines['check'].job_list.jobs)
        project2_config = tenant.layout.project_configs.get(
            'review.example.com/org/project2')
        self.assertTrue('common-config-job' in
                        project2_config[0].pipelines['check'].job_list.jobs)
        self.assertTrue('project2-job' in
                        project2_config[1].pipelines['check'].job_list.jobs)


class TestTenantGroups4(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/groups4.yaml'

    def test_tenant_groups(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(set([]),
                         tpc.load_classes)
        project = list(tenant.untrusted_projects)[1]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(set([]),
                         tpc.load_classes)
        # Check that only one merger:cat job was requested
        # org/project1 and org/project2 have an empty load_classes
        self.assertEqual(1, len(self.merge_job_history.get(MergeRequest.CAT)))
        old_layout = tenant.layout

        # Check that creating a change in project1 doesn't cause a
        # reconfiguration (due to a mistaken belief that we need to
        # load config from it since there is none in memory).
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        new_layout = tenant.layout

        self.assertEqual(old_layout, new_layout)


class TestTenantGroups5(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/groups5.yaml'

    def test_tenant_single_projet_exclude(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(set([]),
                         tpc.load_classes)
        # Check that only one merger:cat job was requested
        # org/project1 and org/project2 have an empty load_classes
        self.assertEqual(1, len(self.merge_job_history.get(MergeRequest.CAT)))


class TestTenantFromScript(TestTenantSimple):
    tenant_config_file = None
    tenant_config_script_file = 'config/tenant-parser/tenant_config_script.py'

    def test_tenant_simple(self):
        TestTenantSimple.test_tenant_simple(self)


class TestTenantUnprotectedBranches(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/unprotected-branches.yaml'

    def test_tenant_unprotected_branches(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(True, tenant.exclude_unprotected_branches)

        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2'],
                         [x.name for x in tenant.untrusted_projects])

        tpc = tenant.project_configs
        project_name = list(tenant.config_projects)[0].canonical_name
        self.assertEqual(False, tpc[project_name].exclude_unprotected_branches)

        project_name = list(tenant.untrusted_projects)[0].canonical_name
        self.assertIsNone(tpc[project_name].exclude_unprotected_branches)

        project_name = list(tenant.untrusted_projects)[1].canonical_name
        self.assertIsNone(tpc[project_name].exclude_unprotected_branches)


class TestTenantIncludeBranches(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/include-branches.yaml'

    def test_tenant_branches(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')

        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2'],
                         [x.name for x in tenant.untrusted_projects])

        tpc = tenant.project_configs
        project_name = list(tenant.config_projects)[0].canonical_name
        self.assertEqual(['master'], tpc[project_name].branches)

        # No branches pass the filter at the start
        project_name = list(tenant.untrusted_projects)[0].canonical_name
        self.assertEqual([], tpc[project_name].branches)

        # Create the foo branch
        self.create_branch('org/project1', 'foo')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'foo'))
        self.waitUntilSettled()

        # It should pass the filter
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        tpc = tenant.project_configs
        project_name = list(tenant.untrusted_projects)[0].canonical_name
        self.assertEqual(['foo'], tpc[project_name].branches)

        # Create the baz branch
        self.create_branch('org/project1', 'baz')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'baz'))
        self.waitUntilSettled()

        # It should not pass the filter
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        tpc = tenant.project_configs
        project_name = list(tenant.untrusted_projects)[0].canonical_name
        self.assertEqual(['foo'], tpc[project_name].branches)


class TestTenantExcludeBranches(TestTenantIncludeBranches):
    tenant_config_file = 'config/tenant-parser/exclude-branches.yaml'

    # Same test results as include-branches


class TestTenantExcludeIncludeBranches(TestTenantIncludeBranches):
    tenant_config_file = 'config/tenant-parser/exclude-include-branches.yaml'

    # Same test results as include-branches


class TestTenantExcludeAll(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/exclude-all.yaml'

    def test_tenant_exclude_all(self):
        """
        Tests that excluding all configuration of project1 in tenant-one
        doesn't remove the configuration of project1 in tenant-two.
        """
        # The config in org/project5 depends on config in org/project1 so
        # validate that there are no config errors in that tenant.
        tenant_two = self.scheds.first.sched.abide.tenants.get('tenant-two')
        self.assertEqual(
            len(tenant_two.layout.loading_errors), 0,
            "No error should have been accumulated")


class TestTenantConfigBranches(ZuulTestCase):
    tenant_config_file = 'config/tenant-parser/simple.yaml'

    def _validate_job(self, job, branch):
        tenant_one = self.scheds.first.sched.abide.tenants.get('tenant-one')
        jobs = tenant_one.layout.getJobs(job)
        self.assertEqual(len(jobs), 1)
        self.assertIn(jobs[0].source_context.branch, branch)

    def test_tenant_config_load_branch(self):
        """
        Tests that when specifying branches for a project only those branches
        are parsed.
        """
        # Job must be defined in master
        common_job = 'common-config-job'
        self._validate_job(common_job, 'master')

        self.log.debug('Creating branches')
        self.create_branch('common-config', 'stable')
        self.create_branch('common-config', 'feat_x')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'common-config', 'stable'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'common-config', 'feat_x'))
        self.waitUntilSettled()

        # Job must be defined in master
        self._validate_job(common_job, 'master')

        # Reconfigure with load-branch stable for common-config
        self.newTenantConfig('config/tenant-parser/branch.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        # Now job must be defined on stable branch
        self._validate_job(common_job, 'stable')

        # Now try to break the config in common-config on stable
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: base
                parent: non-existing
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('common-config', 'stable', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # No job should have run due to the change introducing a config error
        self.assertHistory([])
        self.assertTrue(A.reported)
        self.assertTrue('Job non-existing not defined' in A.messages[0])


class TestSplitConfig(ZuulTestCase):
    tenant_config_file = 'config/split-config/main.yaml'

    def test_split_config(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertIn('project-test1', tenant.layout.jobs)
        self.assertIn('project-test2', tenant.layout.jobs)
        test1 = tenant.layout.getJob('project-test1')
        self.assertEqual(test1.source_context.project_name, 'common-config')
        self.assertEqual(test1.source_context.branch, 'master')
        self.assertEqual(test1.source_context.path, 'zuul.d/jobs.yaml')
        self.assertTrue(
            tenant.isTrusted(test1.source_context.project_canonical_name),
            True)
        test2 = tenant.layout.getJob('project-test2')
        self.assertEqual(test2.source_context.project_name, 'common-config')
        self.assertEqual(test2.source_context.branch, 'master')
        self.assertEqual(test2.source_context.path, 'zuul.d/more-jobs.yaml')
        self.assertTrue(
            tenant.isTrusted(test2.source_context.project_canonical_name),
            True)

        self.assertNotEqual(test1.source_context, test2.source_context)
        self.assertTrue(test1.source_context.isSameProject(
            test2.source_context))

        project_config = tenant.layout.project_configs.get(
            'review.example.com/org/project')
        self.assertIn('project-test1',
                      project_config[0].pipelines['check'].job_list.jobs)
        project1_config = tenant.layout.project_configs.get(
            'review.example.com/org/project1')
        self.assertIn('project1-project2-integration',
                      project1_config[0].pipelines['check'].job_list.jobs)

        # This check ensures the .zuul.ignore flag file is working in
        # the config directory.
        self.assertEqual(
            len(tenant.layout.loading_errors), 0)

    def test_dynamic_split_config(self):
        in_repo_conf = textwrap.dedent(
            """
            - project:
                name: org/project1
                check:
                  jobs:
                    - project-test1
            """)
        file_dict = {'.zuul.d/gate.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        # project1-project2-integration test removed, only want project-test1
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS', changes='1,1')])

    def test_config_path_conflict(self):
        def add_file(project, path):
            new_file = textwrap.dedent(
                """
                - job:
                    name: test-job
                """
            )
            file_dict = {path: new_file}
            A = self.fake_gerrit.addFakeChange(project, 'master', 'A',
                                               files=file_dict)
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
            self.waitUntilSettled()
            return A

        log_fixture = self.useFixture(
            fixtures.FakeLogger(level=logging.WARNING))

        log_fixture._output.truncate(0)
        A = add_file("common-config", "zuul.yaml")
        self.assertIn("Configuration in common-config/zuul.d/jobs.yaml@master "
                      "ignored because project-branch is already configured",
                      log_fixture.output)
        self.assertIn("Configuration in common-config/zuul.d/jobs.yaml@master "
                      "ignored because project-branch is already configured",
                      A.messages[0])

        log_fixture._output.truncate(0)
        add_file("org/project1", ".zuul.yaml")
        self.assertIn("Configuration in org/project1/.zuul.d/gate.yaml@master "
                      "ignored because project-branch is already configured",
                      log_fixture.output)


class TestConfigConflict(ZuulTestCase):
    tenant_config_file = 'config/conflict-config/main.yaml'

    def test_conflict_config(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        jobs = sorted(tenant.layout.jobs.keys())
        self.assertEqual(
            ['base', 'noop', 'trusted-zuul.yaml-job',
             'untrusted-zuul.yaml-job'],
            jobs)

        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 4)

        for error in errors:
            self.assertEqual(error.severity, model.SEVERITY_WARNING)
            self.assertEqual(error.name, 'Multiple Project Configurations')


class TestUnparsedConfigCache(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_config_caching(self):
        sched = self.scheds.first.sched
        cache = sched.unparsed_config_cache
        tenant = sched.abide.tenants["tenant-one"]

        common_cache = cache.getFilesCache("review.example.com/common-config",
                                           "master")
        object_common_cache = sched.abide.getConfigObjectCache(
            "review.example.com/common-config", "master")
        tpc = tenant.project_configs["review.example.com/common-config"]
        self.assertTrue(common_cache.isValidFor(tpc, min_ltime=-1))
        self.assertEqual(len(common_cache), 1)
        self.assertIn("zuul.yaml", common_cache)
        self.assertTrue(len(common_cache["zuul.yaml"]) > 0)
        self.assertEqual(object_common_cache.entries['zuul.yaml'].ltime,
                         common_cache.ltime)

        project_cache = cache.getFilesCache("review.example.com/org/project",
                                            "master")
        object_project_cache = sched.abide.getConfigObjectCache(
            "review.example.com/org/project", "master")
        # Cache of org/project should be valid but empty (no in-repo config)
        tpc = tenant.project_configs["review.example.com/org/project"]
        self.assertTrue(project_cache.isValidFor(tpc, min_ltime=-1))
        self.assertEqual(len(project_cache), 0)
        self.assertEqual(object_project_cache.entries['zuul.yaml'].ltime,
                         project_cache.ltime)

    @okay_tracebacks('cannot schedule new futures after shutdown')
    def test_cache_use(self):
        sched = self.scheds.first.sched
        # Stop cleanup jobs so it's not removing projects from
        # the cache during the test.
        sched.apsched.shutdown()
        tenant = sched.abide.tenants['tenant-one']
        _, project = tenant.getProject('org/project2')

        cache = self.scheds.first.sched.unparsed_config_cache
        files_cache = cache.getFilesCache(
            "review.example.com/org/project2", "master")
        zk_initial_ltime = files_cache.ltime
        config_object_cache = sched.abide.getConfigObjectCache(
            "review.example.com/org/project2", "master")
        self.assertEqual(zk_initial_ltime,
                         config_object_cache.entries['zuul.yaml'].ltime)

        # Get the current ltime from Zookeeper and run a full reconfiguration,
        # so that we know all items in the cache have a larger ltime.
        ltime = self.zk_client.getCurrentLtime()
        self.scheds.first.fullReconfigure()
        self.assertGreater(files_cache.ltime, zk_initial_ltime)
        config_object_cache = sched.abide.getConfigObjectCache(
            "review.example.com/org/project2", "master")
        self.assertEqual(files_cache.ltime,
                         config_object_cache.entries['zuul.yaml'].ltime)

        # Clear the unparsed branch cache so all projects (except for
        # org/project2) are retrieved from the cache in Zookeeper.
        sched.abide.config_object_cache.clear()
        del self.merge_job_history

        # Create a tenant reconfiguration event with a known ltime that is
        # smaller than the ltime of the items in the cache.
        event = model.TenantReconfigureEvent(
            tenant.name, project.canonical_name, branch_name=None)
        event.zuul_event_ltime = ltime
        sched.management_events[tenant.name].put(event, needs_result=False)
        self.waitUntilSettled()

        # As the cache should be valid (cache ltime of org/project2 newer than
        # event ltime) we don't expect any cat jobs.
        self.assertIsNone(self.merge_job_history.get(MergeRequest.CAT))

        # Set canary value so we can detect if the configloader used
        # the cache in Zookeeper (it shouldn't).
        common_cache = cache.getFilesCache("review.example.com/common-config",
                                           "master")
        common_cache.setValidFor({"CANARY"}, set(), common_cache.ltime)
        del self.merge_job_history

        # Create a tenant reconfiguration event with a known ltime that is
        # smaller than the ltime of the items in the cache.
        event = model.TenantReconfigureEvent(
            tenant.name, project.canonical_name, branch_name=None)
        event.zuul_event_ltime = ltime
        sched.management_events[tenant.name].put(event, needs_result=False)
        self.waitUntilSettled()

        config_object_cache = sched.abide.getConfigObjectCache(
            "review.example.com/common-config", "master")
        self.assertEqual(common_cache.ltime,
                         config_object_cache.entries['zuul.yaml'].ltime)
        self.assertNotIn("CANARY", config_object_cache.entries)

        # As the cache should be valid (cache ltime of org/project2 newer than
        # event ltime) we don't expect any cat jobs.
        self.assertIsNone(self.merge_job_history.get(MergeRequest.CAT))
        sched.apsched.start()


class TestAuthorizationRuleParser(ZuulTestCase):
    tenant_config_file = 'config/tenant-parser/authorizations.yaml'

    def test_rules_are_loaded(self):
        rules = self.scheds.first.sched.abide.authz_rules
        self.assertTrue('auth-rule-one' in rules,
                        self.scheds.first.sched.abide)
        self.assertTrue('auth-rule-two' in rules,
                        self.scheds.first.sched.abide)
        claims_1 = {'sub': 'venkman'}
        claims_2 = {'sub': 'gozer',
                    'iss': 'another_dimension'}
        self.assertTrue(rules['auth-rule-one'](claims_1))
        self.assertTrue(not rules['auth-rule-one'](claims_2))
        self.assertTrue(not rules['auth-rule-two'](claims_1))
        self.assertTrue(rules['auth-rule-two'](claims_2))

    def test_parse_simplest_rule_from_yaml(self):
        rule_d = {'name': 'my-rule',
                  'conditions': {'sub': 'user1'}
                 }
        rule = AuthorizationRuleParser().fromYaml(rule_d)
        self.assertEqual('my-rule', rule.name)
        claims = {'iss': 'my-idp',
                  'sub': 'user1',
                  'groups': ['admin', 'ghostbusters']}
        self.assertTrue(rule(claims))
        claims = {'iss': 'my-2nd-idp',
                  'sub': 'user2',
                  'groups': ['admin', 'ghostbusters']}
        self.assertFalse(rule(claims))

    def test_parse_AND_rule_from_yaml(self):
        rule_d = {'name': 'my-rule',
                  'conditions': {'sub': 'user1',
                                 'iss': 'my-idp'}
                 }
        rule = AuthorizationRuleParser().fromYaml(rule_d)
        self.assertEqual('my-rule', rule.name)
        claims = {'iss': 'my-idp',
                  'sub': 'user1',
                  'groups': ['admin', 'ghostbusters']}
        self.assertTrue(rule(claims))
        claims = {'iss': 'my-2nd-idp',
                  'sub': 'user1',
                  'groups': ['admin', 'ghostbusters']}
        self.assertFalse(rule(claims))

    def test_parse_OR_rule_from_yaml(self):
        rule_d = {'name': 'my-rule',
                  'conditions': [{'sub': 'user1',
                                  'iss': 'my-idp'},
                                 {'sub': 'user2',
                                  'iss': 'my-2nd-idp'}
                                ]
                 }
        rule = AuthorizationRuleParser().fromYaml(rule_d)
        self.assertEqual('my-rule', rule.name)
        claims = {'iss': 'my-idp',
                  'sub': 'user1',
                  'groups': ['admin', 'ghostbusters']}
        self.assertTrue(rule(claims))
        claims = {'iss': 'my-2nd-idp',
                  'sub': 'user1',
                  'groups': ['admin', 'ghostbusters']}
        self.assertFalse(rule(claims))
        claims = {'iss': 'my-2nd-idp',
                  'sub': 'user2',
                  'groups': ['admin', 'ghostbusters']}
        self.assertTrue(rule(claims))

    def test_parse_rule_with_list_claim_from_yaml(self):
        rule_d = {'name': 'my-rule',
                  'conditions': [{'groups': 'ghostbusters',
                                  'iss': 'my-idp'},
                                 {'sub': 'user2',
                                  'iss': 'my-2nd-idp'}
                                ],
                 }
        rule = AuthorizationRuleParser().fromYaml(rule_d)
        self.assertEqual('my-rule', rule.name)
        claims = {'iss': 'my-idp',
                  'sub': 'user1',
                  'groups': ['admin', 'ghostbusters']}
        self.assertTrue(rule(claims))
        claims = {'iss': 'my-idp',
                  'sub': 'user1',
                  'groups': ['admin', 'ghostbeaters']}
        self.assertFalse(rule(claims))
        claims = {'iss': 'my-2nd-idp',
                  'sub': 'user2',
                  'groups': ['admin', 'ghostbusters']}
        self.assertTrue(rule(claims))

    def test_check_complex_rule_from_yaml_jsonpath(self):
        rule_d = {'name': 'my-rule',
                  'conditions': [{'hello.this.is': 'a complex value'},
                                ],
                 }
        rule = AuthorizationRuleParser().fromYaml(rule_d)
        self.assertEqual('my-rule', rule.name)
        claims = {'iss': 'my-idp',
                  'hello': {
                      'this': {
                          'is': 'a complex value'
                      },
                      'and': {
                          'this one': 'too'
                      }
                  }
                 }
        self.assertTrue(rule(claims))

    def test_check_complex_rule_from_yaml_nested_dict(self):
        rule_d = {'name': 'my-rule',
                  'conditions': [{'hello': {'this': {'is': 'a complex value'
                                                    }
                                           }
                                 },
                                ],
                 }
        rule = AuthorizationRuleParser().fromYaml(rule_d)
        self.assertEqual('my-rule', rule.name)
        claims = {'iss': 'my-idp',
                  'hello': {
                      'this': {
                          'is': 'a complex value'
                      },
                      'and': {
                          'this one': 'too'
                      }
                  }
                 }
        self.assertTrue(rule(claims))


class TestAuthorizationRuleParserWithTemplating(ZuulTestCase):
    tenant_config_file = 'config/tenant-parser/authorizations-templating.yaml'

    def test_rules_are_loaded(self):
        rules = self.scheds.first.sched.abide.authz_rules
        self.assertTrue('tenant-admin' in rules, self.scheds.first.sched.abide)
        self.assertTrue('tenant-admin-complex' in rules,
                        self.scheds.first.sched.abide)

    def test_tenant_substitution(self):
        claims_1 = {'group': 'tenant-one-admin'}
        claims_2 = {'group': 'tenant-two-admin'}
        rules = self.scheds.first.sched.abide.authz_rules
        tenant_one = self.scheds.first.sched.abide.tenants.get('tenant-one')
        tenant_two = self.scheds.first.sched.abide.tenants.get('tenant-two')
        self.assertTrue(rules['tenant-admin'](claims_1, tenant_one))
        self.assertTrue(rules['tenant-admin'](claims_2, tenant_two))
        self.assertTrue(not rules['tenant-admin'](claims_1, tenant_two))
        self.assertTrue(not rules['tenant-admin'](claims_2, tenant_one))

    def test_tenant_substitution_in_list(self):
        claims_1 = {'group': ['tenant-one-admin', 'some-other-tenant']}
        claims_2 = {'group': ['tenant-two-admin', 'some-other-tenant']}
        rules = self.scheds.first.sched.abide.authz_rules
        tenant_one = self.scheds.first.sched.abide.tenants.get('tenant-one')
        tenant_two = self.scheds.first.sched.abide.tenants.get('tenant-two')
        self.assertTrue(rules['tenant-admin'](claims_1, tenant_one))
        self.assertTrue(rules['tenant-admin'](claims_2, tenant_two))
        self.assertTrue(not rules['tenant-admin'](claims_1, tenant_two))
        self.assertTrue(not rules['tenant-admin'](claims_2, tenant_one))

    def test_tenant_substitution_in_dict(self):
        claims_2 = {
            'path': {
                'to': {
                    'group': 'tenant-two-admin'
                }
            }
        }
        rules = self.scheds.first.sched.abide.authz_rules
        tenant_one = self.scheds.first.sched.abide.tenants.get('tenant-one')
        tenant_two = self.scheds.first.sched.abide.tenants.get('tenant-two')
        self.assertTrue(not rules['tenant-admin-complex'](claims_2,
                                                          tenant_one))
        self.assertTrue(rules['tenant-admin-complex'](claims_2, tenant_two))


class TestAuthorizationRoleParser(ZuulTestCase):
    tenant_config_file = 'config/tenant-parser/rbac.yaml'

    def test_roles_are_loaded(self):
        roles = self.scheds.first.sched.abide.authz_roles
        self.assertTrue('admin' in roles,
                        self.scheds.first.sched.abide)
        self.assertTrue('read' in roles,
                        self.scheds.first.sched.abide)
        self.assertTrue('enqueue' in roles,
                        self.scheds.first.sched.abide)
        self.assertTrue(roles['enqueue'].permissions['enqueue'])
        self.assertTrue('dequeue1' in roles,
                        self.scheds.first.sched.abide)
        self.assertEqual(
            roles['dequeue1'].permissions['dequeue']['conditions'],
            {'project': 'org/project'})
        self.assertTrue('dequeue2' in roles,
                        self.scheds.first.sched.abide)
        self.assertEqual(
            roles['dequeue2'].permissions['dequeue']['conditions'],
            {'ref': 'refs/heads/stable/foo'})
        self.assertTrue('dequeue3' in roles,
                        self.scheds.first.sched.abide)
        self.assertEqual(
            roles['dequeue3'].permissions['dequeue']['conditions'],
            {'project': 'org/project',
             'ref': 'refs/heads/stable/foo'})
        self.assertTrue('build_image' in roles,
                        self.scheds.first.sched.abide)
        self.assertTrue(roles['build_image'].permissions['build-image'])

    def test_invalid_override_of_reserved_roles(self):
        # Check that valid names are fine
        role = {'name': 'enqueue',
                'permissions': {'enqueue': True}
               }
        AuthorizationRoleParser().fromYaml(role)
        # Now check that the two reserved role names can't be overridden.
        role = {'name': 'admin',
                'permissions': {'enqueue': True}
               }
        self.assertRaises(vs.error.MultipleInvalid,
                          AuthorizationRoleParser().fromYaml, role)
        role = {'name': 'read',
                'permissions': {'enqueue': True}
               }
        self.assertRaises(vs.error.MultipleInvalid,
                          AuthorizationRoleParser().fromYaml, role)

    def test_invalid_rbac_conditions(self):
        # Check valid conditions
        role = {
            'name': 'enqueue',
            'permissions': {
                'enqueue': {
                    'conditions': {
                        'project': 'foo/bar',
                        'ref': 'refs/heads/featuretesting'
                    }
                }
            }
        }
        AuthorizationRoleParser().fromYaml(role)
        # Now check extra invalid conditions
        role['permissions']['enqueue']['conditions']['job'] = 'atestjob'
        self.assertRaises(vs.error.MultipleInvalid,
                          AuthorizationRoleParser().fromYaml, role)
        # Now check the lack of conditions
        del role['permissions']['enqueue']['conditions']['project']
        del role['permissions']['enqueue']['conditions']['ref']
        del role['permissions']['enqueue']['conditions']['job']
        self.assertRaises(vs.error.MultipleInvalid,
                          AuthorizationRoleParser().fromYaml, role)

    def test_role_shouldnt_have_conditions(self):
        # Check that conditions don't apply to some permissions
        role = {
            'name': 'autohold',
            'permissions': {
                'autohold': {
                    'conditions': {
                        'project': 'foo/bar',
                    }
                }
            }
        }
        self.assertRaises(vs.error.MultipleInvalid,
                          AuthorizationRoleParser().fromYaml, role)
        # Now check valid role construction
        role['permissions']['autohold'] = True
        AuthorizationRoleParser().fromYaml(role)


class TestTenantInvalidRoleInRoleMappings(ZuulTestCase):
    tenant_config_file = 'config/tenant-parser/invalid_role_rbac.yaml'

    # This test raises a config error during the startup of the test
    # case which makes the first scheduler fail during its startup.
    # The second (or any additional) scheduler won't even run as the
    # startup is serialized in tests/base.py.
    # Thus it doesn't make sense to execute this test with multiple
    # schedulers.
    scheduler_count = 1

    def setUp(self):
        err = 'The Authorization Role "not_a_valid_role" was not found.'
        with testtools.ExpectedException(AuthZRoleNotFoundError, err):
            super().setUp()

    def test_tenant_invalid_role_in_role_mappings(self):
        # The magic is in setUp
        pass


class TestTenantInvalidRuleInRoleMappings(ZuulTestCase):
    tenant_config_file = 'config/tenant-parser/invalid_rule_rbac.yaml'

    # This test raises a config error during the startup of the test
    # case which makes the first scheduler fail during its startup.
    # The second (or any additional) scheduler won't even run as the
    # startup is serialized in tests/base.py.
    # Thus it doesn't make sense to execute this test with multiple
    # schedulers.
    scheduler_count = 1

    def setUp(self):
        err = 'The Authorization Rule "auth-rule-three" was not found.'
        with testtools.ExpectedException(AuthZRuleNotFoundError, err):
            super().setUp()

    def test_tenant_invalid_rule_in_role_mappings(self):
        # The magic is in setUp
        pass


class TestTenantExtra(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/extra.yaml'

    def test_tenant_extra(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertTrue('project2-extra-file' in tenant.layout.jobs)
        self.assertTrue('project2-extra-dir' in tenant.layout.jobs)
        self.assertTrue('project6-extra-dir' in tenant.layout.jobs)

    def test_dynamic_extra(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project2-extra-file2
                parent: common-config-job
            - project:
                name: org/project2
                check:
                  jobs:
                    - project2-extra-file2
            """)
        file_dict = {'extra.yaml': in_repo_conf, '.zuul.yaml': ''}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='common-config-job', result='SUCCESS', changes='1,1'),
            dict(name='project2-extra-file2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_dynamic_extra_dir(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project6-extra-dir2
                parent: common-config-job
            - project:
                check:
                  jobs:
                    - project6-extra-dir
                    - project6-extra-dir2
            """)
        file_dict = {
            'other/extra.d/new/extra.yaml': in_repo_conf,
        }
        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project6-extra-dir', result='SUCCESS', changes='1,1'),
            dict(name='project6-extra-dir2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_extra_reconfigure(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project2-extra-file2
                parent: common-config-job
            - project:
                name: org/project2
                check:
                  jobs:
                    - project2-extra-file2
            """)
        file_dict = {'extra.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(A.getRefUpdatedEvent())
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='common-config-job', result='SUCCESS', changes='2,1'),
            dict(name='project2-job', result='SUCCESS', changes='2,1'),
            dict(name='project2-extra-file2', result='SUCCESS', changes='2,1'),
        ], ordered=False)


class TestTenantExtraConfigsInvalidType(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/extra_invalid_type.yaml'

    # This test raises a config error during the startup of the test
    # case which makes the first scheduler fail during its startup.
    # The second (or any additional) scheduler won't even run as the
    # startup is serialized in tests/base.py.
    # Thus it doesn't make sense to execute this test with multiple
    # schedulers.
    scheduler_count = 1

    def setUp(self):
        err = "Expected str or list of str for extra-config-paths.*"
        with testtools.ExpectedException(vs.MultipleInvalid, err):
            super().setUp()

    def test_tenant_extra_configs_invalid_type(self):
        # The magic is in setUp
        pass


class TestTenantExtraConfigsInvalidValue(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/extra_invalid_value.yaml'

    # This test raises a config error during the startup of the test
    # case which makes the first scheduler fail during its startup.
    # The second (or any additional) scheduler won't even run as the
    # startup is serialized in tests/base.py.
    # Thus it doesn't make sense to execute this test with multiple
    # schedulers.
    scheduler_count = 1

    def setUp(self):
        err = "Default zuul configs are not allowed in extra-config-paths.*"
        with testtools.ExpectedException(vs.MultipleInvalid, err):
            super().setUp()

    def test_tenant_extra_configs_invalid_value(self):
        # The magic is in setUp
        pass


class TestTenantDuplicate(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/duplicate.yaml'

    # This test raises a config error during the startup of the test
    # case which makes the first scheduler fail during its startup.
    # The second (or any additional) scheduler won't even run as the
    # startup is serialized in tests/base.py.
    # Thus it doesn't make sense to execute this test with multiple
    # schedulers.
    scheduler_count = 1

    def setUp(self):
        with testtools.ExpectedException(Exception, 'Duplicate configuration'):
            super().setUp()

    def test_tenant_dupe(self):
        # The magic is in setUp
        pass


class TestTenantTPCLoadingFailure(TenantParserTestCase):
    tenant_config_file = ('config/tenant-parser/'
                          'simple.yaml')
    scheduler_count = 1

    @okay_tracebacks('may not configure')
    def test_tenant_tpc_loading_failure(self):
        # This tests the behavior when we fail to load TPCs,
        # especially after a previously successful loading.  If we
        # fail to fully load information about a tenant, it can be
        # dangerous to proceed with run loop or cleanup handling in
        # the scheduler.

        # The test starts with a valid configuration.  This next
        # configuration raises an error during TPC loading
        self.newTenantConfig('config/tenant-parser/'
                             'superproject-config-project.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        # Counter-intuitively, the tenant *is* up to date on this
        # scheduler.  We are running with the previous configuration
        # since the new one didn't load.
        self.assertTrue(
            self.scheds.first.sched.isTenantLayoutUpToDate('tenant-one')
        )

        # Start a second scheduler which will not have the benefit of
        # a config in memory.
        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # In this case, the layout is not up to date since we don't
        # have anything to fall back on.
        self.assertFalse(
            app.sched.isTenantLayoutUpToDate('tenant-one')
        )


class TestTenantSuperprojectConfigProject(TenantParserTestCase):
    tenant_config_file = ('config/tenant-parser/'
                          'superproject-config-project.yaml')
    scheduler_count = 1

    def setUp(self):
        # Test that we get an error trying to configure a
        # config-project
        err = ".*may not configure config-project.*"
        with testtools.ExpectedException(Exception, err):
            super().setUp()

    def test_tenant_superproject_config_project(self):
        # The magic is in setUp
        pass


class TestTenantSuperprojectConfigProjectRegex(TenantParserTestCase):
    tenant_config_file = ('config/tenant-parser/'
                          'superproject-config-project-regex.yaml')
    scheduler_count = 1

    def setUp(self):
        # Test that we get an error trying to configure a
        # config-project via regex
        err = ".*may not configure config-project.*"
        with testtools.ExpectedException(Exception, err):
            super().setUp()

    def test_tenant_superproject_config_project_regex(self):
        # The magic is in setUp
        pass


class TestTenantConfigSuperproject(TenantParserTestCase):
    tenant_config_file = ('config/tenant-parser/'
                          'config-superproject.yaml')
    scheduler_count = 1

    def setUp(self):
        # Test that we get an error trying to use configure-projects
        # on a config-project
        with testtools.ExpectedException(vs.MultipleInvalid):
            super().setUp()

    def test_tenant_config_superproject(self):
        # The magic is in setUp
        pass


class TestTenantProvider(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/provider.yaml'

    def test_tenant_provider(self):
        # Test config/untrusted defaults
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET, tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET,
                         tpc.load_classes)


class TestTenantProvider2(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/provider2.yaml'

    def test_tenant_provider2(self):
        # Test the include-provider-config setting, and test it with
        # "exclude".
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.CONFIG_SET - set(['semaphore']),
                         tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.UNTRUSTED_SET | self.PROVIDER_SET,
                         tpc.load_classes)
        project = list(tenant.untrusted_projects)[1]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(
            (self.UNTRUSTED_SET | self.PROVIDER_SET) - set(['project']),
            tpc.load_classes)


class TestTenantProvider3(TenantParserTestCase):
    tenant_config_file = 'config/tenant-parser/provider3.yaml'

    def test_tenant_provider3(self):
        # Test the include-provider-config setting, and test it with
        # "include".
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(['common-config'],
                         [x.name for x in tenant.config_projects])
        self.assertEqual(['org/project1', 'org/project2'],
                         [x.name for x in tenant.untrusted_projects])
        project = list(tenant.config_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.PROVIDER_SET | set(['pipeline']),
                         tpc.load_classes)
        project = list(tenant.untrusted_projects)[0]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(self.PROVIDER_SET | set(['job']),
                         tpc.load_classes)
        project = list(tenant.untrusted_projects)[1]
        tpc = tenant.project_configs[project.canonical_name]
        self.assertEqual(
            self.PROVIDER_SET | set(['project']),
            tpc.load_classes)


class TestMergeMode(ZuulTestCase):
    config_file = 'zuul-connections-gerrit-and-github.conf'

    def _test_default_merge_mode(self, driver_default, host):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        md = layout.getProjectMetadata(
            f'{host}/org/project-empty')
        self.assertEqual(driver_default, md.merge_mode)
        md = layout.getProjectMetadata(
            f'{host}/org/regex-empty-project-empty')
        self.assertEqual(driver_default, md.merge_mode)
        md = layout.getProjectMetadata(
            f'{host}/org/regex-empty-project-squash')
        self.assertEqual(model.MERGER_SQUASH_MERGE, md.merge_mode)
        md = layout.getProjectMetadata(
            f'{host}/org/regex-cherry-project-empty')
        self.assertEqual(model.MERGER_CHERRY_PICK, md.merge_mode)
        md = layout.getProjectMetadata(
            f'{host}/org/regex-cherry-project-squash')
        self.assertEqual(model.MERGER_SQUASH_MERGE, md.merge_mode)

    @simple_layout('layouts/merge-mode-default.yaml')
    def test_default_merge_mode_gerrit(self):
        self._test_default_merge_mode(model.MERGER_MERGE_RESOLVE,
                                      'review.example.com')

    @simple_layout('layouts/merge-mode-default.yaml', driver='github')
    def test_default_merge_mode_github(self):
        self._test_default_merge_mode(model.MERGER_MERGE_ORT,
                                      'github.com')


class TestDefaultBranch(ZuulTestCase):
    config_file = 'zuul-connections-gerrit-and-github.conf'

    @simple_layout('layouts/default-branch.yaml')
    def test_default_branch(self):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        md = layout.getProjectMetadata(
            'review.example.com/org/project-default')
        self.assertEqual('master', md.default_branch)
        md = layout.getProjectMetadata(
            'review.example.com/org/regex-default-project-empty')
        self.assertEqual('master', md.default_branch)
        md = layout.getProjectMetadata(
            'review.example.com/org/regex-default-project-develop')
        self.assertEqual('develop', md.default_branch)
        md = layout.getProjectMetadata(
            'review.example.com/org/regex-override-project-empty')
        self.assertEqual('regex', md.default_branch)
        md = layout.getProjectMetadata(
            'review.example.com/org/regex-override-project-develop')
        self.assertEqual('develop', md.default_branch)

    @simple_layout('layouts/default-branch.yaml', driver='github')
    def test_default_branch_upstream(self):
        self.waitUntilSettled()
        github = self.fake_github.getGithubClient()
        repo = github.repo_from_project('org/project-default')
        repo._repodata['default_branch'] = 'foobar'
        connection = self.scheds.first.connections.connections['github']
        connection.clearBranchCache()
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        md = layout.getProjectMetadata(
            'github.com/org/project-default')
        self.assertEqual('foobar', md.default_branch)
        md = layout.getProjectMetadata(
            'github.com/org/regex-default-project-empty')
        self.assertEqual('master', md.default_branch)
        md = layout.getProjectMetadata(
            'github.com/org/regex-default-project-develop')
        self.assertEqual('develop', md.default_branch)
        md = layout.getProjectMetadata(
            'github.com/org/regex-override-project-empty')
        self.assertEqual('regex', md.default_branch)
        md = layout.getProjectMetadata(
            'github.com/org/regex-override-project-develop')
        self.assertEqual('develop', md.default_branch)


class TestNodepoolConfig(LauncherBaseTestCase):
    config_file = 'zuul-connections-nodepool.conf'

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_nodepool_config(self):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.images))
        image = layout.images['debian']
        self.assertEqual('debian', image.name)
        self.assertEqual('cloud', image.type)
        flavor = layout.flavors['normal']
        self.assertEqual('normal', flavor.name)
        label = layout.labels['debian-normal']
        self.assertEqual('debian-normal', label.name)
        self.assertEqual('debian', label.image)
        self.assertEqual('normal', label.flavor)
        label = layout.labels['debian-dedicated']
        self.assertEqual('debian-dedicated', label.name)
        self.assertEqual('debian', label.image)
        self.assertEqual('dedicated', label.flavor)
        label = layout.labels['debian-invalid']
        self.assertEqual('debian-invalid', label.name)
        self.assertEqual('debian', label.image)
        self.assertEqual('invalid', label.flavor)
        section = layout.sections['aws-base']
        self.assertEqual('aws-base', section.name)
        self.assertEqual(True, section.abstract)
        self.assertTrue('launch-timeout' in section.config)
        provider_config = layout.provider_configs['aws-us-east-1-main']
        self.assertEqual('aws-us-east-1-main', provider_config.name)
        self.assertEqual('aws-us-east-1', provider_config.section)
        provider = layout.providers['aws-us-east-1-main']
        self.assertEqual(4, len(provider.labels))
        labels = sorted([x for x in provider.labels.keys()])
        self.assertEqual('debian-dedicated', labels[0])
        self.assertEqual('debian-invalid', labels[1])
        self.assertEqual('debian-large', labels[2])
        self.assertEqual('debian-normal', labels[3])

    @simple_layout('layouts/nodepool-defaults.yaml', enable_nodepool=True)
    def test_nodepool_config_defaults(self):
        # Some notes about the current inheritance scheme:
        # 1) Providers inherit attributes from sections (and so on)
        #   A) Dictionaries are merged
        #   B) Flavors, images, labels lists behave as units
        #   C) Their defaults are applied at each inheritance level
        #   D) Later definitions completely override earlier ones
        #      (are not merged)
        # 2) Labels inherit some attributes from their images and flavors
        #   A) Labels override flavors override images
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout

        self.assertEqual(2, len(layout.images))
        image = layout.images['debian']
        self.assertEqual('debian', image.name)
        self.assertEqual('cloud', image.type)
        image = layout.images['debian-local']
        self.assertEqual('debian-local', image.name)
        self.assertEqual('zuul', image.type)

        self.assertEqual(2, len(layout.flavors))
        flavor = layout.flavors['normal']
        self.assertEqual('normal', flavor.name)
        flavor = layout.flavors['large']
        self.assertEqual('large', flavor.name)

        self.assertEqual(3, len(layout.labels))
        label = layout.labels['debian-normal']
        self.assertEqual('debian-normal', label.name)
        self.assertEqual('debian', label.image)
        self.assertEqual('normal', label.flavor)

        label = layout.labels['debian-large']
        self.assertEqual('debian-large', label.name)
        self.assertEqual('debian', label.image)
        self.assertEqual('large', label.flavor)
        label = layout.labels['debian-local-normal']
        self.assertEqual('debian-local-normal', label.name)
        self.assertEqual('debian-local', label.image)
        self.assertEqual('normal', label.flavor)

        section = layout.sections['aws-base']
        self.assertEqual('aws-base', section.name)
        self.assertEqual(True, section.abstract)
        self.assertTrue('launch-timeout' in section.config)
        provider_config = layout.provider_configs['aws-us-east-1-main']
        self.assertEqual('aws-us-east-1-main', provider_config.name)
        self.assertEqual('aws-us-east-1', provider_config.section)
        provider = layout.providers['aws-us-east-1-main']
        self.assertEqual(3, len(provider.labels))
        labels = sorted([x for x in provider.labels.keys()])
        self.assertEqual('debian-large', labels[0])
        self.assertEqual('debian-local-normal', labels[1])
        self.assertEqual('debian-normal', labels[2])

        # Check the inherited values
        label = provider.labels['debian-normal']
        self.assertEqual('gp2', label.volume_type)
        self.assertEqual(10, label.volume_size)
        self.assertEqual(40, label.iops)
        self.assertEqual(20, label.throughput)
        self.assertEqual(900, label.max_ready_age)
        self.assertEqual('required', label.imds_http_tokens)
        self.assertFalse(label.host_key_checking)
        self.assertEqual(['sg1'], label.security_group_ids)

        label = provider.labels['debian-large']
        self.assertEqual('gp3', label.volume_type)
        self.assertEqual(10, label.volume_size)
        self.assertEqual(70, label.iops)
        self.assertEqual(20, label.throughput)
        self.assertEqual(300, label.max_ready_age)
        self.assertEqual('required', label.imds_http_tokens)
        self.assertFalse(label.host_key_checking)
        self.assertEqual(['sg1'], label.security_group_ids)

        label = provider.labels['debian-local-normal']
        self.assertEqual('gp3', label.volume_type)
        self.assertEqual(10, label.volume_size)
        self.assertEqual(40, label.iops)
        self.assertEqual(60, label.throughput)
        self.assertEqual(900, label.max_ready_age)
        self.assertEqual('required', label.imds_http_tokens)
        self.assertFalse(label.host_key_checking)
        self.assertEqual(['sg1'], label.security_group_ids)

    @simple_layout('layouts/nodepool-defaults-redefinition.yaml',
                   enable_nodepool=True)
    def test_nodepool_config_defaults_redefinition(self):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        provider = layout.providers['aws-us-east-1-main']
        self.assertEqual(3, len(provider.labels))

        # Check the inherited values
        label = provider.labels['debian-normal']
        self.assertEqual(40, label.iops)

        label = provider.labels['debian-large']
        self.assertEqual(70, label.iops)

        label = provider.labels['debian-local-normal']
        self.assertEqual(40, label.iops)

    @simple_layout('layouts/nodepool-defaults-redefinition-final.yaml',
                   enable_nodepool=True)
    def test_nodepool_config_defaults_redefinition_final(self):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('redefine final flavor', layout.loading_errors[0].error)

    @simple_layout('layouts/nodepool-defaults-inheritance.yaml',
                   enable_nodepool=True)
    def test_nodepool_config_defaults_inheritance(self):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout

        provider = layout.providers['aws-us-east-1-main']
        self.assertEqual(2, len(provider.labels))

        # Check the inherited values
        label = provider.labels['debian-normal']
        self.assertEqual(30, label.iops)

        label = provider.labels['debian-local-normal']
        self.assertEqual(30, label.iops)

    @simple_layout('layouts/nodepool-defaults-inheritance-final.yaml',
                   enable_nodepool=True)
    def test_nodepool_config_defaults_inheritance_final(self):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout

        provider = layout.providers['aws-us-east-1-main']
        self.assertEqual(2, len(provider.labels))

        # Check the inherited values
        label = provider.labels['debian-normal']
        self.assertEqual(20, label.iops)

        label = provider.labels['debian-local-normal']
        self.assertEqual(30, label.iops)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_section_inheritance(self):
        # Verify that a section may inherit from a section in a
        # different project.

        in_repo_conf = textwrap.dedent(
            """
            - section:
                name: newsection
                parent: aws-base
            - provider:
                name: newprovider
                section: newsection
                region: us-west-2
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '1')
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertTrue('newprovider' in tenant.layout.providers)

    @simple_layout('layouts/nodepool-error-provider.yaml',
                   enable_nodepool=True)
    def test_nodepool_config_error_provider(self):
        # Test a provider referencing an image unattached to it.
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('Image "debian" is not attached to provider',
                      layout.loading_errors[0].error)

    @simple_layout('layouts/nodepool-error-provider2.yaml',
                   enable_nodepool=True)
    def test_nodepool_config_error_provider2(self):
        # Test a syntax error deep in the provider construct
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn("not a valid value @ data['flavors'][0]['foo']",
                      layout.loading_errors[0].error)

    @simple_layout('layouts/nodepool-error-provider3.yaml',
                   enable_nodepool=True)
    def test_nodepool_config_error_provider3(self):
        # Test a syntax error at the top of the provider construct
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn("expected a dictionary for dictionary "
                      "value @ data['object-storage']",
                      layout.loading_errors[0].error)

    @simple_layout('layouts/nodepool-provider-flavors.yaml',
                   enable_nodepool=True)
    def test_nodepool_provider_flavors(self):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('Flavor configuration not permitted in',
                      layout.loading_errors[0].error)

    @simple_layout('layouts/nodepool-internal-attrs.yaml',
                   enable_nodepool=True)
    def test_nodepool_internal_attrs(self):
        # Test that an attempt to set an internal attribute fails
        # schema validation.
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('extra keys not allowed',
                      layout.loading_errors[0].error)
        self.assertIn('project_canonical_name',
                      layout.loading_errors[0].error)

    @simple_layout('layouts/nodepool-retain-count-inherit1.yaml',
                   enable_nodepool=True)
    def test_nodepool_retain_count_inherit1(self):
        # Test image max-retain-count decreasing down the hierarchy
        # (which is okay)
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(0, len(layout.loading_errors))

    @simple_layout('layouts/nodepool-retain-count-inherit2.yaml',
                   enable_nodepool=True)
    def test_nodepool_retain_count_inherit2(self):
        # Test that we can't increase the max-retain-count default
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('increase image max-retain-count',
                      layout.loading_errors[0].error)

    @simple_layout('layouts/nodepool-retain-count-inherit3.yaml',
                   enable_nodepool=True)
    def test_nodepool_retain_count_inherit3(self):
        # Test that we can't increase the specific image max-retain-count
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('increase image max-retain-count',
                      layout.loading_errors[0].error)

    @simple_layout('layouts/nodepool-max-retain-count.yaml',
                   enable_nodepool=True)
    def test_nodepool_max_retain_count(self):
        # Test that we can't retain more than the max
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('more than the max-retain-count',
                      layout.loading_errors[0].error)


class TestParameterizedBuilds(ZuulTestCase):

    @simple_layout('layouts/parameterized-builds-not-allowed.yaml')
    def test_parameterized_builds_not_allowed(self):
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('does not allow parameters',
                      layout.loading_errors[0].error)
