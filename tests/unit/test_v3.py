# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2021 Acme Gating, LLC
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

import base64
import copy
import io
import json
import jwt
import logging
import os
import sys
import textwrap
import threading
import time
import gc
import re
from time import sleep
from unittest import mock, skip, skipIf
from zuul.exceptions import AlgorithmNotSupportedException
from zuul.lib import encryption, yamlutil
from zuul.lib.keystorage import OIDCSigningKeys
from zuul.scheduler import PendingReconfiguration

import fixtures
import git
import paramiko

import zuul.configloader
from zuul.lib import yamlutil as yaml
from zuul.model import MergeRequest, SEVERITY_WARNING
from zuul.zk.blob_store import BlobStore

from tests.base import (
    AnsibleZuulTestCase,
    FIXTURE_DIR,
    ZuulTestCase,
    gerrit_config,
    iterate_timeout,
    okay_tracebacks,
    simple_layout,
    skipIfMultiScheduler,
)
from tests.util import random_sha1


class TestMultipleTenants(ZuulTestCase):
    # A temporary class to hold new tests while others are disabled

    tenant_config_file = 'config/multi-tenant/main.yaml'

    def test_multiple_tenants(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('project1-test1').result,
                         'SUCCESS')
        self.assertEqual(self.getJobFromHistory('python27').result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2,
                         "A should report start and success")
        self.assertIn('tenant-one-gate', A.messages[1],
                      "A should transit tenant-one gate")
        self.assertNotIn('tenant-two-gate', A.messages[1],
                         "A should *not* transit tenant-two gate")

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('python27',
                                                'org/project2').result,
                         'SUCCESS')
        self.assertEqual(self.getJobFromHistory('project2-test1').result,
                         'SUCCESS')
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertEqual(B.reported, 2,
                         "B should report start and success")
        self.assertIn('tenant-two-gate', B.messages[1],
                      "B should transit tenant-two gate")
        self.assertNotIn('tenant-one-gate', B.messages[1],
                         "B should *not* transit tenant-one gate")

        self.assertEqual(A.reported, 2, "Activity in tenant two should"
                         "not affect tenant one")


class TestProtected(ZuulTestCase):
    tenant_config_file = 'config/protected/main.yaml'

    def test_protected_ok(self):
        # test clean usage of final parent job
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: job-protected
                protected: true
                run: playbooks/job-protected.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - job-child-ok

            - job:
                name: job-child-ok
                parent: job-protected

            - project:
                name: org/project
                check:
                  jobs:
                    - job-child-ok

            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '1')

    def test_protected_reset(self):
        # try to reset protected flag
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: job-protected
                protected: true
                run: playbooks/job-protected.yaml

            - job:
                name: job-child-reset-protected
                parent: job-protected
                protected: false

            - project:
                name: org/project
                check:
                  jobs:
                    - job-child-reset-protected

            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The second patch tried to override some variables.
        # Thus it should fail.
        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '-1')
        self.assertIn('Unable to reset protected attribute', A.messages[0])

    def test_protected_inherit_not_ok(self):
        # try to inherit from a protected job in different project
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: job-child-notok
                run: playbooks/job-child-notok.yaml
                parent: job-protected

            - project:
                name: org/project1
                check:
                  jobs:
                    - job-child-notok

            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '-1')
        self.assertIn("is a protected job in a different project",
                      A.messages[0])


class TestAbstract(ZuulTestCase):
    tenant_config_file = 'config/abstract/main.yaml'

    def test_abstract_fail(self):
        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - job-abstract
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '-1')
        self.assertIn('may not be directly run', A.messages[0])

    def test_child_of_abstract(self):
        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - job-child
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '1')


class TestIntermediate(ZuulTestCase):
    tenant_config_file = 'config/intermediate/main.yaml'

    def test_intermediate_fail(self):
        # you can not instantiate from an intermediate job
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: job-instantiate-intermediate
                parent: job-abstract-intermediate

            - project:
                check:
                  jobs:
                    - job-instantiate-intermediate
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '-1')
        self.assertIn('is not abstract', A.messages[0])

    def test_intermediate_config_fail(self):
        # an intermediate job must also be abstract
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: job-intermediate-but-not-abstract
                intermediate: true
                abstract: false

            - project:
                check:
                  jobs:
                    - job-intermediate-but-not-abstract
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '-1')
        self.assertIn('An intermediate job must also be abstract',
                      A.messages[0])

    def test_intermediate_several(self):
        # test passing through several intermediate jobs
        in_repo_conf = textwrap.dedent(
            """
            - project:
                name: org/project
                check:
                  jobs:
                    - job-actual
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '1')


class TestFinal(ZuulTestCase):

    tenant_config_file = 'config/final/main.yaml'

    def test_final_variant_ok(self):
        # test clean usage of final parent job
        in_repo_conf = textwrap.dedent(
            """
            - project:
                name: org/project
                check:
                  jobs:
                    - job-final
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '1')

    def test_final_variant_error(self):
        # test misuse of final parent job
        in_repo_conf = textwrap.dedent(
            """
            - project:
                name: org/project
                check:
                  jobs:
                    - job-final:
                        vars:
                          dont_override_this: bar
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The second patch tried to override some variables.
        # Thus it should fail.
        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '-1')
        self.assertIn('Unable to modify final job', A.messages[0])


class TestBranchCreation(ZuulTestCase):
    tenant_config_file = 'config/one-project/main.yaml'

    def test_missed_branch_create(self):
        # Test that if we miss a branch creation event, we can recover
        # by issuing a full-reconfiguration.
        self.create_branch('org/project', 'stable/yoga')
        # We do not emit the gerrit event, thus simulating a missed event;
        # verify that nothing happens
        A = self.fake_gerrit.addFakeChange('org/project', 'stable/yoga', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 0)
        self.assertHistory([])

        # Correct the situation with a full reconfiguration
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1)
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1')])


class TestBranchDeletion(ZuulTestCase):
    tenant_config_file = 'config/branch-deletion/main.yaml'

    def test_branch_delete(self):
        # This tests a tenant reconfiguration on deleting a branch
        # *after* an earlier failed tenant reconfiguration.  This
        # ensures that cached data are appropriately removed, even if
        # we are recovering from an invalid config.
        self.create_branch('org/project', 'stable/queens')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/queens'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - nonexistent-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'stable/queens', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        self.delete_branch('org/project', 'stable/queens')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchDeletedEvent(
                'org/project', 'stable/queens'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - base
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(B.reported, 1)
        self.assertHistory([
            dict(name='base', result='SUCCESS', changes='2,1')])

    def test_branch_delete_full_reconfiguration(self):
        # This tests a full configuration after deleting a branch
        # *after* an earlier failed tenant reconfiguration.  This
        # ensures that cached data are appropriately removed, even if
        # we are recovering from an invalid config.
        self.create_branch('org/project', 'stable/queens')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/queens'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - nonexistent-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'stable/queens', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        self.delete_branch('org/project', 'stable/queens')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - base
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(B.reported, 1)
        self.assertHistory([
            dict(name='base', result='SUCCESS', changes='2,1')])

        self.scheds.first.sched.merger.merger_api.cleanup(0)


class TestBranchTag(ZuulTestCase):
    tenant_config_file = 'config/branch-tag/main.yaml'

    def test_no_branch_match(self):
        # Test that tag jobs run with no explicit branch matchers
        event = self.fake_gerrit.addFakeTag('org/project', 'master', 'foo')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='central-job', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='test-job', result='SUCCESS', ref='refs/tags/foo')],
            ordered=False)

    def test_no_branch_match_annotated_tag(self):
        # Test that tag jobs run with no explicit branch matchers against
        # annotated tags.
        event = self.fake_gerrit.addFakeTag('org/project', 'master',
                                            'foo', 'test message')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='central-job', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='test-job', result='SUCCESS', ref='refs/tags/foo')],
            ordered=False)

    def test_no_branch_match_multi_branch(self):
        # Test that tag jobs run with no explicit branch matchers in a
        # multi-branch project (where jobs generally get implied
        # branch matchers)
        self.create_branch('org/project', 'stable/pike')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/pike'))
        self.waitUntilSettled()

        event = self.fake_gerrit.addFakeTag('org/project', 'master', 'foo')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        # test-job does run in this case because it is defined in a
        # branched repo with implied branch matchers, and the tagged
        # commit is in both branches.
        self.assertHistory([
            dict(name='central-job', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='test-job', result='SUCCESS', ref='refs/tags/foo')],
            ordered=False)

    def test_no_branch_match_divergent_multi_branch(self):
        # Test that tag jobs from divergent branches run different job
        # variants.
        self.create_branch('org/project', 'stable/pike')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/pike'))
        self.waitUntilSettled()

        # Add a new job to master
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test2-job
                run: playbooks/test-job.yaml

            - project:
                name: org/project
                tag:
                  jobs:
                    - central-job
                    - test2-job
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        event = self.fake_gerrit.addFakeTag(
            'org/project', 'stable/pike', 'foo')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        # test-job runs because we tagged stable/pike, but test2-job does
        # not, it only applied to master.
        self.assertHistory([
            dict(name='central-job', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='test-job', result='SUCCESS', ref='refs/tags/foo')],
            ordered=False)

        event = self.fake_gerrit.addFakeTag('org/project', 'master', 'bar')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        # test2-job runs because we tagged master, but test-job does
        # not, it only applied to stable/pike.
        self.assertHistory([
            dict(name='central-job', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='test-job', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='central-job', result='SUCCESS', ref='refs/tags/bar'),
            dict(name='test2-job', result='SUCCESS', ref='refs/tags/bar')],
            ordered=False)

    def test_detached_tag(self):
        # Test no jobs run when a tagged commit isn't part of any branch
        event = self.fake_gerrit.addFakeTag('org/project', 'master', 'foo')
        # Fake a commit that doesn't appear on a branch
        event["refUpdate"]["newRev"] = 40 * "f"
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        self.assertHistory([])


class TestBranchNegative(ZuulTestCase):
    tenant_config_file = 'config/branch-negative/main.yaml'

    def test_negative_branch_match(self):
        # Test that a negative branch matcher works with implied branches.
        self.create_branch('org/project', 'stable/pike')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/pike'))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        B = self.fake_gerrit.addFakeChange('org/project', 'stable/pike', 'A')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1')])

    def test_negative_branch_match_regex(self):
        # Test that a negated branch matcher regex works with implied branches.
        self.create_branch('org/project2', 'stable/pike')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'stable/pike'))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        B = self.fake_gerrit.addFakeChange('org/project2', 'stable/pike', 'A')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test-job2', result='SUCCESS', changes='1,1')])


class TestBranchTemplates(ZuulTestCase):
    tenant_config_file = 'config/branch-templates/main.yaml'

    def test_template_removal_from_branch(self):
        # Test that a template can be removed from one branch but not
        # another.
        # This creates a new branch with a copy of the config in master
        self.create_branch('puppet-integration', 'stable/newton')
        self.create_branch('puppet-integration', 'stable/ocata')
        self.create_branch('puppet-tripleo', 'stable/newton')
        self.create_branch('puppet-tripleo', 'stable/ocata')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-integration', 'stable/newton'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-integration', 'stable/ocata'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-tripleo', 'stable/newton'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-tripleo', 'stable/ocata'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - project:
                name: puppet-tripleo
                check:
                  jobs:
                    - puppet-something
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('puppet-tripleo', 'stable/newton',
                                           'A', files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='puppet-something', result='SUCCESS', changes='1,1')])

    def test_template_change_on_branch(self):
        # Test that the contents of a template can be changed on one
        # branch without affecting another.

        # This creates a new branch with a copy of the config in master
        self.create_branch('puppet-integration', 'stable/newton')
        self.create_branch('puppet-integration', 'stable/ocata')
        self.create_branch('puppet-tripleo', 'stable/newton')
        self.create_branch('puppet-tripleo', 'stable/ocata')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-integration', 'stable/newton'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-integration', 'stable/ocata'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-tripleo', 'stable/newton'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-tripleo', 'stable/ocata'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent("""
            - job:
                name: puppet-unit-base
                run: playbooks/run-unit-tests.yaml

            - job:
                name: puppet-unit-3.8
                parent: puppet-unit-base
                branches: ^(stable/(newton|ocata)).*$
                vars:
                  puppet_gem_version: 3.8

            - job:
                name: puppet-something
                run: playbooks/run-unit-tests.yaml

            - project-template:
                name: puppet-unit
                check:
                  jobs:
                    - puppet-something

            - project:
                name: puppet-integration
                templates:
                  - puppet-unit
        """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('puppet-integration',
                                           'stable/newton',
                                           'A', files=file_dict)
        B = self.fake_gerrit.addFakeChange('puppet-tripleo',
                                           'stable/newton',
                                           'B')
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='puppet-something', result='SUCCESS',
                 changes='1,1 2,1')])


class TestBranchVariants(ZuulTestCase):
    tenant_config_file = 'config/branch-variants/main.yaml'

    def test_branch_variants(self):
        # Test branch variants of jobs with inheritance
        self.executor_server.hold_jobs_in_build = True
        # This creates a new branch with a copy of the config in master
        self.create_branch('puppet-integration', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-integration', 'stable'))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('puppet-integration', 'stable', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds[0].job.pre_run), 3)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_branch_variants_reconfigure(self):
        # Test branch variants of jobs with inheritance
        self.executor_server.hold_jobs_in_build = True
        # This creates a new branch with a copy of the config in master
        self.create_branch('puppet-integration', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-integration', 'stable'))
        self.waitUntilSettled()

        with open(os.path.join(FIXTURE_DIR,
                               'config/branch-variants/git/',
                               'puppet-integration/.zuul.yaml')) as f:
            config = f.read()

        # Push a change that triggers a dynamic reconfiguration
        file_dict = {'.zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('puppet-integration', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        ipath = self.builds[0].parameters['zuul']['_inheritance_path']
        for i in ipath:
            self.log.debug("inheritance path %s", i)
        self.assertEqual(len(ipath), 5)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_branch_variants_divergent(self):
        # Test branches can diverge and become independent
        self.executor_server.hold_jobs_in_build = True
        # This creates a new branch with a copy of the config in master
        self.create_branch('puppet-integration', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'puppet-integration', 'stable'))
        self.waitUntilSettled()

        with open(os.path.join(FIXTURE_DIR,
                               'config/branch-variants/git/',
                               'puppet-integration/stable.zuul.yaml')) as f:
            config = f.read()

        file_dict = {'.zuul.yaml': config}
        C = self.fake_gerrit.addFakeChange('puppet-integration', 'stable', 'C',
                                           files=file_dict)
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(C.getChangeMergedEvent())
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('puppet-integration', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        B = self.fake_gerrit.addFakeChange('puppet-integration', 'stable', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(self.builds[0].parameters['zuul']['jobtags'],
                         ['master'])

        self.assertEqual(self.builds[1].parameters['zuul']['jobtags'],
                         ['stable'])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()


class TestBranchMismatch(ZuulTestCase):
    tenant_config_file = 'config/branch-mismatch/main.yaml'

    def test_job_override_branch(self):
        "Test that override-checkout overrides branch matchers as well"

        # Make sure the parent job repo is branched, so it gets
        # implied branch matchers.
        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))

        # The child job repo should have a branch which does not exist
        # in the parent job repo.
        self.create_branch('org/project2', 'devel')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'devel'))
        self.waitUntilSettled()

        # A job in a repo with a weird branch name should use the
        # parent job from the parent job's master (default) branch.
        A = self.fake_gerrit.addFakeChange('org/project2', 'devel', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        # project-test2 should run because it inherits from
        # project-test1 and we will use the fallback branch to find
        # project-test1 variants, but project-test1 itself, even
        # though it is in the project-pipeline config, should not run
        # because it doesn't directly match.
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_implied_branch_matcher_regex(self):
        # Test that branch names that look like regexes aren't treated
        # as such for implied branch matchers.

        # Make sure the parent job repo is branched, so it gets
        # implied branch matchers.

        # The '+' in the branch name would cause the change not to
        # match if it is treated as a regex.
        self.create_branch('org/project1', 'feature/foo-0.1.12+bar')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'feature/foo-0.1.12+bar'))

        A = self.fake_gerrit.addFakeChange(
            'org/project1', 'feature/foo-0.1.12+bar', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_implied_branch_matcher_pragma_syntax_error(self):
        # Test that syntax errors are reported if the implied branch
        # matcher pragma is set.  This catches potential errors when
        # serializing configuration errors since the pragma causes
        # extra information to be added to the error source context.
        self.create_branch('org/project1', 'feature/test')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'feature/test'))

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                nodeset: bar
            - pragma:
                implied-branches:
                  - master
                  - feature/r1
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([])
        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('nodeset "bar" was not found', A.messages[0],
                      "A should have a syntax error reported")

    def _updateConfig(self, config, branch):
        file_dict = {'zuul.yaml': config}
        C = self.fake_gerrit.addFakeChange('org/project1', branch, 'C',
                                           files=file_dict)
        C.setMerged()
        self.fake_gerrit.addEvent(C.getChangeMergedEvent())
        self.waitUntilSettled()

    def test_implied_branch_matcher_similar(self):
        # Test that we perform a full-text match with implied branch
        # matchers.
        self.create_branch('org/project1', 'testbranch')
        self.create_branch('org/project1', 'testbranch2')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'testbranch'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'testbranch2'))

        config = textwrap.dedent(
            """
            - job:
                name: testjob
                vars:
                  this_branch: testbranch
                  testbranch: true
            - project:
                check: {jobs: [testjob]}
            """)
        self._updateConfig(config, 'testbranch')
        config = textwrap.dedent(
            """
            - job:
                name: testjob
                vars:
                  this_branch: testbranch2
                  testbranch2: true
            - project:
                check: {jobs: [testjob]}
            """)
        self._updateConfig(config, 'testbranch2')

        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange(
            'org/project1', 'testbranch', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(
            {'testbranch': True, 'this_branch': 'testbranch'},
            self.builds[0].job.variables)

        self.executor_server.release()
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange(
            'org/project1', 'testbranch2', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The two jobs should have distinct variables.  Notably,
        # testbranch2 should not pick up vars from testbranch.
        self.assertEqual(
            {'testbranch2': True, 'this_branch': 'testbranch2'},
            self.builds[0].job.variables)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='testjob', result='SUCCESS', changes='3,1'),
            dict(name='testjob', result='SUCCESS', changes='4,1'),
        ], ordered=False)

    def test_implied_branch_matcher_similar_override_checkout(self):
        # Overriding a checkout has some branch matching implications.
        # Make sure that we are performing a full-text match on
        # branches when we override a checkout.
        self.create_branch('org/project1', 'testbranch')
        self.create_branch('org/project1', 'testbranch2')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'testbranch'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'testbranch2'))
        config = textwrap.dedent(
            """
            - job:
                name: testjob
                vars:
                  this_branch: testbranch
                  testbranch: true
            - project:
                check: {jobs: [testjob]}
            """)
        self._updateConfig(config, 'testbranch')
        config = textwrap.dedent(
            """
            - job:
                name: testjob
                vars:
                  this_branch: testbranch2
                  testbranch2: true
            - project:
                check: {jobs: [testjob]}
            """)
        self._updateConfig(config, 'testbranch2')

        self.executor_server.hold_jobs_in_build = True
        config = textwrap.dedent(
            """
            - job:
                name: project-test1
            - job:
                name: testjob-testbranch
                parent: testjob
                override-checkout: testbranch
            - job:
                name: testjob-testbranch2
                parent: testjob
                override-checkout: testbranch2
            - project:
                check: {jobs: [testjob-testbranch, testjob-testbranch2]}
            """)
        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange(
            'org/project1', 'master', 'A', files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(
            {'testbranch': True, 'this_branch': 'testbranch'},
            self.builds[0].job.variables)

        # The two jobs should have distinct variables (notably, the
        # variant on testbranch2 should not pick up vars from
        # testbranch.
        self.assertEqual(
            {'testbranch2': True, 'this_branch': 'testbranch2'},
            self.builds[1].job.variables)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='testjob-testbranch', result='SUCCESS', changes='3,1'),
            dict(name='testjob-testbranch2', result='SUCCESS', changes='3,1'),
        ], ordered=False)


class TestBranchRef(ZuulTestCase):
    tenant_config_file = 'config/branch-ref/main.yaml'

    def test_ref_match(self):
        # Test that branch matchers for explicit refs work as expected
        # First, make a branch with another job so we can examine
        # different branches.
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - project:
                tag:
                  jobs:
                    - other-job:
                        branches: "^refs/tags/tag1-.*$"
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'stable', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        # We're going to tag master, which is still at the branch
        # point for stable, so the tagged commit will appear in both
        # branches.  This should cause test-job-1 (from the project
        # config on master) and other-job (from the project config on
        # stable).
        event = self.fake_gerrit.addFakeTag('org/project', 'master', 'tag1-a')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='other-job', result='SUCCESS', ref='refs/tags/tag1-a'),
            dict(name='test-job-1', result='SUCCESS', ref='refs/tags/tag1-a')],
            ordered=False)

        # Next, merge a noop change to master so that we can tag a
        # commit that's unique to master.
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setMerged()
        self.fake_gerrit.addEvent(B.getChangeMergedEvent())
        self.waitUntilSettled()

        # This tag should only run test-job-1, since it doesn't appear
        # in stable, it doesn't get that project config applied.
        event = self.fake_gerrit.addFakeTag('org/project', 'master', 'tag1-b')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='other-job', result='SUCCESS', ref='refs/tags/tag1-a'),
            dict(name='test-job-1', result='SUCCESS', ref='refs/tags/tag1-a'),
            dict(name='test-job-1', result='SUCCESS', ref='refs/tags/tag1-b')],
            ordered=False)

        # Now tag the same commit with the other format; we should get
        # only test-job-2 added.
        event = self.fake_gerrit.addFakeTag('org/project', 'master', 'tag2-a')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='other-job', result='SUCCESS', ref='refs/tags/tag1-a'),
            dict(name='test-job-1', result='SUCCESS', ref='refs/tags/tag1-a'),
            dict(name='test-job-1', result='SUCCESS', ref='refs/tags/tag1-b'),
            dict(name='test-job-2', result='SUCCESS', ref='refs/tags/tag2-a')],
            ordered=False)


class TestAllowedProjects(ZuulTestCase):
    tenant_config_file = 'config/allowed-projects/main.yaml'

    def test_allowed_projects(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1)
        self.assertIn('Build succeeded', A.messages[0])

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(B.reported, 1)
        self.assertIn('Project org/project2 is not allowed '
                      'to run job test-project2', B.messages[0])

        C = self.fake_gerrit.addFakeChange('org/project3', 'master', 'C')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(C.reported, 1)
        self.assertIn('Project org/project3 is not allowed '
                      'to run job restricted-job', C.messages[0])

        self.assertHistory([
            dict(name='test-project1', result='SUCCESS', changes='1,1'),
            dict(name='restricted-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_allowed_projects_dynamic_config(self):
        # It is possible to circumvent allowed-projects with a
        # depends-on.
        in_repo_conf2 = textwrap.dedent(
            """
            - job:
                name: test-project2b
                parent: restricted-job
                allowed-projects:
                  - org/project1
            """)
        in_repo_conf1 = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - test-project2b
            """)

        file_dict = {'zuul.yaml': in_repo_conf2}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        file_dict = {'zuul.yaml': in_repo_conf1}
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B',
                                           files=file_dict)
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test-project2b', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)

    def test_allowed_projects_dynamic_config_secret(self):
        # It is not possible to circumvent allowed-projects with a
        # depends-on if there is a secret involved.
        in_repo_conf2 = textwrap.dedent(
            """
            - secret:
                name: project2_secret
                data: {}
            - job:
                name: test-project2b
                parent: restricted-job
                secrets: project2_secret
                allowed-projects:
                  - org/project1
            """)
        in_repo_conf1 = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - test-project2b
            """)

        file_dict = {'zuul.yaml': in_repo_conf2}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        file_dict = {'zuul.yaml': in_repo_conf1}
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B',
                                           files=file_dict)
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([])
        self.assertEqual(B.reported, 1)
        self.assertIn('Project org/project1 is not allowed '
                      'to run job test-project2b', B.messages[0])


class TestAllowedProjectsTrusted(ZuulTestCase):
    tenant_config_file = 'config/allowed-projects-trusted/main.yaml'

    def test_allowed_projects_secret_trusted(self):
        # Test that an untrusted job defined in project1 can be used
        # in project2, but only if attached by a config project.
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1)
        self.assertIn('Build succeeded', A.messages[0])
        self.assertHistory([
            dict(name='test-project1', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestCentralJobs(ZuulTestCase):
    tenant_config_file = 'config/central-jobs/main.yaml'

    def setUp(self):
        super(TestCentralJobs, self).setUp()
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.waitUntilSettled()

    def _updateConfig(self, config, branch):
        file_dict = {'.zuul.yaml': config}
        C = self.fake_gerrit.addFakeChange('org/project', branch, 'C',
                                           files=file_dict)
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(C.getChangeMergedEvent())
        self.waitUntilSettled()

    def _test_central_job_on_branch(self, branch, other_branch):
        # Test that a job defined on a branchless repo only runs on
        # the branch applied
        config = textwrap.dedent(
            """
            - project:
                name: org/project
                check:
                  jobs:
                    - central-job
            """)
        self._updateConfig(config, branch)

        A = self.fake_gerrit.addFakeChange('org/project', branch, 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='central-job', result='SUCCESS', changes='2,1')])

        # No jobs should run for this change.
        B = self.fake_gerrit.addFakeChange('org/project', other_branch, 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='central-job', result='SUCCESS', changes='2,1')])

    def test_central_job_on_stable(self):
        self._test_central_job_on_branch('master', 'stable')

    def test_central_job_on_master(self):
        self._test_central_job_on_branch('stable', 'master')

    def _test_central_template_on_branch(self, branch, other_branch):
        # Test that a project-template defined on a branchless repo
        # only runs on the branch applied
        config = textwrap.dedent(
            """
            - project:
                name: org/project
                templates: ['central-jobs']
            """)
        self._updateConfig(config, branch)

        A = self.fake_gerrit.addFakeChange('org/project', branch, 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='central-job', result='SUCCESS', changes='2,1')])

        # No jobs should run for this change.
        B = self.fake_gerrit.addFakeChange('org/project', other_branch, 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='central-job', result='SUCCESS', changes='2,1')])

    def test_central_template_on_stable(self):
        self._test_central_template_on_branch('master', 'stable')

    def test_central_template_on_master(self):
        self._test_central_template_on_branch('stable', 'master')


class TestEmptyConfigFile(ZuulTestCase):
    tenant_config_file = 'config/empty-config-file/main.yaml'

    def test_empty_config_file(self):
        # Tests that a config file with only comments does not cause
        # an error.
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(
            len(tenant.layout.loading_errors), 0,
            "No error should have been accumulated")


class TestInRepoConfig(ZuulTestCase):
    # A temporary class to hold new tests while others are disabled

    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/in-repo/main.yaml'

    def test_in_repo_config(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('project-test1').result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2,
                         "A should report start and success")
        self.assertIn('tenant-one-gate', A.messages[1],
                      "A should transit tenant-one gate")

    @skip("This test is useful, but not reliable")
    def test_full_and_dynamic_reconfig(self):
        self.executor_server.hold_jobs_in_build = True
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1

            - project:
                name: org/project
                tenant-one-gate:
                  jobs:
                    - project-test1
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        gc.collect()
        pipelines = [obj for obj in gc.get_objects()
                     if isinstance(obj, zuul.model.Pipeline)]
        self.assertEqual(len(pipelines), 4)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_dynamic_config(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1

            - job:
                name: project-test2
                run: playbooks/project-test2.yaml

            - job:
                name: project-test3
                run: playbooks/project-test2.yaml

            # add a job by the short project name
            - project:
                name: org/project
                tenant-one-gate:
                  jobs:
                    - project-test2

            # add a job by the canonical project name
            - project:
                name: review.example.com/org/project
                tenant-one-gate:
                  jobs:
                    - project-test3
            """)

        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2,
                         "A should report start and success")
        self.assertIn('tenant-one-gate', A.messages[1],
                      "A should transit tenant-one gate")
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-test3', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        # Now that the config change is landed, it should be live for
        # subsequent changes.
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('project-test2').result,
                         'SUCCESS')
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-test3', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
            dict(name='project-test3', result='SUCCESS', changes='2,1'),
        ], ordered=False)

        # Catch time / monotonic errors
        val = self.assertReportedStat('zuul.tenant.tenant-one.pipeline.'
                                      'tenant-one-gate.layout_generation_time',
                                      kind='ms')
        self.assertTrue(0.0 < float(val) < 60000.0)
        events = self.scheds.first.sched.sql.connection.getSystemEvents()
        self.assertEqual(1, len(events))
        self.assertEqual("tenant reconfiguration", events[0].event_type)
        self.assertEqual(
            "Tenant reconfiguration "
            "due to review.example.com/org/project@master",
            events[0].description)

    def test_dynamic_template(self):
        # Tests that a project can't update a template in another
        # project.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1

            - project-template:
                name: common-config-template
                check:
                  jobs:
                    - project-test1

            - project:
                name: org/project
                templates: [common-config-template]
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('Project template common-config-template '
                      'is already defined',
                      A.messages[0],
                      "A should have failed the check pipeline")

    def test_noop_final(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: custom-noop
                parent: noop
                run: playbooks/noop.yaml
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1, 'A should report failure')
        self.assertIn('"noop" is final and can not act as a parent',
                      A.messages[0])

    def test_dynamic_config_errors_not_accumulated(self):
        """Test that requesting broken dynamic configs
        does not appear in tenant layout error accumulator"""
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1

            - project:
                name: org/project
                check:
                  jobs:
                    - non-existent-job
            """)

        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(
            len(tenant.layout.loading_errors), 0,
            "No error should have been accumulated")
        self.assertHistory([])

    def test_dynamic_config_non_existing_job(self):
        """Test that requesting a non existent job fails"""
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1

            - project:
                name: org/project
                check:
                  jobs:
                    - non-existent-job
            """)

        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('Job non-existent-job not defined', A.messages[0],
                      "A should have failed the check pipeline")
        self.assertHistory([])
        self.assertEqual(len(A.comments), 1)
        comments = sorted(A.comments, key=lambda x: x['line'])
        self.assertEqual(comments[0],
                         {'file': '.zuul.yaml',
                          'line': 9,
                          'message': 'Job non-existent-job not defined',
                          'reviewer': {'email': 'zuul@example.com',
                                       'name': 'Zuul',
                                       'username': 'jenkins'},
                          'range': {'end_character': 0,
                                    'end_line': 9,
                                    'start_character': 2,
                                    'start_line': 5},
                          })

    def test_dynamic_config_job_anchors(self):
        # Test the use of anchors in job configuration.  This is a
        # regression test designed to catch a failure where we freeze
        # the first job and in doing so, mutate the vars dict.  The
        # intended behavior is that the two jobs end up with two
        # separate python objects for their vars dicts.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: myvars
                vars: &anchor
                  plugins:
                    foo: bar

            - job:
                name: project-test1
                timeout: 999999999999
                vars: *anchor

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test1
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('max-job-timeout', A.messages[0])
        self.assertHistory([])

    def test_dynamic_config_non_existing_job_in_template(self):
        """Test that requesting a non existent job fails"""
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1

            - project-template:
                name: test-template
                check:
                  jobs:
                    - non-existent-job

            - project:
                name: org/project
                templates:
                  - test-template
            """)

        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('Job non-existent-job not defined', A.messages[0],
                      "A should have failed the check pipeline")
        self.assertHistory([])

    def test_dynamic_nonexistent_job_dependency(self):
        # Tests that a reference to a nonexistent job dependency is an
        # error.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test1:
                        dependencies:
                          - name: non-existent-job
                            soft: true
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('Job non-existent-job not defined', A.messages[0],
                      "A should have failed the check pipeline")
        self.assertNotIn('freezing', A.messages[0])
        self.assertHistory([])

    def test_dynamic_config_new_patchset(self):
        self.executor_server.hold_jobs_in_build = True

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - job:
                name: project-test2
                run: playbooks/project-test2.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test2
            """)

        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'check')
        self.assertEqual(items[0].changes[0].number, '1')
        self.assertEqual(items[0].changes[0].patchset, '1')
        self.assertTrue(items[0].live)

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - job:
                name: project-test2
                run: playbooks/project-test2.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test1
                    - project-test2
            """)
        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}

        A.addPatchset(files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(2))

        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'check')
        self.assertEqual(items[0].changes[0].number, '1')
        self.assertEqual(items[0].changes[0].patchset, '2')
        self.assertTrue(items[0].live)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release('project-test1')
        self.waitUntilSettled()
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-test2', result='ABORTED', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,2'),
            dict(name='project-test2', result='SUCCESS', changes='1,2')])

    def test_in_repo_branch(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1

            - job:
                name: project-test2
                run: playbooks/project-test2.yaml

            - project:
                name: org/project
                tenant-one-gate:
                  jobs:
                    - project-test2
            """)

        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.waitUntilSettled()
        A = self.fake_gerrit.addFakeChange('org/project', 'stable', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2,
                         "A should report start and success")
        self.assertIn('tenant-one-gate', A.messages[1],
                      "A should transit tenant-one gate")
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='1,1')])
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        # The config change should not affect master.
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='2,1')])

        # The config change should be live for further changes on
        # stable.
        C = self.fake_gerrit.addFakeChange('org/project', 'stable', 'C')
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='2,1'),
            dict(name='project-test2', result='SUCCESS', changes='3,1')])

    def test_crd_dynamic_config_branch(self):
        # Test that we can create a job in one repo and be able to use
        # it from a different branch on a different repo.

        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1

            - job:
                name: project-test2
                run: playbooks/project-test2.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test2
            """)

        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)

        second_repo_conf = textwrap.dedent(
            """
            - project:
                name: org/project1
                check:
                  jobs:
                    - project-test2
            """)

        second_file_dict = {'.zuul.yaml': second_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project1', 'stable', 'B',
                                           files=second_file_dict)
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1, "A should report")
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1 2,1'),
        ])

    def test_yaml_list_error(self):
        in_repo_conf = textwrap.dedent(
            """
            job: foo
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('not a list', A.messages[0],
                      "A should have a syntax error reported")
        self.assertIn('job: foo', A.messages[0],
                      "A should display the failing list")

    def test_yaml_dict_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job_not_a_dict
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('not a dictionary', A.messages[0],
                      "A should have a syntax error reported")
        self.assertIn('job_not_a_dict', A.messages[0],
                      "A should list the bad key")

    def test_yaml_dict_error2(self):
        in_repo_conf = textwrap.dedent(
            """
            - foo: {{ not_a_dict }}
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('while constructing a mapping', A.messages[0],
                      "A should have a syntax error reported")

    def test_yaml_dict_error3(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('is not a dictionary', A.messages[0],
                      "A should have a syntax error reported")

    def test_yaml_duplicate_key_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: foo
                name: bar
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('appears more than once', A.messages[0],
                      "A should have a syntax error reported")

    def test_yaml_key_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
              name: project-test2
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('has more than one key', A.messages[0],
                      "A should have a syntax error reported")
        self.assertIn("job: null\n  name: project-test2", A.messages[0],
                      "A should have the failing section displayed")

    # This is non-deterministic without default dict ordering, which
    # happended with python 3.7.
    @skipIf(sys.version_info < (3, 7), "non-deterministic on < 3.7")
    def test_yaml_error_truncation_message(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
              name: project-test2
              this: is
              a: long
              set: of
              keys: that
              should: be
              truncated: ok
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('has more than one key', A.messages[0],
                      "A should have a syntax error reported")
        self.assertIn("job: null\n  name: project-test2", A.messages[0],
                      "A should have the failing section displayed")
        self.assertIn("...", A.messages[0],
                      "A should have the failing section truncated")

    def test_yaml_unknown_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - foobar:
                foo: bar
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('not recognized', A.messages[0],
                      "A should have a syntax error reported")
        self.assertIn('foobar:\n    foo: bar', A.messages[0],
                      "A should report the bad keys")

    def test_invalid_job_secret_var_name(self):
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: foo-bar
                data:
                  dummy: value
            - job:
                name: foobar
                secrets:
                  - name: foo-bar
                    secret: foo-bar
            """)

        file_dict = {".zuul.yaml": in_repo_conf}
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A",
                                           files=file_dict)
        A.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn("Ansible variable name 'foo-bar'", A.messages[0],
                      "A should have a syntax error reported")

    def test_invalid_job_vars(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: foobar
                vars:
                  foo-bar: value
            """)

        file_dict = {".zuul.yaml": in_repo_conf}
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A",
                                           files=file_dict)
        A.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn("Ansible variable name 'foo-bar'", A.messages[0],
                      "A should have a syntax error reported")

    def test_invalid_job_extra_vars(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: foobar
                extra-vars:
                  foo-bar: value
            """)

        file_dict = {".zuul.yaml": in_repo_conf}
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A",
                                           files=file_dict)
        A.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn("Ansible variable name 'foo-bar'", A.messages[0],
                      "A should have a syntax error reported")

    def test_invalid_job_host_vars(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: foobar
                host-vars:
                  host-name:
                    foo-bar: value
            """)

        file_dict = {".zuul.yaml": in_repo_conf}
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A",
                                           files=file_dict)
        A.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn("Ansible variable name 'foo-bar'", A.messages[0],
                      "A should have a syntax error reported")

    def test_invalid_job_group_vars(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: foobar
                group-vars:
                  group-name:
                    foo-bar: value
            """)

        file_dict = {".zuul.yaml": in_repo_conf}
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A",
                                           files=file_dict)
        A.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn("Ansible variable name 'foo-bar'", A.messages[0],
                      "A should have a syntax error reported")

    def test_untrusted_syntax_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test2
                foo: error
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('syntax error', A.messages[0],
                      "A should have a syntax error reported")

    def test_trusted_syntax_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test2
                foo: error
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('syntax error', A.messages[0],
                      "A should have a syntax error reported")

    def test_untrusted_yaml_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
            foo: error
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('syntax error', A.messages[0],
                      "A should have a syntax error reported")

    def test_untrusted_shadow_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: common-config-test
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('not permitted to shadow', A.messages[0],
                      "A should have a syntax error reported")

    def test_untrusted_pipeline_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - pipeline:
                name: test
                manager: independent
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('Pipelines may not be defined', A.messages[0],
                      "A should have a syntax error reported")

    def test_untrusted_project_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - project:
                name: org/project1
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('the only project definition permitted', A.messages[0],
                      "A should have a syntax error reported")

    def test_untrusted_depends_on_trusted(self):
        with open(os.path.join(FIXTURE_DIR,
                               'config/in-repo/git/',
                               'common-config/zuul.yaml')) as f:
            common_config = f.read()

        common_config += textwrap.dedent(
            """
            - job:
                name: project-test9
            """)

        file_dict = {'zuul.yaml': common_config}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
            - project:
                name: org/project
                check:
                  jobs:
                    - project-test9
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(B.data['status'], 'NEW')
        self.assertEqual(B.reported, 1,
                         "B should report failure")
        self.assertIn('depends on a change to a config project',
                      B.messages[0],
                      "A should have a syntax error reported")

    def test_duplicate_node_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: duplicate
                nodes:
                  - name: compute
                    label: foo
                  - name: compute
                    label: foo
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('appears multiple times', A.messages[0],
                      "A should have a syntax error reported")

    def test_duplicate_group_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: duplicate
                nodes:
                  - name: compute
                    label: foo
                groups:
                  - name: group
                    nodes: compute
                  - name: group
                    nodes: compute
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('appears multiple times', A.messages[0],
                      "A should have a syntax error reported")

    def test_group_in_job_with_invalid_node(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test job
                nodeset:
                  nodes: []
                  groups:
                    - name: a_group
                      nodes:
                       - a_node_that_does_not_exist
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('which is not defined in the nodeset', A.messages[0],
                      "A should have a syntax error reported")

    def test_duplicate_group_in_job(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test job
                nodeset:
                  nodes:
                   - name: controller
                     label: ubuntu-focal
                  groups:
                    - name: a_duplicate_group
                      nodes:
                       - controller
                    - name: a_duplicate_group
                      nodes:
                       - controller
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn(
            'Group names must be unique within a nodeset.',
            A.messages[0], "A should have a syntax error reported")

    def test_secret_not_found_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml
                secrets: does-not-exist
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('secret "does-not-exist" was not found', A.messages[0],
                      "A should have a syntax error reported")

    def test_nodeset_not_found_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test
                nodeset: does-not-exist
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('nodeset "does-not-exist" was not found', A.messages[0],
                      "A should have a syntax error reported")

    def test_required_project_not_found_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
            - job:
                name: test
                required-projects:
                  - does-not-exist
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('The project "does-not-exist" was not found',
                      A.messages[0],
                      "A should have a syntax error reported")

    def test_required_project_not_found_multiple_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
            - job:
                name: test
                required-projects:
                  - does-not-exist
                  - also-does-not-exist
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('The projects "does-not-exist", '
                      '"also-does-not-exist" were not found.',
                      A.messages[0],
                      "A should have a syntax error reported")

    def test_template_not_found_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
            - project:
                name: org/project
                templates:
                  - does-not-exist
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('project template "does-not-exist" was not found',
                      A.messages[0],
                      "A should have a syntax error reported")

    def test_job_list_in_project_template_not_dict_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
            - project-template:
                name: some-jobs
                check:
                  jobs:
                    - project-test1:
                        - required-projects:
                            org/project2
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('expected str for dictionary value',
                      A.messages[0], "A should have a syntax error reported")

    def test_job_list_in_project_not_dict_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
            - project:
                name: org/project1
                check:
                  jobs:
                    - project-test1:
                        - required-projects:
                            org/project2
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('expected str for dictionary value',
                      A.messages[0], "A should have a syntax error reported")

    def test_project_template(self):
        # Tests that a project template is not modified when used, and
        # can therefore be used in subsequent reconfigurations.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml
            - project-template:
                name: some-jobs
                tenant-one-gate:
                  jobs:
                    - project-test1:
                        required-projects:
                          - org/project1
            - project:
                name: org/project
                templates:
                  - some-jobs
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        in_repo_conf = textwrap.dedent(
            """
            - project:
                name: org/project1
                templates:
                  - some-jobs
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B',
                                           files=file_dict)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(B.data['status'], 'MERGED')

    def test_job_remove_add(self):
        # Tests that a job can be removed from one repo and added in another.
        # First, remove the current config for project1 since it
        # references the job we want to remove.
        file_dict = {'.zuul.yaml': None}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        # Then propose a change to delete the job from one repo...
        file_dict = {'.zuul.yaml': None}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        # ...and a second that depends on it that adds it to another repo.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - project:
                name: org/project1
                check:
                  jobs:
                    - project-test1
            """)
        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)
        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test1.yaml': in_repo_playbook}
        C = self.fake_gerrit.addFakeChange('org/project1', 'master', 'C',
                                           files=file_dict,
                                           parent='refs/changes/01/1/1')
        C.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            C.subject, B.data['id'])
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS', changes='2,1 3,1'),
        ], ordered=False)

    @skipIfMultiScheduler()
    # This test is failing depending on which scheduler completed the
    # tenant reconfiguration first. As the assertions are done with the
    # objects on scheduler-0, they will fail if scheduler-1 completed
    # the reconfiguration first.
    def test_multi_repo(self):
        downstream_repo_conf = textwrap.dedent(
            """
            - project:
                name: org/project1
                tenant-one-gate:
                  jobs:
                    - project-test1

            - job:
                name: project1-test1
                parent: project-test1
            """)

        file_dict = {'.zuul.yaml': downstream_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        upstream_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - job:
                name: project-test2

            - project:
                name: org/project
                tenant-one-gate:
                  jobs:
                    - project-test1
            """)

        file_dict = {'.zuul.yaml': upstream_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(B.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(B.getChangeMergedEvent())
        self.waitUntilSettled()

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        # Ensure the latest change is reflected in the config; if it
        # isn't this will raise an exception.
        tenant.layout.getJob('project-test2')

    def test_pipeline_error(self):
        with open(os.path.join(FIXTURE_DIR,
                               'config/in-repo/git/',
                               'common-config/zuul.yaml')) as f:
            base_common_config = f.read()

        in_repo_conf_A = textwrap.dedent(
            """
            - pipeline:
                name: periodic
                foo: error
            """)

        file_dict = {'zuul.yaml': None,
                     'zuul.d/main.yaml': base_common_config,
                     'zuul.d/test1.yaml': in_repo_conf_A}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('syntax error',
                      A.messages[0],
                      "A should have an error reported")

    def test_pipeline_supercedes_error(self):
        with open(os.path.join(FIXTURE_DIR,
                               'config/in-repo/git/',
                               'common-config/zuul.yaml')) as f:
            base_common_config = f.read()

        in_repo_conf_A = textwrap.dedent(
            """
            - pipeline:
                name: periodic
                manager: independent
                supercedes: doesnotexist
                trigger: {}
            """)

        file_dict = {'zuul.yaml': None,
                     'zuul.d/main.yaml': base_common_config,
                     'zuul.d/test1.yaml': in_repo_conf_A}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertIn('supercedes an unknown',
                      A.messages[0],
                      "A should have an error reported")

    def test_change_series_error(self):
        with open(os.path.join(FIXTURE_DIR,
                               'config/in-repo/git/',
                               'common-config/zuul.yaml')) as f:
            base_common_config = f.read()

        in_repo_conf_A = textwrap.dedent(
            """
            - pipeline:
                name: periodic
                foo: error
            """)

        file_dict = {'zuul.yaml': None,
                     'zuul.d/main.yaml': base_common_config,
                     'zuul.d/test1.yaml': in_repo_conf_A}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)

        in_repo_conf_B = textwrap.dedent(
            """
            - job:
                name: project-test2
                foo: error
            """)

        file_dict = {'zuul.yaml': None,
                     'zuul.d/main.yaml': base_common_config,
                     'zuul.d/test1.yaml': in_repo_conf_A,
                     'zuul.d/test2.yaml': in_repo_conf_B}
        B = self.fake_gerrit.addFakeChange('common-config', 'master', 'B',
                                           files=file_dict)
        B.setDependsOn(A, 1)
        C = self.fake_gerrit.addFakeChange('common-config', 'master', 'C')
        C.setDependsOn(B, 1)
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(C.reported, 1,
                         "C should report failure")
        self.assertIn('This change depends on a change '
                      'with an invalid configuration.',
                      C.messages[0],
                      "C should have an error reported")

    def test_pipeline_debug(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml
            - project:
                name: org/project
                check:
                  debug: True
                  jobs:
                    - project-test1
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1,
                         "A should report success")
        self.assertIn('Debug information:',
                      A.messages[0], "A should have debug info")

    def test_nodeset_alternates_cycle(self):
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: red
                alternatives: [blue]
            - nodeset:
                name: blue
                alternatives: [red]
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml
                nodeset: blue
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn("cycle detected", A.messages[0])

    def test_nodeset_alternates_missing_from_nodeset(self):
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: red
                alternatives: [blue]
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('nodeset "blue" was not found', A.messages[0])

    def test_nodeset_alternates_missing_from_job(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml
                nodeset:
                  alternatives: [red]
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('nodeset "red" was not found', A.messages[0])

    @skipIfMultiScheduler()
    # See comment in TestInRepoConfigDir.scheduler_count for further
    # details.
    # As this is the only test within this test class, that doesn't work
    # with multi scheduler, we skip it rather than setting the
    # scheduler_count to 1 for the whole test class.
    def test_file_move(self):
        # Tests that a zuul config file can be renamed
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test2
                parent: project-test1

            - project:
                check:
                  jobs:
                    - project-test2
            """)
        file_dict = {'.zuul.yaml': None,
                     '.zuul.d/newfile.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
        ], ordered=True)

        self.scheds[0].sched.stop()
        self.scheds[0].sched.join()
        del self.scheds[0]
        self.log.debug("Restarting scheduler")
        self.createScheduler()
        self.scheds[0].start(self.validate_tenants)

        self.waitUntilSettled()

        # The fake gerrit was lost with the scheduler shutdown;
        # restore the state we care about:
        self.fake_gerrit.change_number = 2

        C = self.fake_gerrit.addFakeChange('org/project1', 'master', 'C')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
            dict(name='project-test2', result='SUCCESS', changes='3,1'),
        ], ordered=True)

    @simple_layout('layouts/empty-check.yaml')
    def test_merge_commit(self):
        # Test a .zuul.yaml content change in a merge commit

        self.create_branch('org/project', 'stable/queens')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/queens'))
        self.waitUntilSettled()

        conf = textwrap.dedent(
            """
            - job:
                name: test-job

            - project:
                name: org/project
                check:
                  jobs:
                    - test-job
            """)

        file_dict = {'.zuul.yaml': conf}

        A = self.fake_gerrit.addFakeChange('org/project', 'stable/queens', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        upstream_path = os.path.join(self.upstream_root, 'org/project')
        upstream_repo = git.Repo(upstream_path)
        master_sha = upstream_repo.heads.master.commit.hexsha

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([])

        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C',
                                           merge_parents=[
                                               master_sha,
                                               A.patchsets[-1]['revision'],
                                           ],
                                           merge_files=['.zuul.yaml'])

        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='3,1'),
        ], ordered=True)

    def test_final_parent(self):
        # If all variants of the parent are final, it is an error.
        # This doesn't catch all possibilities (that is handled during
        # job freezing) but this may catch most errors earlier.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: parent
                final: true
            - job:
                name: project-test1
                parent: parent
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('is final and can not act as a parent', A.messages[0])

    def test_intermediate_parent(self):
        # If all variants of the parent are intermediate and this job
        # is not abstract, it is an error.
        # This doesn't catch all possibilities (that is handled during
        # job freezing) but this may catch most errors earlier.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: parent
                intermediate: true
                abstract: true
            - job:
                name: project-test1
                parent: parent
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('is not abstract', A.messages[0])

    @simple_layout('layouts/protected-parent.yaml')
    def test_protected_parent(self):
        # If a parent is protected, it may only be used by a child in
        # the same project.
        # This doesn't catch all possibilities (that is handled during
        # job freezing) but this may catch most errors earlier.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                parent: protected-job
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('protected job in a different project', A.messages[0])

    def test_window_ceiling(self):
        in_repo_conf = textwrap.dedent(
            """
            - pipeline:
                name: test
                manager: dependent
                window-floor: 3
                window-ceiling: 2
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1)
        self.assertIn('ceiling may not be less than', A.messages[0])

    def test_pre_timeout_config_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test
                pre-timeout: 60
                timeout: 30
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1)
        self.assertIn('exceeds its timeout of 30', A.messages[0])

    def test_pre_timeout_config_error_inheritance(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: parent
                pre-timeout: 60
            - job:
                name: project-test1
                parent: parent
                timeout: 30
                run: playbooks/project-test1.yaml
            - project:
                check:
                  jobs: ['project-test1']
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1)
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
        ], ordered=True)
        build = self.getJobFromHistory('project-test1')
        self.assertEqual(30, build.job.timeout)
        self.assertEqual(30, build.job.pre_timeout)


class TestInRepoConfigSOS(ZuulTestCase):
    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/in-repo/main.yaml'
    # Those tests are testing specific interactions between multiple
    # schedulers. They create additional schedulers as necessary and
    # start or stop them individually to test specific interactions.
    # Using the scheduler_count in addition to create even more
    # schedulers doesn't make sense for those tests.
    scheduler_count = 1

    def test_cross_scheduler_config_update(self):
        # This is a regression test.  We observed duplicate entries in
        # the TPC config cache when a second scheduler updates its
        # layout.  This test performs a reconfiguration on one
        # scheduler, then allows the second scheduler to process the
        # change.

        # Create the second scheduler.
        self.waitUntilSettled()
        self.createScheduler()
        self.scheds[1].start()
        self.waitUntilSettled()

        # Create a change which will trigger a tenant configuration
        # update.
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: test-nodeset
                nodes: []
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        X = self.fake_gerrit.addFakeChange('org/project1', 'master', 'X',
                                           files=file_dict)
        X.setMerged()

        # Let the first scheduler process the reconfiguration.
        with self.scheds[1].sched.run_handler_lock:
            self.fake_gerrit.addEvent(X.getChangeMergedEvent())
            self.waitUntilSettled(matcher=[self.scheds[0]])

        # Wait for the second scheduler to update its config to match.
        self.waitUntilSettled()

        # Do the same process again.
        X = self.fake_gerrit.addFakeChange('org/project1', 'master', 'X',
                                           files=file_dict)
        X.setMerged()
        with self.scheds[1].sched.run_handler_lock:
            self.fake_gerrit.addEvent(X.getChangeMergedEvent())
            self.waitUntilSettled(matcher=[self.scheds[0]])

        # And wait for the second scheduler again.  If we're re-using
        # cache objects, we will have created duplicates at this
        # point.
        self.waitUntilSettled()

        # Create a change which will perform a dynamic config update.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-testx
                parent: common-config-test
            - project:
                check:
                  jobs:
                    - project-testx
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        with self.scheds[0].sched.run_handler_lock:
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[self.scheds[1]])
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-testx', result='SUCCESS', changes='3,1'),
        ], ordered=False)


class TestInRepoConfigDir(ZuulTestCase):
    # Like TestInRepoConfig, but the fixture test files are in zuul.d
    tenant_config_file = 'config/in-repo-dir/main.yaml'

    # These tests fiddle around with the list of schedulers used in
    # the test. They delete the existing scheduler and replace it by
    # a new one. This wouldn't work with multiple schedulers as the
    # new scheduler wouldn't replace the one at self.scheds[0], but
    # any of the other schedulers used within a multi-scheduler setup.
    # As a result, starting self.scheds[0] would fail because it is
    # already running an threads can only be started once.
    scheduler_count = 1

    def test_file_move(self):
        # Tests that a zuul config file can be renamed
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test2

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test2
            """)
        file_dict = {'zuul.d/project.yaml': None,
                     'zuul.d/newfile.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
        ], ordered=True)

        self.scheds[0].sched.stop()
        self.scheds[0].sched.join()
        del self.scheds[0]
        self.log.debug("Restarting scheduler")
        self.createScheduler()
        self.scheds[0].start(self.validate_tenants)

        self.waitUntilSettled()

        # The fake gerrit was lost with the scheduler shutdown;
        # restore the state we care about:
        self.fake_gerrit.change_number = 2

        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
            dict(name='project-test2', result='SUCCESS', changes='3,1'),
        ], ordered=True)

    def test_file_move_dependency(self):
        # Tests that a zuul config file can be modified and renamed
        # while also depending on another unrelated change.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test2

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test2
            """)
        file_dict = {'zuul.d/project.yaml': None,
                     'zuul.d/newfile.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data['url'])

        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='1,1 2,1'),
        ])

    def test_extra_config_move(self):
        # Tests that a extra config file can be renamed
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project1-test2

            - project:
                name: org/project1
                check:
                  jobs:
                    - project1-test2
            """)
        # Wait until settled so that we process both tenant reconfig
        # events in one pass through the scheduler loop.
        self.waitUntilSettled()
        # Add an empty zuul.yaml here so we are triggering a tenant
        # reconfig for both tenants as the extra config dir is only
        # considered for tenant-two.
        file_dict = {'zuul.yaml': '',
                     'extra.d/project.yaml': None,
                     'extra.d/newfile.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project1-test2', result='SUCCESS', changes='2,1'),
        ], ordered=True)

        self.scheds[0].sched.stop()
        self.scheds[0].sched.join()
        del self.scheds[0]
        self.log.debug("Restarting scheduler")
        self.createScheduler()
        self.scheds[0].start(self.validate_tenants)

        self.waitUntilSettled()

        # The fake gerrit was lost with the scheduler shutdown;
        # restore the state we care about:
        self.fake_gerrit.change_number = 2

        C = self.fake_gerrit.addFakeChange('org/project1', 'master', 'C')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project1-test2', result='SUCCESS', changes='2,1'),
            dict(name='project1-test2', result='SUCCESS', changes='3,1'),
        ], ordered=True)


class TestExtraConfigInDependent(ZuulTestCase):
    # in org/project2, jobs are defined in extra config paths, while
    # project is defined in .zuul.yaml
    tenant_config_file = 'config/in-repo-dir/main.yaml'
    scheduler_count = 1

    def test_extra_config_in_dependent_change(self):
        # Test that when jobs are defined in a extra-config-paths in a repo, if
        # another change is dependent on a change of that repo, the jobs should
        # still be loaded.

        # Add an empty zuul.yaml here so we are triggering dynamic layout load
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'zuul.yaml': ''})
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B',
                                           files={'zuul.yaml': ''})
        # A Depends-On: B who has private jobs defined in extra-config-paths
        A.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            A.subject, B.data['url'])

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Jobs in both changes should be success
        self.assertHistory([
            dict(name='project2-private-extra-file', result='SUCCESS',
                 changes='2,1'),
            dict(name='project2-private-extra-dir', result='SUCCESS',
                 changes='2,1'),
            dict(name='project-test1', result='SUCCESS',
                 changes='2,1 1,1'),
        ], ordered=False)

    def test_extra_config_in_bundle_change(self):
        # Test that jobs defined in a extra-config-paths in a repo should be
        # loaded in a bundle with changes from different repos.

        # Add an empty zuul.yaml here so we are triggering dynamic layout load
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'zuul.yaml': ''})
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B',
                                           files={'zuul.yaml': ''})
        C = self.fake_gerrit.addFakeChange('org/project3', 'master', 'C',
                                           files={'zuul.yaml': ''})
        # A B form a bundle, and A depends on C
        A.data['commitMessage'] = '%s\n\nDepends-On: %s\nDepends-On: %s\n' % (
            A.subject, B.data['url'], C.data['url'])
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['url'])

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Jobs in both changes should be success
        self.assertHistory([
            dict(name='project2-private-extra-file', result='SUCCESS',
                 changes='3,1 2,1 1,1'),
            dict(name='project2-private-extra-dir', result='SUCCESS',
                 changes='3,1 2,1 1,1'),
            dict(name='project-test1', result='SUCCESS',
                 changes='3,1 2,1 1,1'),
            dict(name='project3-private-extra-file', result='SUCCESS',
                 changes='3,1'),
            dict(name='project3-private-extra-dir', result='SUCCESS',
                 changes='3,1'),
        ], ordered=False)


class TestGlobalRepoState(AnsibleZuulTestCase):
    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/global-repo-state/main.yaml'

    def test_inherited_playbooks(self):
        # Test that the repo state is restored globally for the whole buildset
        # including inherited projects not in the dependency chain.
        self.executor_server.hold_jobs_in_start = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Approved', 1)
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 2))

        for _ in iterate_timeout(30, 'Wait for build to be in starting phase'):
            if self.executor_server.job_workers:
                sleep(1)
                break

        # The build test1 is running while test2 is waiting for test1.
        self.assertEqual(len(self.builds), 1)

        # Now merge a change to the playbook out of band. This will break test2
        # if it updates common-config to latest master. However due to the
        # buildset-global repo state test2 must not be broken afterwards.
        playbook = textwrap.dedent(
            """
            - hosts: localhost
              tasks:
                - name: fail
                  fail:
                    msg: foobar
            """)

        file_dict = {'playbooks/test2.yaml': playbook}
        B = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        self.log.info('Merge test change on common-config')
        B.setMerged()

        # Reset repo to ensure the cached repo has the failing commit. This
        # is needed to ensure that the repo state has been restored.
        repo = self.executor_server.merger.getRepo('gerrit', 'common-config')
        repo.update()
        repo.reset()

        self.executor_server.hold_jobs_in_start = False
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test1', result='SUCCESS', changes='1,1'),
            dict(name='test2', result='SUCCESS', changes='1,1'),
        ])

    def test_inherited_implicit_roles(self):
        # Test that the repo state is restored globally for the whole buildset
        # including inherited projects not in the dependency chain.
        self.executor_server.hold_jobs_in_start = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Approved', 1)
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 2))

        for _ in iterate_timeout(30, 'Wait for build to be in starting phase'):
            if self.executor_server.job_workers:
                sleep(1)
                break

        # The build test1 is running while test2 is waiting for test1.
        self.assertEqual(len(self.builds), 1)

        # Now merge a change to the role out of band. This will break test2
        # if it updates common-config to latest master. However due to the
        # buildset-global repo state test2 must not be broken afterwards.
        playbook = textwrap.dedent(
            """
            - name: fail
              fail:
                msg: foobar
            """)

        file_dict = {'roles/implicit-role/tasks/main.yaml': playbook}
        B = self.fake_gerrit.addFakeChange('org/implicit-role', 'master', 'A',
                                           files=file_dict)
        self.log.info('Merge test change on org/implicit-role')
        B.setMerged()

        # Reset repo to ensure the cached repo has the failing commit. This
        # is needed to ensure that the repo state has been restored.
        repo = self.executor_server.merger.getRepo(
            'gerrit', 'org/implicit-role')
        repo.update()
        repo.reset()

        self.executor_server.hold_jobs_in_start = False
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test1', result='SUCCESS', changes='1,1'),
            dict(name='test2', result='SUCCESS', changes='1,1'),
        ])

    def test_required_projects_unprotected_override_checkout(self):
        # Setup branch protection for master on org/requiringproject-github
        github = self.fake_github.getGithubClient()
        github.repo_from_project(
            'org/requiringproject-github')._set_branch_protection(
            'master', True)
        self.fake_github.emitEvent(self.fake_github.getPushEvent(
            'org/requiringproject-github', ref='refs/heads/master'))

        # Create unprotected branch feat-x. This branch will be the target
        # of override-checkout
        repo = github.repo_from_project('org/requiredproject-github')
        repo._set_branch_protection('master', True)
        self.create_branch('org/requiredproject-github', 'feat-x')
        repo._create_branch('feat-x')
        self.fake_github.emitEvent(self.fake_github.getPushEvent(
            'org/requiredproject-github', ref='refs/heads/feat-x'))

        # Wait until Zuul has processed the push events and knows about
        # the branch protection
        self.waitUntilSettled()

        A = self.fake_github.openFakePullRequest(
            'org/requiringproject-github', 'master', 'A')
        self.fake_github.emitEvent(A.getPullRequestOpenedEvent())
        self.waitUntilSettled()

        # Job must be successful
        self.assertHistory([
            dict(name='require-test1-github', result='SUCCESS'),
        ])

    def test_required_projects_branch_old_cache(self):
        self.create_branch('org/requiringproject', 'feat-x')
        self.create_branch('org/requiredproject', 'feat-x')

        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/requiringproject', 'feat-x'))

        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_start = True

        B = self.fake_gerrit.addFakeChange('org/requiringproject', 'feat-x',
                                           'A')
        B.addApproval('Approved', 1)
        self.fake_gerrit.addEvent(B.addApproval('Code-Review', 2))

        for _ in iterate_timeout(30, 'Wait for build to be in starting phase'):
            if self.executor_server.job_workers:
                sleep(1)
                break

        # Delete local feat-x from org/requiredproject on the executor cache
        repo = self.executor_server.merger.getRepo(
            'gerrit', 'org/requiredproject')
        repo.deleteRef('refs/heads/feat-x')

        # Let the job continue to the build phase
        self.executor_server.hold_jobs_in_build = True
        self.executor_server.hold_jobs_in_start = False
        self.waitUntilSettled()

        # Assert that feat-x has been checked out in the job workspace
        path = os.path.join(self.builds[0].jobdir.src_root,
                            'review.example.com/org/requiredproject')
        repo = git.Repo(path)
        self.assertEqual(str(repo.active_branch), 'feat-x')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()

        self.waitUntilSettled()
        self.assertHistory([
            dict(name='require-test1', result='SUCCESS', changes='1,1'),
            dict(name='require-test2', result='SUCCESS', changes='1,1'),
        ])

    def test_required_projects(self):
        # Test that the repo state is restored globally for the whole buildset
        # including required projects not in the dependency chain.
        self.executor_server.hold_jobs_in_start = True
        A = self.fake_gerrit.addFakeChange('org/requiringproject', 'master',
                                           'A')
        A.addApproval('Approved', 1)
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 2))

        for _ in iterate_timeout(30, 'Wait for build to be in starting phase'):
            if self.executor_server.job_workers:
                sleep(1)
                break

        # The build require-test1 is running,
        # require-test2 is waiting for require-test1.
        self.assertEqual(len(self.builds), 1)

        # Now merge a change to the test script out of band.
        # This will break required-test2 if it updates requiredproject
        # to latest master. However, due to the buildset-global repo state,
        # required-test2 must not be broken afterwards.
        runscript = textwrap.dedent(
            """
            #!/bin/bash
            exit 1
            """)

        file_dict = {'script.sh': runscript}
        B = self.fake_gerrit.addFakeChange('org/requiredproject', 'master',
                                           'A', files=file_dict)
        self.log.info('Merge test change on common-config')
        B.setMerged()

        # Reset repo to ensure the cached repo has the failing commit. This
        # is needed to ensure that the repo state has been restored.
        repo = self.executor_server.merger.getRepo(
            'gerrit', 'org/requiredproject')
        repo.update()
        repo.reset()

        self.executor_server.hold_jobs_in_start = False
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='require-test1', result='SUCCESS', changes='1,1'),
            dict(name='require-test2', result='SUCCESS', changes='1,1'),
        ])

    def test_repo_state_protected_branches(self):
        """
        Test that the global repo state includes all protected branches
        when we schedule no initial merge for branch/ref events.
        """
        self.create_branch('org/project-branches', 'stable')
        self.create_branch('org/project-branches', 'unprotected')
        github = self.fake_github.getGithubClient()
        repo = github.repo_from_project('org/project-branches')
        repo._set_branch_protection('master', True)
        repo._set_branch_protection('stable', True)
        self.fake_github.emitEvent(
            self.fake_github.getBranchProtectionRuleEvent(
                'org/project-branches', 'created'))
        self.waitUntilSettled()

        del self.merge_job_history

        A = self.fake_github.openFakePullRequest(
            'org/project-branches', 'master', 'A')
        A.setMerged("merging A")
        old_sha = random_sha1()
        new_sha = A.head_sha
        pevent = self.fake_github.getPushEvent(project='org/project-branches',
                                               ref='refs/heads/master',
                                               old_rev=old_sha,
                                               new_rev=new_sha)
        self.fake_github.emitEvent(pevent)
        self.waitUntilSettled()

        refstate_jobs = self.merge_job_history.get(
            zuul.model.MergeRequest.REF_STATE)
        self.assertEqual(len(refstate_jobs), 1)

        merge_request = refstate_jobs[0]
        self.assertEqual(
            set(merge_request.payload["branches"]), {"master", "stable"})

    def test_dependent_project(self):
        # Test that the repo state is restored globally for the whole buildset
        # including dependent projects.
        self.executor_server.hold_jobs_in_start = True
        B = self.fake_gerrit.addFakeChange('org/requiredproject', 'master',
                                           'B')
        A = self.fake_gerrit.addFakeChange('org/dependentproject', 'master',
                                           'A')
        A.setDependsOn(B, 1)
        A.addApproval('Approved', 1)
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 2))

        for _ in iterate_timeout(30, 'Wait for build to be in starting phase'):
            if self.executor_server.job_workers:
                sleep(1)
                break

        # The build dependent-test1 is running,
        # dependent-test2 is waiting for dependent-test1.
        self.assertEqual(len(self.builds), 1)

        # Now merge a change to the test script out of band.
        # This will break dependent-test2 if it updates requiredproject
        # to latest master. However, due to the buildset-global repo state,
        # dependent-test2 must not be broken afterwards.
        runscript = textwrap.dedent(
            """
            #!/bin/bash
            exit 1
            """)

        file_dict = {'script.sh': runscript}
        C = self.fake_gerrit.addFakeChange('org/requiredproject', 'master',
                                           'C', files=file_dict)
        self.log.info('Merge test change on common-config')
        C.setMerged()

        # Reset repo to ensure the cached repo has the failing commit. This
        # is needed to ensure that the repo state has been restored.
        repo = self.executor_server.merger.getRepo(
            'gerrit', 'org/requiredproject')
        repo.reset()

        self.executor_server.hold_jobs_in_start = False
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='dependent-test1', result='SUCCESS', changes='1,1 2,1'),
            dict(name='dependent-test2', result='SUCCESS', changes='1,1 2,1'),
        ])


class TestNonLiveMerges(ZuulTestCase):

    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/in-repo/main.yaml'

    def test_non_live_merges_with_config_updates(self):
        """
        This test checks that we do merges for non-live queue items with
        config updates.

        * Simple dependency chain:
          A -> B -> C

        """

        in_repo_conf_a = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test1
            """)
        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict_a = {'.zuul.yaml': in_repo_conf_a,
                       'playbooks/project-test.yaml': in_repo_playbook}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict_a)

        in_repo_conf_b = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test.yaml
            - job:
                name: project-test2
                run: playbooks/project-test.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test1
                    - project-test2
            """)
        file_dict_b = {'.zuul.yaml': in_repo_conf_b}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict_b,
                                           parent=A.patchsets[0]['ref'])
        B.setDependsOn(A, 1)

        in_repo_conf_c = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test.yaml
            - job:
                name: project-test2
                run: playbooks/project-test.yaml
            - job:
                name: project-test3
                run: playbooks/project-test.yaml

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test1
                    - project-test2
                    - project-test3
            """)
        file_dict_c = {'.zuul.yaml': in_repo_conf_c}
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C',
                                           files=file_dict_c,
                                           parent=B.patchsets[0]['ref'])
        C.setDependsOn(B, 1)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1, "A should report")
        self.assertEqual(B.reported, 1, "B should report")
        self.assertEqual(C.reported, 1, "C should report")

        self.assertIn('Build succeeded', A.messages[0])
        self.assertIn('Build succeeded', B.messages[0])
        self.assertIn('Build succeeded', C.messages[0])

        self.assertHistory([
            # Change A
            dict(name='project-test1', result='SUCCESS', changes='1,1'),

            # Change B
            dict(name='project-test1', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1 2,1'),

            # Change C
            dict(name='project-test1', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
            dict(name='project-test2', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
            dict(name='project-test3', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
        ], ordered=False)

        # We expect one merge call per live change, plus one call for
        # each non-live change with a config update (which is all of them).
        merge_jobs = self.merge_job_history.get(MergeRequest.MERGE)
        self.assertEqual(len(merge_jobs), 6)

    def test_non_live_merges(self):
        """
        This test checks that we don't do merges for non-live queue items.

        * Simple dependency chain:
          A -> B -> C
        """

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)

        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.setDependsOn(B, 1)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # We expect one merge call per live change.
        merge_jobs = self.merge_job_history.get(MergeRequest.MERGE)
        self.assertEqual(len(merge_jobs), 3)


class TestJobContamination(AnsibleZuulTestCase):

    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/zuul-job-contamination/main.yaml'
    # Those tests are also using the fake github implementation which
    # means that every scheduler gets a different fake github instance.
    # Thus, assertions might fail depending on which scheduler did the
    # interaction with Github.
    scheduler_count = 1

    def test_job_contamination_playbooks(self):
        conf = textwrap.dedent(
            """
            - job:
                name: base
                post-run:
                  - playbooks/something-new.yaml
                parent: null
                vars:
                  basevar: basejob
            """)

        file_dict = {'zuul.d/jobs.yaml': conf}
        A = self.fake_github.openFakePullRequest(
            'org/global-config', 'master', 'A', files=file_dict)
        self.fake_github.emitEvent(A.getPullRequestOpenedEvent())
        self.waitUntilSettled()

        B = self.fake_github.openFakePullRequest('org/project1', 'master', 'A')
        self.fake_github.emitEvent(B.getPullRequestOpenedEvent())
        self.waitUntilSettled()

        statuses_b = self.fake_github.getCommitStatuses(
            'org/project1', B.head_sha)

        self.assertEqual(len(statuses_b), 1)

        # B should not be affected by the A PR
        self.assertEqual('success', statuses_b[0]['state'])

    def test_job_contamination_vars(self):
        conf = textwrap.dedent(
            """
            - job:
                name: base
                parent: null
                vars:
                  basevar: basejob-modified
            """)

        file_dict = {'zuul.d/jobs.yaml': conf}
        A = self.fake_github.openFakePullRequest(
            'org/global-config', 'master', 'A', files=file_dict)
        self.fake_github.emitEvent(A.getPullRequestOpenedEvent())
        self.waitUntilSettled()

        B = self.fake_github.openFakePullRequest('org/project1', 'master', 'A')
        self.fake_github.emitEvent(B.getPullRequestOpenedEvent())
        self.waitUntilSettled()

        statuses_b = self.fake_github.getCommitStatuses(
            'org/project1', B.head_sha)

        self.assertEqual(len(statuses_b), 1)

        # B should not be affected by the A PR
        self.assertEqual('success', statuses_b[0]['state'])


class TestInRepoJoin(ZuulTestCase):
    # In this config, org/project is not a member of any pipelines, so
    # that we may test the changes that cause it to join them.

    tenant_config_file = 'config/in-repo-join/main.yaml'

    def test_dynamic_dependent_pipeline(self):
        # Test dynamically adding a project to a
        # dependent pipeline for the first time
        self.executor_server.hold_jobs_in_build = True

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        gate_manager = tenant.layout.pipeline_managers['gate']
        self.assertEqual(gate_manager.state.queues, [])

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - job:
                name: project-test2
                run: playbooks/project-test2.yaml

            - project:
                name: org/project
                gate:
                  jobs:
                    - project-test2
            """)

        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test2.yaml': in_repo_playbook}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(items[0].changes[0].number, '1')
        self.assertEqual(items[0].changes[0].patchset, '1')
        self.assertTrue(items[0].live)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # Make sure the dynamic queue got cleaned up
        self.assertEqual(gate_manager.state.queues, [])

    def test_dynamic_dependent_pipeline_failure(self):
        # Test that a change behind a failing change adding a project
        # to a dependent pipeline is dequeued.
        self.executor_server.hold_jobs_in_build = True

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - project:
                name: org/project
                gate:
                  jobs:
                    - project-test1
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.executor_server.failJob('project-test1', A)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.orderedRelease()
        self.waitUntilSettled()
        self.assertEqual(A.reported, 2,
                         "A should report start and failure")
        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.reported, 1,
                         "B should report start")
        self.assertHistory([
            dict(name='project-test1', result='FAILURE', changes='1,1'),
            dict(name='project-test1', result='ABORTED', changes='1,1 2,1'),
        ], ordered=False)

    def test_dynamic_failure_with_reconfig(self):
        # Test that a reconfig in the middle of adding a change to a
        # pipeline works.
        self.executor_server.hold_jobs_in_build = True

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - project:
                name: org/project
                gate:
                  jobs:
                    - project-test1
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.executor_server.failJob('project-test1', A)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))

        # Execute a reconfig here which will clear the cached layout
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        self.orderedRelease()
        self.waitUntilSettled()
        self.assertEqual(A.reported, 2,
                         "A should report start and failure")
        self.assertEqual(A.data['status'], 'NEW')
        self.assertHistory([
            dict(name='project-test1', result='FAILURE', changes='1,1'),
        ], ordered=False)

    def test_dynamic_dependent_pipeline_merge_failure(self):
        # Test that a merge failure behind a change adding a project
        # to a dependent pipeline is correctly reported.
        self.executor_server.hold_jobs_in_build = True

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test1
                run: playbooks/project-test1.yaml

            - project:
                name: org/project
                gate:
                  jobs:
                    - project-test1
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test2
                run: playbooks/project-test1.yaml

            - project:
                name: org/project
                gate:
                  jobs:
                    - project-test2
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.orderedRelease()
        self.waitUntilSettled()
        self.assertEqual(A.reported, 2,
                         "A should report start and success")
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.reported, 1,
                         "B should report merge failure")
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_dynamic_dependent_pipeline_absent(self):
        # Test that a series of dependent changes don't report merge
        # failures to a pipeline they aren't in.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)

        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 0,
                         "A should not report")
        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.reported, 0,
                         "B should not report")
        self.assertEqual(B.data['status'], 'NEW')
        self.assertHistory([])


class FunctionalAnsibleMixIn(object):
    # A temporary class to hold new tests while others are disabled

    # These should be overridden in child classes.
    tenant_config_file = 'config/ansible/main.yaml'
    ansible_major_minor = 'X.Y'

    def test_playbook(self):
        # This test runs a bit long and needs extra time.
        self.wait_timeout = 300
        # Keep the jobdir around so we can inspect contents if an
        # assert fails.
        self.executor_server.keep_jobdir = True
        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        # Add a site variables file, used by check-vars
        path = os.path.join(FIXTURE_DIR, 'config', 'ansible',
                            'variables.yaml')
        self.config.set('executor', 'variables', path)
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        build_timeout = self.getJobFromHistory('timeout', result='TIMED_OUT')
        with self.jobLog(build_timeout):
            post_flag_path = os.path.join(
                self.jobdir_root, build_timeout.uuid, 'work', 'post_flag')
            self.assertTrue(os.path.exists(post_flag_path))
        build_multiple_run_timeout = self.getJobFromHistory(
            'multiple-run-timeout', result='TIMED_OUT')
        with self.jobLog(build_multiple_run_timeout):
            post_flag_path = os.path.join(
                self.jobdir_root,
                build_multiple_run_timeout.uuid, 'work', 'post_flag')
            self.assertTrue(os.path.exists(post_flag_path))
            post_zuul_success_flag_path = os.path.join(
                self.jobdir_root,
                build_multiple_run_timeout.uuid,
                'work', 'post_zuul_success_flag')
            self.assertFalse(os.path.exists(post_zuul_success_flag_path))
        build_pre_timeout = self.getJobFromHistory('pre-timeout')
        with self.jobLog(build_pre_timeout):
            # Failures in pre-run have a None result and then Zuul determines
            # if they should be retried from there.
            self.assertEqual(build_pre_timeout.result, None)
            post_flag_path = os.path.join(
                self.jobdir_root, build_pre_timeout.uuid, 'work', 'post_flag')
            self.assertTrue(os.path.exists(post_flag_path))
        build_post_timeout = self.getJobFromHistory('post-timeout')
        with self.jobLog(build_post_timeout):
            self.assertEqual(build_post_timeout.result, 'POST_FAILURE')
        build_faillocal = self.getJobFromHistory('faillocal')
        with self.jobLog(build_faillocal):
            self.assertEqual(build_faillocal.result, 'FAILURE')
        build_failpost = self.getJobFromHistory('failpost')
        with self.jobLog(build_failpost):
            self.assertEqual(build_failpost.result, 'POST_FAILURE')
        build_check_vars = self.getJobFromHistory('check-vars')
        with self.jobLog(build_check_vars):
            self.assertEqual(build_check_vars.result, 'SUCCESS')
        build_check_hostvars = self.getJobFromHistory('check-hostvars')
        with self.jobLog(build_check_hostvars):
            self.assertEqual(build_check_hostvars.result, 'SUCCESS')
        build_check_secret_names = self.getJobFromHistory('check-secret-names')
        with self.jobLog(build_check_secret_names):
            self.assertEqual(build_check_secret_names.result, 'SUCCESS')
        build_hello = self.getJobFromHistory('hello-world')
        with self.jobLog(build_hello):
            self.assertEqual(build_hello.result, 'SUCCESS')
        build_add_host = self.getJobFromHistory('add-host')
        with self.jobLog(build_add_host):
            self.assertEqual(build_add_host.result, 'SUCCESS')
        build_multiple_child = self.getJobFromHistory('multiple-child')
        with self.jobLog(build_multiple_child):
            self.assertEqual(build_multiple_child.result, 'SUCCESS')
        build_multiple_child_no_run = self.getJobFromHistory(
            'multiple-child-no-run')
        with self.jobLog(build_multiple_child_no_run):
            self.assertEqual(build_multiple_child_no_run.result, 'SUCCESS')
        build_multiple_run = self.getJobFromHistory('multiple-run')
        with self.jobLog(build_multiple_run):
            self.assertEqual(build_multiple_run.result, 'SUCCESS')
        build_multiple_run_failure = self.getJobFromHistory(
            'multiple-run-failure')
        with self.jobLog(build_multiple_run_failure):
            self.assertEqual(build_multiple_run_failure.result, 'FAILURE')
        build_python27 = self.getJobFromHistory('python27')
        with self.jobLog(build_python27):
            self.assertEqual(build_python27.result, 'SUCCESS')
            flag_path = os.path.join(self.jobdir_root,
                                     build_python27.uuid, 'work', 'flag')
            self.assertTrue(os.path.exists(flag_path))
            copied_path = os.path.join(self.jobdir_root, build_python27.uuid,
                                       'work', 'copied')
            self.assertTrue(os.path.exists(copied_path))
            failed_path = os.path.join(self.jobdir_root, build_python27.uuid,
                                       'work', 'failed')
            self.assertFalse(os.path.exists(failed_path))
            pre_flag_path = os.path.join(
                self.jobdir_root, build_python27.uuid, 'work', 'pre_flag')
            self.assertTrue(os.path.exists(pre_flag_path))
            post_flag_path = os.path.join(
                self.jobdir_root, build_python27.uuid, 'work', 'post_flag')
            self.assertTrue(os.path.exists(post_flag_path))
            bare_role_flag_path = os.path.join(self.jobdir_root,
                                               build_python27.uuid,
                                               'work', 'bare_role_flag')
            self.assertTrue(os.path.exists(bare_role_flag_path))
            secrets_path = os.path.join(self.jobdir_root,
                                        build_python27.uuid, 'work', 'secrets')
            with open(secrets_path) as f:
                self.assertEqual(f.read(), "test-username test-password")
        build_bubblewrap = self.getJobFromHistory('bubblewrap')
        with self.jobLog(build_bubblewrap):
            self.assertEqual(build_bubblewrap.result, 'SUCCESS')

    def test_repo_ansible(self):
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/ansible', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1,
                         "A should report success")
        self.assertHistory([
            dict(name='hello-ansible', result='SUCCESS', changes='1,1'),
        ])
        build = self.getJobFromHistory('hello-ansible', result='SUCCESS')
        with open(build.jobdir.job_output_file) as f:
            output = f.read()
            self.assertIn(f'Ansible version={self.ansible_major_minor}',
                          output)


class TestAnsible9(AnsibleZuulTestCase, FunctionalAnsibleMixIn):
    tenant_config_file = 'config/ansible/main9.yaml'
    ansible_major_minor = '2.16'


class TestAnsible11(AnsibleZuulTestCase, FunctionalAnsibleMixIn):
    tenant_config_file = 'config/ansible/main11.yaml'
    ansible_major_minor = '2.18'


class TestPrePlaybooks(AnsibleZuulTestCase):
    # A temporary class to hold new tests while others are disabled

    tenant_config_file = 'config/pre-playbook/main.yaml'

    def test_pre_playbook_fail(self):
        # Test that we run the post playbooks (but not the actual
        # playbook) when a pre-playbook fails.
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        build = self.getJobFromHistory('python27')
        self.assertIsNone(build.result)
        self.assertIn('RETRY_LIMIT', A.messages[0])
        flag_path = os.path.join(self.test_root, build.uuid, 'work',
                                 'main_flag')
        self.assertFalse(os.path.exists(flag_path))
        pre_flag_path = os.path.join(self.test_root, build.uuid, 'work',
                                     'pre_flag')
        self.assertFalse(os.path.exists(pre_flag_path))
        post_flag_path = os.path.join(
            self.jobdir_root, build.uuid, 'work', 'post_flag')
        self.assertTrue(os.path.exists(post_flag_path),
                        "The file %s should exist" % post_flag_path)

    def test_post_playbook_fail_autohold(self):
        self.addAutohold('tenant-one', 'review.example.com/org/project3',
                         'python27-node-post', '.*', 'reason text', 1, 600)

        A = self.fake_gerrit.addFakeChange('org/project3', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        build = self.getJobFromHistory('python27-node-post')
        self.assertEqual(build.result, 'POST_FAILURE')

        # Check nodepool for a held node
        held_node = None
        for node in self.fake_nodepool.getNodes():
            if node['state'] == zuul.model.STATE_HOLD:
                held_node = node
                break
        self.assertIsNotNone(held_node)
        # Validate node has recorded the failed job
        self.assertEqual(
            held_node['hold_job'],
            " ".join(['tenant-one',
                      'review.example.com/org/project3',
                      'python27-node-post', '.*'])
        )
        self.assertEqual(held_node['comment'], "reason text")

    def test_pre_playbook_fail_autohold(self):
        self.addAutohold('tenant-one', 'review.example.com/org/project2',
                         'python27-node', '.*', 'reason text', 1, 600)

        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        build = self.getJobFromHistory('python27-node')
        self.assertIsNone(build.result)
        self.assertIn('RETRY_LIMIT', A.messages[0])

        # Check nodepool for a held node
        held_node = None
        for node in self.fake_nodepool.getNodes():
            if node['state'] == zuul.model.STATE_HOLD:
                held_node = node
                break
        self.assertIsNotNone(held_node)
        # Validate node has recorded the failed job
        self.assertEqual(
            held_node['hold_job'],
            " ".join(['tenant-one',
                      'review.example.com/org/project2',
                      'python27-node', '.*'])
        )
        self.assertEqual(held_node['comment'], "reason text")


class TestPostPlaybooks(AnsibleZuulTestCase):
    tenant_config_file = 'config/post-playbook/main.yaml'

    def test_post_playbook_abort(self):
        # Test that when we abort a job in the post playbook, that we
        # don't send back POST_FAILURE.
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds):
                break
        build = self.builds[0]

        post_start = os.path.join(self.jobdir_root, build.uuid, 'work',
                                  'post_start_flag')
        for _ in iterate_timeout(60, 'job post running'):
            if os.path.exists(post_start):
                break
        # The post playbook has started, abort the job
        self.fake_gerrit.addEvent(A.getChangeAbandonedEvent())
        self.waitUntilSettled()

        build = self.getJobFromHistory('python27')
        self.assertEqual('ABORTED', build.result)

        post_end = os.path.join(self.jobdir_root, build.uuid, 'work',
                                'post_end_flag')
        self.assertTrue(os.path.exists(post_start))
        self.assertFalse(os.path.exists(post_end))


class TestCleanupPlaybooks(AnsibleZuulTestCase):
    tenant_config_file = 'config/cleanup-playbook/main.yaml'

    def test_cleanup_playbook_success(self):
        # Test that the cleanup run is performed
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds):
                break
        build = self.builds[0]

        post_start = os.path.join(self.jobdir_root, build.uuid, 'work',
                                  'post_start_flag')
        for _ in iterate_timeout(60, 'job post running'):
            if os.path.exists(post_start):
                break
        with open(os.path.join(self.jobdir_root, build.uuid,
                               'work', 'test_wait'),
                  "w") as of:
            of.write("continue")
        self.waitUntilSettled()

        build = self.getJobFromHistory('python27')
        self.assertEqual('SUCCESS', build.result)
        cleanup_flag = os.path.join(self.jobdir_root, build.uuid, 'work',
                                    'cleanup_flag')
        self.assertTrue(os.path.exists(cleanup_flag))
        with open(cleanup_flag) as f:
            self.assertEqual('True', f.readline())

    def test_cleanup_playbook_failure(self):
        # Test that the cleanup run is performed
        self.executor_server.keep_jobdir = True
        self.executor_server.verbose = True

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - python27-failure
            """)
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'.zuul.yaml': in_repo_conf})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds):
                break
        self.waitUntilSettled()

        build = self.getJobFromHistory('python27-failure')
        self.assertEqual('FAILURE', build.result)
        cleanup_flag = os.path.join(self.jobdir_root, build.uuid, 'work',
                                    'cleanup_flag')
        self.assertTrue(os.path.exists(cleanup_flag))
        with open(cleanup_flag) as f:
            self.assertEqual('False', f.readline())

    def test_cleanup_playbook_abort(self):
        # Test that when we abort a job the cleanup run is performed
        self.executor_server.keep_jobdir = True
        self.executor_server.verbose = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds):
                break
        build = self.builds[0]

        post_start = os.path.join(self.jobdir_root, build.uuid, 'work',
                                  'post_start_flag')
        for _ in iterate_timeout(60, 'job post running'):
            if os.path.exists(post_start):
                break
        # The post playbook has started, abort the job
        self.fake_gerrit.addEvent(A.getChangeAbandonedEvent())
        self.waitUntilSettled()

        build = self.getJobFromHistory('python27')
        self.assertEqual('ABORTED', build.result)

        post_end = os.path.join(self.jobdir_root, build.uuid, 'work',
                                'post_end_flag')
        cleanup_flag = os.path.join(self.jobdir_root, build.uuid, 'work',
                                    'cleanup_flag')
        self.assertTrue(os.path.exists(cleanup_flag))
        self.assertTrue(os.path.exists(post_start))
        self.assertFalse(os.path.exists(post_end))

    def test_cleanup_playbook_timeout(self):
        # Test that when the cleanup runs into a timeout, the job
        # still completes.
        self.executor_server.verbose = True

        # Change the zuul config to run the python27-cleanup-timeout job
        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - python27-cleanup-timeout
            """)
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A",
                                           files={".zuul.yaml": in_repo_conf})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="python27-cleanup-timeout", result="POST_FAILURE",
                 changes="1,1")])

    def test_cleanup_playbook_inheritance(self):
        # Test nested level aborting
        self.executor_server.keep_jobdir = True
        self.executor_server.verbose = True

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - child-cleanup-failure
            """)
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'.zuul.yaml': in_repo_conf})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds):
                break
        self.waitUntilSettled()

        build = self.getJobFromHistory('child-cleanup-failure')
        self.assertEqual('FAILURE', build.result)
        cleanup_flag = os.path.join(self.jobdir_root, build.uuid, 'work',
                                    'cleanup_flag')
        self.assertTrue(os.path.exists(cleanup_flag))
        with open(cleanup_flag) as f:
            self.assertEqual('False', f.readline())
        self.assertEqual(2, len(build.job.pre_run))
        self.assertEqual('playbooks/base-pre.yaml',
                         build.job.pre_run[0]['path'])
        self.assertEqual(0,
                         build.job.pre_run[0]['nesting_level'])
        self.assertEqual('playbooks/child-pre.yaml',
                         build.job.pre_run[1]['path'])
        self.assertEqual(2,
                         build.job.pre_run[1]['nesting_level'])

        self.assertEqual(5, len(build.job.post_run))

        self.assertEqual('playbooks/child-post.yaml',
                         build.job.post_run[0]['path'])
        self.assertEqual(2,
                         build.job.post_run[0]['nesting_level'])
        self.assertEqual('playbooks/child-cleanup.yaml',
                         build.job.post_run[1]['path'])
        self.assertEqual(2,
                         build.job.post_run[1]['nesting_level'])

        self.assertEqual('playbooks/cleanup.yaml',
                         build.job.post_run[2]['path'])
        self.assertEqual(1,
                         build.job.post_run[2]['nesting_level'])

        self.assertEqual('playbooks/base-post.yaml',
                         build.job.post_run[3]['path'])
        self.assertEqual(0,
                         build.job.post_run[3]['nesting_level'])
        self.assertEqual('playbooks/base-cleanup.yaml',
                         build.job.post_run[4]['path'])
        self.assertEqual(0,
                         build.job.post_run[4]['nesting_level'])

    def test_cleanup_playbook_inheritance_old_syntax(self):
        # Test nested level aborting with the old cleanup-run syntax
        self.executor_server.keep_jobdir = True
        self.executor_server.verbose = True

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - child-cleanup-failure-old-syntax
            """)
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'.zuul.yaml': in_repo_conf})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds):
                break
        self.waitUntilSettled()

        build = self.getJobFromHistory('child-cleanup-failure-old-syntax')
        self.assertEqual('FAILURE', build.result)
        cleanup_flag = os.path.join(self.jobdir_root, build.uuid, 'work',
                                    'cleanup_flag')
        self.assertTrue(os.path.exists(cleanup_flag))
        with open(cleanup_flag) as f:
            self.assertEqual('False', f.readline())
        self.assertEqual(2, len(build.job.pre_run))
        self.assertEqual('playbooks/base-pre.yaml',
                         build.job.pre_run[0]['path'])
        self.assertEqual(0,
                         build.job.pre_run[0]['nesting_level'])
        self.assertEqual('playbooks/child-pre.yaml',
                         build.job.pre_run[1]['path'])
        self.assertEqual(2,
                         build.job.pre_run[1]['nesting_level'])

        # This job has one old and one new cleanup playbook
        self.assertEqual(4, len(build.job.post_run))

        self.assertEqual('playbooks/child-post.yaml',
                         build.job.post_run[0]['path'])
        self.assertEqual(2,
                         build.job.post_run[0]['nesting_level'])

        self.assertEqual('playbooks/cleanup.yaml',
                         build.job.post_run[1]['path'])
        self.assertEqual(1,
                         build.job.post_run[1]['nesting_level'])

        self.assertEqual('playbooks/base-post.yaml',
                         build.job.post_run[2]['path'])
        self.assertEqual(0,
                         build.job.post_run[2]['nesting_level'])
        self.assertEqual('playbooks/base-cleanup.yaml',
                         build.job.post_run[3]['path'])
        self.assertEqual(0,
                         build.job.post_run[3]['nesting_level'])

        self.assertEqual(1, len(build.job.cleanup_run))

        self.assertEqual('playbooks/child-cleanup.yaml',
                         build.job.cleanup_run[0]['path'])
        self.assertEqual(2,
                         build.job.cleanup_run[0]['nesting_level'])

        # Verify that we have mutated cleanup to post
        self.assertEqual(5, len(build.jobdir.post_playbooks))
        self.assertEqual(0, len(build.jobdir.cleanup_playbooks))

        self.assertTrue(build.jobdir.post_playbooks[0].path.
                        endswith('playbooks/child-post.yaml'))
        self.assertEqual(2,
                         build.jobdir.post_playbooks[0].nesting_level)

        self.assertTrue(build.jobdir.post_playbooks[1].path.
                        endswith('playbooks/child-cleanup.yaml'))
        self.assertEqual(2,
                         build.jobdir.post_playbooks[1].nesting_level)

        self.assertTrue(build.jobdir.post_playbooks[2].path.
                        endswith('playbooks/cleanup.yaml'))
        self.assertEqual(1,
                         build.jobdir.post_playbooks[2].nesting_level)

        self.assertTrue(build.jobdir.post_playbooks[3].path.
                        endswith('playbooks/base-post.yaml'))
        self.assertEqual(0,
                         build.jobdir.post_playbooks[3].nesting_level)
        self.assertTrue(build.jobdir.post_playbooks[4].path.
                        endswith('playbooks/base-cleanup.yaml'))
        self.assertEqual(0,
                         build.jobdir.post_playbooks[4].nesting_level)

        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 2)
        self.assertEqual(errors[0].severity, SEVERITY_WARNING)
        self.assertEqual(errors[0].name, 'Cleanup Run Deprecation')
        self.assertIn('job stanza', errors[0].error)

    def test_cleanup_playbook_unreachable(self):
        # Test that an unreachable cleanup-run playbook produces a
        # POST_FAILURE
        self.executor_server.keep_jobdir = True
        self.executor_server.verbose = True

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - child-cleanup-failure-unreachable
            """)
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'.zuul.yaml': in_repo_conf})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds):
                break
        self.waitUntilSettled()

        build = self.getJobFromHistory('child-cleanup-failure-unreachable')
        self.assertEqual('POST_FAILURE', build.result)
        cleanup_flag = os.path.join(self.jobdir_root, build.uuid, 'work',
                                    'cleanup_flag')
        self.assertTrue(os.path.exists(cleanup_flag))
        with open(cleanup_flag) as f:
            self.assertEqual('False', f.readline())

    def test_cleanup_playbook_unreachable_old_syntax(self):
        # Test that an unreachable cleanup-run playbook produces a
        # does not override the normal FAILURE using the old syntax
        self.executor_server.keep_jobdir = True
        self.executor_server.verbose = True

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - child-cleanup-failure-unreachable-old-syntax
            """)
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'.zuul.yaml': in_repo_conf})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds):
                break
        self.waitUntilSettled()

        build = self.getJobFromHistory(
            'child-cleanup-failure-unreachable-old-syntax')
        self.assertEqual('FAILURE', build.result)
        cleanup_flag = os.path.join(self.jobdir_root, build.uuid, 'work',
                                    'cleanup_flag')
        self.assertTrue(os.path.exists(cleanup_flag))
        with open(cleanup_flag) as f:
            self.assertEqual('False', f.readline())


class TestPlaybookSemaphore(AnsibleZuulTestCase):
    tenant_config_file = 'config/playbook-semaphore/main.yaml'

    def test_playbook_semaphore(self):
        self.executor_server.verbose = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds) == 1:
                break
        build1 = self.builds[0]

        # Wait for the first job to be running the mutexed playbook
        run1_start = os.path.join(self.jobdir_root, build1.uuid, 'work',
                                  'run_start_flag')
        for _ in iterate_timeout(60, 'job1 running'):
            if os.path.exists(run1_start):
                break

        # Start a second build which should wait for the playbook
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))

        # Wait until we are waiting for the playbook
        for _ in iterate_timeout(60, 'job2 waiting for semaphore'):
            found = False
            if len(self.builds) == 2:
                build2 = self.builds[1]
                for job_worker in self.executor_server.job_workers.values():
                    if job_worker.build_request.uuid == build2.uuid:
                        if job_worker.waiting_for_semaphores:
                            found = True
            if found:
                break

        # Wait for build1 to finish
        with open(os.path.join(self.jobdir_root, build1.uuid,
                               'work', 'test_wait'),
                  "w") as of:
            of.write("continue")

        # Wait for the second job to be running the mutexed playbook
        run2_start = os.path.join(self.jobdir_root, build2.uuid, 'work',
                                  'run_start_flag')
        for _ in iterate_timeout(60, 'job2 running'):
            if os.path.exists(run2_start):
                break

        # Release build2 and wait to finish
        with open(os.path.join(self.jobdir_root, build2.uuid,
                               'work', 'test_wait'),
                  "w") as of:
            of.write("continue")
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='2,1'),
        ])

    def test_playbook_and_job_semaphore_runtime(self):
        # Test that a playbook does not specify the same semaphore as
        # the job.  Test via inheritance which is a runtime check.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test-job2
                parent: test-job
                semaphore: test-semaphore

            - project:
                check:
                  jobs:
                    - test-job2
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([])
        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('both job and playbook', A.messages[0])

    def test_playbook_and_job_semaphore_def(self):
        # Test that a playbook does not specify the same semaphore as
        # the job.  Static configuration test.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test-job2
                semaphore: test-semaphore
                run:
                  - name: playbooks/run.yaml
                    semaphores: test-semaphore

            - project:
                check:
                  jobs:
                    - test-job2
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([])
        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('both job and playbook', A.messages[0])

    def test_playbook_semaphore_timeout(self):
        self.wait_timeout = 300
        self.executor_server.verbose = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds) == 1:
                break
        build1 = self.builds[0]

        # Wait for the first job to be running the mutexed playbook
        run1_start = os.path.join(self.jobdir_root, build1.uuid, 'work',
                                  'run_start_flag')
        for _ in iterate_timeout(60, 'job1 running'):
            if os.path.exists(run1_start):
                break

        # Start a second build which should wait for the playbook
        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - test-job:
                        timeout: 20
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))

        # Wait until we are waiting for the playbook
        for _ in iterate_timeout(60, 'job2 waiting for semaphore'):
            found = False
            if len(self.builds) == 2:
                build2 = self.builds[1]
                for job_worker in self.executor_server.job_workers.values():
                    if job_worker.build_request.uuid == build2.uuid:
                        if job_worker.waiting_for_semaphores:
                            found = True
            if found:
                break

        # Wait for the second build to timeout waiting for the semaphore
        for _ in iterate_timeout(60, 'build timed out'):
            if len(self.builds) == 1:
                break

        # Wait for build1 to finish
        with open(os.path.join(self.jobdir_root, build1.uuid,
                               'work', 'test_wait'),
                  "w") as of:
            of.write("continue")

        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='TIMED_OUT', changes='2,1'),
            dict(name='test-job', result='SUCCESS', changes='1,1'),
        ])

    def test_playbook_semaphore_abort(self):
        self.wait_timeout = 300
        self.executor_server.verbose = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        for _ in iterate_timeout(60, 'job started'):
            if len(self.builds) == 1:
                break
        build1 = self.builds[0]

        # Wait for the first job to be running the mutexed playbook
        run1_start = os.path.join(self.jobdir_root, build1.uuid, 'work',
                                  'run_start_flag')
        for _ in iterate_timeout(60, 'job1 running'):
            if os.path.exists(run1_start):
                break

        # Start a second build which should wait for the playbook
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))

        # Wait until we are waiting for the playbook
        for _ in iterate_timeout(60, 'job2 waiting for semaphore'):
            found = False
            if len(self.builds) == 2:
                build2 = self.builds[1]
                for job_worker in self.executor_server.job_workers.values():
                    if job_worker.build_request.uuid == build2.uuid:
                        if job_worker.waiting_for_semaphores:
                            found = True
            if found:
                break

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs: []
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        B.addPatchset(files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(2))

        for _ in iterate_timeout(60, 'build aborted'):
            if len(self.builds) == 1:
                break

        # Wait for build1 to finish
        with open(os.path.join(self.jobdir_root, build1.uuid,
                               'work', 'test_wait'),
                  "w") as of:
            of.write("continue")

        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='ABORTED', changes='2,1'),
            dict(name='test-job', result='SUCCESS', changes='1,1'),
        ])


class TestBrokenTrustedConfig(ZuulTestCase):
    # Test we can deal with a broken config only with trusted projects. This
    # is different then TestBrokenConfig, as it does not have a missing
    # repo error.

    tenant_config_file = 'config/broken-trusted/main.yaml'

    def test_broken_config_on_startup(self):
        # verify get the errors at tenant level.
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        loading_errors = tenant.layout.loading_errors
        self.assertEqual(
            len(tenant.layout.loading_errors), 1,
            "An error should have been stored")
        self.assertIn(
            "Zuul encountered a syntax error",
            str(loading_errors[0].error))

    def test_trusted_broken_tenant_config(self):
        """
        Tests we cannot modify a config-project speculative by replacing
        check jobs with noop.
        """
        in_repo_conf = textwrap.dedent(
            """
            - pipeline:
                name: check
                manager: independent
                trigger:
                  gerrit:
                    - event: patchset-created
                success:
                  gerrit:
                    Verified: 1
                failure:
                  gerrit:
                    Verified: -1

            - job:
                name: base
                parent: null

            - project:
                name: common-config
                check:
                  jobs:
                    - noop
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='gate-noop', result='SUCCESS', changes='1,1')])


class TestBrokenConfig(ZuulTestCase):
    # Test we can deal with a broken config

    tenant_config_file = 'config/broken/main.yaml'

    def test_broken_config_on_startup(self):
        # verify get the errors at tenant level.
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-broken')
        loading_errors = tenant.layout.loading_errors
        self.assertEqual(
            len(tenant.layout.loading_errors), 2,
            "An error should have been stored")
        self.assertIn(
            "Zuul encountered an error while accessing the repo org/project3",
            str(loading_errors[0].error))
        self.assertIn(
            "Zuul encountered a syntax error",
            str(loading_errors[1].error))

    @simple_layout('layouts/broken-template.yaml')
    def test_broken_config_on_startup_template(self):
        # Verify that a missing project-template doesn't break gate
        # pipeline construction.
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(
            len(tenant.layout.loading_errors), 1,
            "An error should have been stored")
        self.assertIn(
            "Zuul encountered a syntax error",
            str(tenant.layout.loading_errors[0].error))

    @simple_layout('layouts/broken-double-gate.yaml')
    def test_broken_config_on_startup_double_gate(self):
        # Verify that duplicated pipeline definitions raise config errors
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(
            len(tenant.layout.loading_errors), 1,
            "An error should have been stored")
        self.assertIn(
            "Zuul encountered a syntax error",
            str(tenant.layout.loading_errors[0].error))

    @simple_layout('layouts/broken-warnings.yaml')
    def test_broken_config_on_startup_warnings(self):
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(
            len(tenant.layout.loading_errors), 1,
            "An error should have been stored")
        self.assertIn(
            "Zuul encountered a deprecated syntax",
            str(tenant.layout.loading_errors[0].error))

    def test_dynamic_ignore(self):
        # Verify dynamic config behaviors inside a tenant broken config
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-broken')
        # There is a configuration error
        self.assertEqual(
            len(tenant.layout.loading_errors), 2,
            "An error should have been stored")

        # Inside a broken tenant configuration environment,
        # send a valid config to an "unbroken" project and verify
        # that tenant configuration have been validated and job executed
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test
                run: playbooks/project-test.yaml

            - project:
                check:
                  jobs:
                    - project-test
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "1")
        self.assertHistory([
            dict(name='project-test', result='SUCCESS', changes='1,1')])

    def test_dynamic_fail_unbroken(self):
        # Verify dynamic config behaviors inside a tenant broken config
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-broken')
        # There is a configuration error
        self.assertEqual(
            len(tenant.layout.loading_errors), 2,
            "An error should have been stored")

        # Inside a broken tenant configuration environment,
        # send an invalid config to an "unbroken" project and verify
        # that tenant configuration have not been validated
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test
                run: playbooks/project-test.yaml

            - project:
                check:
                  jobs:
                    - non-existent-job
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(B.reported, 1,
                         "A should report failure")
        self.assertEqual(B.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('Job non-existent-job not defined', B.messages[0],
                      "A should have failed the check pipeline")

    def test_dynamic_fail_broken(self):
        # Verify dynamic config behaviors inside a tenant broken config
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-broken')
        # There is a configuration error
        self.assertEqual(
            len(tenant.layout.loading_errors), 2,
            "An error should have been stored")

        # Inside a broken tenant configuration environment,
        # send an invalid config to a "broken" project and verify
        # that tenant configuration have not been validated
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test
                run: playbooks/project-test.yaml

            - project:
                check:
                  jobs:
                    - non-existent-job
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        C = self.fake_gerrit.addFakeChange('org/project2', 'master', 'C',
                                           files=file_dict)
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(C.reported, 1,
                         "A should report failure")
        self.assertEqual(C.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('Job non-existent-job not defined', C.messages[0],
                      "A should have failed the check pipeline")

    def test_dynamic_fix_broken(self):
        # Verify dynamic config behaviors inside a tenant broken config
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-broken')
        # There is a configuration error
        self.assertEqual(
            len(tenant.layout.loading_errors), 2,
            "An error should have been stored")

        # Inside a broken tenant configuration environment,
        # send an valid config to a "broken" project and verify
        # that tenant configuration have been validated and job executed
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test2
                run: playbooks/project-test.yaml

            - project:
                check:
                  jobs:
                    - project-test2
         """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        D = self.fake_gerrit.addFakeChange('org/project2', 'master', 'D',
                                           files=file_dict)
        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(D.patchsets[0]['approvals'][0]['value'], "1")
        self.assertHistory([
            dict(name='project-test2', result='SUCCESS', changes='1,1')])

    def test_dynamic_fail_cross_repo(self):
        # Verify dynamic config behaviors inside a tenant broken config
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-broken')
        # There is a configuration error
        self.assertEqual(
            len(tenant.layout.loading_errors), 2,
            "An error should have been stored")

        # Inside a broken tenant configuration environment, remove a
        # job used in another repo and verify that an error is
        # reported despite the error being in a repo other than the
        # change.
        in_repo_conf = textwrap.dedent(
            """
            - pipeline:
                name: check
                manager: independent
                trigger:
                  gerrit:
                    - event: patchset-created
                success:
                  gerrit:
                    Verified: 1
                failure:
                  gerrit:
                    Verified: -1
            - job:
                name: base
                parent: null

            - project:
                name: common-config
                check:
                  jobs:
                    - noop
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('Job central-test not defined', A.messages[0],
                      "A should have failed the check pipeline")


class TestBrokenMultiTenantConfig(ZuulTestCase):
    # Test we can deal with a broken multi-tenant config

    tenant_config_file = 'config/broken-multi-tenant/main.yaml'

    def test_loading_errors(self):
        # This regression test came about when we discovered the following:

        # * We cache configuration objects if they load without error
        #   in their first tenant; that means that they can show up as
        #   errors in later tenants, but as long as those other
        #   tenants aren't proposing changes to that repo (which is
        #   unlikely in this situation; this usually arises if the
        #   tenant just wants to use some foreign jobs), users won't
        #   be blocked by the error.
        #
        # * If a merge job for a dynamic config change arrives out of
        #   order, we will build the new configuration and if there
        #   are errors, we will compare it to the previous
        #   configuration to determine if they are relevant, but that
        #   caused an error since the previous layout had not been
        #   calculated yet.  It's pretty hard to end up with
        #   irrelevant errors except by virtue of the first point
        #   above, which is why this test relies on a second tenant.

        # This test has two tenants.  The first loads project2, and
        # project3 without errors and all config objects are cached.
        # The second tenant loads only project1 and project2.
        # Project2 references a job that is defined in project3, so
        # the tenant loads with an error, but proceeds.

        # Don't run any merge jobs, so we can run them out of order.
        self.hold_merge_jobs_in_queue = True

        # Create a first change which modifies the config (and
        # therefore will require a merge job).
        in_repo_conf = textwrap.dedent(
            """
            - job: {'name': 'foo'}
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)

        # Create a second change which also modifies the config.
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B',
                                           files=file_dict)
        B.setDependsOn(A, 1)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # There should be a merge job for each change.
        self.assertEqual(len(list(self.merger_api.all())), 2)

        jobs = list(self.merger_api.queued())
        # Release the second merge job.
        self.merger_api.release(jobs[-1])
        self.waitUntilSettled()

        # At this point we should still be waiting on the first
        # change's merge job.
        self.assertHistory([])

        # Proceed.
        self.hold_merge_jobs_in_queue = False
        self.merger_api.release()
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='base', result='SUCCESS', changes='1,1 2,1'),
        ])


class TestProjectKeys(ZuulTestCase):
    # Test that we can generate project keys

    # Normally the test infrastructure copies a static key in place
    # for each project before starting tests.  This saves time because
    # Zuul's automatic key-generation on startup can be slow.  To make
    # sure we exercise that code, in this test we allow Zuul to create
    # keys for the project on startup.
    create_project_keys = True
    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/in-repo/main.yaml'

    def test_key_generation(self):
        test_keys = []
        key_fns = ['private.pem', 'ssh.pem']
        for fn in key_fns:
            with open(os.path.join(FIXTURE_DIR, fn)) as i:
                test_keys.append(i.read())

        keystore = self.scheds.first.sched.keystore
        private_secrets_key, public_secrets_key = (
            keystore.getProjectSecretsKeys("gerrit", "org/project")
        )

        # Make sure that we didn't just end up with the static fixture
        # key
        self.assertTrue(private_secrets_key not in test_keys)

        # Make sure it's the right length
        self.assertEqual(4096, private_secrets_key.key_size)

        # Make sure that a proper key was created on startup
        private_ssh_key, public_ssh_key = (
            keystore.getProjectSSHKeys("gerrit", "org/project")
        )

        # Make sure that we didn't just end up with the static fixture
        # key
        self.assertTrue(private_ssh_key not in test_keys)

        with io.StringIO(private_ssh_key) as o:
            ssh_key = paramiko.RSAKey.from_private_key(
                o, password=keystore.password)

        # Make sure it's the right length
        self.assertEqual(2048, ssh_key.get_bits())


class TestOIDCSigningKeys(ZuulTestCase):
    # Test that we can perform OIDC signing key operations

    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/in-repo/main.yaml'

    def test_oidc_rs256_get_latest_signing_keys(self):
        # Test the getLatestOidcSigningKeys method should return
        # private and public keys correctly
        keystore = self.scheds.first.sched.keystore
        private_secrets_key, public_secrets_key, version = (
            keystore.getLatestOidcSigningKeys("RS256")
        )

        # Make sure it's the right length
        self.assertEqual(4096, private_secrets_key.key_size)
        self.assertEqual(version, 0)

    def test_oidc_unsupported_algorithm(self):
        # Test that unknown algorithms would
        # raise UnsupportedAlgorithmError
        keystore = self.scheds.first.sched.keystore

        self.assertRaises(
            AlgorithmNotSupportedException,
            lambda: keystore.getOidcSigningKeyData("UNKNOWN_ALG")
        )

        self.assertRaises(
            AlgorithmNotSupportedException,
            lambda: keystore.getLatestOidcSigningKeys("UNKNOWN_ALG")
        )

    def test_oidc_rs256_key_deletion(self):
        self._test_oidc_key_deletion("RS256")

    def _test_oidc_key_deletion(self, algorithm):
        # Test that we can delete all OIDC keys
        keystore = self.scheds.first.sched.keystore

        # Calling getOidcSigningKeyData() should create keys
        keystore.getOidcSigningKeyData(algorithm)

        # Keys should also be in zk

        with keystore.createZKContext() as context:
            test_keys1 = OIDCSigningKeys.loadKeys(
                context, algorithm)
            self.assertEqual(len(test_keys1.keys), 1)

            # Delete the keys
            keystore.deleteOidcSigningKeys(algorithm)

            # Keys should be gone
            test_keys2 = OIDCSigningKeys.loadKeys(
                context, algorithm)
            self.assertIsNone(test_keys2)

    def test_oidc_rs256_key_generation(self):
        self._test_oidc_key_generation("RS256")

    def _test_oidc_key_generation(self, algorithm):
        # Test that we can generate initial OIDC keys
        keystore = self.scheds.first.sched.keystore

        with keystore.createZKContext() as context:
            # initially there should be no keys
            test_keys1 = OIDCSigningKeys.loadKeys(
                context, algorithm)
            self.assertIsNone(test_keys1)

            # Calling getOidcSigningKeyData() should return keys
            test_keys2 = keystore.getOidcSigningKeyData(algorithm)

            self.assertEqual(len(test_keys2.keys), 1)
            # Keys should also be in zk
            test_keys3 = OIDCSigningKeys.loadKeys(
                context, algorithm)
            self.assertEqual(len(test_keys3.keys), 1)
            # And they should be equal
            self.assertEqual(test_keys2, test_keys3)

        # Calling getOidcSigningKeyData() again should return
        # the same keys
        test_keys4 = keystore.getOidcSigningKeyData(algorithm)
        self.assertEqual(test_keys2, test_keys4)

    def test_oidc_rs256_key_rotation(self):
        self._test_oidc_key_rotation("RS256")

    def _test_oidc_key_rotation(self, algorithm):
        # Test that we can rotate OIDC keys
        keystore = self.scheds.first.sched.keystore
        rotation_interval = 5
        max_ttl = 2

        # Create the initial signing key
        test_keys1 = keystore.getOidcSigningKeyData(algorithm)
        private_key1, _, version1 = keystore.getLatestOidcSigningKeys(
            algorithm)
        self.assertEqual(len(test_keys1.keys), 1)
        self.assertEqual(test_keys1.keys[0]["version"], 0)
        self.assertEqual(version1, 0)

        # Do rotation immediatly should not change anything
        keystore.rotateOidcSigningKeys(algorithm, rotation_interval, max_ttl)
        private_key2, _, version2 = keystore.getLatestOidcSigningKeys(
            algorithm)
        test_keys2 = keystore.getOidcSigningKeyData(algorithm)
        self.assertEqual(test_keys2, test_keys1)
        self.assertEqual(
            encryption.serialize_rsa_private_key(private_key2),
            encryption.serialize_rsa_private_key(private_key1))
        self.assertEqual(version2, 0)

        # Wait for a bit more than rotation_interval and do rotation,
        # a new key should be appended
        time.sleep(rotation_interval + 1)
        keystore.rotateOidcSigningKeys(algorithm, rotation_interval, max_ttl)
        for _ in iterate_timeout(10, 'cache to sync'):
            test_keys3 = keystore.getOidcSigningKeyData(algorithm)
            if len(test_keys3.keys) == 2:
                # avoid test_keys3 being modified in place by cache update
                test_keys3 = copy.deepcopy(test_keys3)
                break
        private_key3, _, version3 = keystore.getLatestOidcSigningKeys(
            algorithm)
        self.assertEqual(
            test_keys3.keys[0]["private_key"].encode("utf-8"),
            test_keys1.keys[0]["private_key"].encode("utf-8"))
        self.assertNotEqual(
            test_keys3.keys[1]["private_key"].encode("utf-8"),
            test_keys1.keys[0]["private_key"].encode("utf-8"))
        self.assertEqual(test_keys3.keys[1]["version"], 1)
        self.assertEqual(version3, 1)
        self.assertGreaterEqual(
            test_keys3.keys[1]["created"],
            test_keys1.keys[0]["created"] + rotation_interval)
        self.assertNotEqual(
            encryption.serialize_rsa_private_key(private_key3),
            encryption.serialize_rsa_private_key(private_key1))

        # Wait for a bit more than max_ttl and do rotation again,
        # the old key should be removed
        time.sleep(max_ttl + 1)
        keystore.rotateOidcSigningKeys(algorithm, rotation_interval, max_ttl)
        for _ in iterate_timeout(10, 'cache to sync'):
            test_keys4 = keystore.getOidcSigningKeyData(algorithm)
            if len(test_keys4.keys) == 1:
                break
        private_key4, _, version4 = keystore.getLatestOidcSigningKeys(
            algorithm)
        self.assertEqual(
            test_keys4.keys[0]["private_key"],
            test_keys3.keys[1]["private_key"])
        self.assertEqual(
            encryption.serialize_rsa_private_key(private_key4),
            encryption.serialize_rsa_private_key(private_key3))
        self.assertEqual(version4, 1)

    def test_oidc_key_rotation_without_old_key(self):
        # Test that when there is no old key, it will create a new one
        keystore = self.scheds.first.sched.keystore
        algorithm = "RS256"
        rotation_interval = 5
        max_ttl = 2

        keystore.rotateOidcSigningKeys(algorithm, rotation_interval, max_ttl)
        with keystore.createZKContext() as context:
            test_keys1 = OIDCSigningKeys.loadKeys(
                context, algorithm)
            self.assertEqual(len(test_keys1.keys), 1)


class TestValidateAllBroken(ZuulTestCase):
    # Test we fail while validating all tenants with one broken tenant

    validate_tenants = []
    tenant_config_file = 'config/broken/main.yaml'

    # This test raises a config error during the startup of the test
    # case which makes the first scheduler fail during its startup.
    # The second (or any additional) scheduler won't even run as the
    # startup is serialized in tests/base.py.
    # Thus it doesn't make sense to execute this test with multiple
    # schedulers.
    scheduler_count = 1

    def setUp(self):
        self.assertRaises(zuul.exceptions.ConfigurationSyntaxError,
                          super().setUp)

    def test_validate_all_tenants_broken(self):
        # If we reach this point we successfully catched the config exception.
        # There is nothing more to test here.
        pass


class TestValidateBroken(ZuulTestCase):
    # Test we fail while validating a broken tenant

    validate_tenants = ['tenant-broken']
    tenant_config_file = 'config/broken/main.yaml'

    # This test raises a config error during the startup of the test
    # case which makes the first scheduler fail during its startup.
    # The second (or any additional) scheduler won't even run as the
    # startup is serialized in tests/base.py.
    # Thus it doesn't make sense to execute this test with multiple
    # schedulers.
    scheduler_count = 1

    def setUp(self):
        self.assertRaises(zuul.exceptions.ConfigurationSyntaxError,
                          super().setUp)

    def test_validate_tenant_broken(self):
        # If we reach this point we successfully catched the config exception.
        # There is nothing more to test here.
        pass


class TestValidateGood(ZuulTestCase):
    # Test we don't fail while validating a good tenant in a multi tenant
    # setup that contains a broken tenant.

    validate_tenants = ['tenant-good']
    tenant_config_file = 'config/broken/main.yaml'

    def test_validate_tenant_good(self):
        # If we reach this point we successfully validated the good tenant.
        # There is nothing more to test here.
        pass


class TestValidateWarnings(ZuulTestCase):
    # Test we don't fail when we only have configuration warnings

    # Note, we use a simple_layout below which defines tenant-one,
    # unlike the test class above.
    validate_tenants = ['tenant-one']

    def setUp(self):
        with self.assertLogs('zuul.ConfigLoader', level='DEBUG') as full_logs:
            super().setUp()
            self.assertRegexInList('Zuul encountered a deprecated syntax',
                                   full_logs.output)

    @simple_layout('layouts/broken-warnings.yaml')
    def test_validate_warnings(self):
        pass


class TestValidateWarningsAcceptable(ZuulTestCase):
    # Test we don't fail new configs when warnings and errors exist

    @simple_layout('layouts/pcre-deprecation.yaml')
    def test_validate_warning_new_config(self):
        # Test that we can add new valid configuration despite the
        # existence of existing errors and warnings.
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 3)
        # Make note of the warnings in our config.
        for error in errors:
            self.assertEqual(error.severity, SEVERITY_WARNING)

        # Put an error on a different branch.  This ensures that we
        # will always have configuration errors, but they are not
        # relevant, so should not affect further changes.
        self.create_branch('org/project', 'stable/queens')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/queens'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - nonexistent-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'stable/queens', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        # Put an existing warning on this branch.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: broken-job
                branches: ^(?!invalid).*$
            """)

        file_dict = {'zuul.d/broken.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        B.setMerged()
        self.fake_gerrit.addEvent(B.getChangeMergedEvent())
        self.waitUntilSettled()

        # Make sure we can add a new job with an existing error on
        # another branch and warning on this one.
        conf = textwrap.dedent(
            """
            - job:
                name: new-job

            - project:
                name: org/project
                check:
                  jobs:
                    - check-job
                    - new-job
            """)

        file_dict = {'zuul.d/new.yaml': conf}
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C',
                                           files=file_dict)
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='3,1'),
            # New job runs despite warnings existing in the config.
            dict(name='new-job', result='SUCCESS', changes='3,1'),
        ], ordered=False)

    @simple_layout('layouts/pcre-deprecation.yaml')
    def test_validate_new_warning(self):
        # Test that we can add new deprecated configuration.

        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 3)
        # Make note of the warnings in our config.
        for error in errors:
            self.assertEqual(error.severity, SEVERITY_WARNING)

        # Put an error on a different branch.  This ensures that we
        # will always have configuration errors, but they are not
        # relevant, so should not affect further changes.
        self.create_branch('org/project', 'stable/queens')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/queens'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - nonexistent-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'stable/queens', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        # Add a new configuration warning.
        self.executor_server.hold_jobs_in_build = True
        conf = textwrap.dedent(
            """
            - job:
                name: new-job
                branches: ^(?!invalid).*$

            - project:
                name: org/project
                check:
                  jobs:
                    - check-job
                    - new-job
            """)

        file_dict = {'zuul.yaml': conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # check-job and new-job
        self.assertEqual(len(self.builds), 2)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertIn('encountered a deprecated syntax', B.messages[-1])
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='2,1'),
            # New job runs despite warnings existing in the config.
            dict(name='new-job', result='SUCCESS', changes='2,1'),
        ], ordered=False)


class TestPCREDeprecation(ZuulTestCase):
    @simple_layout('layouts/pcre-deprecation.yaml')
    def test_pcre_deprecation(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 3)

        # Pragma implied-branches
        idx = 0
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('pragma stanza', errors[idx].error)

        # Job branches
        idx = 1
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('job stanza', errors[idx].error)

        # Project-pipeline job branches
        idx = 2
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('project stanza', errors[idx].error)


class TestPCREDeprecationGerrit(ZuulTestCase):
    @simple_layout('layouts/pcre-deprecation-gerrit.yaml')
    def test_pcre_deprecation_gerrit(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 2)

        # Pipeline gerrit trigger ref
        idx = 0
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('"gate" pipeline stanza', errors[idx].error)

        # Pipeline gerrit require approval
        idx = 1
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('"post" pipeline stanza', errors[idx].error)


class TestPCREDeprecationGit(ZuulTestCase):
    config_file = 'zuul-git-driver.conf'

    @simple_layout('layouts/pcre-deprecation-git.yaml')
    def test_pcre_deprecation_git(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 1)

        # Pipeline git trigger ref
        idx = 0
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('pipeline stanza', errors[idx].error)


class TestPCREDeprecationGithub(ZuulTestCase):
    config_file = 'zuul-connections-gerrit-and-github.conf'

    @simple_layout('layouts/pcre-deprecation-github.yaml')
    def test_pcre_deprecation_github(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 1)

        # Pipeline github trigger ref
        idx = 0
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('pipeline stanza', errors[idx].error)


class TestPCREDeprecationGitlab(ZuulTestCase):
    config_file = 'zuul-gitlab-driver.conf'

    @simple_layout('layouts/pcre-deprecation-gitlab.yaml', driver='gitlab')
    def test_pcre_deprecation_gitlab(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 1)

        # Pipeline gitlab trigger ref
        idx = 0
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('pipeline stanza', errors[idx].error)


class TestPCREDeprecationPagure(ZuulTestCase):
    config_file = 'zuul-pagure-driver.conf'

    @simple_layout('layouts/pcre-deprecation-pagure.yaml', driver='pagure')
    def test_pcre_deprecation_pagure(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 1)

        # Pipeline pagure trigger ref
        idx = 0
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('pipeline stanza', errors[idx].error)


class TestPCREDeprecationZuul(ZuulTestCase):
    @simple_layout('layouts/pcre-deprecation-zuul.yaml')
    def test_pcre_deprecation_zuul(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 1)

        # Pipeline zuul trigger ref
        idx = 0
        self.assertEqual(errors[idx].severity, SEVERITY_WARNING)
        self.assertEqual(errors[idx].name, 'Regex Deprecation')
        self.assertIn('pipeline stanza', errors[idx].error)


class RoleTestCase(ZuulTestCase):
    def _getRolesPaths(self, build, playbook):
        path = os.path.join(self.jobdir_root, build.uuid,
                            'ansible', playbook, 'ansible.cfg')
        roles_paths = []
        with open(path) as f:
            for line in f:
                if line.startswith('roles_path'):
                    roles_paths.append(line)
        return roles_paths

    def _assertRolePath(self, build, playbook, content):
        roles_paths = self._getRolesPaths(build, playbook)
        if content:
            self.assertEqual(len(roles_paths), 1,
                             "Should have one roles_path line in %s" %
                             (playbook,))
            self.assertIn(content, roles_paths[0])
        else:
            self.assertEqual(len(roles_paths), 0,
                             "Should have no roles_path line in %s" %
                             (playbook,))

    def _assertInRolePath(self, build, playbook, files):
        roles_paths = self._getRolesPaths(build, playbook)[0]
        roles_paths = roles_paths.split('=')[-1].strip()
        roles_paths = roles_paths.split(':')

        files = set(files)
        matches = set()
        for rpath in roles_paths:
            for rolename in os.listdir(rpath):
                if rolename in files:
                    matches.add(rolename)
        self.assertEqual(files, matches)


class TestRoleBranches(RoleTestCase):
    tenant_config_file = 'config/role-branches/main.yaml'

    def _addRole(self, project, branch, role, parent=None):
        data = textwrap.dedent("""
            - name: %s
              debug:
                msg: %s
            """ % (role, role))
        file_dict = {'roles/%s/tasks/main.yaml' % role: data}
        A = self.fake_gerrit.addFakeChange(project, branch,
                                           'add %s' % role,
                                           files=file_dict,
                                           parent=parent)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        return A.patchsets[-1]['ref']

    def _addPlaybook(self, project, branch, playbook, role, parent=None):
        data = textwrap.dedent("""
            - hosts: all
              roles:
                - %s
            """ % role)
        file_dict = {'playbooks/%s.yaml' % playbook: data}
        A = self.fake_gerrit.addFakeChange(project, branch,
                                           'add %s' % playbook,
                                           files=file_dict,
                                           parent=parent)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        return A.patchsets[-1]['ref']

    def _assertInFile(self, path, content):
        with open(path) as f:
            self.assertIn(content, f.read())

    def test_playbook_role_branches(self):
        # This tests that the correct branch of a repo which contains
        # a playbook or a role is checked out.  Most of the action
        # happens on project1, which holds a parent job, so that we
        # can test the behavior of a project which is not in the
        # dependency chain.
        # First we create some branch-specific content in project1:
        self.create_branch('project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'project1', 'stable'))
        self.waitUntilSettled()

        # A pre-playbook with unique stable branch content.
        p = self._addPlaybook('project1', 'stable',
                              'parent-job-pre', 'parent-stable-role')
        # A role that only exists on the stable branch.
        self._addRole('project1', 'stable', 'stable-role', parent=p)

        # The same for the master branch.
        p = self._addPlaybook('project1', 'master',
                              'parent-job-pre', 'parent-master-role')
        self._addRole('project1', 'master', 'master-role', parent=p)

        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        # Push a change to project2 which will run 3 jobs which
        # inherit from project1.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 3)

        # This job should use the master branch since that's the
        # zuul.branch for this change.
        build = self.getBuildByName('child-job')
        self._assertInRolePath(build, 'playbook_0', ['master-role'])
        self._assertInFile(build.jobdir.pre_playbooks[1].path,
                           'parent-master-role')

        # The main playbook is on the master branch of project2, but
        # there is a job-level branch override, so the project1 role
        # should be from the stable branch.  The job-level override
        # will cause Zuul to select the project1 pre-playbook from the
        # stable branch as well, so we should see it using the stable
        # role.
        build = self.getBuildByName('child-job-override')
        self._assertInRolePath(build, 'playbook_0', ['stable-role'])
        self._assertInFile(build.jobdir.pre_playbooks[1].path,
                           'parent-stable-role')

        # The same, but using a required-projects override.
        build = self.getBuildByName('child-job-project-override')
        self._assertInRolePath(build, 'playbook_0', ['stable-role'])
        self._assertInFile(build.jobdir.pre_playbooks[1].path,
                           'parent-stable-role')

        inventory = self.getBuildInventory('child-job-override')
        zuul = inventory['all']['vars']['zuul']

        expected = {
            'playbook_projects': {
                'trusted/project_0/review.example.com/common-config': {
                    'canonical_name': 'review.example.com/common-config',
                    'checkout': 'master',
                    'commit': self.getCheckout(
                        build,
                        'trusted/project_0/review.example.com/common-config')},
                'untrusted/project_0/review.example.com/project1': {
                    'canonical_name': 'review.example.com/project1',
                    'checkout': 'stable',
                    'commit': self.getCheckout(
                        build,
                        'untrusted/project_0/review.example.com/project1')},
                'untrusted/project_1/review.example.com/common-config': {
                    'canonical_name': 'review.example.com/common-config',
                    'checkout': 'master',
                    'commit': self.getCheckout(
                        build,
                        'untrusted/project_1/review.example.com/common-config'
                    )},
                'untrusted/project_2/review.example.com/project2': {
                    'canonical_name': 'review.example.com/project2',
                    'checkout': 'master',
                    'commit': self.getCheckout(
                        build,
                        'untrusted/project_2/review.example.com/project2')}},
            'pre_playbooks': [
                {'path': 'trusted/project_0/review.example.com/'
                 'common-config/playbooks/pre-base.yaml',
                 'roles': [
                     {'checkout': 'master',
                      'checkout_description': 'playbook branch',
                      'link_name': 'ansible/pre_playbook_0/role_0/'
                      'common-config',
                      'link_target': 'trusted/project_0/'
                      'review.example.com/common-config',
                      'role_path': 'ansible/pre_playbook_0/role_0/'
                      'common-config/roles'
                      }
                 ]},
                {'path': 'untrusted/project_0/review.example.com/'
                 'project1/playbooks/parent-job-pre.yaml',
                 'roles': [
                     {'checkout': 'stable',
                      'checkout_description': 'playbook branch',
                      'link_name': 'ansible/pre_playbook_1/role_0/project1',
                      'link_target': 'untrusted/project_0/'
                      'review.example.com/project1',
                      'role_path': 'ansible/pre_playbook_1/role_0/'
                      'project1/roles'
                      },
                     {'checkout': 'master',
                      'checkout_description': 'zuul branch',
                      'link_name': 'ansible/pre_playbook_1/role_1/'
                      'common-config',
                      'link_target': 'untrusted/project_1/'
                      'review.example.com/common-config',
                      'role_path': 'ansible/pre_playbook_1/role_1/'
                      'common-config/roles'
                      }
                 ]}
            ],
            'playbooks': [
                {'path': 'untrusted/project_2/review.example.com/'
                 'project2/playbooks/child-job.yaml',
                 'roles': [
                     {'checkout': 'stable',
                      'checkout_description': 'job override ref',
                      'link_name': 'ansible/playbook_0/role_1/project1',
                      'link_target': 'untrusted/project_0/'
                      'review.example.com/project1',
                      'role_path': 'ansible/playbook_0/role_1/project1/roles'
                      },
                     {'checkout': 'master',
                      'checkout_description': 'zuul branch',
                      'link_name': 'ansible/playbook_0/role_2/common-config',
                      'link_target': 'untrusted/project_1/'
                      'review.example.com/common-config',
                      'role_path': 'ansible/playbook_0/role_2/'
                      'common-config/roles'
                      }
                 ]}
            ],
            'post_playbooks': [],
        }

        self.assertEqual(expected, zuul['playbook_context'])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_playbook_role_branches_tag(self):
        # This tests that the correct branch of a repo which contains
        # a playbook or a role is checked out.  Most of the action
        # happens on project1, which holds a parent job, so that we
        # can test the behavior of a project which is not in the
        # dependency chain.
        # First we create stable and release branches on project1 and
        # then the release branch on project2 that will be tagged.
        self.create_branch('project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'project1', 'stable'))
        self.create_branch('project1', 'release')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'project1', 'release'))
        self.create_branch('project2', 'release')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'project2', 'release'))
        self.waitUntilSettled()

        # A pre-playbook with unique release branch content.
        p = self._addPlaybook('project1', 'release',
                              'parent-job-pre', 'parent-release-role')
        # A role that only exists on the release branch.
        self._addRole('project1', 'release', 'release-role', parent=p)

        # The same for the master and stable branch.
        p = self._addPlaybook('project1', 'master',
                              'parent-job-pre', 'parent-master-role')
        self._addRole('project1', 'master', 'master-role', parent=p)
        p = self._addPlaybook('project1', 'stable',
                              'parent-job-pre', 'parent-stable-role')
        self._addRole('project1', 'stable', 'stable-role', parent=p)

        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.executor_server.hold_jobs_in_build = True

        # Create a tag on the release branch
        event = self.fake_gerrit.addFakeTag('project2', 'release', 'foo')
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 3)

        # This job should use the release branch since that's the
        # branch used for this playbook.
        build = self.getBuildByName('child-job')
        self._assertInRolePath(build, 'playbook_0', ['release-role'])
        self._assertInFile(build.jobdir.pre_playbooks[1].path,
                           'parent-release-role')

        # The main playbook is on the master branch of project2, but
        # there is a job-level branch override, so the project1 role
        # should be from the stable branch.  The job-level override
        # will cause Zuul to select the project1 pre-playbook from the
        # stable branch as well, so we should see it using the stable
        # role.
        build = self.getBuildByName('child-job-override')
        self._assertInRolePath(build, 'playbook_0', ['stable-role'])
        self._assertInFile(build.jobdir.pre_playbooks[1].path,
                           'parent-stable-role')

        # The same, but using a required-projects override.
        build = self.getBuildByName('child-job-project-override')
        self._assertInRolePath(build, 'playbook_0', ['stable-role'])
        self._assertInFile(build.jobdir.pre_playbooks[1].path,
                           'parent-stable-role')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def getBuildInventory(self, name):
        build = self.getBuildByName(name)
        inv_path = os.path.join(build.jobdir.root, 'ansible', 'inventory.yaml')
        with open(inv_path, 'r') as f:
            inventory = yaml.safe_load(f)
        return inventory

    def getCheckout(self, build, path):
        root = os.path.join(build.jobdir.root, path)
        repo = git.Repo(root)
        return repo.head.commit.hexsha


class TestRoles(RoleTestCase):
    tenant_config_file = 'config/roles/main.yaml'

    def test_role(self):
        # This exercises a proposed change to a role being checked out
        # and used.
        A = self.fake_gerrit.addFakeChange('bare-role', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test', result='SUCCESS', changes='1,1 2,1'),
        ])

    def test_role_inheritance(self):
        self.executor_server.hold_jobs_in_build = True
        conf = textwrap.dedent(
            """
            - job:
                name: parent
                roles:
                  - zuul: bare-role
                pre-run: playbooks/parent-pre.yaml
                post-run: playbooks/parent-post.yaml

            - job:
                name: project-test
                parent: parent
                run: playbooks/project-test.yaml
                roles:
                  - zuul: org/project

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test
            """)

        file_dict = {'.zuul.yaml': conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)
        build = self.getBuildByName('project-test')
        self._assertRolePath(build, 'pre_playbook_0', 'role_0')
        self._assertRolePath(build, 'playbook_0', 'role_0')
        self._assertRolePath(build, 'playbook_0', 'role_1')
        self._assertRolePath(build, 'post_playbook_0', 'role_0')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-test', result='SUCCESS', changes='1,1'),
        ])

    def test_role_error(self):
        conf = textwrap.dedent(
            """
            - job:
                name: project-test
                run: playbooks/project-test.yaml
                roles:
                  - zuul: common-config

            - project:
                name: org/project
                check:
                  jobs:
                    - project-test
            """)

        file_dict = {'.zuul.yaml': conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertTrue(re.search(
            '- project-test .* ERROR Unable to find role',
            A.messages[-1]))


class TestImplicitRoles(RoleTestCase):
    tenant_config_file = 'config/implicit-roles/main.yaml'

    def test_missing_roles(self):
        # Test implicit and explicit roles for a project which does
        # not have roles.  The implicit role should be silently
        # ignored since the project doesn't supply roles, but if a
        # user declares an explicit role, it should error.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/norole-project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 2)
        build = self.getBuildByName('implicit-role-fail')
        self._assertRolePath(build, 'playbook_0', None)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        # The retry_limit doesn't get recorded
        self.assertHistory([
            dict(name='implicit-role-fail', result='SUCCESS', changes='1,1'),
        ])

    def test_roles(self):
        # Test implicit and explicit roles for a project which does
        # have roles.  In both cases, we should end up with the role
        # in the path.  In the explicit case, ensure we end up with
        # the name we specified.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/role-project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 2)
        build = self.getBuildByName('implicit-role-ok')
        self._assertRolePath(build, 'playbook_0', 'role_0')

        build = self.getBuildByName('explicit-role-ok')
        self._assertRolePath(build, 'playbook_0', 'role_0')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='implicit-role-ok', result='SUCCESS', changes='1,1'),
            dict(name='explicit-role-ok', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestShadow(ZuulTestCase):
    tenant_config_file = 'config/shadow/main.yaml'

    def test_shadow(self):
        # Test that a repo is allowed to shadow another's job definitions.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test1', result='SUCCESS', changes='1,1'),
            dict(name='test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestArtifactReturnSynthetic(ZuulTestCase):
    # Artifact data return tests that don't run Ansible
    tenant_config_file = 'config/single-tenant/main.yaml'

    def _get_artifacts(self):
        connection = self.scheds.first.sched.sql.connection
        builds = connection.getBuilds()
        return [dict(name=a.name,
                     url=a.url,
                     metadata=a.meta)
                for a in builds[0].artifacts]

    def _test_artifact_return(self, artifacts):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.executor_server.returnData(
            'check-job', A,
            {'zuul':
             {'artifacts': artifacts}
             }
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    @simple_layout('layouts/simple.yaml')
    def test_artifact_return_ok(self):
        # Test the normal case.
        self._test_artifact_return(
            [
                {'name': 'image',
                 'url': 'something',
                 'metadata': {
                     'type': 'test_data'
                 }},
            ])
        self.assertEqual([{'name': 'image',
                           'url': 'something',
                           'metadata': '{"type": "test_data"}'}],
                         self._get_artifacts())

    @simple_layout('layouts/simple.yaml')
    def test_artifact_return_no_url(self):
        # Test that if we return malformed data, the scheduler doesn't
        # break.
        self._test_artifact_return(
            [
                {'name': 'image',
                 'metadata': {
                     'type': 'test_data'
                 }},
            ])
        self.assertEqual([], self._get_artifacts())

    @simple_layout('layouts/simple.yaml')
    def test_artifact_return_no_name(self):
        # Test that if we return malformed data, the scheduler doesn't
        # break.
        self._test_artifact_return(
            [
                {'url': 'something',
                 'metadata': {
                     'type': 'test_data'
                 }},
            ])
        self.assertEqual([], self._get_artifacts())

    @simple_layout('layouts/simple.yaml')
    def test_artifact_return_bad_metadata(self):
        # Test that if we return malformed data, the scheduler doesn't
        # break.
        self._test_artifact_return(
            [
                {'name': 'image',
                 'url': 'something',
                 'metadata': [1],
                 },
            ])
        self.assertEqual([], self._get_artifacts())


class TestArtifactReturn(AnsibleZuulTestCase):
    tenant_config_file = 'config/artifact-return/main.yaml'

    def _get_artifacts(self):
        connection = self.scheds.first.sched.sql.connection
        builds = connection.getBuilds()
        return [dict(name=a.name,
                     url=a.url,
                     metadata=a.meta)
                for a in builds[0].artifacts]

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def _test_artifact_return(self, job, error):
        self.executor_server.keep_jobdir = True
        expected_result = error and 'FAILURE' or 'SUCCESS'
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={job: ''})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name=job, result=expected_result, changes='1,1'),
        ], ordered=False)
        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))
        result = j[0]['plays'][0]['tasks'][1]['hosts']['localhost']
        self.assertEqual('zuul_return', result['action'])
        if error:
            self.assertEqual([], self._get_artifacts())
            self.assertIn(error, result['msg'])
        else:
            self.assertEqual([{'name': 'image',
                               'url': 'something',
                               'metadata': '{"type": "test_data"}'}],
                             self._get_artifacts())
            self.assertNotIn('msg', result)

    def test_artifact_return_ok(self):
        # Test the normal case.
        self._test_artifact_return('ok', None)

    def test_artifact_return_no_url(self):
        # Test that bad data results in a user-visible error.
        self._test_artifact_return('no-url', 'required key not provided')

    def test_artifact_return_no_name(self):
        # Test that bad data results in a user-visible error.
        self._test_artifact_return('no-name', 'required key not provided')

    def test_artifact_return_bad_metadata(self):
        # Test that bad data results in a user-visible error.
        self._test_artifact_return('bad-metadata',
                                   'expected dict for dictionary value')


class TestDataReturn(AnsibleZuulTestCase):
    tenant_config_file = 'config/data-return/main.yaml'

    def test_data_return(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='data-return', result='SUCCESS', changes='1,1'),
            dict(name='data-return-relative', result='SUCCESS', changes='1,1'),
            dict(name='child', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertIn('- data-return https://zuul.example.com/',
                      A.messages[-1])
        self.assertIn('- data-return-relative https://zuul.example.com',
                      A.messages[-1])

    def test_data_return_child_jobs(self):
        self.wait_timeout = 120
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('data-return-child-jobs')
        self.waitUntilSettled()

        self.executor_server.release('data-return-child-jobs')
        self.waitUntilSettled()

        # Make sure skipped jobs are not reported as failing
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        status = tenant.layout.pipeline_managers["check"].formatStatusJSON()
        self.assertEqual(
            status["change_queues"][0]["heads"][0][0]["failing_reasons"], [])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='data-return-child-jobs', result='SUCCESS',
                 changes='1,1'),
            dict(name='data-return', result='SUCCESS', changes='1,1'),
        ])
        self.assertIn(
            '- data-return-child-jobs https://zuul.example.com/',
            A.messages[-1])
        self.assertIn(
            '- data-return https://zuul.example.com/',
            A.messages[-1])
        self.assertTrue('Skipped 1 job' in A.messages[-1])
        self.assertIn('Build succeeded', A.messages[-1])
        connection = self.scheds.first.sched.sql.connection
        builds = connection.getBuilds()
        builds.sort(key=lambda x: x.job_name)
        self.assertEqual(builds[0].job_name, 'child')
        self.assertEqual(builds[0].error_detail,
                         'Skipped due to child_jobs return value '
                         'in job data-return-child-jobs')
        self.assertEqual(builds[1].job_name, 'data-return')
        self.assertIsNone(builds[1].error_detail)
        self.assertEqual(builds[2].job_name, 'data-return-child-jobs')
        self.assertIsNone(builds[2].error_detail)

    def test_data_return_invalid_child_job(self):
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='data-return-invalid-child-job', result='SUCCESS',
                 changes='1,1')])
        self.assertIn(
            '- data-return-invalid-child-job https://zuul.example.com',
            A.messages[-1])
        self.assertTrue('Skipped 1 job' in A.messages[-1])
        self.assertIn('Build succeeded', A.messages[-1])

    def test_data_return_skip_all_child_jobs(self):
        A = self.fake_gerrit.addFakeChange('org/project3', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='data-return-skip-all', result='SUCCESS',
                 changes='1,1'),
        ])
        self.assertIn(
            '- data-return-skip-all https://zuul.example.com/',
            A.messages[-1])
        self.assertTrue('Skipped 2 jobs' in A.messages[-1])
        self.assertIn('Build succeeded', A.messages[-1])

    def test_data_return_skip_all_child_jobs_with_soft_dependencies(self):
        A = self.fake_gerrit.addFakeChange('org/project-soft', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='data-return-cd', result='SUCCESS', changes='1,1'),
            dict(name='data-return-c', result='SUCCESS', changes='1,1'),
            dict(name='data-return-d', result='SUCCESS', changes='1,1'),
        ])
        self.assertIn('- data-return-cd https://zuul.example.com/',
                      A.messages[-1])
        self.assertTrue('Skipped 2 jobs' in A.messages[-1])
        self.assertIn('Build succeeded', A.messages[-1])

    def test_several_zuul_return(self):
        A = self.fake_gerrit.addFakeChange('org/project4', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='several-zuul-return-child', result='SUCCESS',
                 changes='1,1'),
        ])
        self.assertIn(
            '- several-zuul-return-child https://zuul.example.com/',
            A.messages[-1])
        self.assertTrue('Skipped 1 job' in A.messages[-1])
        self.assertIn('Build succeeded', A.messages[-1])

    def test_data_return_skip_retry(self):
        A = self.fake_gerrit.addFakeChange(
            'org/project-skip-retry',
            'master',
            'A'
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='skip-retry-return', result='FAILURE',
                 changes='1,1'),
        ])

    def test_data_return_child_jobs_failure(self):
        A = self.fake_gerrit.addFakeChange('org/project5', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='data-return-child-jobs-failure',
                 result='FAILURE', changes='1,1'),
        ])

    def test_data_return_child_from_paused_job(self):
        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='data-return', result='SUCCESS', changes='1,1'),
            dict(name='paused-data-return-child-jobs',
                 result='SUCCESS', changes='1,1'),
        ])

    def test_data_return_child_from_retried_paused_job(self):
        """
        Tests that the data returned to the child job is overwritten if the
        paused job is lost and gets retried (e.g.: executor restart or node
        unreachable).
        """

        def _get_file(path):
            with open(path) as f:
                return f.read()

        self.wait_timeout = 120
        self.executor_server.hold_jobs_in_build = True
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project7', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled("patchset uploaded")

        self.executor_server.release('paused-data-return-vars')
        self.waitUntilSettled("till job is paused")

        paused_job = self.builds[0]
        self.assertTrue(paused_job.paused)

        # zuul_return data is set correct
        j = json.loads(_get_file(paused_job.jobdir.result_data_file))
        self.assertEqual(j["data"]["build_id"], paused_job.uuid)

        # Stop the job worker to simulate an executor restart
        for job_worker in self.executor_server.job_workers.values():
            if job_worker.build_request.uuid == paused_job.uuid:
                job_worker.stop()
        self.waitUntilSettled("stop job worker")

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled("all jobs are done")
        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for x in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        # First build of paused job (gets retried)
        first_build = self.history[0]
        # Second build of the paused job (the retried one)
        retried_build = self.history[3]
        # The successful child job (second build)
        print_build = self.history[2]

        # zuul_return data is set correct to new build id
        j = json.loads(_get_file(retried_build.jobdir.result_data_file))
        self.assertEqual(j["data"]["build_id"], retried_build.uuid)

        self.assertNotIn(first_build.uuid,
                         _get_file(print_build.jobdir.job_output_file))
        self.assertIn(retried_build.uuid,
                      _get_file(print_build.jobdir.job_output_file))


class TestInitJobs(ZuulTestCase):
    @simple_layout('layouts/init-job.yaml')
    def test_data_return_init_job(self):
        # Test an init job, and its ability to skip child jobs
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.executor_server.returnData(
            "init-job", A, data={
                'zuul': {
                    'child_jobs': ['check-job'],
                }
            })

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='init-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertTrue('Skipped 1 job' in A.messages[0])
        self.assertIn('Build succeeded', A.messages[0])

    @simple_layout('layouts/init-job2.yaml')
    def test_data_return_two_init_jobs(self):
        # Test two init jobs and the set intersection behavior of child_jobs
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.executor_server.returnData(
            "init1-job", A, data={
                'zuul': {
                    'child_jobs': ['check-job', 'skip2-job'],
                }
            })
        self.executor_server.returnData(
            "init2-job", A, data={
                'zuul': {
                    'child_jobs': ['check-job', 'skip1-job'],
                }
            })

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='init1-job', result='SUCCESS', changes='1,1'),
            dict(name='init2-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertTrue('Skipped 2 jobs' in A.messages[0])
        self.assertIn('Build succeeded', A.messages[0])

    @simple_layout('layouts/init-job-skip.yaml')
    def test_skipped_init_job(self):
        # Test that an init job can be skipped due to file matchers
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertTrue('Skipped' not in A.messages[0])
        self.assertIn('Build succeeded', A.messages[0])

    @simple_layout('layouts/init-job-override.yaml')
    def test_override_init_job(self):
        # Test that we can override init to a regular job
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        # This skip is ineffective since we're overriding "init-job"
        # to be a regular job.
        self.executor_server.returnData(
            "init-job", A, data={
                'zuul': {
                    'child_jobs': ['check-job'],
                }
            })
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='init-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='1,1'),
            dict(name='skip-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertTrue('Skipped' not in A.messages[0])
        self.assertIn('Build succeeded', A.messages[0])

    @simple_layout('layouts/init-job-dependency.yaml')
    def test_init_job_dependency(self):
        # Test an init job depending on another init
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.executor_server.returnData(
            "init1-job", A, data={
                'zuul': {
                    'child_jobs': ['check-job', 'init2-job'],
                }
            })

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='init1-job', result='SUCCESS', changes='1,1'),
            dict(name='init2-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertTrue('Skipped 1 job' in A.messages[0])
        self.assertIn('Build succeeded', A.messages[0])

    @simple_layout('layouts/init-job-dependency2.yaml')
    @okay_tracebacks('prepareItem')
    def test_init_job_dependency2(self):
        # Test an init job depending on a regular job
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([])
        self.assertIn('Dependency cycle detected', A.messages[0])


class TestReporterJobs(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    @simple_layout('layouts/reporter-job.yaml')
    def test_reporter_job(self):
        # Test a successful reporter job, with a change behind it
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.executor_server.returnData(
            'reporter-job', A,
            {'zuul':
             {'log_url': 'http://example.com/logs/',
              }},
        )

        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertBuilds([dict(name='test-job', changes='1,1')])

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.executor_server.returnData(
            'reporter-job', B,
            {'zuul':
             {'log_url': 'http://example.com/logs2/',
              }},
        )
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertBuilds([
            dict(name='test-job', changes='1,1'),
            dict(name='test-job', changes='1,1 2,1'),
        ])

        # Make sure that reporter jobs show up in the status json
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        status = tenant.layout.pipeline_managers['gate'].formatStatusJSON()
        jobs = status['change_queues'][0]['heads'][0][0]['jobs']
        self.assertEqual(2, len(jobs))
        self.assertEqual('reporter-job', jobs[0]['name'])
        self.assertEqual('test-job', jobs[1]['name'])

        # Release first test job
        test1 = self.builds[0]
        test1.release()
        self.waitUntilSettled()
        self.assertBuilds([
            dict(name='test-job', changes='1,1 2,1'),
            dict(name='reporter-job', changes='1,1'),
        ])

        # Start a second scheduler to exercise serialization
        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)
        self.waitUntilSettled()

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.assertEqual(A.data['status'], 'MERGED')
            # This doesn't happen with Gerrit, but other drivers may
            # "close" the PR (change), which is treated as abandoning
            # the change.  Simulate that here to make sure we don't
            # cancel builds.
            self.fake_gerrit.addEvent(A.getChangeAbandonedEvent())
            self.waitUntilSettled(matcher=[app])

        reporter1 = self.builds[1]
        # Release first reporter job
        reporter1.release()
        self.waitUntilSettled()

        # Ensure that the reporter job does not appear in the gerrit
        # report
        self.assertEqual(2, len(A.messages))
        self.assertIn('Starting gate jobs', A.messages[0])
        self.assertIn('Build succeeded', A.messages[1])
        self.assertNotIn('Skipped', A.messages[1])
        self.assertIn('test-job', A.messages[1])
        self.assertNotIn('reporter-job', A.messages[1])

        # The reporter job should appear in the database
        conn = self.scheds.first.sched.sql.connection
        buildsets = list(reversed(conn.getBuildsets()))
        results = {build.job_name: build.result
                   for build in conn.getBuilds()
                   if build.buildset_id == buildsets[0].id}
        self.assertEqual({
            'test-job': 'SUCCESS',
            'reporter-job': 'SUCCESS',
        }, results)
        # The buildset for the first change:
        self.assertEqual('SUCCESS', buildsets[0].result)

        # Check the commit sha
        upstream_path = os.path.join(self.upstream_root, 'org/project')
        upstream_repo = git.Repo(upstream_path)
        master_sha = upstream_repo.heads.master.commit.hexsha
        reporter_job = self.getJobFromHistory('reporter-job')
        self.assertEqual(
            master_sha,
            reporter_job.parameters['zuul']['buildset_refs'][0]
            ['merge_commit_id'])

        # Release the second change
        self.executor_server.hold_jobs_in_build = False
        self.orderedRelease()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='reporter-job', result='SUCCESS', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='1,1 2,1'),
            dict(name='reporter-job', result='SUCCESS', changes='1,1 2,1'),
        ])
        buildsets = list(reversed(conn.getBuildsets()))
        self.assertEqual(2, len(buildsets))
        # The buildset for the second change:
        self.assertEqual('SUCCESS', buildsets[1].result)

        # Make sure we store the return data
        connection = self.scheds.first.sched.sql.connection
        builds = connection.getBuilds()
        self.assertEqual('http://example.com/logs/', builds[1].log_url)
        self.assertEqual('http://example.com/logs2/', builds[0].log_url)

    @simple_layout('layouts/reporter-job.yaml')
    def test_reporter_job_failure(self):
        # Test a failed reporter job, with a change behind it
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.executor_server.failJob('reporter-job', A)

        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertBuilds([dict(name='test-job', changes='1,1')])

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertBuilds([
            dict(name='test-job', changes='1,1'),
            dict(name='test-job', changes='1,1 2,1'),
        ])
        test1 = self.builds[0]
        # Release first test job
        test1.release()
        self.waitUntilSettled()
        self.assertBuilds([
            dict(name='test-job', changes='1,1 2,1'),
            dict(name='reporter-job', changes='1,1'),
        ])
        reporter1 = self.builds[1]
        # Release first reporter job
        reporter1.release()
        self.waitUntilSettled()

        # Ensure that the reporter job does not appear in the gerrit
        # report
        self.assertEqual(2, len(A.messages))
        self.assertIn('Starting gate jobs', A.messages[0])
        self.assertIn('Build succeeded', A.messages[1])
        self.assertNotIn('Skipped', A.messages[1])
        self.assertIn('test-job', A.messages[1])
        self.assertNotIn('reporter-job', A.messages[1])

        # The reporter job should appear in the database
        conn = self.scheds.first.sched.sql.connection
        buildsets = list(reversed(conn.getBuildsets()))
        results = {build.job_name: build.result
                   for build in conn.getBuilds()
                   if build.buildset_id == buildsets[0].id}
        self.assertEqual({
            'test-job': 'SUCCESS',
            'reporter-job': 'FAILURE',
        }, results)
        # We report the buildset to the db before the reporter jobs
        # finish, so it gets the original value.  Currently unclear
        # whether this is worth changing.
        # The first buildset for the first change:
        self.assertEqual('SUCCESS', buildsets[0].result)
        # The first buildset for the second change:
        self.assertEqual('DEQUEUED', buildsets[1].result)

        # Check the commit sha
        upstream_path = os.path.join(self.upstream_root, 'org/project')
        upstream_repo = git.Repo(upstream_path)
        master_sha = upstream_repo.heads.master.commit.hexsha
        reporter_job = self.getJobFromHistory('reporter-job')
        self.assertEqual(
            master_sha,
            reporter_job.parameters['zuul']['buildset_refs'][0]
            ['merge_commit_id'])

        # Release the second change
        self.executor_server.hold_jobs_in_build = False
        self.orderedRelease()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='reporter-job', result='FAILURE', changes='1,1'),
            dict(name='test-job', result='ABORTED', changes='1,1 2,1'),
            dict(name='test-job', result='SUCCESS', changes='2,1'),
            # We didn't configure this job to fail specifically, but
            # after A merges, it does run with the contents of change
            # A in it, which is how we trigger the failure.
            dict(name='reporter-job', result='FAILURE', changes='2,1'),
        ])
        buildsets = list(reversed(conn.getBuildsets()))
        self.assertEqual(3, len(buildsets))
        # The second buildset for the second change:
        self.assertEqual('SUCCESS', buildsets[2].result)

    @okay_tracebacks('prepareItem')
    def test_configure_reporter_job_error(self):
        in_repo_conf = textwrap.dedent(
            """
            - job: {name: 'reporter-job', type: 'reporter'}
            - project:
                name: org/project
                check:
                  jobs:
                    - reporter-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('The project org/project is not permitted '
                      'to configure a reporter job',
                      A.messages[0])
        self.assertHistory([])


class TestDiskAccounting(AnsibleZuulTestCase):
    config_file = 'zuul-disk-accounting.conf'
    tenant_config_file = 'config/disk-accountant/main.yaml'

    def test_disk_accountant_kills_job(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='dd-big-empty-file', result='ABORTED', changes='1,1')])


class TestEarlyFailure(AnsibleZuulTestCase):
    tenant_config_file = 'config/early-failure/main.yaml'

    def test_early_failure(self):
        file_dict = {'early-failure.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))

        self.log.debug("Wait for the first change to start its job")
        for _ in iterate_timeout(30, 'job A started'):
            if len(self.builds) == 1:
                break
        A_build = self.builds[0]
        start = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                             'failure_start_flag')
        for _ in iterate_timeout(30, 'job A running'):
            if os.path.exists(start):
                break

        self.log.debug("Add a second change which will test with the first")
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.log.debug("Wait for the second change to start its job")
        for _ in iterate_timeout(30, 'job B started'):
            if len(self.builds) == 2:
                break
        B_build = self.builds[1]
        start = os.path.join(self.jobdir_root, B_build.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job B running'):
            if os.path.exists(start):
                break

        self.log.debug("Continue the first job which will fail early")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'failure_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.log.debug("Wait for the second job to be aborted "
                       "and restarted without the first change")
        for _ in iterate_timeout(30, 'job B restarted'):
            if len(self.builds) == 2:
                B_build2 = self.builds[1]
                if B_build2 != B_build:
                    break

        self.log.debug("Wait for the first job to be in its post-run playbook")
        start = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job A post running'):
            if os.path.exists(start):
                break

        self.log.debug("Allow the first job to finish")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'wait_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.log.debug("Wait for the first job to finish")
        for _ in iterate_timeout(30, 'job A complete'):
            if A_build not in self.builds:
                break

        self.log.debug("Allow the restarted second job to finish")
        flag_path = os.path.join(self.jobdir_root, B_build2.uuid, 'work',
                                 'wait_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.waitUntilSettled()

        self.assertHistory([
            dict(name='wait', result='ABORTED', changes='1,1 2,1'),
            dict(name='early-failure', result='FAILURE', changes='1,1'),
            dict(name='wait', result='SUCCESS', changes='2,1'),
        ], ordered=True)

    def test_pre_run_failure_retry(self):
        # Test that we don't set pre_fail when a pre-run playbook fails
        # (so we honor the retry logic and restart the job).
        file_dict = {'pre-failure.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))

        self.log.debug("Wait for the first change to start its job")
        for _ in iterate_timeout(30, 'job A started'):
            if len(self.builds) == 1:
                break
        A_build = self.builds[0]
        start = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                             'failure_start_flag')
        for _ in iterate_timeout(30, 'job A running'):
            if os.path.exists(start):
                break

        self.log.debug("Add a second change which will test with the first")
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.log.debug("Wait for the second change to start its job")
        for _ in iterate_timeout(30, 'job B started'):
            if len(self.builds) == 2:
                break
        B_build = self.builds[1]
        start = os.path.join(self.jobdir_root, B_build.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job B running'):
            if os.path.exists(start):
                break

        self.log.debug("Continue the first job which will fail early")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'failure_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        # From here out, allow any pre-failure job to
        # continue until it has run two times

        self.log.debug("Wait for all jobs to finish")
        for _ in iterate_timeout(30, 'all jobs finished'):
            if len(self.builds) == 1 and len(self.history) == 3:
                break
            for b in self.builds[:]:
                if b.name == 'pre-failure':
                    try:
                        flag_path = os.path.join(
                            self.jobdir_root, b.uuid,
                            'work', 'failure_continue_flag')
                        with open(flag_path, "w") as of:
                            of.write("continue")
                    except Exception:
                        self.log.debug("Unable to write flag path %s",
                                       flag_path)
        self.log.debug("Done")

        self.log.debug("Wait for the second job to be aborted "
                       "and restarted without the first change")
        for _ in iterate_timeout(30, 'job B restarted'):
            if len(self.builds) == 1 and self.builds[0].name == 'wait':
                B_build2 = self.builds[0]
                if B_build2 != B_build:
                    break

        self.log.debug("Wait for the second change to start its job")
        start = os.path.join(self.jobdir_root, B_build2.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job B running'):
            if os.path.exists(start):
                break

        self.log.debug("Allow the restarted second job to finish")
        flag_path = os.path.join(self.jobdir_root, B_build2.uuid, 'work',
                                 'wait_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.waitUntilSettled()

        self.assertHistory([
            dict(name='pre-failure', result=None, changes='1,1'),
            dict(name='pre-failure', result=None, changes='1,1'),
            dict(name='wait', result='ABORTED', changes='1,1 2,1'),
            dict(name='wait', result='SUCCESS', changes='2,1'),
        ], ordered=True)

    def test_pre_run_post_run_failure_retry(self):
        # Test that we don't set pre_fail when a pre-run playbook fails and
        # the post-run playbook fails. This test is basically identical to
        # the one above except that we check the behavior remains if post run
        # playbooks also fail using a different job.
        # (so we honor the retry logic and restart the job).
        file_dict = {'pre-failure.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project5', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))

        self.log.debug("Wait for the first change to start its job")
        for _ in iterate_timeout(30, 'job A started'):
            if len(self.builds) == 1:
                break
        A_build = self.builds[0]
        start = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                             'failure_start_flag')
        for _ in iterate_timeout(30, 'job A running'):
            if os.path.exists(start):
                break

        self.log.debug("Add a second change which will test with the first")
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.log.debug("Wait for the second change to start its job")
        for _ in iterate_timeout(30, 'job B started'):
            if len(self.builds) == 2:
                break
        B_build = self.builds[1]
        start = os.path.join(self.jobdir_root, B_build.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job B running'):
            if os.path.exists(start):
                break

        self.log.debug("Continue the first job which will fail early")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'failure_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        # From here out, allow any pre-post-failure job to
        # continue until it has run two times

        self.log.debug("Wait for all jobs to finish")
        for _ in iterate_timeout(30, 'all jobs finished'):
            if len(self.builds) == 1 and len(self.history) == 3:
                break
            for b in self.builds[:]:
                if b.name == 'pre-post-failure':
                    try:
                        flag_path = os.path.join(
                            self.jobdir_root, b.uuid, 'work',
                            'failure_continue_flag')
                        with open(flag_path, "w") as of:
                            of.write("continue")
                    except Exception:
                        self.log.debug("Unable to write flag path %s",
                                       flag_path)
        self.log.debug("Done")

        self.log.debug("Wait for the second job to be aborted "
                       "and restarted without the first change")
        for _ in iterate_timeout(30, 'job B restarted'):
            if len(self.builds) == 1 and self.builds[0].name == 'wait':
                B_build2 = self.builds[0]
                if B_build2 != B_build:
                    break

        self.log.debug("Wait for the second change to start its job")
        start = os.path.join(self.jobdir_root, B_build2.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job B running'):
            if os.path.exists(start):
                break

        self.log.debug("Allow the restarted second job to finish")
        flag_path = os.path.join(self.jobdir_root, B_build2.uuid, 'work',
                                 'wait_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.waitUntilSettled()

        self.assertHistory([
            dict(name='pre-post-failure', result=None, changes='1,1'),
            dict(name='pre-post-failure', result=None, changes='1,1'),
            dict(name='wait', result='ABORTED', changes='1,1 2,1'),
            dict(name='wait', result='SUCCESS', changes='2,1'),
        ], ordered=False)

    def test_early_failure_fail_fast(self):
        file_dict = {'early-failure.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project3', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))

        self.log.debug("Wait for the first change to start its job")
        for _ in iterate_timeout(30, 'job A started'):
            if len(self.builds) == 1:
                break
        A_build = self.builds[0]
        start = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                             'failure_start_flag')
        for _ in iterate_timeout(30, 'job A running'):
            if os.path.exists(start):
                break

        self.log.debug("Add a second change which will test with the first")
        B = self.fake_gerrit.addFakeChange('org/project4', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.log.debug("Wait for the second change to start its job")
        for _ in iterate_timeout(30, 'job B started'):
            if len(self.builds) == 3:
                break
        B_build = self.builds[2]
        start = os.path.join(self.jobdir_root, B_build.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job B running'):
            if os.path.exists(start):
                break

        self.log.debug("Continue the first job which will fail early")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'failure_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.log.debug("Wait for the second job to be aborted "
                       "and restarted without the first change")
        for _ in iterate_timeout(30, 'job B restarted'):
            if len(self.builds) == 3:
                B_build2 = self.builds[2]
                if B_build2 != B_build:
                    break

        self.log.debug("Wait for the first job to be in its post-run playbook")
        start = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job A post running'):
            if os.path.exists(start):
                break

        self.log.debug("Allow the first job to finish")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'wait_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.log.debug("Wait for the first job to finish")
        for _ in iterate_timeout(30, 'job A complete'):
            if A_build not in self.builds:
                break

        self.log.debug("Wait for the second change to start its job")
        start = os.path.join(self.jobdir_root, B_build2.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job B running'):
            if os.path.exists(start):
                break

        self.log.debug("Allow the restarted second job to finish")
        flag_path = os.path.join(self.jobdir_root, B_build2.uuid, 'work',
                                 'wait_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.waitUntilSettled()

        self.assertHistory([
            dict(name='wait', result='ABORTED', changes='1,1 2,1'),
            dict(name='early-failure', result='FAILURE', changes='1,1'),
            dict(name='wait', result='ABORTED', changes='1,1'),
            dict(name='wait', result='SUCCESS', changes='2,1'),
        ], ordered=True)

    def test_early_failure_output(self):
        file_dict = {'output-failure.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))

        self.log.debug("Wait for the first change to start its job")
        for _ in iterate_timeout(30, 'job A started'):
            if len(self.builds) == 1:
                break
        A_build = self.builds[0]
        start = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                             'output_failure_start_flag')
        for _ in iterate_timeout(30, 'job A running'):
            if os.path.exists(start):
                break

        self.log.debug("Add a second change which will test with the first")
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.log.debug("Wait for the second change to start its job")
        for _ in iterate_timeout(30, 'job B started'):
            if len(self.builds) == 2:
                break
        B_build = self.builds[1]
        start = os.path.join(self.jobdir_root, B_build.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job B running'):
            if os.path.exists(start):
                break

        self.log.debug("Continue the first job which will output failure text")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'output_failure_continue1_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.log.debug("Wait for the second job to be aborted "
                       "and restarted without the first change")
        for _ in iterate_timeout(30, 'job B restarted'):
            if len(self.builds) == 2:
                B_build2 = self.builds[1]
                if B_build2 != B_build:
                    break

        self.log.debug("Continue the first job on to actual failure")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'output_failure_continue2_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.log.debug("Wait for the first job to be in its post-run playbook")
        start = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                             'wait_start_flag')
        for _ in iterate_timeout(30, 'job A post running'):
            if os.path.exists(start):
                break

        path = os.path.join(self.jobdir_root, A_build.uuid,
                            'work/logs/job-output.txt')
        with open(path) as f:
            output = f.read()
            self.log.info(output)
            self.assertTrue('Early failure in job, matched regex '
                            '"^.*output indicates failure.*$"' in output)

        self.log.debug("Allow the first job to finish")
        flag_path = os.path.join(self.jobdir_root, A_build.uuid, 'work',
                                 'wait_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.log.debug("Wait for the first job to finish")
        for _ in iterate_timeout(30, 'job A complete'):
            if A_build not in self.builds:
                break

        self.log.debug("Allow the restarted second job to finish")
        flag_path = os.path.join(self.jobdir_root, B_build2.uuid, 'work',
                                 'wait_continue_flag')
        self.log.debug("Writing %s", flag_path)
        with open(flag_path, "w") as of:
            of.write("continue")

        self.waitUntilSettled()

        self.assertHistory([
            dict(name='wait', result='ABORTED', changes='1,1 2,1'),
            dict(name='output-failure', result='FAILURE', changes='1,1'),
            dict(name='wait', result='SUCCESS', changes='2,1'),
        ], ordered=True)

    def test_early_failure_unreachable(self):
        # Test that a job with an early failure detected, that also
        # encounters an unreachable hosts is not retried.
        file_dict = {'early-failure-unreachable.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='early-failure-unreachable', result='FAILURE',
                 changes='1,1'),
        ])


class TestMaxNodesPerJob(AnsibleZuulTestCase):
    tenant_config_file = 'config/multi-tenant/main.yaml'

    def test_max_timeout_exceeded(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test-job
                nodeset:
                  nodes:
                    - name: node01
                      label: fake
                    - name: node02
                      label: fake
                    - name: node03
                      label: fake
                    - name: node04
                      label: fake
                    - name: node05
                      label: fake
                    - name: node06
                      label: fake
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('The job "test-job" exceeds tenant max-nodes-per-job 5.',
                      A.messages[0], "A should fail because of nodes limit")

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertNotIn("exceeds tenant max-nodes", B.messages[0],
                         "B should not fail because of nodes limit")


class TestMaxTimeout(ZuulTestCase):
    tenant_config_file = 'config/multi-tenant/main.yaml'

    def test_max_nodes_reached(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test-job
                timeout: 3600
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('The job "test-job" exceeds tenant max-job-timeout',
                      A.messages[0], "A should fail because of timeout limit")

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertNotIn("exceeds tenant max-job-timeout", B.messages[0],
                         "B should not fail because of timeout limit")


class TestOIDCConfiguration(ZuulTestCase):
    tenant_config_file = 'config/multi-tenant/main.yaml'

    def test_default_attributes(self):
        # Test that the secret oidc config with all default configurations
        # in different format.
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: my-oidc1
                oidc: {}
            - secret:
                name: my-oidc2
                oidc: null
            - secret:
                name: my-oidc3
                oidc:
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}

        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn("Build succeeded.", A.messages[0],
                      "oidc config should allow null value")

    def test_max_ttl_reached(self):
        # Test that the secret oidc ttl is within the tenant max-oidc-ttl
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: my-oidc
                oidc:
                  ttl: 400
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        # max-oidc-ttl for tenant-one is 300, it should cause error
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('The oidc secret "my-oidc" exceeds tenant max-oidc-ttl',
                      A.messages[0], "A should fail because of ttl limit")
        # max-oidc-ttl for tenant-two is the default 10800,
        # which should not cause error
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertNotIn("exceeds tenant max-oidc-ttl", B.messages[0],
                         "B should not fail because of ttl limit")

    def test_custom_issuer(self):
        # Test that custom issuer is handled correctly
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: my-oidc
                oidc:
                  ttl: 200
                  iss: https://zuul.custom-issuer.com
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        # The issuer is not white listed in tenant one, it should cause error
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn(
            'The iss "https://zuul.custom-issuer.com" in'
            ' oidc secret "my-oidc" is\n  not allowed',
            A.messages[0], "A should fail because the issuer is not allowed"
        )
        # The issuer is white listed for tenant-two, no error
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertNotIn(
            'The iss "https://zuul.custom-issuer.com" in'
            ' oidc secret "my-oidc" is\n  not allowed',
            B.messages[0], "B should not fail because the issuer is allowed"
        )

    def test_mutual_exclusive(self):
        # Test that `oidc` and `data` should be mutually exclusive
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: my-oidc
                oidc: {}
                data: {}
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('two or more values in the same group'
                      ' of exclusion \'secret_type\'',
                      A.messages[0],
                      "A should fail because of mutual exclusive")

    def test_required(self):
        # Test that one of `oidc` and `data` must be present
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: my-oidc
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('Either \'data\' or \'oidc\' must be present',
                      A.messages[0],
                      "A should fail because both are missing")

    def test_unsupported_algorithm(self):
        # Test that if the secret oidc algorithm is not supported,
        # there should be an error message
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: my-oidc
                oidc:
                  algorithm: XX256
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn("Algorithm 'XX256' is not supported",
                      A.messages[0],
                      "A should fail because of unsupported algorithm")


class TestAllowedConnection(AnsibleZuulTestCase):
    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/multi-tenant/main.yaml'

    def test_allowed_triggers(self):
        in_repo_conf = textwrap.dedent(
            """
            - pipeline:
                name: test
                manager: independent
                trigger:
                  github:
                    - event: pull_request
            """)
        file_dict = {'zuul.d/test.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange(
            'tenant-two-config', 'master', 'A', files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn(
            'Unknown connection named "github"', A.messages[0],
            "A should fail because of allowed-trigger")

        B = self.fake_gerrit.addFakeChange(
            'tenant-one-config', 'master', 'A', files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertNotIn(
            'Unknown connection named "github"', B.messages[0],
            "B should not fail because of allowed-trigger")

    def test_allowed_reporters(self):
        in_repo_conf = textwrap.dedent(
            """
            - pipeline:
                name: test
                manager: independent
                success:
                  outgoing_smtp:
                    to: you@example.com
            """)
        file_dict = {'zuul.d/test.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange(
            'tenant-one-config', 'master', 'A', files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn(
            'Unknown connection named "outgoing_smtp"', A.messages[0],
            "A should fail because of allowed-reporters")

        B = self.fake_gerrit.addFakeChange(
            'tenant-two-config', 'master', 'A', files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertNotIn(
            'Unknown connection named "outgoing_smtp"', B.messages[0],
            "B should not fail because of allowed-reporters")


class TestAllowedLabels(AnsibleZuulTestCase):
    config_file = 'zuul-connections-gerrit-and-github.conf'
    tenant_config_file = 'config/multi-tenant/main.yaml'

    def test_allowed_labels(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test
                nodeset:
                  nodes:
                    - name: controller
                      label: tenant-two-label
            """)
        file_dict = {'zuul.d/test.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange(
            'tenant-one-config', 'master', 'A', files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn(
            'Label named "tenant-two-label" is not part of the allowed',
            A.messages[0],
            "A should fail because of allowed-labels")

    def test_disallowed_labels(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test
                nodeset:
                  nodes:
                    - name: controller
                      label: tenant-one-label
            """)
        file_dict = {'zuul.d/test.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange(
            'tenant-two-config', 'master', 'A', files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn(
            'Label named "tenant-one-label" is not part of the allowed',
            A.messages[0],
            "A should fail because of disallowed-labels")


class TestPragma(ZuulTestCase):
    tenant_config_file = 'config/pragma/main.yaml'

    # These tests are failing depending on which scheduler completed the
    # tenant reconfiguration first. As the assertions are done with the
    # objects on scheduler-0, they will fail if scheduler-1 completed
    # the reconfiguration first.
    scheduler_count = 1

    def test_no_pragma(self):
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.waitUntilSettled()
        with open(os.path.join(FIXTURE_DIR,
                               'config/pragma/git/',
                               'org_project/nopragma.yaml')) as f:
            config = f.read()
        file_dict = {'.zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        # This is an untrusted repo with 2 branches, so it should have
        # an implied branch matcher for the job.
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        jobs = tenant.layout.getJobs('test-job')
        self.assertEqual(len(jobs), 1)
        for job in tenant.layout.getJobs('test-job'):
            self.assertIsNotNone(job.getBranchMatcher(tenant))

    def test_pragma(self):
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.waitUntilSettled()
        with open(os.path.join(FIXTURE_DIR,
                               'config/pragma/git/',
                               'org_project/pragma.yaml')) as f:
            config = f.read()
        file_dict = {'.zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        # This is an untrusted repo with 2 branches, so it would
        # normally have an implied branch matcher, but our pragma
        # overrides it.
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        jobs = tenant.layout.getJobs('test-job')
        self.assertEqual(len(jobs), 1)
        for job in tenant.layout.getJobs('test-job'):
            self.assertIsNone(job.getBranchMatcher(tenant))


class TestPragmaMultibranch(ZuulTestCase):
    tenant_config_file = 'config/pragma-multibranch/main.yaml'

    def test_no_branch_matchers(self):
        self.create_branch('org/project1', 'stable/pike')
        self.create_branch('org/project2', 'stable/jewel')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable/pike'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'stable/jewel'))
        self.waitUntilSettled()
        # We want the jobs defined on the stable/pike branch of
        # project1 to apply to the stable/jewel branch of project2.

        # First, without the pragma line, the jobs should not run
        # because in project1 they have branch matchers for pike, so
        # they will not match a jewel change.
        B = self.fake_gerrit.addFakeChange('org/project2', 'stable/jewel', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([])

        # Add a pragma line to disable implied branch matchers in
        # project1, so that the jobs and templates apply to both
        # branches.
        with open(os.path.join(FIXTURE_DIR,
                               'config/pragma-multibranch/git/',
                               'org_project1/zuul.yaml')) as f:
            config = f.read()
        extra_conf = textwrap.dedent(
            """
            - pragma:
                implied-branch-matchers: False
            """)
        config = extra_conf + config
        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project1', 'stable/pike', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        # Now verify that when we propose a change to jewel, we get
        # the pike/jewel jobs.
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test-job1', result='SUCCESS', changes='1,1'),
            dict(name='test-job2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_supplied_branch_matchers(self):
        self.create_branch('org/project1', 'stable/pike')
        self.create_branch('org/project2', 'stable/jewel')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable/pike'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'stable/jewel'))
        self.waitUntilSettled()
        # We want the jobs defined on the stable/pike branch of
        # project1 to apply to the stable/jewel branch of project2.

        # First, without the pragma line, the jobs should not run
        # because in project1 they have branch matchers for pike, so
        # they will not match a jewel change.
        B = self.fake_gerrit.addFakeChange('org/project2', 'stable/jewel', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([])

        # Add a pragma line to disable implied branch matchers in
        # project1, so that the jobs and templates apply to both
        # branches.
        with open(os.path.join(FIXTURE_DIR,
                               'config/pragma-multibranch/git/',
                               'org_project1/zuul.yaml')) as f:
            config = f.read()
        extra_conf = textwrap.dedent(
            """
            - pragma:
                implied-branches:
                  - stable/pike
                  - stable/jewel
            """)
        config = extra_conf + config
        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project1', 'stable/pike', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        # Now verify that when we propose a change to jewel, we get
        # the pike/jewel jobs.
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test-job1', result='SUCCESS', changes='1,1'),
            dict(name='test-job2', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestTenantImpliedBranchMatchers(ZuulTestCase):
    tenant_config_file = 'config/tenant-implied-branch-matchers/main.yaml'

    def test_tenant_implied_branch_matchers(self):
        # Test that we can force implied branch matchers in the tenant
        # config even in the case where a project only has one branch.
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        jobs = tenant.layout.getJobs('test-job')
        self.assertEqual(len(jobs), 1)
        for job in tenant.layout.getJobs('test-job'):
            self.assertIsNotNone(job.getBranchMatcher(tenant))

    def test_pragma_overrides_tenant_implied_branch_matchers(self):
        # Test that we can force implied branch matchers off with a pragma
        # even if the tenant config has it set on.
        config = textwrap.dedent(
            """
            - job:
                name: test-job
            - pragma:
                implied-branch-matchers: False
            """)
        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        self.create_branch('org/project', 'stable/pike')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/pike'))
        self.waitUntilSettled()

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        jobs = tenant.layout.getJobs('test-job')
        self.assertEqual(len(jobs), 2)
        for job in tenant.layout.getJobs('test-job'):
            self.assertIsNone(job.getBranchMatcher(tenant))


class TestBaseJobs(ZuulTestCase):
    tenant_config_file = 'config/base-jobs/main.yaml'

    def test_multiple_base_jobs(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='my-job', result='SUCCESS', changes='1,1'),
            dict(name='other-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(self.getJobFromHistory('my-job').
                         parameters['zuul']['jobtags'],
                         ['mybase'])
        self.assertEqual(self.getJobFromHistory('other-job').
                         parameters['zuul']['jobtags'],
                         ['otherbase'])

    def test_untrusted_base_job(self):
        """Test that a base job may not be defined in an untrusted repo"""
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: fail-base
                parent: null
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('Base jobs must be defined in config projects',
                      A.messages[0])
        self.assertHistory([])


class TestSecrets(ZuulTestCase):
    tenant_config_file = 'config/secrets/main.yaml'
    secret = {'password': 'test-password',
              'username': 'test-username'}

    def _getSecrets(self, job, pbtype):
        secrets = []
        build = self.getJobFromHistory(job)
        for pb in getattr(build.jobdir, pbtype):
            if pb.secrets_content:
                secrets.append(
                    yamlutil.ansible_unsafe_load(pb.secrets_content))
            else:
                secrets.append({})
        return secrets

    def test_secret_branch(self):
        # Test that we can use a secret defined in another branch of
        # the same project.
        self.create_branch('org/project2', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'stable'))
        self.waitUntilSettled()

        with open(os.path.join(FIXTURE_DIR,
                               'config/secrets/git/',
                               'org_project2/zuul-secret.yaml')) as f:
            config = f.read()

        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - job:
                parent: base
                name: project2-secret
                run: playbooks/secret.yaml
                secrets: [project2_secret]

            - project:
                check:
                  jobs:
                    - project2-secret
                gate:
                  jobs:
                    - noop
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project2', 'stable', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(B.reported, 1, "B should report success")
        self.assertHistory([
            dict(name='project2-secret', result='SUCCESS', changes='2,1'),
        ])
        self.assertEqual(
            self._getSecrets('project2-secret', 'playbooks'),
            [{'project2_secret': self.secret}])

    def test_secret_branch_duplicate(self):
        # Test that we can create a duplicate secret on a different
        # branch of the same project -- i.e., that when we branch
        # master to stable on a project with a secret, nothing
        # changes.
        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project1', 'stable', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report success")
        self.assertHistory([
            dict(name='project1-secret', result='SUCCESS', changes='1,1'),
        ])
        self.assertEqual(
            [{'secret_name': self.secret}],
            self._getSecrets('project1-secret', 'playbooks'))
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(
            len(tenant.layout.loading_errors), 0,
            "No error should have been accumulated")

    def test_secret_branch_error_same_branch(self):
        # Test that we are unable to define a secret twice on the same
        # project-branch.
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: project1_secret
                data: {}
            - secret:
                name: project1_secret
                data: {}
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('already defined', A.messages[0])

    def test_secret_branch_error_same_project(self):
        # Test that we are unable to create a secret which differs
        # from another with the same name -- i.e., that if we have a
        # duplicate secret on multiple branches of the same project,
        # they must be identical.
        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: project1_secret
                data: {}
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'stable', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('does not match existing definition in branch master',
                      A.messages[0])

    def test_secret_branch_error_other_project(self):
        # Test that we are unable to create a secret with the same
        # name as another.  We're never allowed to have a secret with
        # the same name outside of a project.
        in_repo_conf = textwrap.dedent(
            """
            - secret:
                name: project1_secret
                data: {}
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('already defined in project org/project1',
                      A.messages[0])

    def test_complex_secret(self):
        # Test that we can use a complex secret
        with open(os.path.join(FIXTURE_DIR,
                               'config/secrets/git/',
                               'org_project2/zuul-complex.yaml')) as f:
            config = f.read()

        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1, "A should report success")
        self.assertHistory([
            dict(name='project2-complex', result='SUCCESS', changes='1,1'),
        ])
        secret = {'complex_secret':
                  {'dict': {'password': 'test-password',
                            'username': 'test-username'},
                   'list': ['one', 'test-password', 'three'],
                   'profile': 'cloudy'}}

        self.assertEqual(
            self._getSecrets('project2-complex', 'playbooks'),
            [secret])

    def test_blobstore_secret(self):
        # Test the large secret blob store
        self.executor_server.hold_jobs_in_build = True
        self.useFixture(fixtures.MonkeyPatch(
            'zuul.model.Job.SECRET_BLOB_SIZE',
            1))

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        with self.scheds.first.sched.createZKContext(None, self.log)\
             as context:
            bs = BlobStore(context)
            # 1 secret, 2 repo states (project1 and common-config)
            self.assertEqual(len(bs), 3)

            self.scheds.first.sched._runBlobStoreCleanup()
            self.assertEqual(len(bs), 3)

            self.executor_server.hold_jobs_in_build = False
            self.executor_server.release()
            self.waitUntilSettled()

            self.assertEqual(A.reported, 1, "A should report success")
            self.assertHistory([
                dict(name='project1-secret', result='SUCCESS', changes='1,1'),
            ])
            self.assertEqual(
                [{'secret_name': self.secret}],
                self._getSecrets('project1-secret', 'playbooks'))

            self.scheds.first.sched._runBlobStoreCleanup()
            self.assertEqual(len(bs), 0)

    def test_oidc_single(self):
        # Test that oidc token is generated correctly when there is
        # a single oidc secret defined.
        with open(os.path.join(FIXTURE_DIR,
                               'config/secrets/git/',
                               'org_project2/zuul-oidc-single.yaml')) as f:
            config = f.read()
        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1, "A should report success")
        self.assertHistory([
            dict(name='project2-oidc-secret', result='SUCCESS', changes='1,1')
        ])

        pre_secrets = self._getSecrets(
            'project2-oidc-secret', 'pre_playbooks'
        )[0]
        self.assertEqual(len(pre_secrets), 1)
        for secret_name, secret_content in pre_secrets.items():
            self._validate_oidc_token(
                # The var name is with '_' while the secret.name is with '-'
                # which should be used in the value of the 'sub' claim
                secret_name.replace('_', '-'),
                secret_content.value, self.history[0].uuid,
                # The sub should be for the config project
                expected_sub=('secret:tenant-one/review.example.com/'
                              'common-config/base-oidc-secret'),
                expected_playbook=('review.example.com/common-config/'
                                   'playbooks/secret.yaml'),
                expected_claim='base_secret',
                expected_ttl=3600,
            )

        secrets = self._getSecrets(
            'project2-oidc-secret', 'playbooks'
        )[0]
        self.assertEqual(len(secrets), 1)
        for secret_name, secret_content in secrets.items():
            self._validate_oidc_token(
                # The var name is with '_' while the secret.name is with '-'
                # which should be used in the value of the 'sub' claim
                secret_name.replace('_', '-'),
                secret_content.value, self.history[0].uuid)

    def test_oidc_multi(self):
        # Test that oidc token is generated correctly when there are
        # multiple oidc secrets defined.
        with open(os.path.join(FIXTURE_DIR,
                               'config/secrets/git/',
                               'org_project2/zuul-oidc-multi.yaml')) as f:
            config = f.read()
        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        # Mock zuul_return data defined in playbook
        self.executor_server.returnData(
            "project2-dependent-with-return", A, data={'foo': 'bar'},
            secret_data={'login_secret1': "login_secret1_value"}
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1, "A should report success")
        self.assertHistory([
            dict(
                name='project2-dependent-with-return',
                result='SUCCESS', changes='1,1'
            ),
            dict(
                name='project2-oidc-secret',
                result='SUCCESS', changes='1,1'
            )
        ])

        secrets = self._getSecrets(
            'project2-oidc-secret', 'playbooks'
        )[0]
        self.assertEqual(len(secrets), 3)

        # OIDC secret should override job var with the same name
        self.assertNotEqual(secrets['login_secret0'], 'login_secret0_value')

        # OIDC secret should override zuul_return secret with the same name
        self.assertNotEqual(secrets['login_secret1'], 'login_secret1_value')

        # OIDC secret passed to parent
        parent_pre_secrets = self._getSecrets(
            'project2-oidc-secret', 'pre_playbooks'
        )[0]

        for secret_name, secret_content in parent_pre_secrets.items():
            self._validate_oidc_token(
                # var: "login_secretx" -> secret: "project2-oidc-secretx"
                'project2-oidc-secret' + secret_name[-1],
                secret_content.value,
                self.history[1].uuid, "parent.yaml")
        parent_post_secrets = self._getSecrets(
            'project2-oidc-secret', 'post_playbooks'
        )[0]
        for secret_name, secret_content in parent_post_secrets.items():
            self._validate_oidc_token(
                'project2-oidc-secret' + secret_name[-1], secret_content.value,
                self.history[1].uuid, "parent.yaml")

        for secret_name, secret_content in secrets.items():
            self._validate_oidc_token(
                'project2-oidc-secret' + secret_name[-1], secret_content.value,
                self.history[1].uuid)

    def test_oidc_mix(self):
        # Test that oidc token is generated correctly when there are
        # both oidc and normal secrets defined.
        with open(os.path.join(FIXTURE_DIR,
                               'config/secrets/git/',
                               'org_project2/zuul-oidc-mix.yaml')) as f:
            config = f.read()
        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1, "A should report success")
        self.assertHistory([
            dict(name='project2-oidc-secret', result='SUCCESS', changes='1,1')
        ])

        secrets = self._getSecrets(
            'project2-oidc-secret', 'playbooks'
        )[0]
        self.assertEqual(len(secrets), 4)

        # Validate normal secrets
        self.assertEqual(
            secrets['project2_secret'],
            {'username': 'test-username', 'password': 'test-password'}
        )
        # Remove the normal secret from the dict and validate the oidc tokens
        del secrets['project2_secret']
        for secret_name, secret_content in secrets.items():
            self._validate_oidc_token(
                secret_name.replace('_', '-'),
                secret_content.value, self.history[0].uuid)

    def test_oidc_iss_override(self):
        # Test that the custom 'iss' is allowed when configured
        with open(os.path.join(
            FIXTURE_DIR,
            'config/secrets/git/',
            'org_project2/zuul-oidc-iss-override.yaml')) as f:
            config = f.read()
        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1, "A should report success")
        self.assertHistory([
            dict(name='project2-oidc-secret', result='SUCCESS', changes='1,1')
        ])

        secrets = self._getSecrets(
            'project2-oidc-secret', 'playbooks'
        )[0]
        self.assertEqual(len(secrets), 2)

        expected_isses = {
            "oidc_secret": "https://zuul.example.com",
            "oidc_secret_iss_override_allowed": "https://zuul.allowed.com",
        }
        for secret_name, secret_content in secrets.items():
            self._validate_oidc_token(
                secret_name.replace('_', '-'),
                secret_content.value, self.history[0].uuid,
                expected_iss=expected_isses[secret_name])

    def _validate_oidc_token(
            self, oidc_name, oidc_token, build_uuid,
            playbook="secret.yaml",
            expected_sub=None,
            expected_playbook=None,
            expected_iss="https://zuul.example.com",
            expected_claim='my_claim_value',
            expected_ttl=300,
    ):

        if expected_sub is None:
            expected_sub = ('secret:tenant-one/review.example.com'
                            f'/org/project2/{oidc_name}')

        # Check header
        header_base64 = oidc_token.split('.')[0]
        header = json.loads(base64.b64decode(header_base64 + '==').decode())
        self.assertEqual('RS256', header['alg'])
        self.assertEqual('RS256-0', header['kid'])
        self.assertEqual('JWT', header['typ'])

        # Decode and check payload
        keystore = self.scheds.first.sched.keystore
        _, public_key, _ = (keystore.getLatestOidcSigningKeys("RS256"))
        pem_public_key = encryption.serialize_rsa_public_key(public_key)
        payload = jwt.decode(
            oidc_token, pem_public_key, algorithms=["RS256"],
            audience="sts.amazonaws.com")
        if expected_playbook is None:
            expected_playbook = (
                f'review.example.com/org/project2/playbooks/{playbook}')
        self.assertEqual(expected_iss, payload['iss'])
        self.assertEqual(expected_sub, payload['sub'])
        self.assertEqual(build_uuid, payload['build-uuid'])
        self.assertEqual('project2-oidc-secret', payload['job-name'])
        self.assertEqual(expected_playbook, payload['playbook'])
        self.assertEqual('check', payload['pipeline'])
        self.assertEqual('tenant-one', payload['tenant'])
        self.assertEqual('sts.amazonaws.com', payload['aud'])
        self.assertEqual(expected_claim, payload['my_claim'])
        self.assertEqual(expected_ttl, payload['exp'] - payload['iat'])


class TestSecretInheritance(ZuulTestCase):
    tenant_config_file = 'config/secret-inheritance/main.yaml'

    def _getSecrets(self, job, pbtype):
        secrets = []
        build = self.getJobFromHistory(job)
        for pb in getattr(build.jobdir, pbtype):
            if pb.secrets_content:
                secrets.append(
                    yamlutil.ansible_unsafe_load(pb.secrets_content))
            else:
                secrets.append({})
        return secrets

    def _checkTrustedSecrets(self):
        secret = {'longpassword': 'test-passwordtest-password',
                  'password': 'test-password',
                  'username': 'test-username'}
        base_secret = {'username': 'base-username'}
        self.assertEqual(
            self._getSecrets('trusted-secrets', 'playbooks'),
            [{'trusted_secret': secret}])
        self.assertEqual(
            self._getSecrets('trusted-secrets', 'pre_playbooks'),
            [{'base_secret': base_secret}])
        self.assertEqual(
            self._getSecrets('trusted-secrets', 'post_playbooks'), [])

        self.assertEqual(
            self._getSecrets('trusted-secrets-trusted-child',
                             'playbooks'), [{}])
        self.assertEqual(
            self._getSecrets('trusted-secrets-trusted-child',
                             'pre_playbooks'),
            [{'base_secret': base_secret}])
        self.assertEqual(
            self._getSecrets('trusted-secrets-trusted-child',
                             'post_playbooks'), [])

        self.assertEqual(
            self._getSecrets('trusted-secrets-untrusted-child',
                             'playbooks'), [{}])
        self.assertEqual(
            self._getSecrets('trusted-secrets-untrusted-child',
                             'pre_playbooks'),
            [{'base_secret': base_secret}])
        self.assertEqual(
            self._getSecrets('trusted-secrets-untrusted-child',
                             'post_playbooks'), [])

    def _checkUntrustedSecrets(self):
        secret = {'longpassword': 'test-passwordtest-password',
                  'password': 'test-password',
                  'username': 'test-username'}
        base_secret = {'username': 'base-username'}
        self.assertEqual(
            self._getSecrets('untrusted-secrets', 'playbooks'),
            [{'untrusted-secret': secret}])
        self.assertEqual(
            self._getSecrets('untrusted-secrets', 'pre_playbooks'),
            [{'base-secret': base_secret}])
        self.assertEqual(
            self._getSecrets('untrusted-secrets', 'post_playbooks'), [])

        self.assertEqual(
            self._getSecrets('untrusted-secrets-trusted-child',
                             'playbooks'), [{}])
        self.assertEqual(
            self._getSecrets('untrusted-secrets-trusted-child',
                             'pre_playbooks'),
            [{'base-secret': base_secret}])
        self.assertEqual(
            self._getSecrets('untrusted-secrets-trusted-child',
                             'post_playbooks'), [])

        self.assertEqual(
            self._getSecrets('untrusted-secrets-untrusted-child',
                             'playbooks'), [{}])
        self.assertEqual(
            self._getSecrets('untrusted-secrets-untrusted-child',
                             'pre_playbooks'),
            [{'base-secret': base_secret}])
        self.assertEqual(
            self._getSecrets('untrusted-secrets-untrusted-child',
                             'post_playbooks'), [])

    def test_trusted_secret_inheritance_check(self):
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='trusted-secrets', result='SUCCESS', changes='1,1'),
            dict(name='trusted-secrets-trusted-child',
                 result='SUCCESS', changes='1,1'),
            dict(name='trusted-secrets-untrusted-child',
                 result='SUCCESS', changes='1,1'),
        ], ordered=False)

        self._checkTrustedSecrets()

    def test_untrusted_secret_inheritance_check(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        # This configuration tries to run untrusted secrets in an
        # non-post-review pipeline and should therefore run no jobs.
        self.assertHistory([])


class TestSecretPassToParent(ZuulTestCase):
    tenant_config_file = 'config/pass-to-parent/main.yaml'

    def _getSecrets(self, job, pbtype):
        secrets = []
        build = self.getJobFromHistory(job)
        for pb in getattr(build.jobdir, pbtype):
            if pb.secrets_content:
                secrets.append(
                    yamlutil.ansible_unsafe_load(pb.secrets_content))
            else:
                secrets.append({})
        return secrets

    def test_secret_no_pass_to_parent(self):
        # Test that secrets are not available in the parent if
        # pass-to-parent is not set.
        file_dict = {'no-pass.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertHistory([
            dict(name='no-pass', result='SUCCESS', changes='1,1'),
        ])

        self.assertEqual(
            self._getSecrets('no-pass', 'playbooks'),
            [{'parent_secret': {'password': 'password3'}}])
        self.assertEqual(
            self._getSecrets('no-pass', 'pre_playbooks'),
            [{'parent_secret': {'password': 'password3'}}])
        self.assertEqual(
            self._getSecrets('no-pass', 'post_playbooks'),
            [{'parent_secret': {'password': 'password3'}}])

    def test_secret_pass_to_parent(self):
        # Test that secrets are available in the parent if
        # pass-to-parent is set.
        file_dict = {'pass.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertHistory([
            dict(name='pass', result='SUCCESS', changes='1,1'),
        ])

        self.assertEqual(
            self._getSecrets('pass', 'playbooks'),
            [{'parent_secret': {'password': 'password3'},
              'secret1': {'password': 'password1'},
              'secret2': {'password': 'password2'}}])
        self.assertEqual(
            self._getSecrets('pass', 'pre_playbooks'),
            [{'parent_secret': {'password': 'password3'},
              'secret1': {'password': 'password1'},
              'secret2': {'password': 'password2'}}])
        self.assertEqual(
            self._getSecrets('pass', 'post_playbooks'),
            [{'parent_secret': {'password': 'password3'},
              'secret1': {'password': 'password1'},
              'secret2': {'password': 'password2'}}])

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='pass', result='SUCCESS', changes='1,1'),
        ])
        self.assertIn('does not allow post-review', B.messages[0])

    def test_secret_pass_to_parent_missing(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: parent-job-without-secret
                pre-run: playbooks/pre.yaml
                run: playbooks/run.yaml
                post-run: playbooks/post.yaml

            - job:
                name: test-job
                parent: trusted-parent-job-without-secret
                secrets:
                  - name: my_secret
                    secret: missing-secret
                    pass-to-parent: true

            - project:
                check:
                  jobs:
                    - test-job
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('Secret missing-secret not found', A.messages[0])

    def test_secret_override(self):
        # Test that secrets passed to parents don't override existing
        # secrets.
        file_dict = {'override.txt': ''}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertHistory([
            dict(name='override', result='SUCCESS', changes='1,1'),
        ])

        self.assertEqual(
            self._getSecrets('override', 'playbooks'),
            [{'parent_secret': {'password': 'password3'},
              'secret1': {'password': 'password1'},
              'secret2': {'password': 'password2'}}])
        self.assertEqual(
            self._getSecrets('override', 'pre_playbooks'),
            [{'parent_secret': {'password': 'password3'},
              'secret1': {'password': 'password1'},
              'secret2': {'password': 'password2'}}])
        self.assertEqual(
            self._getSecrets('override', 'post_playbooks'),
            [{'parent_secret': {'password': 'password3'},
              'secret1': {'password': 'password1'},
              'secret2': {'password': 'password2'}}])

    def test_secret_ptp_trusted_untrusted(self):
        # Test if we pass a secret to a parent and one of the parents
        # is untrusted, the job becomes post-review.
        file_dict = {'trusted-under-untrusted.txt': ''}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertHistory([
            dict(name='trusted-under-untrusted',
                 result='SUCCESS', changes='1,1'),
        ])

        self.assertEqual(
            self._getSecrets('trusted-under-untrusted', 'playbooks'),
            [{'secret': {'password': 'trustedpassword1'}}])
        self.assertEqual(
            self._getSecrets('trusted-under-untrusted', 'pre_playbooks'),
            [{'secret': {'password': 'trustedpassword1'}}])
        self.assertEqual(
            self._getSecrets('trusted-under-untrusted', 'post_playbooks'),
            [{'secret': {'password': 'trustedpassword1'}}])

        B = self.fake_gerrit.addFakeChange('common-config', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='trusted-under-untrusted',
                 result='SUCCESS', changes='1,1'),
        ])
        self.assertIn('does not allow post-review', B.messages[0])

    def test_secret_ptp_trusted_trusted(self):
        # Test if we pass a secret to a parent and all of the parents
        # are trusted, the job does not become post-review.
        file_dict = {'trusted-under-trusted.txt': ''}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertHistory([
            dict(name='trusted-under-trusted',
                 result='SUCCESS', changes='1,1'),
        ])

        self.assertEqual(
            self._getSecrets('trusted-under-trusted', 'playbooks'),
            [{'secret': {'password': 'trustedpassword1'}}])
        self.assertEqual(
            self._getSecrets('trusted-under-trusted', 'pre_playbooks'),
            [{'secret': {'password': 'trustedpassword1'}}])
        self.assertEqual(
            self._getSecrets('trusted-under-trusted', 'post_playbooks'),
            [{'secret': {'password': 'trustedpassword1'}}])

        B = self.fake_gerrit.addFakeChange('common-config', 'master', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='trusted-under-trusted',
                 result='SUCCESS', changes='1,1'),
            dict(name='trusted-under-trusted',
                 result='SUCCESS', changes='2,1'),
        ])


class TestSecretLeaks(AnsibleZuulTestCase):
    tenant_config_file = 'config/secret-leaks/main.yaml'

    def searchForContent(self, path, content):
        matches = []
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                with open(filepath, 'rb') as f:
                    if content in f.read():
                        matches.append(filepath[len(path):])
        return matches

    def _test_secret_file(self):
        # Or rather -- test that they *don't* leak.
        # Keep the jobdir around so we can inspect contents.
        self.executor_server.keep_jobdir = True
        conf = textwrap.dedent(
            """
            - project:
                name: org/project
                check:
                  jobs:
                    - secret-file
            """)

        file_dict = {'.zuul.yaml': conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='secret-file', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        matches = self.searchForContent(self.history[0].jobdir.root,
                                        b'test-password')
        self.assertEqual(set(['/work/secret-file.txt']),
                         set(matches))

    def test_secret_file(self):
        self._test_secret_file()

    def test_secret_file_verbose(self):
        # Output extra ansible info to exercise alternate logging code
        # paths.
        self.executor_server.verbose = True
        self._test_secret_file()

    def _test_secret_file_fail(self):
        # Or rather -- test that they *don't* leak.
        # Keep the jobdir around so we can inspect contents.
        self.executor_server.keep_jobdir = True
        conf = textwrap.dedent(
            """
            - project:
                name: org/project
                check:
                  jobs:
                    - secret-file-fail
            """)

        file_dict = {'.zuul.yaml': conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='secret-file-fail', result='FAILURE', changes='1,1'),
        ], ordered=False)
        matches = self.searchForContent(self.history[0].jobdir.root,
                                        b'test-password')
        self.assertEqual(set(['/work/failure-file.txt']),
                         set(matches))

    def test_secret_file_fail(self):
        self._test_secret_file_fail()

    def test_secret_file_fail_verbose(self):
        # Output extra ansible info to exercise alternate logging code
        # paths.
        self.executor_server.verbose = True
        self._test_secret_file_fail()


class TestParseErrors(AnsibleZuulTestCase):
    tenant_config_file = 'config/parse-errors/main.yaml'

    def searchForContent(self, path, content):
        matches = []
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                with open(filepath, 'rb') as f:
                    if content in f.read():
                        matches.append(filepath[len(path):])
        return matches

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_parse_error_leak(self):
        # Test that parse errors don't leak inventory information
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
        ])
        matches = self.searchForContent(self.history[0].jobdir.root,
                                        b'xyzzy')
        self.assertEqual(set([
            '/trusted/project_0/review.example.com/common-config/zuul.yaml']),
            set(matches))


class TestNodesets(ZuulTestCase):
    tenant_config_file = 'config/nodesets/main.yaml'

    def test_nodeset_branch(self):
        # Test that we can use a nodeset defined in another branch of
        # the same project.
        self.create_branch('org/project2', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'stable'))
        self.waitUntilSettled()

        with open(os.path.join(FIXTURE_DIR,
                               'config/nodesets/git/',
                               'org_project2/zuul-nodeset.yaml')) as f:
            config = f.read()

        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - job:
                parent: base
                name: project2-test
                nodeset: project2-nodeset

            - project:
                check:
                  jobs:
                    - project2-test
                gate:
                  jobs:
                    - noop
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project2', 'stable', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(B.reported, 1, "B should report success")
        self.assertHistory([
            dict(name='project2-test', result='SUCCESS', changes='2,1',
                 node='ubuntu-xenial'),
        ])

    def test_nodeset_branch_duplicate(self):
        # Test that we can create a duplicate nodeset on a different
        # branch of the same project -- i.e., that when we branch
        # master to stable on a project with a nodeset, nothing
        # changes.
        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project1', 'stable', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report success")
        self.assertHistory([
            dict(name='project1-test', result='SUCCESS', changes='1,1',
                 node='ubuntu-xenial'),
        ])

    def test_nodeset_branch_error_same_branch(self):
        # Test that we are unable to define a nodeset twice on the same
        # project-branch.
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: project1-nodeset
                nodes: []
            - nodeset:
                name: project1-nodeset
                nodes: []
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('already defined', A.messages[0])

    def test_nodeset_branch_error_same_project(self):
        # Test that we are unable to create a nodeset which differs
        # from another with the same name -- i.e., that if we have a
        # duplicate nodeset on multiple branches of the same project,
        # they must be identical.
        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: project1-nodeset
                nodes: []
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'stable', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('does not match existing definition in branch master',
                      A.messages[0])

    def test_nodeset_branch_error_other_project(self):
        # Test that we are unable to create a nodeset with the same
        # name as another.  We're never allowed to have a nodeset with
        # the same name outside of a project.
        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: project1-nodeset
                nodes: []
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('already defined in project org/project1',
                      A.messages[0])


class TestSemaphoreBranches(ZuulTestCase):
    tenant_config_file = 'config/semaphore-branches/main.yaml'

    def test_semaphore_branch(self):
        # Test that we can use a semaphore defined in another branch of
        # the same project.
        self.create_branch('org/project2', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'stable'))
        self.waitUntilSettled()

        with open(os.path.join(FIXTURE_DIR,
                               'config/semaphore-branches/git/',
                               'org_project2/zuul-semaphore.yaml')) as f:
            config = f.read()

        file_dict = {'zuul.yaml': config}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - job:
                parent: base
                name: project2-test
                semaphore: project2-semaphore

            - project:
                check:
                  jobs:
                    - project2-test
                gate:
                  jobs:
                    - noop
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project2', 'stable', 'B',
                                           files=file_dict)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(B.reported, 1, "B should report success")
        self.assertHistory([
            dict(name='project2-test', result='SUCCESS', changes='2,1')
        ])

    def test_semaphore_branch_duplicate(self):
        # Test that we can create a duplicate semaphore on a different
        # branch of the same project -- i.e., that when we branch
        # master to stable on a project with a semaphore, nothing
        # changes.
        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project1', 'stable', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report success")
        self.assertHistory([
            dict(name='project1-test', result='SUCCESS', changes='1,1')
        ])

    def test_semaphore_branch_error_same_branch(self):
        # Test that we are unable to define a semaphore twice on the same
        # project-branch.
        in_repo_conf = textwrap.dedent(
            """
            - semaphore:
                name: project1-semaphore
                max: 2
            - semaphore:
                name: project1-semaphore
                max: 2
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('already defined', A.messages[0])

    def test_semaphore_branch_error_same_project(self):
        # Test that we are unable to create a semaphore which differs
        # from another with the same name -- i.e., that if we have a
        # duplicate semaphore on multiple branches of the same project,
        # they must be identical.
        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - semaphore:
                name: project1-semaphore
                max: 4
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'stable', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('does not match existing definition in branch master',
                      A.messages[0])

    def test_semaphore_branch_error_other_project(self):
        # Test that we are unable to create a semaphore with the same
        # name as another.  We're never allowed to have a semaphore with
        # the same name outside of a project.
        in_repo_conf = textwrap.dedent(
            """
            - semaphore:
                name: project1-semaphore
                max: 2
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn('already defined in project org/project1',
                      A.messages[0])


class TestJobOutput(AnsibleZuulTestCase):
    tenant_config_file = 'config/job-output/main.yaml'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_job_output_split_streams(self):
        # Verify that command standard output appears in the job output,
        # and that failures in the final playbook get logged.

        # This currently only verifies we receive output from
        # localhost.  Notably, it does not verify we receive output
        # via zuul_console streaming.
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project4', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='job-output-split-streams',
                 result='SUCCESS', changes='1,1'),
        ], ordered=False)

        token_stdout = "Standard output test {}".format(
            self.history[0].jobdir.src_root)
        token_stderr = "Standard error test {}".format(
            self.history[0].jobdir.src_root)

        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))
        result = j[0]['plays'][0]['tasks'][0]['hosts']['test_node']
        self.assertEqual(token_stdout, result['stdout'])
        self.assertEqual(token_stderr, result['stderr'])

        job_output = self._get_file(self.history[0],
                                    'work/logs/job-output.txt')
        self.log.info(job_output)
        self.assertIn(token_stdout, job_output)
        self.assertIn(token_stderr, job_output)

    def test_job_output(self):
        # Verify that command standard output appears in the job output,
        # and that failures in the final playbook get logged.

        # This currently only verifies we receive output from
        # localhost.  Notably, it does not verify we receive output
        # via zuul_console streaming.
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='job-output', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        token_stdout = "Standard output test {}".format(
            self.history[0].jobdir.src_root)
        token_stderr = "Standard error test {}".format(
            self.history[0].jobdir.src_root)

        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))

        result = j[0]['plays'][0]['tasks'][0]['hosts']['test_node']
        self.assertEqual("\n".join((token_stdout, token_stderr)),
                         result['stdout'])
        self.assertEqual("", result['stderr'])
        self.assertTrue(j[0]['plays'][0]['tasks'][1]
                        ['hosts']['test_node']['skipped'])
        self.assertTrue(j[0]['plays'][0]['tasks'][2]
                        ['hosts']['test_node']['failed'])
        self.assertEqual(
            "This is a handler",
            j[0]['plays'][0]['tasks'][3]
            ['hosts']['test_node']['stdout'])

        job_output = self._get_file(self.history[0],
                                    'work/logs/job-output.txt')
        self.log.info(job_output)
        self.assertIn(token_stdout, job_output)
        self.assertIn(token_stderr, job_output)
        self.assertIn("Job console starting", job_output)
        self.assertIn("Cloning repos into workspace", job_output)
        self.assertIn("Merging changes", job_output)
        self.assertIn("Restoring repo states", job_output)
        self.assertIn("Checking out repos", job_output)
        self.assertIn("Preparing playbooks", job_output)
        self.assertIn("Running Ansible setup", job_output)

    def test_job_output_missing_role(self):
        # Verify that ansible errors such as missing roles are part of the
        # buildlog.

        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project3', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='job-output-missing-role', result='FAILURE',
                 changes='1,1'),
            dict(name='job-output-missing-role-include', result='FAILURE',
                 changes='1,1'),
        ], ordered=False)

        for history in self.history:
            job_output = self._get_file(history,
                                        'work/logs/job-output.txt')
            self.assertIn('the role \'not_existing\' was not found',
                          job_output)

    def test_job_output_failure_log_split_streams(self):
        logger = logging.getLogger('zuul.AnsibleJob')
        output = io.StringIO()
        logger.addHandler(logging.StreamHandler(output))

        # Verify that a failure in the last post playbook emits the contents
        # of the json output to the log
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project5', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='job-output-failure-split-streams',
                 result='POST_FAILURE', changes='1,1'),
        ], ordered=False)

        token_stdout = "Standard output test {}".format(
            self.history[0].jobdir.src_root)
        token_stderr = "Standard error test {}".format(
            self.history[0].jobdir.src_root)

        json_output = self._get_file(self.history[0],
                                     'work/logs/job-output.json')
        self.log.info(json_output)
        j = json.loads(json_output)
        result = j[0]['plays'][0]['tasks'][0]['hosts']['test_node']
        self.assertEqual(token_stdout, result['stdout'])
        self.assertEqual(token_stderr, result['stderr'])

        job_output = self._get_file(self.history[0],
                                    'work/logs/job-output.txt')
        self.log.info(job_output)
        self.assertIn(token_stdout, job_output)
        self.assertIn(token_stderr, job_output)

        log_output = output.getvalue()
        self.assertIn('Final playbook failed', log_output)
        self.assertIn('Failure stdout test', log_output)
        self.assertIn('Failure stderr test', log_output)

    def test_job_output_failure_log(self):
        logger = logging.getLogger('zuul.AnsibleJob')
        output = io.StringIO()
        logger.addHandler(logging.StreamHandler(output))

        # Verify that a failure in the last post playbook emits the contents
        # of the json output to the log
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='job-output-failure',
                 result='POST_FAILURE', changes='1,1'),
        ], ordered=False)

        token_stdout = "Standard output test {}".format(
            self.history[0].jobdir.src_root)
        token_stderr = "Standard error test {}".format(
            self.history[0].jobdir.src_root)

        json_output = self._get_file(self.history[0],
                                     'work/logs/job-output.json')
        self.log.info(json_output)
        j = json.loads(json_output)
        result = j[0]['plays'][0]['tasks'][0]['hosts']['test_node']
        self.assertEqual("\n".join((token_stdout, token_stderr)),
                         result['stdout'])
        self.assertEqual("", result['stderr'])

        job_output = self._get_file(self.history[0],
                                    'work/logs/job-output.txt')
        self.log.info(job_output)
        self.assertIn(token_stdout, job_output)
        self.assertIn(token_stderr, job_output)

        log_output = output.getvalue()
        self.assertIn('Final playbook failed', log_output)
        self.assertIn('Failure stdout test', log_output)
        self.assertIn('Failure stderr test', log_output)

    def test_job_POST_FAILURE_reports_statsd(self):
        """Test that POST_FAILURES output job stats."""
        self.statsd.clear()
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='job-output-failure',
                 result='POST_FAILURE', changes='1,1'),
        ], ordered=False)
        post_failure_stat = 'zuul.tenant.tenant-one.pipeline.check.project.' \
                            'review_example_com.org_project2.master.job.' \
                            'job-output-failure.POST_FAILURE'
        self.assertReportedStat(post_failure_stat, value='1', kind='c')
        self.assertReportedStat(post_failure_stat, kind='ms')

    @mock.patch("zuul.executor.server.OUTPUT_MAX_LINE_BYTES", 50)
    def test_job_output_max_line_bytes(self):
        logger = logging.getLogger('zuul.AnsibleJob')
        output = io.StringIO()
        logger.addHandler(logging.StreamHandler(output))

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='job-output', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        log_output = output.getvalue()
        self.assertIn('Ansible output exceeds max. line size of', log_output)

    def test_job_output_pre_timed_out_json(self):
        self.executor_server.keep_jobdir = True
        files = {'pretimeout': ''}
        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Failures in pre-run have a None result and then Zuul determines
        # if they should be retried from there.
        self.assertHistory([
            dict(name='pre-timeout', result=None, changes='1,1'),
        ], ordered=False)
        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))
        self.assertEqual('pre', j[0]['phase'])
        self.assertEqual('0', j[0]['index'])
        self.assertEqual(True, j[0]['trusted'])
        self.assertEqual({}, j[0]['stats'])
        expected_str = 'This playbook timed out.'
        result = j[0]['plays'][0]['play']['name']
        self.assertEqual(expected_str, result)
        self.assertIn('duration', j[0]['plays'][0]['play'])
        self.assertIn('start', j[0]['plays'][0]['play']['duration'])
        self.assertIn('end', j[0]['plays'][0]['play']['duration'])

    def test_job_output_run_timed_out_json(self):
        self.executor_server.keep_jobdir = True
        files = {'runtimeout': ''}
        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='run-timeout', result='TIMED_OUT', changes='1,1'),
        ], ordered=False)
        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))
        self.assertEqual('run', j[0]['phase'])
        self.assertEqual('0', j[0]['index'])
        self.assertEqual(True, j[0]['trusted'])
        self.assertEqual({}, j[0]['stats'])
        expected_str = 'This playbook timed out.'
        result = j[0]['plays'][0]['play']['name']
        self.assertEqual(expected_str, result)
        self.assertIn('duration', j[0]['plays'][0]['play'])
        self.assertIn('start', j[0]['plays'][0]['play']['duration'])
        self.assertIn('end', j[0]['plays'][0]['play']['duration'])

    def test_job_output_post_timed_out_json(self):
        self.executor_server.keep_jobdir = True
        files = {'posttimeout': ''}
        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='post-timeout', result='POST_FAILURE', changes='1,1'),
        ], ordered=False)
        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))
        self.assertEqual('post', j[1]['phase'])
        self.assertEqual('0', j[1]['index'])
        self.assertEqual(True, j[1]['trusted'])
        self.assertEqual({}, j[1]['stats'])
        expected_str = 'This playbook timed out.'
        result = j[1]['plays'][0]['play']['name']
        self.assertEqual(expected_str, result)
        self.assertIn('duration', j[1]['plays'][0]['play'])
        self.assertIn('start', j[1]['plays'][0]['play']['duration'])
        self.assertIn('end', j[1]['plays'][0]['play']['duration'])

    def test_job_output_pre_syntax_error_json(self):
        self.executor_server.keep_jobdir = True
        files = {'presyntax': ''}
        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Failures in pre-run have a None result and then Zuul determines
        # if they should be retried from there.
        self.assertHistory([
            dict(name='pre-syntax', result=None, changes='1,1'),
        ], ordered=False)
        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))
        self.assertEqual('pre', j[0]['phase'])
        self.assertEqual('0', j[0]['index'])
        self.assertEqual(True, j[0]['trusted'])
        self.assertEqual({}, j[0]['stats'])
        expected_str = 'ERROR! A playbook must be a list of plays'
        result = j[0]['plays'][0]['play']['name']
        self.assertIn(expected_str, result)

    def test_job_output_run_syntax_error_json(self):
        self.executor_server.keep_jobdir = True
        files = {'runsyntax': ''}
        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='run-syntax', result='FAILURE', changes='1,1'),
        ], ordered=False)
        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))
        self.assertEqual('run', j[0]['phase'])
        self.assertEqual('0', j[0]['index'])
        self.assertEqual(True, j[0]['trusted'])
        self.assertEqual({}, j[0]['stats'])
        expected_str = 'ERROR! A playbook must be a list of plays'
        result = j[0]['plays'][0]['play']['name']
        self.assertIn(expected_str, result)

    def test_job_output_post_syntax_error_json(self):
        self.executor_server.keep_jobdir = True
        files = {'postsyntax': ''}
        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='post-syntax', result='POST_FAILURE', changes='1,1'),
        ], ordered=False)
        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))
        self.assertEqual('post', j[1]['phase'])
        self.assertEqual('0', j[1]['index'])
        self.assertEqual(True, j[1]['trusted'])
        self.assertEqual({}, j[1]['stats'])
        expected_str = 'ERROR! A playbook must be a list of plays'
        result = j[1]['plays'][0]['play']['name']
        self.assertIn(expected_str, result)


class TestNoLog(AnsibleZuulTestCase):
    tenant_config_file = 'config/ansible-no-log/main.yaml'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_no_log_unreachable(self):
        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        json_log = self._get_file(self.history[0], 'work/logs/job-output.json')
        text_log = self._get_file(self.history[0], 'work/logs/job-output.txt')

        self.assertNotIn('my-very-secret-password-1', json_log)
        self.assertNotIn('my-very-secret-password-2', json_log)
        self.assertNotIn('my-very-secret-password-1', text_log)
        self.assertNotIn('my-very-secret-password-2', text_log)


class TestJsonStringResults(AnsibleZuulTestCase):
    tenant_config_file = 'config/ansible-json-string-results/main.yaml'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_ansible_json_string_results(self):
        """Test modules that return string results are captured

        The yum/dnf modules are seemily almost unique in setting
        "results" in their module return value to a list of strings
        (other things might too, but not many other built-in
        components).  Confusingly, when using loops in ansible the
        output also has a "results" which is a list of dicts with
        return values from each iteration.

        The zuul_json callback handler needs to deal with both; We've
        broken this before making changes to its results parsing.
        This test fakes some string return values like the yum modules
        do, and ensures they are captured.

        """

        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        json_log = self._get_file(self.history[0], 'work/logs/job-output.json')
        text_log = self._get_file(self.history[0], 'work/logs/job-output.txt')

        self.assertIn('if you see this string, it is working', json_log)
        # Note the text log doesn't include the detail of the returned
        # results, just the msg field, hence to following "not in"
        self.assertNotIn('if you see this string, it is working', text_log)
        self.assertIn('A plugin message', text_log)
        # no_log checking
        self.assertNotIn('this is a secret string', json_log)
        self.assertNotIn('this is a secret string', text_log)


class TestUnreachable(AnsibleZuulTestCase):
    tenant_config_file = 'config/ansible-unreachable/main.yaml'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_unreachable(self):
        self.wait_timeout = 120

        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The result must be retry limit because jobs with unreachable nodes
        # will be retried.
        self.assertIn('RETRY_LIMIT', A.messages[0])
        self.assertHistory([
            dict(name='pre-unreachable', result=None, changes='1,1'),
            dict(name='pre-unreachable', result=None, changes='1,1'),
            dict(name='run-unreachable', result=None, changes='1,1'),
            dict(name='run-unreachable', result=None, changes='1,1'),
            dict(name='post-unreachable', result=None, changes='1,1'),
            dict(name='post-unreachable', result=None, changes='1,1'),
        ], ordered=False)

        retried_builds = set()
        for build in self.history:
            will_retry_flag = os.path.join(
                self.jobdir_root, build.uuid, 'work', 'will_retry_flag')
            self.assertTrue(os.path.exists(will_retry_flag))
            with open(will_retry_flag) as f:
                will_retry = f.readline()
                expect_retry = build.name not in retried_builds
                self.assertEqual(str(expect_retry), will_retry)
            output_path = os.path.join(build.jobdir.root,
                                       'work/logs/job-output.txt')
            with open(output_path) as f:
                job_output = f.read()
            self.log.debug(job_output)
            self.assertNotIn("This host is not unreachable", job_output)
            self.assertIn("This host is unreachable: fake", job_output)

            retried_builds.add(build.name)

        conn = self.scheds.first.sched.sql.connection
        for build in conn.getBuilds():
            self.assertEqual(build.error_detail, 'Host unreachable')

    def test_unreachable_nodeset_alternates(self):
        # Test nodeset alternative behavior with unreachable hosts.
        # We increment the nodeset alternative if we encounter an
        # unreachable host; but we don't increment it for other types
        # of retries.
        self.wait_timeout = 120

        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True

        in_repo_conf = textwrap.dedent(
            """
            - nodeset:
                name: test-nodeset
                alternatives:
                  - nodes:
                      - name: controller
                        label: fast-label
                  - nodes:
                      - name: controller
                        label: slow-label
            - job:
                name: pre-failure
                attempts: 2
                nodeset: test-nodeset
                pre-run: playbooks/pre-fail.yaml
            - project:
                check:
                  jobs:
                    - pre-failure
                    - pre-unreachable:
                        attempts: 3
                        nodeset: test-nodeset
                    - run-unreachable
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The result must be retry limit because jobs with unreachable nodes
        # will be retried.
        self.assertIn('RETRY_LIMIT', A.messages[0])
        self.assertHistory([
            dict(name='pre-unreachable', result=None, changes='1,1'),
            dict(name='pre-unreachable', result=None, changes='1,1'),
            dict(name='pre-unreachable', result=None, changes='1,1'),
            dict(name='run-unreachable', result=None, changes='1,1'),
            dict(name='run-unreachable', result=None, changes='1,1'),
            dict(name='pre-failure', result=None, changes='1,1'),
            dict(name='pre-failure', result=None, changes='1,1'),
        ], ordered=False)

        pre_unreachable_labels = []
        pre_failure_labels = []
        for build in self.history:
            inv_path = os.path.join(build.jobdir.root,
                                    'ansible', 'inventory.yaml')
            with open(inv_path, 'r') as f:
                inventory = yaml.safe_load(f)
            if build.name == 'pre-unreachable':
                # Get an ordered list of label requests we make for
                # the pre jobs
                label = inventory['all']['hosts'][
                    'controller']['nodepool']['label']
                pre_unreachable_labels.append(label)
            elif build.name == 'pre-failure':
                # Get an ordered list of label requests we make for
                # the pre jobs
                label = inventory['all']['hosts'][
                    'controller']['nodepool']['label']
                pre_failure_labels.append(label)
            else:
                # The other jobs don't request nodes
                self.assertEqual({}, inventory['all']['hosts'])
        # The pre-unreachable job should continue to iterate through
        # the nodeset alternatives (and repeat the last one).
        self.assertEqual(['fast-label', 'slow-label', 'slow-label'],
                         pre_unreachable_labels)
        # The pre-failure job, since it is a "normal" failure of the
        # pre-run playbook, should continue to use the first nodeset
        # alternative.
        self.assertEqual(['fast-label', 'fast-label'],
                         pre_failure_labels)


class TestJobPause(AnsibleZuulTestCase):
    tenant_config_file = 'config/job-pause/main.yaml'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_job_pause(self):
        """
        compile1
        +--> compile2
        |    +--> test-after-compile2
        +--> test1-after-compile1
        +--> test2-after-compile1
        test-good
        test-fail
        """

        self.wait_timeout = 120

        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for _ in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-fail', result='FAILURE', changes='1,1'),
            dict(name='test-good', result='SUCCESS', changes='1,1'),
            dict(name='test1-after-compile1', result='SUCCESS', changes='1,1'),
            dict(name='test2-after-compile1', result='SUCCESS', changes='1,1'),
            dict(name='test-after-compile2', result='SUCCESS', changes='1,1'),
            dict(name='compile2', result='SUCCESS', changes='1,1'),
            dict(name='compile1', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # The order of some of these tests is not deterministic so check that
        # the last two are compile2, compile1 in this order.
        history_compile1 = self.history[-1]
        history_compile2 = self.history[-2]
        self.assertEqual('compile1', history_compile1.name)
        self.assertEqual('compile2', history_compile2.name)

    def test_job_pause_retry(self):
        """
        Tests that a paused job that gets lost due to an executor restart is
        retried together with all child jobs.

        This test will wait until compile1 is paused and then fails it. The
        expectation is that all child jobs are retried even if they already
        were successful.

        compile1 --+
                   +--> test1-after-compile1
                   +--> test2-after-compile1
                   +--> compile2 --+
                                   +--> test-after-compile2
        test-good
        test-fail
        """
        self.wait_timeout = 120

        self.executor_server.hold_jobs_in_build = True

        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled("patchset uploaded")

        self.executor_server.release('test-.*')
        self.executor_server.release('compile1')
        self.waitUntilSettled("released compile1")

        # test-fail and test-good must be finished by now
        self.assertHistory([
            dict(name='test-fail', result='FAILURE', changes='1,1'),
            dict(name='test-good', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # Further compile1 must be in paused state and its three children in
        # the queue. waitUltilSettled can return either directly after the job
        # pause or after the child jobs are enqueued. So to make this
        # deterministic we wait for the child jobs here
        for _ in iterate_timeout(60, 'waiting for child jobs'):
            if len(self.builds) == 4:
                break
        self.waitUntilSettled("child jobs are running")

        compile1 = self.builds[0]
        self.assertTrue(compile1.paused)

        # Now resume resume the compile2 sub tree so we can later check if all
        # children restarted
        self.executor_server.release('compile2')
        for _ in iterate_timeout(60, 'waiting for child jobs'):
            if len(self.builds) == 5:
                break
        self.waitUntilSettled("release compile2")
        self.executor_server.release('test-after-compile2')
        self.waitUntilSettled("release test-after-compile2")
        self.executor_server.release('compile2')
        self.waitUntilSettled("release compile2 again")
        self.assertHistory([
            dict(name='test-fail', result='FAILURE', changes='1,1'),
            dict(name='test-good', result='SUCCESS', changes='1,1'),
            dict(name='compile2', result='SUCCESS', changes='1,1'),
            dict(name='test-after-compile2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # Stop the job worker of compile1 to simulate an executor restart
        for job_worker in self.executor_server.job_workers.values():
            if job_worker.build_request.uuid == compile1.unique:
                job_worker.stop()
        self.waitUntilSettled("Stop job")

        # Only compile1 must be waiting
        for _ in iterate_timeout(60, 'waiting for compile1 job'):
            if len(self.builds) == 1:
                break
        self.waitUntilSettled("only compile1 is running")
        self.assertBuilds([dict(name='compile1', changes='1,1')])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled("global release")

        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for x in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-fail', result='FAILURE', changes='1,1'),
            dict(name='test-good', result='SUCCESS', changes='1,1'),
            dict(name='compile2', result='SUCCESS', changes='1,1'),
            dict(name='compile2', result='SUCCESS', changes='1,1'),
            dict(name='test-after-compile2', result='SUCCESS', changes='1,1'),
            dict(name='test-after-compile2', result='SUCCESS', changes='1,1'),
            dict(name='compile1', result='ABORTED', changes='1,1'),
            dict(name='compile1', result='SUCCESS', changes='1,1'),
            dict(name='test1-after-compile1', result='ABORTED', changes='1,1'),
            dict(name='test2-after-compile1', result='ABORTED', changes='1,1'),
            dict(name='test1-after-compile1', result='SUCCESS', changes='1,1'),
            dict(name='test2-after-compile1', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_job_pause_fail(self):
        """
        Test that only succeeding jobs are allowed to pause.

        compile-fail
        +--> after-compile
        """
        A = self.fake_gerrit.addFakeChange('org/project4', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='compile-fail', result='FAILURE', changes='1,1'),
        ])

    def test_job_node_failure_resume(self):
        self.wait_timeout = 120

        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True

        # Second node request should fail
        fail = {'_oid': '199-0000000001'}
        self.fake_nodepool.addFailRequest(fail)

        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for x in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        self.assertEqual([], self.builds)
        self.assertHistory([
            dict(name='just-pause', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_job_reconfigure_resume(self):
        """
        Tests that a paused job is resumed after reconfiguration
        """
        self.wait_timeout = 120

        # Output extra ansible info so we might see errors.
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project6', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1, 'compile in progress')
        self.executor_server.release('compile')
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 2, 'compile and test in progress')

        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        self.executor_server.release('test')
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='compile', result='SUCCESS', changes='1,1'),
            dict(name='test', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_job_pause_skipped_child(self):
        """
        Tests that a paused job is resumed with externally skipped jobs.

        Tests that this situation won't lead to stuck buildsets.
        Compile pauses before pre-test fails.

        1. compile (pauses) --+
                              |
                              +--> test (skipped because of pre-test)
                              |
        2. pre-test (fails) --+
        """
        self.wait_timeout = 120
        self.executor_server.hold_jobs_in_build = True

        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project3', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('compile')
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='pre-test', result='FAILURE', changes='1,1'),
            dict(name='compile', result='SUCCESS', changes='1,1'),
        ])

        self.assertTrue('Skipped due to failed job pre-test' in A.messages[0])

    def test_job_pause_pre_skipped_child(self):
        """
        Tests that a paused job is resumed with pre-existing skipped jobs.

        Tests that this situation won't lead to stuck buildsets.
        The pre-test fails before compile pauses so test is already skipped
        when compile pauses.

        1. pre-test (fails) --+
                              |
                              +--> test (skipped because of pre-test)
                              |
        2. compile (pauses) --+
        """
        self.wait_timeout = 120
        self.executor_server.hold_jobs_in_build = True

        # Output extra ansible info so we might see errors.
        self.executor_server.verbose = True
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project3', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('pre-test')
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for x in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='pre-test', result='FAILURE', changes='1,1'),
            dict(name='compile', result='SUCCESS', changes='1,1'),
        ])

        self.assertTrue('Skipped due to failed job pre-test' in A.messages[0])

    def test_job_pause_skipped_child_retry(self):
        """
        Tests that a paused job is resumed with skipped jobs and retries.

        Tests that this situation won't lead to stuck buildsets.
        1. cache pauses
        2. skip-upload skips upload
        3. test does a retry which resets upload which must get skipped
           again during the reset process because of pre-test skipping it.

        cache (pauses) -+
                        |
                        |
                        +--> test (retries) -----------+
                                                       |
                                                       +--> upload (skipped)
                                                       |
                        +--> prepare-upload (skipped) -+
                        |
        skip-upload ----+
        """
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project5', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('cache')
        self.waitUntilSettled()

        self.executor_server.release('skip-upload')
        self.waitUntilSettled()

        # Stop the job worker of test to simulate an executor restart
        job_test = self.builds[1]
        for job_worker in self.executor_server.job_workers.values():
            if job_worker.build_request.uuid == job_test.uuid:
                job_worker.stop()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # All builds must be finished by now
        self.assertEqual(len(self.builds), 0, 'All builds must be finished')

        # upload must not be run as this should have been skipped
        self.assertHistory([
            dict(name='skip-upload', result='SUCCESS', changes='1,1'),
            dict(name='test', result='ABORTED', changes='1,1'),
            dict(name='test', result='SUCCESS', changes='1,1'),
            dict(name='cache', result='SUCCESS', changes='1,1'),
        ])


class TestJobPausePostFail(AnsibleZuulTestCase):
    tenant_config_file = 'config/job-pause2/main.yaml'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_job_pause_post_fail(self):
        """Tests that a parent job which has a post failure does not
        retroactively set its child job's result to SKIPPED.

        compile
        +--> test

        """
        # Output extra ansible info so we might see errors.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for x in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test', result='SUCCESS', changes='1,1'),
            dict(name='compile', result='POST_FAILURE', changes='1,1'),
        ])


class TestContainerJobs(AnsibleZuulTestCase):
    tenant_config_file = "config/container-build-resources/main.yaml"

    def test_container_jobs(self):
        self.patch(zuul.executor.server.KubeFwd,
                   'kubectl_command',
                   os.path.join(FIXTURE_DIR, 'fake_kubectl.sh'))

        def noop(*args, **kw):
            return 1, 0

        self.patch(zuul.executor.server.AnsibleJob,
                   'runAnsibleFreeze',
                   noop)

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='container-machine', result='SUCCESS', changes='1,1'),
            dict(name='container-native', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestProvidesRequiresPause(AnsibleZuulTestCase):
    tenant_config_file = "config/provides-requires-pause/main.yaml"

    def test_provides_requires_pause(self):
        # Changes share a queue, with both running at the same time.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)

        # Release image-build, it should cause both instances of
        # image-user to run.
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for _ in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)
        build = self.getJobFromHistory('image-user', project='org/project2')
        self.assertEqual(
            build.parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'job': 'image-builder',
                'url': 'http://example.com/image',
                'name': 'image',
            }])


class TestProvidesRequiresBuildset(ZuulTestCase):
    tenant_config_file = "config/provides-requires-buildset/main.yaml"

    def test_provides_requires_buildset(self):
        # Changes share a queue, with both running at the same time.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.returnData(
            'image-builder', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image',
                  'url': 'http://example.com/image',
                  'metadata': {
                      'type': 'container_image'
                  }},
             ]}}
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1'),
        ])

        build = self.getJobFromHistory('image-user', project='org/project1')
        self.assertEqual(
            build.parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'branch': 'master',
                'job': 'image-builder',
                'url': 'http://example.com/image',
                'name': 'image',
                'metadata': {
                    'type': 'container_image',
                }
            }])

    def test_provides_with_tag_requires_buildset(self):
        self.executor_server.hold_jobs_in_build = True
        event = self.fake_gerrit.addFakeTag('org/project1', 'master', 'foo')
        self.executor_server.returnData(
            'image-builder', 'refs/tags/foo',
            {'zuul':
             {'artifacts': [
                 {'name': 'image',
                  'url': 'http://example.com/image',
                  'metadata': {
                      'type': 'container_image'
                  }},
             ]}}
        )
        self.fake_gerrit.addEvent(event)

        self.waitUntilSettled()
        self.assertEqual(len(self.builds), 1)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='image-user', result='SUCCESS', ref='refs/tags/foo'),
        ])

        build = self.getJobFromHistory('image-user', project='org/project1')
        self.assertEqual(
            build.parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'ref': 'refs/tags/foo',
                'tag': 'foo',
                'oldrev': event['refUpdate']['oldRev'],
                'newrev': event['refUpdate']['newRev'],
                'job': 'image-builder',
                'url': 'http://example.com/image',
                'name': 'image',
                'metadata': {
                    'type': 'container_image',
                }
            }])


class TestProvidesRequiresMysql(ZuulTestCase):
    config_file = "zuul-sql-driver-mysql.conf"

    @simple_layout('layouts/provides-requires.yaml')
    def test_provides_requires_shared_queue_fast(self):
        # Changes share a queue, but with only one job, the first
        # merges before the second starts.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.returnData(
            'image-builder', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image',
                  'url': 'http://example.com/image',
                  'metadata': {
                      'type': 'container_image'
                  }},
             ]}}
        )
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1 2,1'),
        ])
        # Data are not passed in this instance because the builder
        # change merges before the user job runs.
        self.assertFalse('artifacts' in self.history[-1].parameters['zuul'])

    @simple_layout('layouts/provides-requires-two-jobs.yaml')
    def test_provides_requires_shared_queue_slow(self):
        # Changes share a queue, with both running at the same time.

        # This also seems to be a defacto waiting_status test since it
        # exercises so many of the statuses.
        self.hold_merge_jobs_in_queue = True

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.returnData(
            'image-builder', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image', 'url': 'http://example.com/image',
                  'metadata': {'type': 'container_image'}},
             ]}}
        )
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        # We should have a merge job for the buildset
        jobs = list(self.merger_api.queued())
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].job_type, 'merge')

        # Release the merge job.
        self.merger_api.release(jobs[0])
        self.waitUntilSettled()

        # We should have a global repo state refstate job for the buildset
        jobs = list(self.merger_api.queued())
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].job_type, 'refstate')

        # Verify the waiting status for both jobs is "repo state"
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        status = tenant.layout.pipeline_managers["gate"].formatStatusJSON()
        jobs = status["change_queues"][0]["heads"][0][0]["jobs"]
        self.assertEqual(jobs[0]["waiting_status"], 'repo state')
        self.assertEqual(jobs[1]["waiting_status"], 'repo state')

        # Return the merge queue to normal behavior, but pause nodepool
        self.fake_nodepool.pause()
        self.hold_merge_jobs_in_queue = False
        self.merger_api.release()
        self.waitUntilSettled()

        # Verify the nodepool waiting status
        status = tenant.layout.pipeline_managers["gate"].formatStatusJSON()
        jobs = status["change_queues"][0]["heads"][0][0]["jobs"]
        self.assertEqual(jobs[0]["waiting_status"],
                         'node request: 100-0000000000')
        self.assertEqual(jobs[1]["waiting_status"],
                         'dependencies: image-builder')

        # Return nodepool operation to normal, but hold executor jobs
        # in queue
        self.hold_jobs_in_queue = True
        self.fake_nodepool.unpause()
        self.waitUntilSettled()

        # Verify the executor waiting status
        status = tenant.layout.pipeline_managers["gate"].formatStatusJSON()
        jobs = status["change_queues"][0]["heads"][0][0]["jobs"]
        self.assertEqual(jobs[0]["waiting_status"], 'executor')
        self.assertEqual(jobs[1]["waiting_status"],
                         'dependencies: image-builder')

        # Return the executor queue to normal, but hold jobs in build
        self.hold_jobs_in_queue = False
        self.executor_server.hold_jobs_in_build = True
        self.executor_api.release()
        self.waitUntilSettled()

        status = tenant.layout.pipeline_managers["gate"].formatStatusJSON()
        jobs = status["change_queues"][0]["heads"][0][0]["jobs"]
        self.assertIsNone(jobs[0]["waiting_status"])
        self.assertEqual(jobs[1]["waiting_status"],
                         'dependencies: image-builder')

        self.assertEqual(len(self.builds), 1)

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)

        status = tenant.layout.pipeline_managers["gate"].formatStatusJSON()

        # First change
        jobs = status["change_queues"][0]["heads"][0][0]["jobs"]
        self.assertIsNone(jobs[0]["waiting_status"])
        self.assertEqual(jobs[1]["waiting_status"],
                         'dependencies: image-builder')

        # Second change
        jobs = status["change_queues"][0]["heads"][0][1]["jobs"]
        self.assertEqual(jobs[0]["waiting_status"],
                         'requirements: images')

        # Release image-build, it should cause both instances of
        # image-user to run.
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(len(self.builds), 2)
        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
        ])

        self.orderedRelease()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1 2,1'),
        ])
        self.assertEqual(
            self.history[-1].parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'job': 'image-builder',
                'url': 'http://example.com/image',
                'name': 'image',
                'metadata': {
                    'type': 'container_image',
                }
            }])

        # Catch time / monotonic errors
        val = self.assertReportedStat('zuul.tenant.tenant-one.pipeline.'
                                      'gate.repo_state_time',
                                      kind='ms')
        self.assertTrue(0.0 < float(val) < 60000.0)

    @simple_layout('layouts/provides-requires-unshared.yaml')
    def test_provides_requires_unshared_queue(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.returnData(
            'image-builder', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image', 'url': 'http://example.com/image',
                  'metadata': {'type': 'container_image'}},
             ]}}
        )
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
        ])

        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='image-user', result='SUCCESS', changes='2,1'),
        ])
        # Data are not passed in this instance because the builder
        # change merges before the user job runs.
        self.assertFalse('artifacts' in self.history[-1].parameters['zuul'])

    @simple_layout('layouts/provides-requires.yaml')
    def test_provides_requires_check_current(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.returnData(
            'image-builder', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image', 'url': 'http://example.com/image',
                  'metadata': {'type': 'container_image'}},
             ]}}
        )
        self.executor_server.returnData(
            'library-builder', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'library', 'url': 'http://example.com/library',
                  'metadata': {'type': 'library_object'}},
             ]}}
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 3)

        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.executor_server.returnData(
            'image-builder', B,
            {'zuul':
             {'artifacts': [
                 {'name': 'image2', 'url': 'http://example.com/image2',
                  'metadata': {'type': 'container_image'}},
             ]}}
        )
        self.executor_server.returnData(
            'library-builder', B,
            {'zuul':
             {'artifacts': [
                 {'name': 'library2', 'url': 'http://example.com/library2',
                  'metadata': {'type': 'library_object'}},
             ]}}
        )
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 6)

        C = self.fake_gerrit.addFakeChange('org/project2', 'master', 'C')
        C.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            C.subject, B.data['id'])
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 7)

        self.executor_server.release('image-*')
        self.executor_server.release('library-*')
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
            dict(name='image-builder', result='SUCCESS', changes='1,1 2,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1 2,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1 2,1 3,1'),
            dict(name='library-user', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
            dict(name='library-user2', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1 3,1'),
        ], ordered=False)
        image_user = self.getJobFromHistory('image-user')
        self.assertEqual(
            image_user.parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'job': 'image-builder',
                'url': 'http://example.com/image',
                'name': 'image',
                'metadata': {
                    'type': 'container_image',
                }
            }, {
                'project': 'org/project1',
                'change': '2',
                'patchset': '1',
                'job': 'image-builder',
                'url': 'http://example.com/image2',
                'name': 'image2',
                'metadata': {
                    'type': 'container_image',
                }
            }])
        library_user = self.getJobFromHistory('library-user')
        self.assertEqual(
            library_user.parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'job': 'library-builder',
                'url': 'http://example.com/library',
                'name': 'library',
                'metadata': {
                    'type': 'library_object',
                }
            }, {
                'project': 'org/project1',
                'change': '2',
                'patchset': '1',
                'job': 'library-builder',
                'url': 'http://example.com/library2',
                'name': 'library2',
                'metadata': {
                    'type': 'library_object',
                }
            }])

    @simple_layout('layouts/provides-requires.yaml')
    def test_provides_requires_check_old_success(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.returnData(
            'image-builder', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image', 'url': 'http://example.com/image',
                  'metadata': {'type': 'container_image'}},
             ]}}
        )
        self.executor_server.returnData(
            'library-builder', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'library', 'url': 'http://example.com/library',
                  'metadata': {'type': 'library_object'}},
             ]}}
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.executor_server.returnData(
            'image-builder', B,
            {'zuul':
             {'artifacts': [
                 {'name': 'image2', 'url': 'http://example.com/image2',
                  'metadata': {'type': 'container_image'}},
             ]}}
        )
        self.executor_server.returnData(
            'library-builder', B,
            {'zuul':
             {'artifacts': [
                 {'name': 'library2', 'url': 'http://example.com/library2',
                  'metadata': {'type': 'library_object'}},
             ]}}
        )
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
            dict(name='image-builder', result='SUCCESS', changes='1,1 2,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1 2,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)

        C = self.fake_gerrit.addFakeChange('org/project2', 'master', 'C')
        C.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            C.subject, B.data['id'])
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
            dict(name='image-builder', result='SUCCESS', changes='1,1 2,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1 2,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1 2,1 3,1'),
            dict(name='library-user', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
            dict(name='library-user2', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1 3,1'),
        ], ordered=False)

        D = self.fake_gerrit.addFakeChange('org/project3', 'master', 'D')
        D.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            D.subject, B.data['id'])
        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='SUCCESS', changes='1,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
            dict(name='image-builder', result='SUCCESS', changes='1,1 2,1'),
            dict(name='library-builder', result='SUCCESS', changes='1,1 2,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1'),
            dict(name='image-user', result='SUCCESS', changes='1,1 2,1 3,1'),
            dict(name='library-user', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
            dict(name='library-user2', result='SUCCESS',
                 changes='1,1 2,1 3,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1 3,1'),
            dict(name='both-user', result='SUCCESS', changes='1,1 2,1 4,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1 4,1'),
        ], ordered=False)

        image_user = self.getJobFromHistory('image-user')
        self.assertEqual(
            image_user.parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'job': 'image-builder',
                'url': 'http://example.com/image',
                'name': 'image',
                'metadata': {
                    'type': 'container_image',
                }
            }, {
                'project': 'org/project1',
                'change': '2',
                'patchset': '1',
                'job': 'image-builder',
                'url': 'http://example.com/image2',
                'name': 'image2',
                'metadata': {
                    'type': 'container_image',
                }
            }])
        library_user = self.getJobFromHistory('library-user')
        self.assertEqual(
            library_user.parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'job': 'library-builder',
                'url': 'http://example.com/library',
                'name': 'library',
                'metadata': {
                    'type': 'library_object',
                }
            }, {
                'project': 'org/project1',
                'change': '2',
                'patchset': '1',
                'job': 'library-builder',
                'url': 'http://example.com/library2',
                'name': 'library2',
                'metadata': {
                    'type': 'library_object',
                }
            }])
        both_user = self.getJobFromHistory('both-user')
        self.assertEqual(
            both_user.parameters['zuul']['artifacts'],
            [{
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'job': 'image-builder',
                'url': 'http://example.com/image',
                'name': 'image',
                'metadata': {
                    'type': 'container_image',
                }
            }, {
                'project': 'org/project1',
                'change': '1',
                'patchset': '1',
                'job': 'library-builder',
                'url': 'http://example.com/library',
                'name': 'library',
                'metadata': {
                    'type': 'library_object',
                }
            }, {
                'project': 'org/project1',
                'change': '2',
                'patchset': '1',
                'job': 'image-builder',
                'url': 'http://example.com/image2',
                'name': 'image2',
                'metadata': {
                    'type': 'container_image',
                }
            }, {
                'project': 'org/project1',
                'change': '2',
                'patchset': '1',
                'job': 'library-builder',
                'url': 'http://example.com/library2',
                'name': 'library2',
                'metadata': {
                    'type': 'library_object',
                }
            }])

    @simple_layout('layouts/provides-requires.yaml')
    def test_provides_requires_check_old_failure(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.failJob('image-builder', A)
        self.executor_server.failJob('library-builder', A)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='FAILURE', changes='1,1'),
            dict(name='library-builder', result='FAILURE', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='FAILURE', changes='1,1'),
            dict(name='library-builder', result='FAILURE', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)
        self.assertTrue(re.search('image-user .* FAILURE', B.messages[0]))
        self.assertEqual(
            B.messages[0].count(
                'Job image-user requires artifact(s) images'),
            1,
            B.messages[0])
        self.assertEqual(
            B.messages[0].count(
                'Job library-user requires artifact(s) libraries'),
            1,
            B.messages[0])

    @simple_layout('layouts/provides-requires-single-project.yaml')
    def test_provides_requires_check_old_failure_single_project(self):
        # Similar to above test, but has job dependencies which will
        # cause the requirements check to potentially run multiple
        # times as the queue processor runs repeatedly.
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.failJob('image-builder', A)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='FAILURE', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertTrue(
            'Skipped due to failed job image-builder' in A.messages[0])

        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='image-builder', result='FAILURE', changes='1,1'),
            dict(name='hold', result='SUCCESS', changes='1,1'),
            dict(name='image-builder', result='FAILURE', changes='1,1 2,1'),
            dict(name='hold', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)
        self.assertTrue(re.search('image-user .* FAILURE', B.messages[0]))
        self.assertEqual(
            B.messages[0].count(
                'Job image-user requires artifact(s) images'),
            1, B.messages[0])


class TestProvidesRequiresPostgres(TestProvidesRequiresMysql):
    config_file = "zuul-sql-driver-postgres.conf"


class TestForceMergeMissingTemplate(ZuulTestCase):
    tenant_config_file = "config/force-merge-template/main.yaml"

    def test_force_merge_missing_template(self):
        """
        Tests that force merging a change using a non-existent project
        template triggering a post job doesn't wedge zuul on reporting.
        """

        # Create change that adds uses a non-existent project template
        conf = textwrap.dedent(
            """
            - project:
                templates:
                  - non-existent
                check:
                  jobs:
                    - noop
                post:
                  jobs:
                    - post-job
            """)

        file_dict = {'zuul.yaml': conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)

        # Now force merge the change
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(A.getRefUpdatedEvent())
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(B.reported, 1)
        self.assertHistory([
            dict(name='other-job', result='SUCCESS', changes='2,1'),
        ])


class TestJobPausePriority(AnsibleZuulTestCase):
    tenant_config_file = 'config/job-pause-priority/main.yaml'

    def test_paused_job_priority(self):
        "Test that nodes for children of paused jobs have a higher priority"

        self.fake_nodepool.pause()
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        reqs = self.fake_nodepool.getNodeRequests()
        self.assertEqual(len(reqs), 1)
        self.assertEqual(reqs[0]['_oid'], '100-0000000000')
        self.assertEqual(reqs[0]['provider'], None)

        self.fake_nodepool.unpause()
        self.waitUntilSettled()
        self.fake_nodepool.pause()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()

        for x in iterate_timeout(60, 'paused job'):
            reqs = self.fake_nodepool.getNodeRequests()
            if reqs:
                break

        self.assertEqual(len(reqs), 1)
        self.assertEqual(reqs[0]['_oid'], '099-0000000001')
        self.assertEqual(reqs[0]['provider'], 'test-provider')

        self.fake_nodepool.unpause()
        self.waitUntilSettled()


class TestAnsibleVersion(AnsibleZuulTestCase):
    tenant_config_file = 'config/ansible-versions/main.yaml'

    def test_ansible_versions(self):
        """
        Tests that jobs run with the requested ansible version.
        """
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='ansible-default', result='SUCCESS', changes='1,1'),
            dict(name='ansible-9', result='SUCCESS', changes='1,1'),
            dict(name='ansible-11', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestDefaultAnsibleVersion(AnsibleZuulTestCase):
    config_file = 'zuul-default-ansible-version.conf'
    tenant_config_file = 'config/ansible-versions/main.yaml'

    def test_ansible_versions(self):
        """
        Tests that jobs run with the requested ansible version.
        """
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='ansible-default-zuul-conf', result='SUCCESS',
                 changes='1,1'),
            dict(name='ansible-9', result='SUCCESS', changes='1,1'),
            dict(name='ansible-11', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestReturnWarnings(AnsibleZuulTestCase):
    tenant_config_file = 'config/return-warnings/main.yaml'

    def test_return_warnings(self):
        """
        Tests that jobs can emit custom warnings that get reported.
        """

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='emit-warnings', result='SUCCESS', changes='1,1'),
        ])

        self.assertTrue(A.reported)
        self.assertIn('This is the first warning', A.messages[0])
        self.assertIn('This is the second warning', A.messages[0])


class TestUnsafeVars(AnsibleZuulTestCase):
    tenant_config_file = 'config/unsafe-vars/main.yaml'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_mark_unsafe(self):
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master',
                                           'unsafecommitmessage')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        build = self.getJobFromHistory('testjob')
        path = os.path.join(build.jobdir.root,
                            'ansible', 'zuul_vars.yaml')

        with open(path, 'r') as f:
            inventory = yamlutil.ansible_unsafe_load(f)

        # The repr for an AnsibleUnsafeStr object does not include the
        # value, so we use that here to assert that the commit message
        # does not appear without an unsafe marking.
        self.assertFalse('unsafecommitmessage' in repr(inventory))

    def test_unsafe_vars(self):
        self.executor_server.keep_jobdir = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        testjob = self.getJobFromHistory('testjob')
        job_output = self._get_file(testjob, 'work/logs/job-output.txt')
        self.log.debug(job_output)
        # base_secret wasn't present when frozen
        self.assertIn("BASE JOBSECRET: undefined", job_output)
        # secret variables are marked unsafe
        self.assertIn("BASE SECRETSUB: {{ subtext }}", job_output)
        # latefact wasn't present when frozen
        self.assertIn("BASE LATESUB: undefined", job_output)
        # check the !unsafe tagged version
        self.assertIn("BASE LATESUB UNSAFE: "
                      "{{ latefact | default('undefined') }}", job_output)

        # Both of these are dynamically evaluated
        self.assertIn("TESTJOB SUB: text", job_output)
        self.assertIn("TESTJOB LATESUB: late", job_output)

        # check the !unsafe tagged version
        self.assertIn("TESTJOB LATESUB UNSAFE: "
                      "{{ latefact | default('undefined') }}", job_output)

        # The project secret is not defined
        self.assertNotIn("TESTJOB SECRET:", job_output)

        testjob = self.getJobFromHistory('testjob-secret')
        job_output = self._get_file(testjob, 'work/logs/job-output.txt')
        self.log.debug(job_output)
        # base_secret wasn't present when frozen
        self.assertIn("BASE JOBSECRET: undefined", job_output)
        # secret variables are marked unsafe
        self.assertIn("BASE SECRETSUB: {{ subtext }}", job_output)
        # latefact wasn't present when frozen
        self.assertIn("BASE LATESUB: undefined", job_output)
        # check the !unsafe tagged version
        self.assertIn("BASE LATESUB UNSAFE: "
                      "{{ latefact | default('undefined') }}", job_output)

        # These are frozen
        self.assertIn("TESTJOB SUB: text", job_output)
        self.assertIn("TESTJOB LATESUB: undefined", job_output)

        # check the !unsafe tagged version
        self.assertIn("TESTJOB LATESUB UNSAFE: "
                      "{{ latefact | default('undefined') }}", job_output)

        # This is marked unsafe
        self.assertIn("TESTJOB SECRET: {{ subtext }}", job_output)


class TestConnectionVars(AnsibleZuulTestCase):
    tenant_config_file = 'config/connection-vars/main.yaml'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_ansible_connection(self):
        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - test-job:
                        vars:
                          ansible_shell_executable: /bin/du
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertIn(
            "Defining a variable named 'ansible_shell_executable'"
            " is not allowed",
            A.messages[0])
        self.assertHistory([])

    def test_return_data(self):
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # Currently, second-job errors; if it ever runs, add these assertions:
        # job = self.getJobFromHistory('second-job')
        # job_output = self._get_file(job, 'work/logs/job-output.txt')
        # self.log.debug(job_output)
        # self.assertNotIn("/bin/du", job_output)


class TestSymlinkEscape(AnsibleZuulTestCase):
    tenant_config_file = 'config/symlink-escape/main.yaml'

    @okay_tracebacks('ValueError: Symlink detected')
    def test_symlink_escape(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([])
        self.assertEqual(A.reported, 1)
        self.assertIn("ERROR", A.messages[0])
        self.assertIn("Symlink detected", A.messages[0])


class IncludeBranchesTestCase(ZuulTestCase):
    def _test_include_branches(self, history1, history2, history3, history4):
        self.create_branch('org/project', 'stable')
        self.create_branch('org/project', 'feature/foo')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'feature/foo'))
        self.waitUntilSettled()

        # Test the jobs on the master branch.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory(history1, ordered=False)

        # Test the jobs on the excluded feature branch.
        B = self.fake_gerrit.addFakeChange('org/project', 'feature/foo', 'A')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory(history1 + history2, ordered=False)

        # Test in-repo config proposed on the excluded feature branch.
        conf = textwrap.dedent(
            """
            - job:
                name: project-dynamic

            - project:
                check:
                  jobs:
                    - project-dynamic
            """)
        file_dict = {'zuul.yaml': conf}
        C = self.fake_gerrit.addFakeChange('org/project', 'feature/foo', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory(history1 + history2 + history3, ordered=False)

        old = self.scheds.first.sched.tenant_layout_state.get('tenant-one')
        # Merge a change to the excluded feature branch.
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertHistory(history1 + history2 + history3 + history4,
                           ordered=False)
        new = self.scheds.first.sched.tenant_layout_state.get('tenant-one')
        # Verify we haven't performed a tenant reconfiguration
        self.assertTrue(old == new)


class TestIncludeBranchesProject(IncludeBranchesTestCase):
    tenant_config_file = 'config/dynamic-only-project/include.yaml'

    def test_include_branches(self):
        history1 = [
            dict(name='central-test', result='SUCCESS', changes='1,1'),
            dict(name='project-test', result='SUCCESS', changes='1,1'),
        ]
        history2 = [
            dict(name='central-test', result='SUCCESS', changes='2,1'),
        ]
        history3 = [
            dict(name='central-test', result='SUCCESS', changes='3,1'),
        ]
        history4 = [
            dict(name='central-test', result='SUCCESS', changes='2,1'),
        ]
        self._test_include_branches(history1, history2, history3, history4)


class TestExcludeBranchesProject(IncludeBranchesTestCase):
    tenant_config_file = 'config/dynamic-only-project/exclude.yaml'

    def test_exclude_branches(self):
        history1 = [
            dict(name='central-test', result='SUCCESS', changes='1,1'),
            dict(name='project-test', result='SUCCESS', changes='1,1'),
        ]
        history2 = [
            dict(name='central-test', result='SUCCESS', changes='2,1'),
        ]
        history3 = [
            dict(name='central-test', result='SUCCESS', changes='3,1'),
        ]
        history4 = [
            dict(name='central-test', result='SUCCESS', changes='2,1'),
        ]
        self._test_include_branches(history1, history2, history3, history4)


class TestDynamicBranchesProject(IncludeBranchesTestCase):
    tenant_config_file = 'config/dynamic-only-project/dynamic.yaml'

    def test_dynamic_branches(self):
        history1 = [
            dict(name='central-test', result='SUCCESS', changes='1,1'),
            dict(name='project-test', result='SUCCESS', changes='1,1'),
        ]
        history2 = [
            dict(name='central-test', result='SUCCESS', changes='2,1'),
            dict(name='project-test', result='SUCCESS', changes='2,1'),
        ]
        history3 = [
            dict(name='central-test', result='SUCCESS', changes='3,1'),
            dict(name='project-dynamic', result='SUCCESS', changes='3,1'),
        ]
        history4 = [
            dict(name='central-test', result='SUCCESS', changes='2,1'),
            dict(name='project-test', result='SUCCESS', changes='2,1'),
        ]
        self._test_include_branches(history1, history2, history3, history4)

    def test_new_dynamic_branch(self):
        # Create an included branch so that we ensure we're using the
        # "more than one branch" code path (ie, not "master" only).
        # We otherwise don't use this.
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.waitUntilSettled()

        # Create a new dynamic-only branch.  Since it's dynamic only,
        # it won't trigger a reconfiguration.
        self.create_branch('org/project', 'feature/foo')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'feature/foo'))
        self.waitUntilSettled()

        # Test that we load configuration from the dynamic branch even
        # though the tenant layout doesn't know about it yet (since
        # there hasn't been a reconfiguration since it was created).
        conf = textwrap.dedent(
            """
            - job:
                name: project-dynamic

            - project:
                check:
                  jobs:
                    - project-dynamic
            """)
        file_dict = {'zuul.yaml': conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'feature/foo', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='central-test', result='SUCCESS', changes='1,1'),
            dict(name='project-dynamic', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_post_jobs(self):
        self.create_branch('org/project', 'feature/bar')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'feature/bar'))
        self.waitUntilSettled()
        A = self.fake_gerrit.addFakeChange('org/project', 'feature/bar', 'A')
        A.setMerged()
        self.fake_gerrit.addEvent(A.getRefUpdatedEvent())
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='central-post', result='SUCCESS',
                 ref='refs/heads/feature/bar'),
        ], ordered=False)


class TestDynamicBranchesMultiProject(ZuulTestCase):
    tenant_config_file = 'config/dynamic-only-project/dynamic2.yaml'

    def test_dynamic_branches_multi_project(self):
        # Test that configuration errors on a branch which is static
        # in one tenant do not cause issue in another tenant where the
        # branch is dynamic.

        # Create branches which are dynamic in tenant-one, and static
        # in tenant-two.
        self.create_branch('org/project', 'feature/foo')
        self.create_branch('org/project2', 'feature/bar')

        # The reconfiguration should cause config errors in project2
        # to be stored with the objects since they are loaded in
        # tenant-two.
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project', 'feature/foo', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-test', result='SUCCESS', changes='1,1'),
            dict(name='central-test', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestMaxDeps(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main-max-deps.yaml'

    def test_max_deps(self):
        # max_dependencies for the connection is 1, so this is okay
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # So is this
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-merge', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)

        # This is not
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.setDependsOn(B, 1)
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-merge', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)

    def test_max_deps_extended(self):
        self.executor_server.hold_jobs_in_build = True
        # max_dependencies for the connection is 1, so this is okay
        A = self.fake_gerrit.addFakeChange('org/project4', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project4', 'master', 'B',
                                           topic='test-topic')
        B.setDependsOn(A, 1)

        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Increase the number of dependencies for B by adding a
        # change with the same topic (dependencies-by-topic is enabled).
        # With this C should not be enqueued and A is removed.
        C = self.fake_gerrit.addFakeChange('org/project4', 'master', 'C',
                                           topic='test-topic')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))

        self.executor_server.hold_jobs_in_build = True
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)


class TestPipelineLimits(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main-max-changes.yaml'

    def test_pipeline_max_changes_check(self):
        # Max-changes is 1, so this is allowed
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # And this is not
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(A.reported, 1)
        self.assertEqual(B.reported, 1)
        self.assertIn('Unable to enqueue change: 2 changes to enqueue '
                      'greater than pipeline max of 1', B.messages[0])

    def test_pipeline_max_changes_current_check(self):
        # Max-changes is 1, so this is allowed
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # And this is not
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(A.reported, 1)
        self.assertEqual(B.reported, 1)
        self.assertIn('Unable to enqueue change: 1 additional changes would '
                      'exceed pipeline max of 1 under current conditions',
                      B.messages[0])

    def test_pipeline_max_changes_gate(self):
        # Max-changes is 1, so this is allowed
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # And this is not
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.setDependsOn(B, 1)
        B.addApproval('Code-Review', 2)
        C.addApproval('Code-Review', 2)
        B.addApproval('Approved', 1)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))

        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 0)
        self.assertEqual(C.reported, 1)
        self.assertIn('Unable to enqueue change: 2 changes to enqueue '
                      'greater than pipeline max of 1', C.messages[0])


class TestBlobStorePipelineProcessing(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_blob_store_pipeline_processing(self):
        # This verifies that the blob store cleanup does not remove
        # entries that belong to a queue item with no job graph.
        self.hold_merge_jobs_in_queue = True

        sched = self.scheds.first.sched
        old_handler = zuul.scheduler.Scheduler._doMergeCompletedEvent
        old_abort = zuul.scheduler.Scheduler.abortIfPendingReconfig
        should_latch = threading.Event()
        should_latch.set()
        latch = threading.Event()

        def abort_on_reconfig(*args, **kw):
            if latch.is_set():
                raise PendingReconfiguration()
            return old_abort(*args, **kw)

        def merge_complete(*args, **kw):
            ret = old_handler(*args, **kw)
            if should_latch.is_set():
                latch.set()
            return ret

        self.patch(zuul.scheduler.Scheduler,
                   '_doMergeCompletedEvent',
                   merge_complete)
        self.patch(zuul.scheduler.Scheduler,
                   'abortIfPendingReconfig',
                   abort_on_reconfig)

        # Add an item to the check pipeline
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        # The merge job is waiting now.  Make sure there's nothing in
        # the blob store.
        with sched.createZKContext(None, sched.log) as ctx:
            blobstore = BlobStore(ctx)
            self.assertEqual(0, len(blobstore))

        # Release the merge job
        self.hold_merge_jobs_in_queue = False
        self.merger_api.release()
        self.waitUntilSettled()
        # The merge job should have been processed and the latch set;
        # confirm that
        latch.wait()
        with sched.run_handler_lock:
            with sched.general_cleanup_lock:
                # The scheduler should be idle; run the blob store
                # cleanup and verify that it has an entry in it, even
                # though there's no job graph yet.
                sched._runBlobStoreCleanup()
                with sched.createZKContext(None, sched.log) as ctx:
                    blobstore = BlobStore(ctx)
                    self.assertEqual(1, len(blobstore))

        should_latch.clear()
        latch.clear()

        # We can't wait until settled because we interrupted in the
        # middle of a run.
        for _ in iterate_timeout(60, 'job to finish'):
            if len(self.history):
                break

        self.waitUntilSettled()


class TestConfigProjectBranchMatcher(ZuulTestCase):
    tenant_config_file = 'config/config-project-branch-matcher/main.yaml'

    def test_config_project_branch_matcher(self):
        for project in ['org/project1', 'org/project2',
                        'org/reproject3', 'org/reproject4']:
            for branch in ['stable/implied', 'stable/explicit']:
                self.create_branch(project, branch)
                self.fake_gerrit.addEvent(
                    self.fake_gerrit.getFakeBranchCreatedEvent(
                        project, branch))
        self.waitUntilSettled("initial reconfig")

        # Test project1: implied only
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/project1', 'stable/implied', 'A'
        ).getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/project1', 'stable/explicit', 'B'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Test project2: explicit only
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/project2', 'stable/implied', 'C'
        ).getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/project2', 'stable/explicit', 'D'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Test reproject3: implied only
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/reproject3', 'stable/implied', 'E'
        ).getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/reproject3', 'stable/explicit', 'F'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Test reproject4: explicit only
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/reproject4', 'stable/implied', 'G'
        ).getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/reproject4', 'stable/explicit', 'H'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='testjob', result='SUCCESS', changes='1,1'),  # A
            dict(name='testjob', result='SUCCESS', changes='4,1'),  # D
            dict(name='testjob', result='SUCCESS', changes='5,1'),  # E
            dict(name='testjob', result='SUCCESS', changes='8,1'),  # H
        ], ordered=False)


class TestUntrustedProjectBranchMatcher(ZuulTestCase):
    tenant_config_file = 'config/untrusted-project-branch-matcher/main.yaml'

    def test_untrusted_project_branch_matcher(self):
        for project in ['org/project1', 'org/project2',
                        'org/reproject3', 'org/reproject4', 'superproject']:
            for branch in ['stable/implied', 'stable/explicit']:
                self.create_branch(project, branch)
                self.fake_gerrit.addEvent(
                    self.fake_gerrit.getFakeBranchCreatedEvent(
                        project, branch))
        self.waitUntilSettled("initial reconfig")

        # Test project1: implied only
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/project1', 'stable/implied', 'A'
        ).getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/project1', 'stable/explicit', 'B'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Test project2: explicit only
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/project2', 'stable/implied', 'C'
        ).getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/project2', 'stable/explicit', 'D'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Test reproject3: implied only
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/reproject3', 'stable/implied', 'E'
        ).getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/reproject3', 'stable/explicit', 'F'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Test reproject4: explicit only
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/reproject4', 'stable/implied', 'G'
        ).getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'org/reproject4', 'stable/explicit', 'H'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Test superproject: master (the implied branch matcher is
        # forced to the same branch for the same project).
        self.fake_gerrit.addEvent(self.fake_gerrit.addFakeChange(
            'superproject', 'master', 'I'
        ).getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='testjob', result='SUCCESS', changes='1,1'),  # A
            dict(name='testjob', result='SUCCESS', changes='4,1'),  # D
            dict(name='testjob', result='SUCCESS', changes='5,1'),  # E
            dict(name='testjob', result='SUCCESS', changes='8,1'),  # H
            dict(name='testjob', result='SUCCESS', changes='9,1'),  # I
        ], ordered=False)


class TestSuperproject(ZuulTestCase):
    tenant_config_file = 'config/superproject/main.yaml'

    def test_project_configs(self):
        A = self.fake_gerrit.addFakeChange('superproject', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        B = self.fake_gerrit.addFakeChange('submodule1', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        C = self.fake_gerrit.addFakeChange('othermodule', 'master', 'C')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        D = self.fake_gerrit.addFakeChange('submodules/foo', 'master', 'D')
        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='integration-job', result='SUCCESS', changes='1,1'),
            dict(name='superproject-job', result='SUCCESS', changes='1,1'),
            dict(name='integration-job', result='SUCCESS', changes='2,1'),
            dict(name='integration-job', result='SUCCESS', changes='3,1'),
            dict(name='integration-job', result='SUCCESS', changes='4,1'),
        ], ordered=False)

    def test_configure_unrelated_project(self):
        # Ensure we can't configure an unpermitted project
        in_repo_conf = textwrap.dedent(
            """
            - project:
                name: unrelated-project
                check:
                  jobs:
                    - integration-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('superproject', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertIn('the only project definition permitted is',
                      A.messages[0])
        self.assertHistory([])

    def test_configure_reporter_job(self):
        # Ensure we can configure a reporter job
        in_repo_conf = textwrap.dedent(
            """
            - job: {name: 'reporter-job', type: 'reporter'}
            - project:
                name: submodule1
                check:
                  jobs:
                    - reporter-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('superproject', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 0)


class TestIncludeVars(ZuulTestCase):
    tenant_config_file = 'config/include-vars/main.yaml'

    def getBuildInventory(self, name):
        build = self.getBuildByName(name)
        inv_path = os.path.join(build.jobdir.root, 'ansible', 'inventory.yaml')
        with open(inv_path, 'r') as f:
            inventory = yaml.safe_load(f)
        return inventory

    def test_include_vars(self):
        # Test including from the same project and another project
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.executor_server.returnData(
            'parent-job', A,
            {
                'parent_var_precedence': 'parent-vars',
                'parent_vars': True,
            }
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled("waiting for parent job to be running")

        self.executor_server.release('parent-job')
        self.waitUntilSettled("waiting for remaining jobs to start")

        inventory = self.getBuildInventory('same-project')
        ivars = inventory['all']['vars']
        # We read a variable from the expected file
        self.assertTrue(ivars['project1'])
        # Job and project vars have higher precedence than file
        self.assertEqual('job-vars', ivars['job_var_precedence'])
        self.assertEqual('project-vars', ivars['project_var_precedence'])
        # Files have higher precedence than returned parent data
        self.assertEqual('include-vars', ivars['parent_var_precedence'])
        # Make sure we did get returned parent data
        self.assertTrue(ivars['parent_vars'])

        inventory = self.getBuildInventory('other-project')
        ivars = inventory['all']['vars']
        self.assertTrue(ivars['project2'])
        self.assertEqual('original', ivars['project2_var'])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='parent-job', result='SUCCESS', changes='1,1'),
            dict(name='same-project', result='SUCCESS', changes='1,1'),
            dict(name='other-project', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_include_vars_override_checkout(self):
        # Regression test to verify that we are using the correct
        # checkout when a job needs a project as a required-project
        # and for include-vars.
        self.create_branch('org/project1', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable'))

        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project3', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)
        build = self.builds[0]
        projects = build.parameters['projects']
        # We expect org/project1 and org/project3
        self.assertEqual(len(projects), 2)

        workspace_repos = build.getWorkspaceRepos(
            [p['canonical_name'] for p in projects])
        for project in projects:
            expected_branch = 'master'
            if project['name'] == 'org/project1':
                expected_branch = 'stable'
                self.assertEqual(project['override_checkout'], expected_branch)
            repo = workspace_repos[project['canonical_name']]
            self.assertEqual(repo.active_branch.name, expected_branch)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='required-project-override', result='SUCCESS',
                 changes='1,1'),
        ], ordered=False)

    def test_include_vars_zuul_project(self):
        # Test zuul-project vars (required and not)
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Required and present
        inventory = self.getBuildInventory('zuul-project')
        ivars = inventory['all']['vars']
        self.assertTrue(ivars['zuulproject'])

        # Optional and not present
        inventory = self.getBuildInventory('zuul-project-missing-ok')
        ivars = inventory['all']['vars']
        self.assertFalse('zuulproject' in ivars)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='zuul-project', result='SUCCESS', changes='1,1'),
            dict(name='zuul-project-missing-ok',
                 result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # Required and not present
        self.assertTrue(re.search(
            '- zuul-project-required .* ERROR Required vars file '
            'missing-vars.yaml not found',
            A.messages[-1]))

    def test_include_vars_config_update(self):
        # Regression test to make sure we can deal with None values
        # for include-vars from the current project when checking if
        # the job updates config.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: dynamic-job
                include-vars:
                  - name: foo.yaml
                    required: true
                    zuul-project: true
                # Empty file matcher, so we are checking
                # if the job updates config
                files: []

            - project:
                check:
                  jobs:
                    - dynamic-job
            """)

        vars_file = textwrap.dedent(
            """
            foo: bar
            """)

        file_dict = {
            'zuul.yaml': in_repo_conf,
            'foo.yaml': vars_file,
        }
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='dynamic-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_include_vars_config_error(self):
        # Test the mutual exclusion of project and zuul-project
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: badjob
                include-vars:
                  - name: foo.yaml
                    project: org/project
                    zuul-project: true
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1,
                         "A should report failure")
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('extra keys not allowed', A.messages[0])

    def test_include_vars_tag(self):
        # Test including vars in a tag job
        self.executor_server.hold_jobs_in_build = True

        # Create a tag with the original values
        event = self.fake_gerrit.addFakeTag('org/project1', 'master',
                                            'foo', 'test message')
        # Create a tag on the "other" project too
        self.fake_gerrit.addFakeTag('org/project2', 'master',
                                    'foo', 'test message')

        # Update master with new values
        in_repo_vars = textwrap.dedent(
            """
            project1: true
            job_var_precedence: include-vars-master
            project_var_precedence: include-vars-master
            parent_var_precedence: include-vars-master
            """)
        file_dict = {'project1-vars.yaml': in_repo_vars}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        in_repo_vars = textwrap.dedent(
            """
            project2: true
            project2_var: master
            """)
        file_dict = {'project2-vars.yaml': in_repo_vars}
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B',
                                           files=file_dict)
        B.setMerged()
        self.fake_gerrit.addEvent(B.getChangeMergedEvent())
        self.waitUntilSettled()

        # Send the tag
        self.executor_server.returnData(
            'parent-job', 'refs/tags/foo',
            {
                'parent_var_precedence': 'parent-vars',
                'parent_vars': True,
            }
        )
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled("waiting for parent job to be running")

        self.executor_server.release('parent-job')
        self.waitUntilSettled("waiting for remaining jobs to start")

        inventory = self.getBuildInventory('same-project')
        ivars = inventory['all']['vars']
        # We read a variable from the expected file
        self.assertTrue(ivars['project1'])
        # Job and project vars have higher precedence than file
        self.assertEqual('job-vars', ivars['job_var_precedence'])
        self.assertEqual('project-vars', ivars['project_var_precedence'])
        # Files have higher precedence than returned parent data
        self.assertEqual('include-vars', ivars['parent_var_precedence'])
        # Make sure we did get returned parent data
        self.assertTrue(ivars['parent_vars'])

        inventory = self.getBuildInventory('other-project')
        ivars = inventory['all']['vars']
        self.assertTrue(ivars['project2'])
        # We don't check out the tag on project2 (even though it has a
        # matching tag) because this is not an event for that tag.
        self.assertEqual('master', ivars['project2_var'])

        inventory = self.getBuildInventory('same-project-no-ref')
        ivars = inventory['all']['vars']
        # We read a variable from the expected file
        self.assertTrue(ivars['project1'])
        # Files have higher precedence than returned parent data
        self.assertEqual('include-vars-master', ivars['parent_var_precedence'])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='parent-job', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='same-project', result='SUCCESS', ref='refs/tags/foo'),
            dict(name='same-project-no-ref', result='SUCCESS',
                 ref='refs/tags/foo'),
            dict(name='other-project', result='SUCCESS', ref='refs/tags/foo'),
        ], ordered=False)


class TestAttributeControl(ZuulTestCase):
    @simple_layout('layouts/final-control.yaml')
    def test_final_control(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        for attr in [
                'requires',
                'provides',
                'tags',
                'files',
                'irrelevant-files',
                'required-projects',
                'include-projects',
                'exclude-projects',
                'vars',
                'extra-vars',
                'host-vars',
                'group-vars',
                'include-vars',
                'dependencies',
                'failure-output',
                'voting',
                'pre-run',
                'run',
                'post-run',
        ]:
            content = "- project: {check: {jobs: [test-%s]}}" % (attr,)
            file_dict = {'zuul.yaml': content}
            B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B',
                                               files=file_dict)
            self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
            self.waitUntilSettled()
            self.assertIn('Unable to modify job', B.messages[0])

        self.assertHistory([
            dict(name='final-job', result='SUCCESS'),
        ], ordered=False)


class TestIncludeExcludeProjects(ZuulTestCase):
    config_file = "zuul-gerrit-github.conf"
    tenant_config_file = "config/circular-dependencies/main.yaml"

    @simple_layout('layouts/include-exclude-projects.yaml')
    def test_include_exclude_projects(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.setDependsOn(B, 1)
        B.setDependsOn(A, 1)
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 7)

        expected_map = {
            'normal-job': ['org/project', 'org/project1', 'org/project2'],
            'include-job': ['org/project'],
            'exclude-job': ['org/project', 'org/project2'],
            'both-job': ['org/project', 'org/project1'],
            'exclude-all-job': [],
            'exclude-all-but-change-job': ['org/project'],
            'exclude-all-but-item-job': ['org/project'],
        }
        projects = [
            'review.example.com/org/project',
            'review.example.com/org/project1',
            'review.example.com/org/project2',
        ]
        for build in self.builds:
            expected = expected_map[build.job.name]
            workspace_cnames = set()
            for p in projects:
                path = os.path.join(build.jobdir.src_root, p)
                if os.path.exists(path):
                    workspace_cnames.add(p)
            expected_cnames = set(
                f'review.example.com/{p}' for p in expected
            )
            self.assertEqual(expected_cnames, workspace_cnames,
                             f'for {build.job.name}')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='normal-job', result='SUCCESS'),
            dict(name='include-job', result='SUCCESS'),
            dict(name='exclude-job', result='SUCCESS'),
            dict(name='both-job', result='SUCCESS'),
            dict(name='exclude-all-job', result='SUCCESS'),
            dict(name='exclude-all-but-change-job', result='SUCCESS'),
            dict(name='exclude-all-but-item-job', result='SUCCESS'),
        ], ordered=False)

    @gerrit_config(submit_whole_topic=True)
    @simple_layout('layouts/include-exclude-projects.yaml')
    def test_include_exclude_projects_cycle(self):
        self.executor_server.hold_jobs_in_build = True
        self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                       topic='test')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           topic='test')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 7)

        expected_map = {
            'normal-job': ['org/project', 'org/project1'],
            'include-job': ['org/project'],
            'exclude-job': ['org/project'],
            'both-job': ['org/project', 'org/project1'],
            'exclude-all-job': [],
            'exclude-all-but-change-job': ['org/project'],
            'exclude-all-but-item-job': ['org/project', 'org/project1'],
        }
        projects = [
            'review.example.com/org/project',
            'review.example.com/org/project1',
            'review.example.com/org/project2',
        ]
        for build in self.builds:
            expected = expected_map[build.job.name]
            workspace_cnames = set()
            for p in projects:
                path = os.path.join(build.jobdir.src_root, p)
                if os.path.exists(path):
                    workspace_cnames.add(p)
            expected_cnames = set(
                f'review.example.com/{p}' for p in expected
            )
            self.assertEqual(expected_cnames, workspace_cnames,
                             f'for {build.job.name}')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='normal-job', result='SUCCESS'),
            dict(name='include-job', result='SUCCESS'),
            dict(name='exclude-job', result='SUCCESS'),
            dict(name='both-job', result='SUCCESS'),
            dict(name='exclude-all-job', result='SUCCESS'),
            dict(name='exclude-all-but-change-job', result='SUCCESS'),
            dict(name='exclude-all-but-item-job', result='SUCCESS'),
        ], ordered=False)

    def test_dynamic_playbook_checkout(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: project-test
                run: playbooks/project-test.yaml
                workspace-checkout: false
                include-projects: []

            - project:
                check:
                  jobs:
                    - project-test
            """)
        in_repo_playbook = textwrap.dedent(
            """
            - hosts: all
              tasks: []
            """)
        file_dict = {'.zuul.yaml': in_repo_conf,
                     'playbooks/project-test.yaml': in_repo_playbook}

        A = self.fake_gerrit.addFakeChange('org/project4', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-test', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestBranchAssignedQueues(ZuulTestCase):
    @simple_layout('layouts/branch-assigned.yaml')
    def test_branch_assigned_queues(self):
        self.executor_server.hold_jobs_in_build = True
        self.create_branch('org/project1', 'red-one')
        self.create_branch('org/project1', 'gold-one')
        self.create_branch('org/project2', 'red-two')
        self.create_branch('org/project2', 'gold-two')

        # 1
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        # 2
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        # 3
        C = self.fake_gerrit.addFakeChange('org/project1', 'red-one', 'C')
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()
        # 4
        D = self.fake_gerrit.addFakeChange('org/project2', 'red-two', 'D')
        D.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(D.addApproval('Approved', 1))
        self.waitUntilSettled()

        # 5
        E = self.fake_gerrit.addFakeChange('org/project1', 'gold-one', 'E')
        E.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(E.addApproval('Approved', 1))
        self.waitUntilSettled()
        # 6
        F = self.fake_gerrit.addFakeChange('org/project2', 'gold-two', 'F')
        F.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(F.addApproval('Approved', 1))
        self.waitUntilSettled()

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        gate_manager = tenant.layout.pipeline_managers['gate']
        self.assertEqual(4, len(gate_manager.state.queues))
        self.assertEqual(1, len(gate_manager.state.queues[0].queue))
        self.assertEqual(1, len(gate_manager.state.queues[1].queue))
        self.assertEqual(2, len(gate_manager.state.queues[2].queue))
        self.assertEqual(2, len(gate_manager.state.queues[3].queue))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='2,1'),
            dict(name='test-job', result='SUCCESS', changes='3,1'),
            dict(name='test-job', result='SUCCESS', changes='3,1 4,1'),
            dict(name='test-job', result='SUCCESS', changes='5,1'),
            dict(name='test-job', result='SUCCESS', changes='5,1 6,1'),
        ], ordered=False)

    @simple_layout('layouts/branch-assigned-single.yaml')
    def test_branch_assigned_queues_single(self):
        # Test projects with a single branch and no valid branch
        # assignment (only the config project assigns the project to
        # the queue, and its branch is ignored).
        self.executor_server.hold_jobs_in_build = True

        # 1
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        # 2
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        gate_manager = tenant.layout.pipeline_managers['gate']
        self.assertEqual(1, len(gate_manager.state.queues))
        self.assertEqual(2, len(gate_manager.state.queues[0].queue))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)

    @simple_layout('layouts/branch-assigned.yaml')
    def test_branch_assigned_queues_sos(self):
        self.executor_server.hold_jobs_in_build = True
        self.create_branch('org/project1', 'red-one')
        self.create_branch('org/project1', 'gold-one')
        self.create_branch('org/project2', 'red-two')
        self.create_branch('org/project2', 'gold-two')

        # 1
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        # 2
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        # 3
        C = self.fake_gerrit.addFakeChange('org/project1', 'red-one', 'C')
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()

        sched1 = self.scheds.first
        sched2 = self.createScheduler()
        sched2.start()

        # Pause scheduler 1
        with sched1.sched.run_handler_lock:
            # 4
            D = self.fake_gerrit.addFakeChange('org/project2', 'red-two', 'D')
            D.addApproval('Code-Review', 2)
            self.fake_gerrit.addEvent(D.addApproval('Approved', 1))
            self.waitUntilSettled(matcher=[sched2])

            # 5
            E = self.fake_gerrit.addFakeChange('org/project1', 'gold-one', 'E')
            E.addApproval('Code-Review', 2)
            self.fake_gerrit.addEvent(E.addApproval('Approved', 1))
            self.waitUntilSettled(matcher=[sched2])
            # 6
            F = self.fake_gerrit.addFakeChange('org/project2', 'gold-two', 'F')
            F.addApproval('Code-Review', 2)
            self.fake_gerrit.addEvent(F.addApproval('Approved', 1))
            self.waitUntilSettled(matcher=[sched2])

            tenant = sched2.sched.abide.tenants.get('tenant-one')
            gate_manager = tenant.layout.pipeline_managers['gate']
            self.assertEqual(4, len(gate_manager.state.queues))
            self.assertEqual(1, len(gate_manager.state.queues[0].queue))
            self.assertEqual(1, len(gate_manager.state.queues[1].queue))
            self.assertEqual(2, len(gate_manager.state.queues[2].queue))
            self.assertEqual(2, len(gate_manager.state.queues[3].queue))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='2,1'),
            dict(name='test-job', result='SUCCESS', changes='3,1'),
            dict(name='test-job', result='SUCCESS', changes='3,1 4,1'),
            dict(name='test-job', result='SUCCESS', changes='5,1'),
            dict(name='test-job', result='SUCCESS', changes='5,1 6,1'),
        ], ordered=False)
