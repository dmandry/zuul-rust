# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2014 Wikimedia Foundation Inc.
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

import json
import logging
import configparser
import multiprocessing
import os
import re
import time
from unittest import mock

from tests.base import (
    AnsibleZuulTestCase,
    BaseTestCase,
    FIXTURE_DIR,
    ZuulTestCase,
    gerrit_config,
    iterate_timeout,
    okay_tracebacks,
    simple_layout,
)

from zuul.executor.sensors.startingbuilds import StartingBuildsSensor
from zuul.executor.sensors.process import ProcessSensor
from zuul.executor.sensors.ram import RAMSensor
from zuul.executor.server import squash_variables
from zuul.model import NodeSet, Group


class ExecutorReposMixin:
    def assertRepoState(self, repo, state, project, build, number):
        if 'branch' in state:
            self.assertFalse(repo.head.is_detached,
                             'Project %s commit for build %s #%s should '
                             'not have a detached HEAD' % (
                                 project, build, number))
            self.assertEqual(repo.active_branch.name,
                             state['branch'],
                             'Project %s commit for build %s #%s should '
                             'be on the correct branch' % (
                                 project, build, number))
            # Remote 'origin' needs to be kept intact with a bogus URL
            self.assertEqual('file:///dev/null', repo.remotes.origin.url)
            self.assertIn(state['branch'], repo.remotes.origin.refs)
        if 'commit' in state:
            self.assertEqual(state['commit'],
                             str(repo.commit('HEAD')),
                             'Project %s commit for build %s #%s should '
                             'be correct' % (
                                 project, build, number))
        ref = repo.commit('HEAD')
        repo_messages = set(
            [c.message.strip() for c in repo.iter_commits(ref)])
        if 'present' in state:
            for change in state['present']:
                msg = '%s-1' % change.subject
                self.assertTrue(msg in repo_messages,
                                'Project %s for build %s #%s should '
                                'have change %s' % (
                                    project, build, number, change.subject))
        if 'absent' in state:
            for change in state['absent']:
                msg = '%s-1' % change.subject
                self.assertTrue(msg not in repo_messages,
                                'Project %s for build %s #%s should '
                                'not have change %s' % (
                                    project, build, number, change.subject))

    def assertBuildStates(self, states, projects):
        for number, build in enumerate(self.builds):
            work = build.getWorkspaceRepos(projects)
            state = states[number]

            for project in projects:
                self.assertRepoState(work[project], state[project],
                                     project, build, number)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()


class TestExecutorRepos(ZuulTestCase, ExecutorReposMixin):
    tenant_config_file = 'config/single-tenant/main.yaml'

    log = logging.getLogger("zuul.test.executor")

    @simple_layout('layouts/repo-checkout-two-project.yaml')
    def test_one_branch(self):
        self.executor_server.hold_jobs_in_build = True

        p1 = 'review.example.com/org/project1'
        p2 = 'review.example.com/org/project2'
        projects = [p1, p2]
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(2, len(self.builds), "Two builds are running")

        upstream = self.getUpstreamRepos(projects)
        states = [
            {p1: dict(present=[A], absent=[B], branch='master'),
             p2: dict(commit=str(upstream[p2].commit('master')),
                      branch='master'),
             },
            {p1: dict(present=[A], absent=[B], branch='master'),
             p2: dict(present=[B], absent=[A], branch='master'),
             },
        ]

        self.assertBuildStates(states, projects)

    @simple_layout('layouts/repo-checkout-four-project.yaml')
    def test_multi_branch(self):
        self.executor_server.hold_jobs_in_build = True

        p1 = 'review.example.com/org/project1'
        p2 = 'review.example.com/org/project2'
        p3 = 'review.example.com/org/project3'
        p4 = 'review.example.com/org/project4'
        projects = [p1, p2, p3, p4]

        self.create_branch('org/project2', 'stable/havana')
        self.create_branch('org/project4', 'stable/havana')
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'stable/havana',
                                           'B')
        C = self.fake_gerrit.addFakeChange('org/project3', 'master', 'C')
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(3, len(self.builds), "Three builds are running")

        upstream = self.getUpstreamRepos(projects)
        states = [
            {p1: dict(present=[A], absent=[B, C], branch='master'),
             p2: dict(commit=str(upstream[p2].commit('master')),
                      branch='master'),
             p3: dict(commit=str(upstream[p3].commit('master')),
                      branch='master'),
             p4: dict(commit=str(upstream[p4].commit('master')),
                      branch='master'),
             },
            {p1: dict(present=[A], absent=[B, C], branch='master'),
             p2: dict(present=[B], absent=[A, C], branch='stable/havana'),
             p3: dict(commit=str(upstream[p3].commit('master')),
                      branch='master'),
             p4: dict(commit=str(upstream[p4].commit('stable/havana')),
                      branch='stable/havana'),
             },
            {p1: dict(present=[A], absent=[B, C], branch='master'),
             p2: dict(commit=str(upstream[p2].commit('master')),
                      branch='master'),
             p3: dict(present=[C], absent=[A, B], branch='master'),
             p4: dict(commit=str(upstream[p4].commit('master')),
                      branch='master'),
             },
        ]

        self.assertBuildStates(states, projects)

    @simple_layout('layouts/repo-checkout-six-project.yaml')
    def test_project_override(self):
        self.executor_server.hold_jobs_in_build = True

        p1 = 'review.example.com/org/project1'
        p2 = 'review.example.com/org/project2'
        p3 = 'review.example.com/org/project3'
        p4 = 'review.example.com/org/project4'
        p5 = 'review.example.com/org/project5'
        p6 = 'review.example.com/org/project6'
        projects = [p1, p2, p3, p4, p5, p6]

        self.create_branch('org/project3', 'stable/havana')
        self.create_branch('org/project4', 'stable/havana')
        self.create_branch('org/project6', 'stable/havana')
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project2', 'master', 'C')
        D = self.fake_gerrit.addFakeChange('org/project3', 'stable/havana',
                                           'D')
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        C.addApproval('Code-Review', 2)
        D.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(D.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(4, len(self.builds), "Four builds are running")

        upstream = self.getUpstreamRepos(projects)
        states = [
            {p1: dict(present=[A], absent=[B, C, D], branch='master'),
             p2: dict(commit=str(upstream[p2].commit('master')),
                      branch='master'),
             p3: dict(commit=str(upstream[p3].commit('master')),
                      branch='master'),
             p4: dict(commit=str(upstream[p4].commit('master')),
                      branch='master'),
             p5: dict(commit=str(upstream[p5].commit('master')),
                      branch='master'),
             p6: dict(commit=str(upstream[p6].commit('master')),
                      branch='master'),
             },
            {p1: dict(present=[A, B], absent=[C, D], branch='master'),
             p2: dict(commit=str(upstream[p2].commit('master')),
                      branch='master'),
             p3: dict(commit=str(upstream[p3].commit('master')),
                      branch='master'),
             p4: dict(commit=str(upstream[p4].commit('master')),
                      branch='master'),
             p5: dict(commit=str(upstream[p5].commit('master')),
                      branch='master'),
             p6: dict(commit=str(upstream[p6].commit('master')),
                      branch='master'),
             },
            {p1: dict(present=[A, B], absent=[C, D], branch='master'),
             p2: dict(present=[C], absent=[A, B, D], branch='master'),
             p3: dict(commit=str(upstream[p3].commit('master')),
                      branch='master'),
             p4: dict(commit=str(upstream[p4].commit('master')),
                      branch='master'),
             p5: dict(commit=str(upstream[p5].commit('master')),
                      branch='master'),
             p6: dict(commit=str(upstream[p6].commit('master')),
                      branch='master'),
             },
            {p1: dict(present=[A, B], absent=[C, D], branch='master'),
             p2: dict(present=[C], absent=[A, B, D], branch='master'),
             p3: dict(present=[D], absent=[A, B, C],
                      branch='stable/havana'),
             p4: dict(commit=str(upstream[p4].commit('master')),
                      branch='master'),
             p5: dict(commit=str(upstream[p5].commit('master')),
                      branch='master'),
             p6: dict(commit=str(upstream[p6].commit('stable/havana')),
                      branch='stable/havana'),
             },
        ]

        self.assertBuildStates(states, projects)

    def test_periodic_override(self):
        # This test can not use simple_layout because it must start
        # with a configuration which does not include a
        # timer-triggered job so that we have an opportunity to set
        # the hold flag before the first job.

        # This tests that we can override the branch in a timer
        # trigger (mostly to ensure backwards compatability for jobs).
        self.executor_server.hold_jobs_in_build = True

        p1 = 'review.example.com/org/project1'
        projects = [p1]
        self.create_branch('org/project1', 'stable/havana')

        # Start timer trigger - also org/project
        self.commitConfigUpdate('common-config',
                                'layouts/repo-checkout-timer-override.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        # The pipeline triggers every second, so we should have seen
        # several by now.
        time.sleep(5)
        self.waitUntilSettled()

        # Stop queuing timer triggered jobs so that the assertions
        # below don't race against more jobs being queued.
        self.commitConfigUpdate('common-config',
                                'layouts/repo-checkout-no-timer-override.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        # If APScheduler is in mid-event when we remove the job, we
        # can end up with one more event firing, so give it an extra
        # second to settle.
        time.sleep(1)
        self.waitUntilSettled()

        self.assertEqual(1, len(self.builds), "One build is running")

        upstream = self.getUpstreamRepos(projects)
        states = [
            {p1: dict(commit=str(upstream[p1].commit('stable/havana')),
                      branch='stable/havana'),
             },
        ]

        self.assertBuildStates(states, projects)

    def test_periodic(self):
        # This test can not use simple_layout because it must start
        # with a configuration which does not include a
        # timer-triggered job so that we have an opportunity to set
        # the hold flag before the first job.
        self.executor_server.hold_jobs_in_build = True
        # Start timer trigger - also org/project
        self.commitConfigUpdate('common-config',
                                'layouts/repo-checkout-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        p1 = 'review.example.com/org/project1'
        projects = [p1]
        self.create_branch('org/project1', 'stable/havana')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project1', 'stable/havana'))
        self.waitUntilSettled()

        # The pipeline triggers every second, so we should have seen
        # several by now.
        time.sleep(5)
        self.waitUntilSettled()

        # Stop queuing timer triggered jobs so that the assertions
        # below don't race against more jobs being queued.
        self.commitConfigUpdate('common-config',
                                'layouts/repo-checkout-no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        # If APScheduler is in mid-event when we remove the job, we
        # can end up with one more event firing, so give it an extra
        # second to settle.
        time.sleep(1)
        self.waitUntilSettled()

        self.assertEqual(2, len(self.builds), "Two builds are running")

        upstream = self.getUpstreamRepos(projects)
        states = [
            {p1: dict(commit=str(upstream[p1].commit('stable/havana')),
                      branch='stable/havana'),
             },
            {p1: dict(commit=str(upstream[p1].commit('master')),
                      branch='master'),
             },
        ]
        if self.builds[0].parameters['zuul']['ref'] == 'refs/heads/master':
            states = list(reversed(states))

        self.assertBuildStates(states, projects)

    @simple_layout('layouts/repo-checkout-post.yaml')
    def test_post_and_master_checkout(self):
        self.executor_server.hold_jobs_in_build = True
        p1 = "review.example.com/org/project1"
        p2 = "review.example.com/org/project2"
        projects = [p1, p2]
        upstream = self.getUpstreamRepos(projects)

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        event = A.getRefUpdatedEvent()
        A.setMerged()
        A_commit = str(upstream[p1].commit('master'))
        self.log.debug("A commit: %s" % A_commit)

        # Add another commit to the repo that merged right after this
        # one to make sure that our post job runs with the one that we
        # intended rather than simply the current repo state.
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B',
                                           parent='refs/changes/01/1/1')
        B.setMerged()
        B_commit = str(upstream[p1].commit('master'))
        self.log.debug("B commit: %s" % B_commit)

        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()

        states = [
            {p1: dict(commit=A_commit,
                      present=[A], absent=[B], branch='master'),
             p2: dict(commit=str(upstream[p2].commit('master')),
                      absent=[A, B], branch='master'),
             },
        ]

        self.assertBuildStates(states, projects)

    @simple_layout('layouts/repo-checkout-tag.yaml')
    def test_tag_checkout(self):
        self.executor_server.hold_jobs_in_build = True
        p1 = "review.example.com/org/project1"
        p2 = "review.example.com/org/project2"
        projects = [p1, p2]
        upstream = self.getUpstreamRepos(projects)

        self.create_branch('org/project2', 'stable/havana')
        files = {'README': 'tagged readme'}
        self.addCommitToRepo('org/project2', 'tagged commit',
                             files, branch='stable/havana', tag='test-tag')

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        states = [
            {p1: dict(present=[A], branch='master'),
             p2: dict(commit=str(upstream[p2].commit('test-tag')),
                      absent=[A]),
             },
        ]

        self.assertBuildStates(states, projects)

    @simple_layout('layouts/reproducible-repo.yaml')
    def test_reproducible_repo(self):
        # Verify that we get exactly the same commit shas when merging
        # items.
        self.executor_server.hold_jobs_in_build = True
        self.hold_jobs_in_queue = True

        project = 'review.example.com/org/project'
        projects = [project]

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        time.sleep(1)
        self.waitUntilSettled()

        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        time.sleep(1)
        self.waitUntilSettled()

        # Four builds running:
        #   job1, job2 for change A
        #   job1, job2 for change A+B
        queue = list(self.executor_api.queued())
        self.assertEqual(len(self.builds), 0)
        self.assertEqual(len(queue), 4)

        for x in range(4):
            self.executor_api.release(queue[x])
            time.sleep(1)
            self.waitUntilSettled()

        self.assertEqual(len(self.builds), 4)
        work = [build.getWorkspaceRepos(projects) for build in self.builds]
        shas = [
            [c.hexsha for c in repos[project].iter_commits(
                repos[project].commit('HEAD'))]
            for repos in work
        ]
        self.log.debug("Shas: %s", shas)

        # Verify that the commit shas are the same for each pair of builds
        self.assertEqual(shas[0], shas[1])
        self.assertEqual(shas[2], shas[3])
        # Verify that every sha in the first 2 builds are in the last
        # two as well
        for sha in shas[0]:
            self.assertTrue(sha in shas[2])

        states = [
            {project: dict(present=[A], absent=[B], branch='master')},
            {project: dict(present=[A], absent=[B], branch='master')},
            {project: dict(present=[A, B], absent=[], branch='master')},
            {project: dict(present=[A, B], absent=[], branch='master')},
        ]
        self.assertBuildStates(states, projects)

    @simple_layout('layouts/repo-state-minimal.yaml')
    def test_repo_state_minimal(self):
        # Test that we don't restore unecessary repos
        self.executor_server.hold_jobs_in_build = True

        files = {'test.yaml': 'foo: bar'}
        self.addCommitToRepo('org/project3', 'add vars', files)

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        build = self.getBuildByName('testjob1')
        path = os.path.join(build.jobdir.src_root,
                            'review.example.com', 'org', 'project1')
        self.assertTrue(os.path.exists(path))
        path = os.path.join(build.jobdir.src_root,
                            'review.example.com', 'org', 'project2')
        self.assertTrue(os.path.exists(path))
        path = os.path.join(build.jobdir.src_root,
                            'review.example.com', 'org', 'project3')
        # org/project3 is used for include-vars but it shouldn't be
        # part of the workspace checkout
        self.assertFalse(os.path.exists(path))

        build = self.getBuildByName('testjob2')
        path = os.path.join(build.jobdir.src_root,
                            'review.example.com', 'org', 'project1')
        self.assertTrue(os.path.exists(path))
        path = os.path.join(build.jobdir.src_root,
                            'review.example.com', 'org', 'project2')
        self.assertFalse(os.path.exists(path))
        path = os.path.join(build.jobdir.src_root,
                            'review.example.com', 'org', 'project3')
        self.assertFalse(os.path.exists(path))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_repo_state_file(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        build = self.getBuildByName('project-merge')
        path = os.path.join(build.jobdir.log_root, 'workspace-repos.json')
        with open(path, 'r') as f:
            data = json.load(f)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # Don't check the timestamp, but verify it exists
        self.assertIsNotNone(data['merge_ops'][2].pop('timestamp', None))
        self.assertEqual(
            data['merge_ops'],
            [{'cmd': ['git', 'checkout', 'master'],
              'path': 'review.example.com/org/project1'},
             {'cmd': ['git', 'fetch', 'origin', 'refs/changes/01/1/1'],
              'path': 'review.example.com/org/project1'},
             {'cmd': ['git',
                      'merge',
                      '-m',
                      "Merge 'refs/changes/01/1/1'",
                      '-s',
                      'resolve',
                      'FETCH_HEAD'],
              'path': 'review.example.com/org/project1'},
             {'cmd': ['git', 'checkout', 'master'],
              'path': 'review.example.com/org/project1'}])
        self.assertEqual(
            set(data['repo_state']['review.example.com/org/project1'].keys()),
            {'refs/heads/master', 'refs/remotes/origin/master',
             'refs/tags/init'})
        self.assertEqual(
            "zuul@example.com",
            data['merge_email'])
        self.assertEqual(
            "zuul",
            data['merge_name'])


class TestExecutorRepoRoles(ZuulTestCase, ExecutorReposMixin):
    tenant_config_file = 'config/executor-repos/main.yaml'

    def test_repo_checkout_roles_branch(self):
        self.executor_server.hold_jobs_in_build = True
        p0 = "review.example.com/org/project"
        p1 = "review.example.com/org/project1"
        p2 = "review.example.com/org/project2"
        projects = [p0, p1, p2]

        self.create_branch('org/project1', 'test-branch')
        self.create_branch('org/project2', 'test-branch')

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'branch': 'run branch'})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        states = [
            {
                p0: dict(present=[A], branch='master'),
                p1: dict(absent=[A], branch='test-branch'),
                p2: dict(absent=[A], branch='test-branch'),
            },
        ]

        self.assertBuildStates(states, projects)

    def test_repo_checkout_roles_tag(self):
        self.executor_server.hold_jobs_in_build = True
        p0 = "review.example.com/org/project"
        p1 = "review.example.com/org/project1"
        p2 = "review.example.com/org/project2"
        projects = [p0, p1, p2]
        upstream = self.getUpstreamRepos(projects)

        self.create_branch('org/project1', 'unused-branch')
        files = {'README': 'tagged readme'}
        self.addCommitToRepo('org/project1', 'tagged commit',
                             files, branch='unused-branch', tag='test-tag')
        self.create_branch('org/project2', 'unused-branch')
        files = {'README': 'tagged readme'}
        self.addCommitToRepo('org/project2', 'tagged commit',
                             files, branch='unused-branch', tag='test-tag')

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'tag': 'run tag'})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        states = [
            {
                p0: dict(present=[A], branch='master'),
                p1: dict(commit=str(upstream[p1].commit('test-tag')),
                         absent=[A]),
                p2: dict(commit=str(upstream[p2].commit('test-tag')),
                         absent=[A]),
            },
        ]

        self.assertBuildStates(states, projects)


class TestAnsibleJob(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def run_job(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        return list(self.executor_server.job_workers.values())[0]

    def test_host_keys(self):
        self.fake_nodepool.host_keys = ['fake-host-key']
        job = self.run_job()
        keys = job.host_list[0]['host_keys']
        self.assertEqual(keys[0], '127.0.0.1 fake-host-key')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_host_keys_connection_port(self):
        # Test with custom connection_port set
        self.fake_nodepool.host_keys = ['fake-host-key']
        self.fake_nodepool.connection_port = 22022
        job = self.run_job()
        keys = job.host_list[0]['host_keys']
        self.assertEqual(keys[0], '[127.0.0.1]:22022 fake-host-key')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_no_host_keys_connection_port(self):
        # Test with no host keys
        self.fake_nodepool.host_keys = []
        self.fake_nodepool.connection_port = 22022
        job = self.run_job()
        keys = job.host_list[0]['host_keys']
        self.assertEqual(keys, [])
        self.assertEqual(
            job.host_list[0]['host_vars']['ansible_ssh_common_args'],
            '-o StrictHostKeyChecking=false')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_no_shell_type(self):
        # Test without shell type set
        job = self.run_job()
        host = job.host_list[0]

        self.assertNotIn('ansible_shell_type', host['host_vars'])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_custom_shell_type(self):
        # Test with custom shell type set.
        self.fake_nodepool.shell_type = 'cmd'
        job = self.run_job()
        host = job.host_list[0]

        self.assertIn('ansible_shell_type', host['host_vars'])
        self.assertEqual(
            host['host_vars']['ansible_shell_type'],
            'cmd')

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()


class TestExecutorHostname(ZuulTestCase):
    config_file = 'zuul-executor-hostname.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_executor_hostname(self):
        self.assertEqual('test-executor-hostname.example.com',
                         self.executor_server.hostname)


class TestStartingBuildsSensor(ZuulTestCase):
    config_file = 'zuul.conf'
    tenant_config_file = 'config/governor/main.yaml'

    def test_default_case(self):
        # Given
        cores = multiprocessing.cpu_count()

        # When
        sensor = StartingBuildsSensor(None, '', None, cores * 2.5, None)

        # Then
        coefficient = 2 if multiprocessing.cpu_count() <= 4 else 1
        max_default = int(cores * 2.5 * coefficient)
        self.assertEqual(sensor.max_starting_builds, max_default)
        self.assertEqual(sensor.min_starting_builds, max(int(cores / 2), 1))

    def test_configuration_not_exists(self):
        # Given
        cores = multiprocessing.cpu_count()

        # When
        sensor = StartingBuildsSensor(None, '', None, cores * 2.5, self.config)

        # Then
        coefficient = 2 if multiprocessing.cpu_count() <= 4 else 1
        max_default = int(cores * 2.5 * coefficient)
        self.assertEqual(sensor.max_starting_builds, max_default)
        self.assertEqual(sensor.min_starting_builds, max(int(cores / 2), 1))

    def test_configuration_override(self):
        # Given
        cores = multiprocessing.cpu_count()
        self.config.set('executor', 'max_starting_builds', '5')

        # When
        sensor = StartingBuildsSensor(None, '', None, cores * 2.5, self.config)

        # Then
        self.assertEqual(sensor.max_starting_builds, 5)
        self.assertEqual(sensor.min_starting_builds, min(
            max(int(cores / 2), 1), sensor.max_starting_builds))

    def test_configuration_override_affecting_min(self):
        # Given
        cores = multiprocessing.cpu_count()
        self.config.set('executor', 'max_starting_builds', '1')

        # When
        sensor = StartingBuildsSensor(None, '', None, cores * 2.5, self.config)

        # Then
        self.assertEqual(sensor.max_starting_builds, 1)
        self.assertEqual(sensor.min_starting_builds, 1)


class TestGovernor(ZuulTestCase):
    config_file = 'zuul-executor-governor.conf'
    tenant_config_file = 'config/governor/main.yaml'

    @mock.patch('os.getloadavg')
    @mock.patch('psutil.virtual_memory')
    def test_load_governor(self, vm_mock, loadavg_mock):
        class Dummy(object):
            pass
        ram = Dummy()
        ram.percent = 20.0  # 20% used
        ram.total = 8 * 1024 * 1024 * 1024  # 8GiB
        vm_mock.return_value = ram
        loadavg_mock.return_value = (0.0, 0.0, 0.0)
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # fake the load to be higher than permitted
        fake_load = multiprocessing.cpu_count() * 2.6
        loadavg_mock.return_value = (fake_load, fake_load, fake_load)
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

    @mock.patch('os.getloadavg')
    @mock.patch('psutil.virtual_memory')
    def test_ram_governor(self, vm_mock, loadavg_mock):
        class Dummy(object):
            pass
        ram = Dummy()
        ram.percent = 20.0  # 20% used
        ram.total = 8 * 1024 * 1024 * 1024  # 8GiB
        vm_mock.return_value = ram
        loadavg_mock.return_value = (0.0, 0.0, 0.0)
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)
        ram.percent = 99.0  # 99% used
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

    @mock.patch('os.getloadavg')
    @mock.patch('psutil.virtual_memory')
    def test_ram_cgroup_governor(self, vm_mock, loadavg_mock):
        class Dummy(object):
            pass
        ram = Dummy()
        ram.percent = 20.0  # 20% used
        ram.total = 8 * 1024 * 1024 * 1024  # 8GiB
        vm_mock.return_value = ram
        loadavg_mock.return_value = (0.0, 0.0, 0.0)

        # Set no cgroup limit
        ram_sensor = [x for x in self.executor_server.sensors
                      if isinstance(x, RAMSensor)][0]
        ram_sensor.cgroup_stats_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'memory.stat.nolimit')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Set cgroup limit 5GiB and ram usage 20%
        ram_sensor.cgroup_stats_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'memory.stat.ok')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Set cgroup limit 5GiB and ram usage 96%
        ram_sensor.cgroup_stats_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'memory.stat.bad')
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

    @mock.patch('os.getloadavg')
    @mock.patch('psutil.virtual_memory')
    @okay_tracebacks("invalid literal for int() with base 10: 'foo'")
    def test_process_governor(self, vm_mock, loadavg_mock):
        # Set up load average and memory sensors to accept work.
        class Dummy(object):
            pass
        ram = Dummy()
        ram.percent = 20.0  # 20% used
        ram.total = 8 * 1024 * 1024 * 1024  # 8GiB
        vm_mock.return_value = ram
        loadavg_mock.return_value = (0.0, 0.0, 0.0)

        # Test no limit
        process_sensor = [x for x in self.executor_server.sensors
                          if isinstance(x, ProcessSensor)][0]
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.max')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.max')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Test typical runtime values
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.8k')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.8k')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Test within 5%
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.15600')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.15600')
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

        # Test within 10 keep running
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.150')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.100')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.150')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.100')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Test within 10 stop
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.150')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.141')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.150')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.141')
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

        # Test at limit
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

        # Test no limit only root cgroup files
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.max')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Test typical runtime values only root cgroup files
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.8k')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Test at limit only root cgroup files
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

        # Test no limit only user cgroup files
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.max')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Test typical runtime values only user cgroup files
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.8k')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Test at limit only user cgroup files
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.16k')
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

        # Test no cgroup files found
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'doesnotexist')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        # Test inability to parse cgroup files
        process_sensor._root_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.foo')
        process_sensor._root_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.foo')
        process_sensor._user_cgroup_max_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.foo')
        process_sensor._user_cgroup_cur_file = os.path.join(
            FIXTURE_DIR, 'cgroup', 'pids.foo')
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

    @mock.patch('os.getloadavg')
    @mock.patch('os.statvfs')
    def test_hdd_governor(self, statvfs_mock, loadavg_mock):
        class Dummy(object):
            pass
        hdd = Dummy()
        hdd.f_frsize = 4096
        hdd.f_blocks = 120920708
        hdd.f_bfree = 95716701  # 20.84% used
        hdd.f_files = 61022208
        hdd.f_ffree = 32147841  # 47.31% used
        statvfs_mock.return_value = hdd
        loadavg_mock.return_value = (0.0, 0.0, 0.0)

        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        self.assertReportedStat(
            'zuul.executor.test-executor-hostname_example_com.pct_used_hdd',
            value='2084', kind='g')
        self.assertReportedStat(
            'zuul.executor.test-executor-hostname_example_com.pct_used_inodes',
            value='4731', kind='g')

        hdd.f_bfree = 5716701  # 95.27% used
        statvfs_mock.return_value = hdd

        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

        self.assertReportedStat(
            'zuul.executor.test-executor-hostname_example_com.pct_used_hdd',
            value='9527', kind='g')
        self.assertReportedStat(
            'zuul.executor.test-executor-hostname_example_com.pct_used_inodes',
            value='4731', kind='g')

        hdd.f_bfree = 95716701  # 20.84% used
        hdd.f_ffree = 1336387  # 97.80% used
        statvfs_mock.return_value = hdd

        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

        self.assertReportedStat(
            'zuul.executor.test-executor-hostname_example_com.pct_used_hdd',
            value='2084', kind='g')
        self.assertReportedStat(
            'zuul.executor.test-executor-hostname_example_com.pct_used_inodes',
            value='9780', kind='g')

    @mock.patch('os.getloadavg')
    def test_pause_governor(self, loadavg_mock):
        loadavg_mock.return_value = (0.0, 0.0, 0.0)

        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)

        self.executor_server.pause_sensor.pause = True
        self.executor_server.manageLoad()
        self.assertFalse(self.executor_server.accepting_work)

    def waitForExecutorBuild(self, jobname):
        self.log.debug("Waiting for %s to start", jobname)
        timeout = time.time() + 30
        build = None
        while time.time() < timeout and not build:
            for b in self.builds:
                if b.name == jobname:
                    build = b
                    break
            time.sleep(0.1)
        self.log.debug("Found build %s", jobname)
        build_id = build.uuid
        while (time.time() < timeout and
               build_id not in self.executor_server.job_workers):
            time.sleep(0.1)
        worker = self.executor_server.job_workers[build_id]
        self.log.debug("Found worker %s", jobname)
        while (time.time() < timeout and
               not worker.started):
            time.sleep(0.1)
        self.log.debug("Worker for %s started: %s", jobname, worker.started)
        return build

    @mock.patch('os.getloadavg')
    def test_slow_start(self, loadavg_mock):
        loadavg_mock.return_value = (0.0, 0.0, 0.0)

        def _set_starting_builds(min, max):
            for sensor in self.executor_server.sensors:
                if isinstance(sensor, StartingBuildsSensor):
                    sensor.min_starting_builds = min
                    sensor.max_starting_builds = max

        # Note: This test relies on the fact that manageLoad is only
        # run at specific points.  Several times in the test we check
        # that manageLoad has disabled or enabled job acceptance based
        # on the number of "starting" jobs.  Some of those jobs may
        # have actually moved past the "starting" phase and are
        # actually "running".  But because manageLoad hasn't run
        # again, it still uses the old values.  Keep this in mind when
        # double checking its calculations.
        #
        # Disable the periodic governor runs to make sure they don't
        # interefere (only possible if the test runs longer than 10
        # seconds).
        self.executor_server.governor_stop_event.set()
        self.executor_server.hold_jobs_in_build = True
        _set_starting_builds(1, 1)
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        build1 = self.waitForExecutorBuild('test1')
        # With one job (test1) being started, we should no longer
        # be accepting new work
        self.assertFalse(self.executor_server.accepting_work)
        self.assertEqual(len(self.executor_server.job_workers), 1)
        # Allow enough starting builds for the test to complete.
        _set_starting_builds(1, 3)
        # We must wait for build1 to enter a waiting state otherwise
        # the subsequent release() is a noop and the build is never
        # released.  We don't use waitUntilSettled as that requires
        # the other two builds to start which can't happen while we
        # don't accept jobs.
        for x in iterate_timeout(30, "build1 is waiting"):
            if build1.waiting:
                break
        build1.release()
        for x in iterate_timeout(30, "Wait for build1 to complete"):
            if build1.uuid not in self.executor_server.job_workers:
                break
        self.executor_server.manageLoad()
        # This manageLoad call has determined that there are 0 workers
        # running, so our full complement of 3 starting builds is
        # available.  It will re-register for work and pick up the
        # next two jobs.

        self.waitForExecutorBuild('test2')
        self.waitForExecutorBuild('test3')
        # When each of these jobs started, they caused manageLoad to
        # be called, the second invocation calculated that there were
        # 2 workers running, so our starting build limit was reduced
        # to 1.  Usually it will calculate that there are 2 starting
        # builds, but theoretically it could count only 1 if the first
        # build manages to leave the starting phase before the second
        # build starts.  It should always count the second build as
        # starting.  As long as at least one build is still in the
        # starting phase, this will exceed the limit and unregister.
        self.assertFalse(self.executor_server.accepting_work)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.executor_server.manageLoad()
        self.assertTrue(self.executor_server.accepting_work)


class TestLineMapping(AnsibleZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'
    tenant_config_file = 'config/line-mapping/main.yaml'

    def _test_line_mapping(self, method):
        with open(os.path.join(FIXTURE_DIR,
                               'config/line-mapping/git/',
                               'org_project/README')) as f:
            content = f.read().split('\n')

        # The change under test adds a line to the end.
        change_content = content + ['end line']
        file_dict = {'README': '\n'.join(change_content)}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        if method == 'merge':
            pass
        elif method == 'cherry_pick':
            A.cherry_pick = True
        else:
            raise Exception("unsupported method")

        # An intervening change adds a line near the top.
        # And changes a line in the middle (no net offset from that),
        # just to make sure we have more than one diff hunk.
        head_content = (
            content[:4] +
            ['new line 5'] +
            content[4:50] +
            ['changed line 51'] +
            content[51:]
        )
        file_dict = {'README': '\n'.join(head_content)}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        B.setMerged()

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('file-comments').result,
                         'SUCCESS')
        self.assertEqual(len(A.comments), 5)
        comments = sorted(A.comments, key=lambda x: x['line'])
        reviewer = {'email': 'zuul@example.com',
                    'name': 'Zuul',
                    'username': 'jenkins'}
        self.assertEqual(comments[0], {
            'file': 'README',
            'line': 3,
            'message': 'line 3 comment',
            'reviewer': reviewer,
        })
        self.assertEqual(comments[1], {
            'file': 'README',
            'line': 7,
            'message': 'line 7 comment',
            'reviewer': reviewer,
        })
        self.assertEqual(comments[2], {
            "file": "README",
            "line": 14,
            "message": "range comment for line 14",
            "range": {
                "end_character": 26,
                "end_line": 14,
                "start_character": 0,
                "start_line": 11,
            },
            "reviewer": {
                "email": "zuul@example.com",
                "name": "Zuul",
                "username": "jenkins",
            }
        })
        self.assertEqual(comments[3], {
            'file': 'README',
            'line': 100,
            'message': 'line 100 comment',
            'reviewer': reviewer,
        })
        self.assertEqual(comments[4], {
            'file': 'README',
            'line': 101,
            'message': 'line 101 comment',
            'reviewer': reviewer,
        })

    def test_line_mapping_merge(self):
        self._test_line_mapping(method='merge')

    def test_line_mapping_cherry_pick(self):
        self._test_line_mapping(method='cherry_pick')

    def test_line_mapping_fast_forward(self):
        with open(os.path.join(FIXTURE_DIR,
                               'config/line-mapping/git/',
                               'org_project/README')) as f:
            content = f.read().split('\n')

        # The change under test adds a line to the end and also the middle.
        change_content = (content[:4] + ['new line 5'] +
                          content[4:] + ['end line'])
        file_dict = {'README': '\n'.join(change_content)}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('file-comments').result,
                         'SUCCESS')
        self.assertEqual(len(A.comments), 5)
        comments = sorted(A.comments, key=lambda x: x['line'])
        reviewer = {'email': 'zuul@example.com',
                    'name': 'Zuul',
                    'username': 'jenkins'}
        self.assertEqual(comments[0], {
            'file': 'README',
            'line': 3,
            'message': 'line 3 comment',
            'reviewer': reviewer,
        })
        self.assertEqual(comments[1], {
            'file': 'README',
            'line': 8,
            'message': 'line 7 comment',
            'reviewer': reviewer,
        })
        self.assertEqual(comments[2], {
            "file": "README",
            "line": 15,
            "message": "range comment for line 14",
            "range": {
                "end_character": 26,
                "end_line": 15,
                "start_character": 0,
                "start_line": 12,
            },
            "reviewer": {
                "email": "zuul@example.com",
                "name": "Zuul",
                "username": "jenkins",
            }
        })
        self.assertEqual(comments[3], {
            'file': 'README',
            'line': 101,
            'message': 'line 100 comment',
            'reviewer': reviewer,
        })
        self.assertEqual(comments[4], {
            'file': 'README',
            'line': 102,
            'message': 'line 101 comment',
            'reviewer': reviewer,
        })


class ExecutorFactsMixin:
    # These should be overridden in child classes.
    tenant_config_file = 'config/executor-facts/main.yaml'
    ansible_major_minor = 'X.Y'

    def _get_file(self, build, path):
        p = os.path.join(build.jobdir.root, path)
        with open(p) as f:
            return f.read()

    def test_datetime_fact(self):
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        self.assertEqual(self.getJobFromHistory('datetime-fact').result,
                         'SUCCESS')

        j = json.loads(self._get_file(self.history[0],
                                      'work/logs/job-output.json'))

        date_time = \
            j[0]['plays'][0]['tasks'][0]['hosts']['localhost']['date_time']
        self.assertEqual(18, len(date_time))
        build = self.getJobFromHistory('datetime-fact', result='SUCCESS')
        with open(build.jobdir.job_output_file) as f:
            output = f.read()
            self.assertIn(f'Ansible version={self.ansible_major_minor}',
                          output)


class TestExecutorFacts9(AnsibleZuulTestCase, ExecutorFactsMixin):
    tenant_config_file = 'config/executor-facts/main9.yaml'
    ansible_major_minor = '2.16'


class TestExecutorFacts11(AnsibleZuulTestCase, ExecutorFactsMixin):
    tenant_config_file = 'config/executor-facts/main11.yaml'
    ansible_major_minor = '2.18'


class AnsibleCallbackConfigsMixin:
    config_file = 'zuul-executor-ansible-callback.conf'

    # These should be overridden in child classes.
    tenant_config_file = 'config/ansible-callbacks/main.yaml'
    ansible_major_minor = 'X.Y'

    def test_ansible_callback_config(self):
        self.executor_server.keep_jobdir = True
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        callbacks = [
            'callback_test_callback',
            'callback_nocows = True',
            'callback_nocows = False',
            'callback_\\nnocows = True',
            'callback_\\nnocows = False',
            'callback_ansible_interpolation'
        ]

        p = os.path.join(self.getJobFromHistory('callback-test').jobdir.root,
                         'ansible/playbook_0/ansible.cfg')
        self.assertEqual(self.getJobFromHistory('callback-test').result,
                         'SUCCESS')

        c = configparser.ConfigParser(interpolation=None)
        c.read(p)
        for callback in callbacks:
            self.assertIn(callback, c.sections())
        self.assertIn('test_field', c['callback_ansible_interpolation'])
        self.assertIn('test-%-value',
                      c['callback_ansible_interpolation']['test_field'])

        self.assertIn('file_name', c['callback_test_callback'])
        self.assertEqual('callback-success',
                         c['callback_test_callback']['file_name'])
        callback_result_file = os.path.join(
            self.getJobFromHistory('callback-test').jobdir.root,
            'work',
            c['callback_test_callback']['file_name'])
        self.assertTrue(os.path.isfile(callback_result_file))
        build = self.getJobFromHistory('callback-test', result='SUCCESS')
        with open(build.jobdir.job_output_file) as f:
            output = f.read()
            self.assertIn(f'Ansible version={self.ansible_major_minor}',
                          output)


class TestAnsibleCallbackConfigs9(AnsibleZuulTestCase,
                                  AnsibleCallbackConfigsMixin):
    config_file = 'zuul-executor-ansible-callback.conf'
    tenant_config_file = 'config/ansible-callbacks/main9.yaml'
    ansible_major_minor = '2.16'


class TestAnsibleCallbackConfigs11(AnsibleZuulTestCase,
                                   AnsibleCallbackConfigsMixin):
    config_file = 'zuul-executor-ansible-callback.conf'
    tenant_config_file = 'config/ansible-callbacks/main11.yaml'
    ansible_major_minor = '2.18'


class TestExecutorEnvironment(AnsibleZuulTestCase):
    tenant_config_file = 'config/zuul-environment-filter/main.yaml'

    @mock.patch.dict('os.environ', {'ZUUL_TEST_VAR': 'some-value',
                                    'TEST_VAR': 'not-empty'})
    def test_zuul_environment_filter(self):
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        self.assertEqual(
            self.getJobFromHistory('zuul-environment-filter').result,
            'SUCCESS')


class TestExecutorStart(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def setup_config(self, config_file: str):
        config = super(TestExecutorStart, self).setup_config(config_file)
        self.junk_dir = os.path.join(self.jobdir_root, 'junk')
        os.makedirs(self.junk_dir)
        return config

    def test_executor_start(self):
        self.assertFalse(os.path.exists(self.junk_dir))


class TestExecutorExtraPackages(AnsibleZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    test_package = 'pywinrm'

    def setUp(self):
        super(TestExecutorExtraPackages, self).setUp()
        import subprocess
        ansible_manager = self.executor_server.ansible_manager
        for version in ansible_manager._supported_versions:
            command = [ansible_manager.getAnsibleCommand(version, 'pip'),
                       'uninstall', '-y', self.test_package]
            # We want to error if the uninstall fails as the test below
            # relies on the package not being installed to be properly
            # exercised.
            s = subprocess.run(command,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
            self.log.info(s.stdout)
            self.assertEqual(s.returncode, 0)

    @mock.patch('zuul.lib.ansible.ManagedAnsible.extra_packages',
                new_callable=mock.PropertyMock)
    def test_extra_packages(self, mock_extra_packages):
        mock_extra_packages.return_value = [self.test_package]
        ansible_manager = self.executor_server.ansible_manager
        self.assertFalse(ansible_manager.validate())
        ansible_manager.install()
        self.assertTrue(ansible_manager.validate())


class TestVarSquash(BaseTestCase):
    def test_squash_variables(self):
        # Test that we correctly squash job variables
        nodeset = NodeSet()
        nodes = [
            {'name': 'node1', 'host_vars': {
                'host': 'node1_host',
                'extra': 'node1_extra',
            }},
            {'name': 'node2', 'host_vars': {
                'host': 'node2_host',
                'extra': 'node2_extra',
            }},
        ]
        nodeset.addGroup(Group('group1', ['node1']))
        nodeset.addGroup(Group('group2', ['node2']))
        groupvars = {
            'group1': {
                'host': 'group1_host',
                'group': 'group1_group',
                'extra': 'group1_extra',
            },
            'group2': {
                'host': 'group2_host',
                'group': 'group2_group',
                'extra': 'group2_extra',
            },
            'all': {
                'all2': 'groupvar_all2',
            }
        }
        jobvars = {
            'host': 'jobvar_host',
            'group': 'jobvar_group',
            'all': 'jobvar_all',
            'extra': 'jobvar_extra',
        }
        extravars = {
            'extra': 'extravar_extra',
        }
        out = squash_variables(
            nodes, nodeset, jobvars, groupvars, extravars)

        expected = {
            'node1': {
                'all': 'jobvar_all',
                'all2': 'groupvar_all2',
                'group': 'group1_group',
                'host': 'node1_host',
                'extra': 'extravar_extra'},
            'node2': {
                'all': 'jobvar_all',
                'all2': 'groupvar_all2',
                'group': 'group2_group',
                'host': 'node2_host',
                'extra': 'extravar_extra'},
        }
        self.assertEqual(out, expected)


class TestExecutorFailure(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    @mock.patch('zuul.executor.server.ExecutorServer.executeJob')
    @okay_tracebacks('Failed to start')
    def test_executor_job_start_failure(self, execute_job_mock):
        execute_job_mock.side_effect = Exception('Failed to start')
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertTrue(re.search(
            '- project-merge .* ERROR',
            A.messages[-1]))

    @okay_tracebacks("Transient error")
    def test_executor_transient_error(self):
        self.hold_jobs_in_queue = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        with mock.patch('zuul.merger.merger.Merger.updateRepo') as update_mock:
            update_mock.side_effect = IOError("Transient error")
            self.executor_api.release()
            self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(len(items), 1)

        self.hold_jobs_in_queue = False
        self.executor_api.release()
        self.waitUntilSettled()

        job = items[0].current_build_set.job_graph.getJob(
            'project-merge', items[0].changes[0].cache_key)
        build_retries = items[0].current_build_set.getRetryBuildsForJob(job)
        self.assertEqual(len(build_retries), 1)
        self.assertIsNotNone(build_retries[0].error_detail)
        self.assertTrue(
            build_retries[0].error_detail.startswith(
                "Failed to update project"))


class TestExecutorWorkspaceCheckout(ZuulTestCase, ExecutorReposMixin):
    tenant_config_file = 'config/workspace-checkout/main.yaml'

    def _test_workspace_checkout(self, jobname):
        self.executor_server.keep_jobdir = True
        self.executor_server.hold_jobs_in_build = True
        p1 = "review.example.com/org/project1"
        p2 = "review.example.com/org/project2"
        projects = [p1, p2]
        upstream = self.getUpstreamRepos(projects)

        files = {jobname: 'run'}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        states = [
            {p1: dict(present=[A]),
             p2: dict(commit=str(upstream[p2].commit('master')),
                      absent=[A]),
             },
        ]

        self.assertBuildStates(states, projects)

    def _assert_not_sparse(self):
        cc = "review.example.com/common-config"
        rt = "review.example.com/org/roletest"
        p1 = "review.example.com/org/project1"
        p2 = "review.example.com/org/project2"
        path = os.path.join(self.history[0].jobdir.root)
        file_checks = {
            f'trusted/project_0/{cc}/otherdirectory/something.txt': True,
            f'trusted/project_0/{cc}/playbooks/run.yaml': True,
            f'trusted/project_0/{cc}/includevars/vars.yaml': True,
            f'trusted/project_1/{rt}/otherdirectory/something.txt': True,
            f'trusted/project_1/{rt}/tasks/main.yaml': True,
            f'work/src/{p1}/otherdirectory/something.txt': True,
            f'work/src/{p2}/otherdirectory/something.txt': True,
        }
        for f, expected in file_checks.items():
            exists = os.path.exists(os.path.join(path, f))
            self.assertEqual(expected, exists, f)

    def _assert_sparse(self):
        cc = "review.example.com/common-config"
        rt = "review.example.com/org/roletest"
        p1 = "review.example.com/org/project1"
        p2 = "review.example.com/org/project2"
        path = os.path.join(self.history[0].jobdir.root)
        file_checks = {
            f'trusted/project_0/{cc}/otherdirectory/something.txt': False,
            f'trusted/project_0/{cc}/playbooks/run.yaml': True,
            f'trusted/project_0/{cc}/includevars/vars.yaml': True,
            f'trusted/project_1/{rt}/otherdirectory/something.txt': False,
            f'trusted/project_1/{rt}/tasks/main.yaml': True,
            f'work/src/{p1}/otherdirectory/something.txt': False,
            f'work/src/{p2}/otherdirectory/something.txt': False,
        }
        for f, expected in file_checks.items():
            exists = os.path.exists(os.path.join(path, f))
            self.assertEqual(expected, exists, f)

    def test_workspace_checkout_true(self):
        self._test_workspace_checkout('test-workspace-checkout-true')
        self._assert_not_sparse()

    def test_workspace_checkout_false(self):
        self._test_workspace_checkout('test-workspace-checkout-false')
        self._assert_sparse()

    def test_workspace_checkout_auto_nodeset(self):
        self._test_workspace_checkout(
            'test-workspace-checkout-auto-nodeset')
        self._assert_sparse()

    def test_workspace_checkout_auto_no_nodeset(self):
        self._test_workspace_checkout(
            'test-workspace-checkout-auto-no-nodeset')
        self._assert_not_sparse()

    def test_workspace_checkout_line_mapping(self):
        self.executor_server.keep_jobdir = True
        self.executor_server.hold_jobs_in_build = True
        cc = "review.example.com/common-config"
        rt = "review.example.com/org/roletest"
        p1 = "review.example.com/org/project1"
        p2 = "review.example.com/org/project2"
        projects = [p1, p2]
        upstream = self.getUpstreamRepos(projects)

        jobname = 'test-workspace-checkout-false'
        files = {jobname: 'run'}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=files)
        self.executor_server.returnData(
            jobname, A,
            {'zuul':
             {'file_comments':
              {'otherdirectory/something.txt': [{
                  'line': 1,
                  'message': 'test message'}]
               }
              }
             }
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        states = [
            {p1: dict(present=[A]),
             p2: dict(commit=str(upstream[p2].commit('master')),
                      absent=[A]),
             },
        ]

        self.assertBuildStates(states, projects)
        path = os.path.join(self.history[0].jobdir.root)
        file_checks = {
            f'trusted/project_0/{cc}/otherdirectory/something.txt': False,
            f'trusted/project_0/{cc}/playbooks/run.yaml': True,
            f'trusted/project_0/{cc}/includevars/vars.yaml': True,
            f'trusted/project_1/{rt}/otherdirectory/something.txt': False,
            f'trusted/project_1/{rt}/tasks/main.yaml': True,
            f'work/src/{p1}/otherdirectory/something.txt': False,
            f'work/src/{p2}/otherdirectory/something.txt': False,
        }
        for f, expected in file_checks.items():
            exists = os.path.exists(os.path.join(path, f))
            self.assertEqual(expected, exists, f)

    def test_workspace_checkout_multi_include_vars(self):
        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(
                name='test-workspace-checkout-multi-include-vars',
                result='SUCCESS'
            ),
        ], ordered=False)


class TestExecutorOriginRemote(ZuulTestCase):
    config_file = "zuul-gerrit-github.conf"

    @gerrit_config(submit_whole_topic=True)
    @simple_layout('layouts/executor-origin-remote.yaml')
    def test_executor_origin_remote(self):
        self.executor_server.keep_jobdir = True
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")

        B = self.fake_gerrit.addFakeChange("org/project", "master", "B",
                                           topic='test-topic')
        self.fake_gerrit.addFakeChange("org/project", "master", "C",
                                       topic='test-topic')
        D = self.fake_gerrit.addFakeChange("org/project", "master", "D",
                                           topic='test-topic')

        # B -> A (via commit-depends)
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        p = 'review.example.com/org/project'
        for build in self.builds:
            work = build.getWorkspaceRepos([p])
            repo = work[p]
            # Verify that the previous queue item is the base
            self.assertEqual(
                "A-1", repo.remotes.origin.refs.master.commit.message)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()


class TestExecutorHomedir(AnsibleZuulTestCase):
    tenant_config_file = 'config/homedir/main.yaml'

    def test_path_blocklist(self):
        # Test that the files in the path blocklist are deleted
        # between runs.
        files = {'test': 'run'}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='testjob', result='SUCCESS'),
        ], ordered=False)

    def test_path_blocklist_symlink(self):
        # Test that we detect symlinks when unlinking.
        files = {'link': 'run'}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=files)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([])
        self.assertEqual(A.reported, 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '-1')
        self.assertIn('Symlink detected', A.messages[0])
