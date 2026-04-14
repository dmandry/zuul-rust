# Copyright 2015 BMW Car IT GmbH
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

import os
import threading
import time
import textwrap
from unittest import mock
import yaml

import zuul.model
import tests.base
from tests.base import (
    AnsibleZuulTestCase,
    BaseTestCase,
    ZuulTestCase,
    gerrit_config,
    iterate_timeout,
    okay_tracebacks,
    simple_layout,
    skipIfMultiScheduler,
)
from zuul.lib import strings
from zuul.driver.gerrit import GerritDriver
from zuul.driver.gerrit.gerritconnection import (
    ChangeNetworkConflict,
    GerritConnection,
    PeekQueue,
)

import git
import paramiko
import testtools

FIXTURE_DIR = os.path.join(tests.base.FIXTURE_DIR, 'gerrit')


def read_fixture(file):
    with open('%s/%s' % (FIXTURE_DIR, file), 'r') as fixturefile:
        lines = fixturefile.readlines()
        command = lines[0].replace('\n', '')
        value = ''.join(lines[1:])
        return command, value


def read_fixtures(files):
    calls = []
    values = []
    for fixture_file in files:
        command, value = read_fixture(fixture_file)
        calls.append(mock.call(command))
        values.append([value, ''])
    return calls, values


class TestGerrit(BaseTestCase):

    @mock.patch('zuul.driver.gerrit.gerritconnection.GerritConnection._ssh')
    def run_query(self, files, expected_patches, _ssh_mock):
        gerrit_config = {
            'user': 'gerrit',
            'server': 'localhost',
        }
        driver = GerritDriver()
        gerrit = GerritConnection(driver, 'review_gerrit', gerrit_config)

        calls, values = read_fixtures(files)
        _ssh_mock.side_effect = values

        result = gerrit.simpleQuery('project:zuul/zuul')

        _ssh_mock.assert_has_calls(calls)
        self.assertEqual(len(calls), _ssh_mock.call_count,
                         '_ssh should be called %d times' % len(calls))
        self.assertIsNotNone(result, 'Result is not none')
        self.assertEqual(len(result), expected_patches,
                         'There must be %d patches.' % expected_patches)

    def test_simple_query_pagination_new(self):
        files = ['simple_query_pagination_new_1',
                 'simple_query_pagination_new_2']
        expected_patches = 5
        self.run_query(files, expected_patches)

    def test_simple_query_pagination_old(self):
        files = ['simple_query_pagination_old_1',
                 'simple_query_pagination_old_2',
                 'simple_query_pagination_old_3']
        expected_patches = 5
        self.run_query(files, expected_patches)

    def test_ref_name_check_rules(self):
        # See man git-check-ref-format for the rules referenced here
        test_strings = [
            ('refs/heads/normal', True),
            ('refs/heads/.bad', False),  # rule 1
            ('refs/heads/bad.lock', False),  # rule 1
            ('refs/heads/good.locked', True),
            ('refs/heads/go.od', True),
            ('refs/heads//bad', False),  # rule 6
            ('refs/heads/b?d', False),  # rule 5
            ('refs/heads/b[d', False),  # rule 5
            ('refs/heads/b..ad', False),  # rule 3
            ('bad', False),  # rule 2
            ('refs/heads/\nbad', False),  # rule 4
            ('/refs/heads/bad', False),  # rule 6
            ('refs/heads/bad/', False),  # rule 6
            ('refs/heads/bad.', False),  # rule 7
            ('.refs/heads/bad', False),  # rule 1
            ('refs/he@{ads/bad', False),  # rule 8
            ('@', False),  # rule 9
            ('refs\\heads/bad', False)  # rule 10
        ]

        for ref, accepted in test_strings:
            self.assertEqual(
                accepted,
                GerritConnection._checkRefFormat(ref),
                ref + ' shall be ' + ('accepted' if accepted else 'rejected'))

    def test_getGitURL(self):
        gerrit_config = {
            'user': 'gerrit',
            'server': 'localhost',
            'password': '1/badpassword',
        }
        # The 1/ in the password ensures we test the url encoding
        # path; this is the format of password we get from
        # googlesource.com.
        driver = GerritDriver()
        gerrit = GerritConnection(driver, 'review_gerrit', gerrit_config)
        project = gerrit.source.getProject('org/project')
        url = gerrit.source.getGitUrl(project)
        self.assertEqual(
            'https://gerrit:1%2Fbadpassword@localhost/a/org/project',
            url)

    def test_git_over_ssh_getGitURL(self):
        gerrit_config = {
            'user': 'gerrit',
            'server': 'localhost',
            'password': '1/badpassword',
            'git_over_ssh': 'true',
        }
        # The 1/ in the password ensures we test the url encoding
        # path; this is the format of password we get from
        # googlesource.com.
        driver = GerritDriver()
        gerrit = GerritConnection(driver, 'review_gerrit', gerrit_config)
        project = gerrit.source.getProject('org/project')
        url = gerrit.source.getGitUrl(project)
        self.assertEqual(
            'ssh://gerrit@localhost:29418/org/project',
            url)

    def test_ssh_server_getGitURL(self):
        gerrit_config = {
            'user': 'gerrit',
            'server': 'otherserver',
            'password': '1/badpassword',
            'ssh_server': 'localhost',
            'git_over_ssh': 'true',
        }
        # The 1/ in the password ensures we test the url encoding
        # path; this is the format of password we get from
        # googlesource.com.
        driver = GerritDriver()
        gerrit = GerritConnection(driver, 'review_gerrit', gerrit_config)
        project = gerrit.source.getProject('org/project')
        url = gerrit.source.getGitUrl(project)
        self.assertEqual(
            'ssh://gerrit@localhost:29418/org/project',
            url)

    def test_ssh_exec_timeout(self):
        with mock.patch('paramiko.SSHClient') as mock_ssh_client:
            mock_client = mock.Mock()
            mock_ssh_client.return_value = mock_client
            stdin = mock.Mock()
            stdout = mock.Mock()
            stdout.channel = mock.Mock()
            stdout.channel.recv_exit_status.return_value = 0
            stderr = mock.Mock()
            mock_client.exec_command.return_value = (stdin, stdout, stderr)
            gerrit_config = {
                'user': 'gerrit',
                'server': 'localhost',
            }
            driver = GerritDriver()
            gerrit = GerritConnection(driver, 'review_gerrit', gerrit_config)
            gerrit._ssh("echo hi")
            expected = [
                mock.call('echo hi', timeout=30),
            ]
            self.assertEqual(expected, mock_client.exec_command.mock_calls)

        with mock.patch('paramiko.SSHClient') as mock_ssh_client:
            mock_client = mock.Mock()
            mock_ssh_client.return_value = mock_client
            mock_client.exec_command.side_effect =\
                paramiko.buffered_pipe.PipeTimeout()

            gerrit_config = {
                'user': 'gerrit',
                'server': 'localhost',
            }
            driver = GerritDriver()
            gerrit = GerritConnection(driver, 'review_gerrit', gerrit_config)
            with testtools.ExpectedException(
                    paramiko.buffered_pipe.PipeTimeout):
                gerrit._ssh("echo hi")
            expected = [
                mock.call('echo hi', timeout=30),
                mock.call('echo hi', timeout=30),
            ]
            self.assertEqual(expected, mock_client.exec_command.mock_calls)


class TestGerritWeb(ZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_jobs_executed(self):
        "Test that jobs are executed and a change is merged"
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('project-merge').result,
                         'SUCCESS')
        self.assertEqual(self.getJobFromHistory('project-test1').result,
                         'SUCCESS')
        self.assertEqual(self.getJobFromHistory('project-test2').result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(self.getJobFromHistory('project-test1').node,
                         'label1')
        self.assertEqual(self.getJobFromHistory('project-test2').node,
                         'label1')

    def test_dynamic_line_comment(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: garbage-job
                garbage: True
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertEqual(A.patchsets[0]['approvals'][0]['__tag'],
                         "autogenerated:zuul:check")
        self.assertIn('Zuul encountered a syntax error',
                      A.messages[0])
        comments = sorted(A.comments, key=lambda x: x['line'])
        self.assertEqual(comments[0],
                         {'file': '.zuul.yaml',
                          'line': 4,
                          'message': "extra keys not allowed @ "
                                     "data['garbage']",
                          'range': {'end_character': 0,
                                    'end_line': 4,
                                    'start_character': 2,
                                    'start_line': 2},
                          'reviewer': {'email': 'zuul@example.com',
                                       'name': 'Zuul',
                                       'username': 'jenkins'}}
        )

    def test_message_too_long(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: garbage-job
                description: %s
                garbage: True
            """
        ) % ('x' * 16384)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertEqual(A.patchsets[0]['approvals'][0]['__tag'],
                         "autogenerated:zuul:check")
        self.assertIn('... (truncated)',
                      A.messages[0])

    def test_message_too_long_failed_job(self):
        # Test that if we have a failed job in a long message, we
        # at least include the failed jobs.
        config = []
        joblist = []
        # Use enough jobs with long names to trip the 16k limit
        for x in range(60):
            name = 'job-%s-%s' % ('x' * 200, x)
            job = dict(
                name=name,
                parent='test-job',
            )
            config.append(dict(job=job))
            joblist.append(name)
        config.append(dict(
            project=dict(
                check=dict(
                    jobs=joblist,
                )
            )
        ))
        in_repo_conf = yaml.dump(config)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        self.executor_server.failJob('project-test1', A)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertIn('length restrictions', A.messages[0])
        self.assertIn('FAILURE', A.messages[0])
        self.assertNotIn('SUCCESS', A.messages[0])

    def test_dependent_dynamic_line_comment(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: garbage-job
                garbage: True
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        B.data['commitMessage'] = '%s\n\nDepends-On: %s\n' % (
            B.subject, A.data['id'])

        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(B.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('This change depends on a change '
                      'with an invalid configuration',
                      B.messages[0])
        self.assertEqual(B.comments, [])

    @simple_layout('layouts/single-file-matcher.yaml')
    def test_single_file(self):
        # HTTP requests don't return a commit_msg entry in the files
        # list, but the rest of zuul always expects one.  This test
        # returns a single file to exercise the single-file code path
        # in the files matcher.
        files = {'README': 'please!\n'}

        change = self.fake_gerrit.addFakeChange('org/project',
                                                'master',
                                                'test irrelevant-files',
                                                files=files)
        self.fake_gerrit.addEvent(change.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        tested_change_ids = [x.changes[0] for x in self.history
                             if x.name == 'project-test-irrelevant-files']

        self.assertEqual([], tested_change_ids)

    def test_recheck(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(3, len(self.history))

        self.fake_gerrit.addEvent(A.getChangeCommentEvent(1))
        self.waitUntilSettled()
        self.assertEqual(3, len(self.history))

        self.fake_gerrit.addEvent(A.getChangeCommentEvent(1,
                                  'recheck'))
        self.waitUntilSettled()
        self.assertEqual(6, len(self.history))

        self.fake_gerrit.addEvent(A.getChangeCommentEvent(1,
                                  patchsetcomment='recheck'))
        self.waitUntilSettled()
        self.assertEqual(9, len(self.history))

        self.fake_gerrit.addEvent(A.getChangeCommentEvent(1,
                                  patchsetcomment='do not recheck'))
        self.waitUntilSettled()
        self.assertEqual(9, len(self.history))

    def test_submitted_together_git(self):
        # This tests that the circular dependency handling for submit
        # whole topic doesn't activate for changes which are only in a
        # git dependency.
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A")
        B = self.fake_gerrit.addFakeChange('org/project1', "master", "B")
        C = self.fake_gerrit.addFakeChange('org/project1', "master", "C")
        D = self.fake_gerrit.addFakeChange('org/project1', "master", "D")
        E = self.fake_gerrit.addFakeChange('org/project1', "master", "E")
        F = self.fake_gerrit.addFakeChange('org/project1', "master", "F")
        G = self.fake_gerrit.addFakeChange('org/project1', "master", "G")
        G.setDependsOn(F, 1)
        F.setDependsOn(E, 1)
        E.setDependsOn(D, 1)
        D.setDependsOn(C, 1)
        C.setDependsOn(B, 1)
        B.setDependsOn(A, 1)

        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")
        self.assertEqual(A.queried, 1)
        self.assertEqual(B.queried, 1)
        self.assertEqual(C.queried, 1)
        self.assertEqual(D.queried, 1)
        self.assertEqual(E.queried, 1)
        self.assertEqual(F.queried, 1)
        self.assertEqual(G.queried, 1)
        self.assertHistory([
            dict(name="project-merge", result="SUCCESS",
                 changes="1,1 2,1 3,1"),
            dict(name="project-test1", result="SUCCESS",
                 changes="1,1 2,1 3,1"),
            dict(name="project-test2", result="SUCCESS",
                 changes="1,1 2,1 3,1"),
            dict(name="project1-project2-integration", result="SUCCESS",
                 changes="1,1 2,1 3,1"),
        ], ordered=False)

    def test_submit_failure(self):
        # Test that we log the reason for a submit failure (403 error)
        self.fake_gerrit._fake_submit_permission = False
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A")
        A.addApproval('Code-Review', 2)
        with self.assertLogs('zuul.test.FakeGerritConnection', level='INFO'
                             ) as full_logs:
            self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
            self.waitUntilSettled()
            self.log.debug("Full logs:")
            for x in full_logs.output:
                self.log.debug(x)
            self.assertRegexInList(
                r'Error submitting data to gerrit on attempt 3: '
                'Received response 403: submit not permitted',
                full_logs.output)

        self.assertEqual(A.data['status'], 'NEW')

    @okay_tracebacks('_handleEvent')
    def test_event_failure(self):
        # Test that a failure to process a Gerrit event is handled
        # correctly
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        event = A.getPatchsetCreatedEvent(1)
        self.fake_gerrit._fake_return_api_error = True
        self.fake_gerrit.addEvent(event)
        # If the event is not processed, then we will never settle and
        # the test will time-out.
        self.waitUntilSettled()

    def test_get_project_branch_sha(self):
        # Exercise this method since it's only called from timer
        # triggers
        source = self.fake_gerrit.source
        project = source.getProject('org/project')
        self.assertIsNotNone(source.getProjectBranchSha(project, 'master'))

    @simple_layout('layouts/gerrit-pipeline-trigger-debug.yaml')
    def test_gerrit_pipeline_trigger_debug(self):
        # Test that we get debug info for a pipeline debug trigger
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1)
        self.assertIn('Debug information:',
                      A.messages[0])

        # Test that we get debug info for a pipeline debug trigger
        # when a change depends on an abandoned change
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        # We check approvals before state, so set those
        B.addApproval('Code-Review', 2)
        B.addApproval('Approved', 1)
        B.setAbandoned()
        A.setDependsOn(B, 1)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 2)
        # Full message: Change 2,1 in project org/project is needed
        # but can not be merged due to: change is closed
        # Meaning we can not enqueue A because it depends on B which
        # is abandoned.
        self.assertIn('change is closed',
                      A.messages[1])

        # Test that we get debug info for a pipeline debug trigger
        # when a change is missing a pipeline requirement
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(B.data['status'], 'ABANDONED')
        self.assertEqual(B.reported, 1)
        self.assertIn('does not match pipeline requirement',
                      B.messages[0])

        # Test that we get debug info for a pipeline debug trigger
        # when a change is missing a merge requirement
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(C.data['status'], 'NEW')
        self.assertEqual(C.reported, 1)
        self.assertIn('can not be merged due to: missing labels:',
                      C.messages[0])

        # Test that we get debug info for a pipeline debug trigger
        # when a change depends on a change missing a merge requirement
        D = self.fake_gerrit.addFakeChange('org/project', 'master', 'D')
        D.setDependsOn(C, 1)
        D.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(D.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(D.data['status'], 'NEW')
        self.assertEqual(D.reported, 1)
        self.assertIn(
            'is needed but can not be merged due to: missing labels:',
            D.messages[0])


class TestFileComments(AnsibleZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'
    tenant_config_file = 'config/gerrit-file-comments/main.yaml'

    def test_file_comments(self):
        A = self.fake_gerrit.addFakeChange(
            'org/project', 'master', 'A',
            files={'path/to/file.py': 'test1',
                   'otherfile.txt': 'test2',
                   })
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('file-comments').result,
                         'SUCCESS')
        self.assertEqual(self.getJobFromHistory('file-comments-error').result,
                         'SUCCESS')
        self.assertEqual(len(A.comments), 7)
        comments = sorted(A.comments, key=lambda x: (x['file'], x['line']))
        self.assertEqual(
            comments[0],
            {
                'file': '/COMMIT_MSG',
                'line': 1,
                'message': 'commit message comment',
                'reviewer': {
                    'email': 'zuul@example.com',
                    'name': 'Zuul',
                    'username': 'jenkins'
                },
            },
        )

        self.assertEqual(
            comments[1],
            {
                'file': 'otherfile.txt',
                'line': 21,
                'message': 'This is a much longer message.\n\n'
                'With multiple paragraphs.\n',
                'reviewer': {
                    'email': 'zuul@example.com',
                    'name': 'Zuul',
                    'username': 'jenkins'
                },
            },
        )

        self.assertEqual(
            comments[2],
            {
                "file": "path/to/file.py",
                "line": 2,
                "message": "levels are ignored by gerrit",
                "reviewer": {
                    "email": "zuul@example.com",
                    "name": "Zuul",
                    "username": "jenkins",
                },
            },
        )

        self.assertEqual(
            comments[3],
            {
                "file": "path/to/file.py",
                "line": 21,
                "message": (
                    "A second zuul return value using the same file should not"
                    "\noverride the first result, but both should be merged.\n"
                ),
                "reviewer": {
                    "email": "zuul@example.com",
                    "name": "Zuul",
                    "username": "jenkins",
                },
            },
        )

        self.assertEqual(
            comments[4],
            {
                'file': 'path/to/file.py',
                'line': 42,
                'message': 'line too long',
                'reviewer': {
                    'email': 'zuul@example.com',
                    'name': 'Zuul',
                    'username': 'jenkins'
                },
            },
        )

        self.assertEqual(
            comments[5],
            {
                "file": "path/to/file.py",
                "line": 42,
                "message": (
                    "A second comment applied to the same line in the same "
                    "file\nshould also be added to the result.\n"
                ),
                "reviewer": {
                    "email": "zuul@example.com",
                    "name": "Zuul",
                    "username": "jenkins",
                }
            }
        )

        self.assertEqual(comments[6],
                         {'file': 'path/to/file.py',
                          'line': 82,
                          'message': 'line too short',
                          'reviewer': {'email': 'zuul@example.com',
                                       'name': 'Zuul',
                                       'username': 'jenkins'}}
        )
        self.assertIn('expected a dictionary', A.messages[0],
                      "A should have a validation error reported")
        self.assertIn('invalid file missingfile.txt', A.messages[0],
                      "A should have file error reported")

    def test_merge_commit_file_comments(self):
        self.create_branch('org/project', 'stable/queens')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/queens'))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project', 'stable/queens', 'A',
                                           files={'path/to/file.py': 'test1',
                                                  'otherfile.txt': 'test2',
                                                  })
        A.setMerged()
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        upstream_path = os.path.join(self.upstream_root, 'org/project')
        upstream_repo = git.Repo(upstream_path)
        master_sha = upstream_repo.heads.master.commit.hexsha

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           merge_parents=[
                                               master_sha,
                                               A.patchsets[-1]['revision'],
                                           ],
                                           files={'path/to/file.py': 'test1',
                                                  'otherfile.txt': 'test2',
                                                  })

        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='file-comments', result='SUCCESS', changes='2,1'),
            dict(name='file-comments-error', result='SUCCESS', changes='2,1'),
        ], ordered=False)
        self.assertEqual(len(B.comments), 1)
        comments = sorted(B.comments, key=lambda x: (x['file'], x['line']))
        self.assertEqual(
            comments[0],
            {
                'file': '/COMMIT_MSG',
                'line': 1,
                'message': 'commit message comment',
                'reviewer': {
                    'email': 'zuul@example.com',
                    'name': 'Zuul',
                    'username': 'jenkins'
                },
            },
        )
        self.assertIn('invalid file missingfile.txt', B.messages[0],
                      "A should have file error reported")


class TestChecksApi(ZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'

    @simple_layout('layouts/gerrit-checks.yaml')
    def test_check_pipeline(self):
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setDependsOn(B, 1)
        A.setCheck('zuul:check', reset=True)
        self.waitForPoll('gerrit')
        self.waitUntilSettled()

        self.assertEqual(A.checks_history[0]['zuul:check']['state'],
                         'NOT_STARTED')

        self.assertEqual(A.checks_history[1]['zuul:check']['state'],
                         'SCHEDULED')
        self.assertEqual(
            A.checks_history[1]['zuul:check']['url'],
            'http://zuul.example.com/t/tenant-one/status/change/2,1')

        self.assertEqual(A.checks_history[2]['zuul:check']['state'],
                         'RUNNING')
        self.assertEqual(
            A.checks_history[2]['zuul:check']['url'],
            'http://zuul.example.com/t/tenant-one/status/change/2,1')

        self.assertEqual(A.checks_history[3]['zuul:check']['state'],
                         'SUCCESSFUL')
        self.assertTrue(
            A.checks_history[3]['zuul:check']['url'].startswith(
                'http://zuul.example.com/t/tenant-one/buildset/'))

        self.assertEqual(len(A.checks_history), 4)

        self.assertTrue(isinstance(
            A.checks_history[3]['zuul:check']['started'], str))
        self.assertTrue(isinstance(
            A.checks_history[3]['zuul:check']['finished'], str))
        self.assertTrue(
            A.checks_history[3]['zuul:check']['finished'] >
            A.checks_history[3]['zuul:check']['started'])
        self.assertEqual(A.checks_history[3]['zuul:check']['message'],
                         'Change passed all voting jobs')
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1 2,1')])
        self.assertEqual(A.reported, 0, "no messages should be reported")
        self.assertEqual(A.messages, [], "no messages should be reported")
        # Make sure B was never updated
        self.assertEqual(len(B.checks_history), 0)

    @simple_layout('layouts/gerrit-checks.yaml')
    def test_gate_pipeline(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)
        A.setCheck('zuul:gate', reset=True)
        self.waitForPoll('gerrit')
        self.waitUntilSettled()

        self.assertEqual(A.checks_history[0]['zuul:gate']['state'],
                         'NOT_STARTED')
        self.assertEqual(A.checks_history[1]['zuul:gate']['state'],
                         'SCHEDULED')
        self.assertEqual(A.checks_history[2]['zuul:gate']['state'],
                         'RUNNING')
        self.assertEqual(A.checks_history[3]['zuul:gate']['state'],
                         'SUCCESSFUL')
        self.assertEqual(len(A.checks_history), 4)
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1')])
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2,
                         "start and success messages should be reported")

    @simple_layout('layouts/gerrit-checks-scheme.yaml')
    @skipIfMultiScheduler()
    # This is the only gerrit checks API test which is failing because
    # it uses a check scheme rather than an UUID. The scheme must first
    # be evaluated and mapped to an UUID.
    # This shouldn't be a problem in production as the evaluation takes
    # place on the gerrit webserver. However, in the tests we get a
    # dedicated (fake) gerrit webserver for each fake gerrrit
    # connection. Since each scheduler gets a new connection, only one
    # of those webservers will be aware of the check. If any other
    # webserver tries to evaluate the check it will fail with
    # "Unable to find matching checker".
    def test_check_pipeline_scheme(self):
        self.fake_gerrit.addFakeChecker(uuid='zuul_check:abcd',
                                        repository='org/project',
                                        status='ENABLED')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setCheck('zuul_check:abcd', reset=True)
        self.waitForPoll('gerrit')
        self.waitUntilSettled()

        self.assertEqual(A.checks_history[0]['zuul_check:abcd']['state'],
                         'NOT_STARTED')
        self.assertEqual(A.checks_history[1]['zuul_check:abcd']['state'],
                         'SCHEDULED')
        self.assertEqual(A.checks_history[2]['zuul_check:abcd']['state'],
                         'RUNNING')
        self.assertEqual(A.checks_history[3]['zuul_check:abcd']['state'],
                         'SUCCESSFUL')
        self.assertEqual(len(A.checks_history), 4)
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1')])

    @simple_layout('layouts/gerrit-checks-nojobs.yaml')
    def test_no_jobs(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setCheck('zuul:check', reset=True)
        self.waitForPoll('gerrit')
        self.waitUntilSettled()

        self.assertEqual(A.checks_history[0]['zuul:check']['state'],
                         'NOT_STARTED')
        self.assertEqual(A.checks_history[1]['zuul:check']['state'],
                         'SCHEDULED')
        self.assertEqual(A.checks_history[2]['zuul:check']['state'],
                         'NOT_RELEVANT')
        self.assertEqual(len(A.checks_history), 3)
        self.assertEqual(A.data['status'], 'NEW')

    @simple_layout('layouts/gerrit-checks.yaml')
    def test_config_error(self):
        # Test that line comments are reported on config errors
        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - bad-job
            """)
        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.setCheck('zuul:check', reset=True)
        self.waitForPoll('gerrit')
        self.waitUntilSettled()

        self.assertEqual(A.checks_history[0]['zuul:check']['state'],
                         'NOT_STARTED')
        self.assertEqual(A.checks_history[1]['zuul:check']['state'],
                         'SCHEDULED')
        self.assertEqual(A.checks_history[2]['zuul:check']['state'],
                         'FAILED')
        self.assertEqual(len(A.checks_history), 3)
        comments = sorted(A.comments, key=lambda x: x['line'])
        self.assertEqual(comments[0],
                         {'file': '.zuul.yaml',
                          'line': 5,
                          'message': 'Job bad-job not defined',
                          'range': {'end_character': 0,
                                    'end_line': 5,
                                    'start_character': 2,
                                    'start_line': 2},
                          'reviewer': {'email': 'zuul@example.com',
                                       'name': 'Zuul',
                                       'username': 'jenkins'}}
        )
        self.assertEqual(A.reported, 0, "no messages should be reported")
        self.assertEqual(A.messages, [], "no messages should be reported")

    @simple_layout('layouts/gerrit-checks.yaml')
    def test_new_patchset(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setCheck('zuul:check', reset=True)
        self.waitForPoll('gerrit')
        self.waitUntilSettled()

        self.assertEqual(A.checks_history[0]['zuul:check']['state'],
                         'NOT_STARTED')
        self.assertEqual(A.checks_history[1]['zuul:check']['state'],
                         'SCHEDULED')
        self.assertEqual(A.checks_history[2]['zuul:check']['state'],
                         'RUNNING')
        self.assertEqual(len(A.checks_history), 3)

        A.addPatchset()
        A.setCheck('zuul:check', reset=True)
        self.waitForPoll('gerrit')
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.log.info(A.checks_history)
        self.assertEqual(A.checks_history[3]['zuul:check']['state'],
                         'NOT_STARTED')
        self.assertEqual(A.checks_history[4]['zuul:check']['state'],
                         'SCHEDULED')
        self.assertEqual(A.checks_history[5]['zuul:check']['state'],
                         'RUNNING')
        self.assertEqual(A.checks_history[6]['zuul:check']['state'],
                         'SUCCESSFUL')
        self.assertEqual(len(A.checks_history), 7)
        self.assertHistory([
            dict(name='test-job', result='ABORTED', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='1,2'),
        ], ordered=False)


class TestPolling(ZuulTestCase):
    config_file = 'zuul-gerrit-no-stream.conf'

    @simple_layout('layouts/gerrit-checks.yaml')
    def test_config_update(self):
        # Test that the config is updated via polling when a change
        # merges without stream-events enabled.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: test-job2
                parent: test-job
            - project:
                check:
                  jobs:
                    - test-job2
            """)

        file_dict = {'.zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.waitForPoll('gerrit')
        self.waitUntilSettled('poll 1')

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setCheck('zuul:check', reset=True)
        self.waitForPoll('gerrit')
        self.waitUntilSettled('poll 2')

        self.assertEqual(B.checks_history[0]['zuul:check']['state'],
                         'NOT_STARTED')
        self.assertEqual(B.checks_history[1]['zuul:check']['state'],
                         'SCHEDULED')
        self.assertEqual(B.checks_history[2]['zuul:check']['state'],
                         'RUNNING')
        self.assertEqual(B.checks_history[3]['zuul:check']['state'],
                         'SUCCESSFUL')
        self.assertEqual(len(B.checks_history), 4)
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='2,1'),
            dict(name='test-job2', result='SUCCESS', changes='2,1'),
        ], ordered=False)

        # There may be an extra change-merged event because we poll
        # too often during the tests.
        self.waitForPoll('gerrit')
        self.waitUntilSettled('poll 3')

    @simple_layout('layouts/gerrit-poll-post.yaml')
    def test_post(self):
        # Test that ref-updated events trigger post jobs.
        self.waitUntilSettled()
        # Wait for an initial poll to get the original sha.
        self.waitForPoll('gerrit-ref')

        # Merge a change.
        self.create_commit('org/project')

        # Wait for the job to run.
        self.waitForPoll('gerrit-ref')
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='post-job', result='SUCCESS'),
        ])

    @simple_layout('layouts/gerrit-poll-post.yaml')
    def test_tag(self):
        # Test that ref-updated events trigger post jobs.
        self.waitUntilSettled()
        # Wait for an initial poll to get the original sha.
        self.waitForPoll('gerrit-ref')

        # Merge a change.
        self.fake_gerrit.addFakeTag('org/project', 'master', 'foo')

        # Wait for the job to run.
        self.waitForPoll('gerrit-ref')
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='tag-job', result='SUCCESS'),
        ])


class TestWrongConnection(ZuulTestCase):
    config_file = 'zuul-connections-multiple-gerrits.conf'
    tenant_config_file = 'config/wrong-connection-in-pipeline/main.yaml'

    def test_wrong_connection(self):
        # Test if the wrong connection is configured in a gate pipeline

        # Our system has two gerrits, and we have configured a gate
        # pipeline to trigger on the "review_gerrit" connection, but
        # report (and merge) via "another_gerrit".
        A = self.fake_review_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.addApproval('Verified', 2)
        self.fake_review_gerrit.addEvent(A.addApproval('Approved', 1))

        self.waitUntilSettled()

        B = self.fake_review_gerrit.addFakeChange('org/project', 'master', 'B')
        # Let's try this as if the change was merged (say, via another tenant).
        B.setMerged()
        B.addApproval('Code-Review', 2)
        B.addApproval('Verified', 2)
        self.fake_review_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        self.assertEqual(A.reported, 0)
        self.assertEqual(B.reported, 0)
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='2,1'),
        ], ordered=False)


class TestMultiGerritAllowNeeds(ZuulTestCase):
    config_file = 'zuul-connections-multiple-gerrits.conf'
    tenant_config_file = 'config/multi-gerrit-allow-needs/main.yaml'

    def test_allow_needs(self):
        # Test that we don't apply the allow_needs from one gerrit to
        # another.  We report "ReviewVerified" to the gerrit for the
        # change we're testing, and there's another gerrit where we
        # report "Verified".  Our gerrit also uses the Verified label,
        # but it's for human review and it's a submit requirement.

        # Make sure the use of Verified for zuul in the other gerrit
        # doesn't mess up our Verified submit requirement.
        A = self.fake_review_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.categories = {
            'Approved': ('Approved', -1, 1),
            'Code-Review': ('Code-Review', -2, 2),
            'ReviewVerified': ('ReviewVerified', -2, 2),
        }
        A.setSubmitRequirements([
            {
                "name": "Verified",
                "status": "UNSATISFIED",
                "is_legacy": True,
                "submittability_expression_result": {
                    "expression": "label:Verified=MAX -label:Verified=MIN",
                    "fulfilled": False,
                    "passing_atoms": [],
                    "failing_atoms": [
                        "label:Verified=MAX",
                        "-label:Verified=MIN"
                    ]
                }
            },
        ])

        # First, test it while missing the Verified approval
        self.fake_review_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 0)
        self.assertHistory([])

        # Add the verified approval (well, really remove the submit
        # requirement for it) and test again:
        A.setSubmitRequirements([])
        self.fake_review_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestGerritFake(ZuulTestCase):
    config_file = "zuul-gerrit-github.conf"
    tenant_config_file = "config/circular-dependencies/main.yaml"

    def _make_tuple(self, data):
        ret = []
        for c in data:
            dep_change = c['number']
            dep_ps = c['currentPatchSet']['number']
            ret.append((int(dep_change), int(dep_ps)))
        return sorted(ret)

    def _get_tuple(self, change_number):
        ret = []
        data = self.fake_gerrit.get(
            f'changes/{change_number}/submitted_together')
        for c in data:
            dep_change = c['_number']
            dep_ps = c['revisions'][c['current_revision']]['_number']
            ret.append((dep_change, dep_ps))
        return sorted(ret)

    def test_submitted_together_normal(self):
        # Test that the fake submitted together endpoint returns
        # expected data

        # This test verifies behavior with submitWholeTopic=False

        # A single change
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        data = self._get_tuple(1)
        self.assertEqual(data, [])

        # A dependent series (B->A)
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)
        data = self._get_tuple(2)
        self.assertEqual(data, [(1, 1), (2, 1)])

        # A topic cycle
        self.fake_gerrit.addFakeChange('org/project', 'master', 'C1',
                                       topic='test-topic')
        self.fake_gerrit.addFakeChange('org/project', 'master', 'C2',
                                       topic='test-topic')
        data = self._get_tuple(3)
        self.assertEqual(data, [])

    @gerrit_config(submit_whole_topic=True)
    def test_submitted_together_whole_topic(self):
        # Test that the fake submitted together endpoint returns
        # expected data
        # This test verifies behavior with submitWholeTopic=True

        # A single change
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        data = self._get_tuple(1)
        self.assertEqual(data, [])

        # A dependent series (B->A)
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)
        data = self._get_tuple(2)
        self.assertEqual(data, [(1, 1), (2, 1)])
        # The Gerrit connection method filters out the queried change

        # A topic cycle
        self.fake_gerrit.addFakeChange('org/project', 'master', 'C1',
                                       topic='test-topic')
        self.fake_gerrit.addFakeChange('org/project', 'master', 'C2',
                                       topic='test-topic')
        data = self._get_tuple(3)
        self.assertEqual(data, [(3, 1), (4, 1)])
        # The Gerrit connection method filters out the queried change

        # Test also the query used by the GerritConnection:
        ret = self.fake_gerrit._simpleQuery('status:open topic:test-topic')
        ret = self._make_tuple(ret)
        self.assertEqual(ret, [(3, 1), (4, 1)])


class TestGerritConnection(ZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_zuul_query_ltime(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)

        # We want the three events below to proceed so that all three
        # events are processed simultaneously by three threads.  We
        # will allow the first event we submit to complete its change
        # network and therefore perform all the queries.  The other
        # two events should receive ChangeNetworkConflict errors and
        # eventually use the results of the first network.  To
        # accomplish that, we: Allow the network to proceed if it
        # includes change A (this will be our first network) * Wait
        # for two ChangeNetworkConflict errors to occur
        permission_barrier = threading.Barrier(2)
        permission_event = threading.Event()
        cn_manager = self.fake_gerrit.change_network_manager
        orig_permission = cn_manager.permissionToProceed

        def permissionToProceed(future):
            if permission_event.is_set():
                # We're past the critical section, revert to normal
                # behavior.
                return orig_permission(future)
            try:
                changes = [int(c.stable_id) for c in future.changes]
                if 1 in changes:
                    # This change network includes change A; this is a
                    # characteristic of the first event, so we let it
                    # through.
                    return orig_permission(future)
                for _ in iterate_timeout(30, "queries to occur"):
                    # Wait until the first change network has expanded
                    # to include both changes so that it will
                    # conflict.
                    for f in cn_manager.futures:
                        if len(f.changes) > 1:
                            return orig_permission(future)
            except ChangeNetworkConflict:
                # The second and third events will conflict; once the
                # barrier tells us that they have both gotten to this
                # point, we can allow the first network to finish and
                # these two threads to resume normal operation (they
                # will use the results of their failed networks to try
                # again.
                permission_barrier.wait()
                permission_event.set()
                raise

        self.patch(self.fake_gerrit.change_network_manager,
                   'permissionToProceed', permissionToProceed)

        orig_complete = cn_manager.setComplete

        def setComplete(future):
            # The first change network must wait here until the second
            # and third have seen conflict errors from it, so that it
            # is not removed from the manager before they see the
            # conflict.
            permission_event.wait()
            return orig_complete(future)

        self.patch(self.fake_gerrit.change_network_manager,
                   'setComplete', setComplete)

        # These are the three events with critical ordering described
        # above.
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 2))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Code-Review', 2))

        self.waitUntilSettled()
        self.assertHistory([])
        # One query for each change in the above cluster of events.
        self.assertEqual(A.queried, 1)
        self.assertEqual(B.queried, 1)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name="project-merge", result="SUCCESS", changes="1,1"),
            dict(name="project-test1", result="SUCCESS", changes="1,1"),
            dict(name="project-test2", result="SUCCESS", changes="1,1"),
            dict(name="project-merge", result="SUCCESS", changes="1,1 2,1"),
            dict(name="project-test1", result="SUCCESS", changes="1,1 2,1"),
            dict(name="project-test2", result="SUCCESS", changes="1,1 2,1"),
        ], ordered=False)
        # One query due to the event on change A, followed by a query
        # to verify the merge.
        self.assertEqual(A.queried, 3)
        # No query for change B necessary since our cache is up to
        # date with respect for the triggering event.  One query to
        # verify the merge.
        self.assertEqual(B.queried, 2)
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')

    def test_submit_requirements(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        # Set an unsatisfied submit requirement
        A.setSubmitRequirements([
            {
                "name": "Code-Review",
                "description": "Disallow self-review",
                "status": "UNSATISFIED",
                "is_legacy": False,
                "submittability_expression_result": {
                    "expression": "label:Code-Review=MAX,user=non_uploader "
                        "AND -label:Code-Review=MIN",
                    "fulfilled": False,
                    "passing_atoms": [],
                    "failing_atoms": [
                        "label:Code-Review=MAX,user=non_uploader",
                        "label:Code-Review=MIN"
                    ]
                }
            },
            {
                "name": "Verified",
                "status": "UNSATISFIED",
                "is_legacy": True,
                "submittability_expression_result": {
                    "expression": "label:Verified=MAX -label:Verified=MIN",
                    "fulfilled": False,
                    "passing_atoms": [],
                    "failing_atoms": [
                        "label:Verified=MAX",
                        "-label:Verified=MIN"
                    ]
                }
            },
        ])
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertHistory([])
        self.assertEqual(A.queried, 1)
        self.assertEqual(A.data['status'], 'NEW')

        # Mark the requirement satisfied
        A.setSubmitRequirements([
            {
                "name": "Code-Review",
                "description": "Disallow self-review",
                "status": "SATISFIED",
                "is_legacy": False,
                "submittability_expression_result": {
                    "expression": "label:Code-Review=MAX,user=non_uploader "
                        "AND -label:Code-Review=MIN",
                    "fulfilled": False,
                    "passing_atoms": [
                        "label:Code-Review=MAX,user=non_uploader",
                    ],
                    "failing_atoms": [
                        "label:Code-Review=MIN"
                    ]
                }
            },
            {
                "name": "Verified",
                "status": "UNSATISFIED",
                "is_legacy": True,
                "submittability_expression_result": {
                    "expression": "label:Verified=MAX -label:Verified=MIN",
                    "fulfilled": False,
                    "passing_atoms": [],
                    "failing_atoms": [
                        "label:Verified=MAX",
                        "-label:Verified=MIN"
                    ]
                }
            },
        ])
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name="project-merge", result="SUCCESS", changes="1,1"),
            dict(name="project-test1", result="SUCCESS", changes="1,1"),
            dict(name="project-test2", result="SUCCESS", changes="1,1"),
        ], ordered=False)
        self.assertEqual(A.queried, 3)
        self.assertEqual(A.data['status'], 'MERGED')

    def test_submit_missing_labels(self):
        # This test checks that missing labels will not allow a change to
        # enqueued. Specifically in this case the Code-Review and Verified is
        # missing (but Verified is ignored because Zuul will score it)
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertHistory([])

        # Even adding the Verified score already provided by Zuul will not
        # cause the change to gate
        self.fake_gerrit.addEvent(A.addApproval('Verified', 2))

        # Reapply this score to trigger gate, which shouldn't happen because of
        # the still missing Code-Review label
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertHistory([])

    def test_query_abandoned_changes_check(self):
        # Test that we stop querying when we encounter an abandoned change
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        D = self.fake_gerrit.addFakeChange('org/project', 'master', 'D')

        # D(open) -> C(abandoned) -> B(abandoned) -> A(open)
        D.setDependsOn(C, 1)
        C.setDependsOn(B, 1)
        B.setDependsOn(A, 1)
        C.setAbandoned()
        B.setAbandoned()
        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.queried, 0)
        self.assertEqual(B.queried, 0)
        self.assertEqual(C.queried, 1)
        self.assertEqual(D.queried, 1)

        self.assertHistory([
            dict(name="project-merge", result="SUCCESS", changes="3,1 4,1"),
            dict(name="project-test1", result="SUCCESS", changes="3,1 4,1"),
            dict(name="project-test2", result="SUCCESS", changes="3,1 4,1"),
        ], ordered=False)

    def test_query_abandoned_changes_gate(self):
        # Test that we stop querying when we encounter an abandoned change
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        D = self.fake_gerrit.addFakeChange('org/project', 'master', 'D')

        # D(open) -> C(abandoned) -> B(abandoned) -> A(open)
        D.setDependsOn(C, 1)
        C.setDependsOn(B, 1)
        B.setDependsOn(A, 1)
        C.setAbandoned()
        B.setAbandoned()
        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)
        B.addApproval('Code-Review', 2)
        B.addApproval('Approved', 1)
        C.addApproval('Code-Review', 2)
        C.addApproval('Approved', 1)
        D.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(D.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertEqual(A.queried, 0)
        self.assertEqual(B.queried, 0)
        self.assertEqual(C.queried, 1)
        self.assertEqual(D.queried, 1)

        self.assertHistory([])


class TestGerritConnectionDelay(ZuulTestCase):
    config_file = 'zuul-gerrit-web-delay.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'
    replication_delay = 10

    def test_ref_updated_reconfig(self):
        # Gerrit emits change-merged events after ref-updated events for the
        # change; make sure that job configuration changes take effect
        # for post pipelines that trigger off of ref-updated.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: new-post-job
                parent: project-post
            - project:
                post:
                  jobs:
                    - new-post-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getRefUpdatedEvent())
        # We add a delay here so that we exercise the behavior at the end
        # of the ZK queue.
        time.sleep(1)
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-post', result='SUCCESS'),
            dict(name='new-post-job', result='SUCCESS'),
        ], ordered=False)

    def test_ref_updated_reconfig_immediate(self):
        # Same as the above test, but with no delay between the two
        # events.
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: new-post-job
                parent: project-post
            - project:
                post:
                  jobs:
                    - new-post-job
            """)

        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.setMerged()
        self.fake_gerrit.addEvent(A.getRefUpdatedEvent())
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-post', result='SUCCESS'),
            dict(name='new-post-job', result='SUCCESS'),
        ], ordered=False)

    def test_ref_updated_timeout(self):
        # Test the behavior when the second event is missed
        self.hold_merge_jobs_in_queue = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setMerged()
        start = time.time()
        self.fake_gerrit.addEvent(A.getRefUpdatedEvent())
        self.waitUntilSettled()
        end = time.time()
        elapsed = end - start
        self.log.debug("Elapsed time: %s", elapsed)
        # We are waiting replication_delay + peekqueue timeout to pass
        # for the corresponding change merged event that matches our
        # ref updated event.
        self.assertTrue(10 + self.replication_delay
                        <= elapsed <
                        20 + self.replication_delay)
        self.hold_merge_jobs_in_queue = False
        self.merger_api.release()
        self.waitUntilSettled()

    def test_replication_delay_one_change(self):
        # Test that a single change uses the correct replication delay
        self.hold_merge_jobs_in_queue = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        start = time.time()
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        end = time.time()
        elapsed = end - start
        self.log.debug("Elapsed time: %s", elapsed)
        self.assertTrue(self.replication_delay
                        <= elapsed <
                        self.replication_delay + 10)
        self.hold_merge_jobs_in_queue = False
        self.merger_api.release()
        self.waitUntilSettled()

    def test_replication_delay_two_changes(self):
        # Test that two changes are collectively delayed only 10 seconds
        self.hold_merge_jobs_in_queue = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        start = time.time()
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        end = time.time()
        elapsed = end - start
        self.log.debug("Elapsed time: %s", elapsed)
        self.assertTrue(self.replication_delay
                        <= elapsed <
                        self.replication_delay + 10)
        self.hold_merge_jobs_in_queue = False
        self.merger_api.release()
        self.waitUntilSettled()


class TestGerritConnectionNoDelay(TestGerritConnectionDelay):
    config_file = 'zuul-gerrit-web.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'
    replication_delay = 0


class TestGerritConnectionPreFilter(ZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'
    tenant_config_file = 'config/pre-filter/main.yaml'

    def test_ignored_events(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        event_queue = self.fake_gerrit.gerrit_event_connector.event_queue

        # Reconfigure the second tenant to ensure that it doesn't
        # delete the pre-filters for the first tenant.
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-two')
        (trusted, project1) = tenant.getProject('org/project2')
        event = zuul.model.TriggerEvent()
        event.zuul_event_ltime = -1
        self.scheds.first.sched.reconfigureTenant(
            self.scheds.first.sched.abide.tenants['tenant-two'],
            project1, event)
        self.waitUntilSettled()

        # For efficiency, ensure that the total filter length is the
        # number of filters in the first tenant plus 1 (there is one
        # unique filter in the second tenant).
        self.assertEqual(6, len(self.fake_gerrit.watched_event_filters))

        # Stop the event connector so it does not pull any events from
        # the queue
        self.fake_gerrit.stopEventConnector()
        # This is ignored unconditionally
        self.fake_gerrit.addEvent({
            "type": "cache-eviction",
        })
        self.assertEqual(0, len(event_queue._listEvents()))
        # This is ignored for meta refs only
        self.fake_gerrit.addEvent({
            "type": "ref-updated",
            "submitter": {
                "name": "User Name",
            },
            "refUpdate": {
                "oldRev": '0',
                "newRev": '0',
                "refName": 'refs/changes/01/1/meta',
                "project": 'org/project1',
            }
        })
        self.assertEqual(0, len(event_queue._listEvents()))
        # This is ignored because the ref doesn't match the pre-filter
        self.fake_gerrit.addEvent({
            "type": "ref-updated",
            "submitter": {
                "name": "User Name",
            },
            "refUpdate": {
                "oldRev": '0',
                "newRev": '0',
                "refName": 'refs/something/else',
                "project": 'org/project1',
            }
        })
        self.assertEqual(0, len(event_queue._listEvents()))
        # This is not ignored
        self.fake_gerrit.addEvent({
            "type": "ref-updated",
            "submitter": {
                "name": "User Name",
            },
            "refUpdate": {
                "oldRev": '0',
                "newRev": 'abcd',
                "refName": 'refs/tags/1234',
                "project": 'org/project1',
            }
        })
        self.assertEqual(1, len(event_queue._listEvents()))
        # This is not ignored
        A.setMerged()
        self.fake_gerrit.addEvent(A.getRefUpdatedEvent())
        self.assertEqual(2, len(event_queue._listEvents()))

        self.fake_gerrit.startEventConnector()
        self.waitUntilSettled()


class TestGerritUnicodeRefs(ZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'

    upload_pack_data = (b'014452944ee370db5c87691e62e0f9079b6281319b4e HEAD'
                        b'\x00multi_ack thin-pack side-band side-band-64k '
                        b'ofs-delta shallow deepen-since deepen-not '
                        b'deepen-relative no-progress include-tag '
                        b'multi_ack_detailed allow-tip-sha1-in-want '
                        b'allow-reachable-sha1-in-want '
                        b'symref=HEAD:refs/heads/faster filter '
                        b'object-format=sha1 agent=git/2.37.1.gl1\n'
                        b'003d5f42665d737b3fd4ec22ca0209e6191859f09fd6 '
                        b'refs/for/faster\n'
                        b'004952944ee370db5c87691e62e0f9079b6281319b4e '
                        b'refs/heads/foo/\xf0\x9f\x94\xa5\xf0\x9f\x94\xa5'
                        b'\xf0\x9f\x94\xa5\n'
                        b'003f52944ee370db5c87691e62e0f9079b6281319b4e '
                        b'refs/heads/faster\n0000').decode("utf-8")

    def test_mb_unicode_refs(self):
        gerrit_config = {
            'user': 'gerrit',
            'server': 'localhost',
        }
        driver = GerritDriver()
        gerrit = GerritConnection(driver, 'review_gerrit', gerrit_config)

        def _uploadPack(project):
            return self.upload_pack_data

        self.patch(gerrit, '_uploadPack', _uploadPack)

        project = gerrit.source.getProject('org/project')
        refs = gerrit.getInfoRefs(project)

        self.assertEqual(refs,
                         {'refs/for/faster':
                          '5f42665d737b3fd4ec22ca0209e6191859f09fd6',
                          'refs/heads/foo/🔥🔥🔥':
                          '52944ee370db5c87691e62e0f9079b6281319b4e',
                          'refs/heads/faster':
                          '52944ee370db5c87691e62e0f9079b6281319b4e'})


class TestGerritDefaultBranch(ZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'
    scheduler_count = 1

    def test_default_branch_changed(self):
        """Test the project-head-updated event"""
        self.waitUntilSettled()

        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        md = layout.getProjectMetadata('review.example.com/org/project1')
        self.assertEqual('master', md.default_branch)
        prev_layout = layout.uuid

        self.fake_gerrit._fake_project_default_branch[
            'org/project1'] = 'foobar'
        self.fake_gerrit.addEvent(self.fake_gerrit.getProjectHeadUpdatedEvent(
            project='org/project1',
            old='master',
            new='foobar'
        ))
        self.waitUntilSettled()

        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        md = layout.getProjectMetadata('review.example.com/org/project1')
        self.assertEqual('foobar', md.default_branch)
        new_layout = layout.uuid
        self.assertNotEqual(new_layout, prev_layout)


class TestGerritMaxDeps(ZuulTestCase):
    config_file = 'zuul-gerrit-max-deps.conf'

    @simple_layout('layouts/simple.yaml')
    def test_max_deps(self):
        # max_dependencies for the connection is 1, so this is okay
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ])

        # So is this
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.setDependsOn(A, 1)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='1,1 2,1'),
        ])

        # This is not
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.data['commitMessage'] = '%s\n\nDepends-On: %s\nDepends-On: %s\n' % (
            C.subject, A.data['id'], B.data['id'])
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='1,1 2,1'),
        ])


class TestGerritDriver(ZuulTestCase):
    # Most of the Zuul test suite tests the Gerrit driver, to some
    # extent.  The other classes in this file test specific methods of
    # Zuul interacting with Gerrit.  But the other drivers also test
    # some basic driver functionality that, if tested for Gerrit at
    # all, is spread out in random tests.  This class adds some
    # (potentially duplicative) testing to validate parity with the
    # other drivers.

    @simple_layout('layouts/simple.yaml')
    def test_change_event(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           topic='test-topic')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ])

        job = self.getJobFromHistory('check-job')
        zuulvars = job.parameters['zuul']
        self.assertEqual(str(A.number), zuulvars['change'])
        self.assertEqual('1', zuulvars['patchset'])
        self.assertEqual(str(A.patchsets[-1]['revision']),
                         zuulvars['commit_id'])
        self.assertEqual('master', zuulvars['branch'])
        self.assertEqual('https://review.example.com/1',
                         zuulvars['items'][0]['change_url'])
        self.assertEqual(zuulvars["message"], strings.b64encode('A'))
        self.assertEqual(1, len(self.history))
        self.assertEqual(1, len(A.messages))
        self.assertEqual('test-topic', zuulvars['topic'])

    @simple_layout('layouts/simple.yaml')
    def test_tag_event(self):
        event = self.fake_gerrit.addFakeTag('org/project', 'master', 'foo')
        tagsha = event['refUpdate']['newRev']
        self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='tag-job', result='SUCCESS', ref='refs/tags/foo'),
        ])

        job = self.getJobFromHistory('tag-job')
        zuulvars = job.parameters['zuul']
        zuulvars = job.parameters['zuul']
        self.assertEqual('refs/tags/foo', zuulvars['ref'])
        self.assertEqual('tag', zuulvars['pipeline'])
        self.assertEqual('tag-job', zuulvars['job'])
        self.assertEqual(tagsha, zuulvars['newrev'])
        self.assertEqual(tagsha, zuulvars['commit_id'])

    @simple_layout('layouts/gerrit-hashtags.yaml')
    def test_hashtags_event(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.data['hashtags'] = ['check']
        self.fake_gerrit.addEvent(A.getHashtagsChangedEvent(added=['check']))
        self.waitUntilSettled()

        # Does not meet pipeline requirement
        self.assertHistory([])

        A.data['hashtags'] = ['okay', 'check']
        self.fake_gerrit.addEvent(A.getHashtagsChangedEvent(added=['check']))
        self.waitUntilSettled()

        # This should work
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ])

        # Matches reject
        A.data['hashtags'] = ['okay', 'check', 'nope']
        self.fake_gerrit.addEvent(A.getHashtagsChangedEvent(
            removed=['nocheck']))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ])

    @simple_layout('layouts/gerrit-approval-change.yaml')
    def test_approval_change(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.data['hashtags'] = ['check']
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 2))
        self.waitUntilSettled()

        # Does not meet pipeline requirements, because old value is not present
        self.assertHistory([])

        # Still not sufficient because old value matches new value
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 2, old_value=2))
        self.waitUntilSettled()
        self.assertHistory([])

        # Old value present, but new value is insufficient
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 0, old_value=2))
        self.waitUntilSettled()
        self.assertHistory([])

        # This should work now
        self.fake_gerrit.addEvent(A.addApproval('Code-Review', 2, old_value=0))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ])

    @simple_layout('layouts/gerrit-notify.yaml')
    def test_notify(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS'),
        ])

        self.assertEqual(A.notify, 'NONE')


class TestGerritCherryPickSSH(ZuulTestCase):
    @simple_layout('layouts/cherry-pick.yaml')
    def test_gerrit_cherry_pick_ssh(self):
        # Test the simple case case where we approve A then B.
        # Because the gerrit driver doesn't store the new patchset
        # that is created on a cherry-pick at the time of submission,
        # this test actually works because we intentionally
        # erroneously mark the old revision as merged.  So when we
        # enqueue B, it sees its dependency of 1,1 as merged (even
        # though 1,2 was what was actually created-and-merged.  This
        # test is here to ensure that works.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.cherry_pick = True
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.cherry_pick = True
        B.setDependsOn(A, 1)
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ])

        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='2,1'),
        ])
    # Unlike the web test below, we are unable to handle a case where
    # we add a patchset to change A before enqueing B, because the ssh
    # query does not tell us the current patchset of dependencies, so
    # there is no corresponding test for the ssh code path.


class TestGerritCherryPickWeb(ZuulTestCase):
    config_file = 'zuul-gerrit-web.conf'

    @simple_layout('layouts/cherry-pick.yaml')
    def test_gerrit_cherry_pick_web(self):
        # Test the simple case case where we approve A then B.
        # Because the gerrit driver doesn't store the new patchset
        # that is created on a cherry-pick at the time of submission,
        # this test actually works because we intentionally
        # erroneously mark the old revision as merged.  So when we
        # enqueue B, it sees its dependency of 1,1 as merged (even
        # though 1,2 was what was actually created-and-merged.  This
        # test is here to ensure that works.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.cherry_pick = True
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.cherry_pick = True
        B.setDependsOn(A, 1)
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ])

        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='2,1'),
        ])

    @simple_layout('layouts/cherry-pick.yaml')
    def test_gerrit_cherry_pick_web_with_update(self):
        # Test updating A behind B's back then approving B.
        # Unlike the test above, this test forces the issue of
        # patchset updates by having the user create a second patchset
        # on A before anything is enqueued.  But B still depends on
        # the first patchset of A.  So when the gerrit driver
        # intentionally erroneously marks 1,2 as merged, that won't
        # help B since it points at 1,1.  However, the web query code
        # path will update B's dependency to point at 1,2 in the case
        # of a cherry-pick, and we will query 1,2 and discover that it
        # is merged, so B can proceed.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.cherry_pick = True
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.cherry_pick = True
        # Set the dependency on the first patchset of A
        B.setDependsOn(A, 1)
        # Then add a new patchset to A, so that B points to an old
        # version.
        A.addPatchset()
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,2'),
        ])

        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,2'),
            dict(name='check-job', result='SUCCESS', changes='2,1'),
        ])

    @simple_layout('layouts/cherry-pick.yaml')
    def test_gerrit_cherry_pick_web_with_update_queue(self):
        # Test that two cherry-pick changes enqued simultaneously work
        # This test should exercise the code path where the second
        # change remains blissfully unaware that there ever was a
        # 1,3 patchset.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.cherry_pick = True
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.cherry_pick = True
        # Set the dependency on the first patchset of A
        B.setDependsOn(A, 1)
        # Then add a new patchset to A, so that B points to an old
        # version.
        A.addPatchset()
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        B.addApproval('Approved', 1)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,2'),
            dict(name='check-job', result='SUCCESS', changes='1,2 2,1'),
        ], ordered=False)


class TestGerritPeekQueue(BaseTestCase):
    def make_ref_updated_event(self, ref, newrev, ltime):
        e = zuul.model.ConnectionEvent({
            "timestamp": time.time(),
            "payload": {
                "refUpdate": {
                    "oldRev": "old",
                    "newRev": newrev,
                    "refName": ref,
                    "project": "org/project"
                },
                "type": "ref-updated",
            }
        })
        e.zuul_event_ltime = ltime
        return e

    def make_change_merged_event(self, number, newrev, ltime):
        e = zuul.model.ConnectionEvent({
            "timestamp": time.time(),
            "payload": {
                "newRev": newrev,
                "patchSet": {
                    "number": 1,
                },
                "change": {
                    "project": "org/project",
                    "branch": "master",
                    "number": number,
                },
                "type": "change-merged",
            }
        })
        e.zuul_event_ltime = ltime
        return e

    def test_peek_queue(self):
        handled = []

        def handler(x):
            handled.append(x)

        q = PeekQueue(handler, 0)

        # Check noop
        q.run()
        self.assertEqual([], handled)

        # Check all at once (even though this shouldn't happen
        orig = [
            self.make_ref_updated_event('refs/heads/master', 'new1', 1),
            self.make_ref_updated_event('refs/meta/foo', 'foo', 2),
            self.make_change_merged_event(1, 'new1', 3),
        ]
        [q.append(x) for x in orig]
        q.run()
        expected = [orig[2], orig[0], orig[1]]
        self.assertEqual(expected, handled)
        self.assertEqual(1, handled[0].zuul_event_ltime)
        self.assertEqual(1, handled[1].zuul_event_ltime)
        self.assertEqual(2, handled[2].zuul_event_ltime)

        # Check one at a time (typical case)
        handled.clear()
        orig = [
            self.make_ref_updated_event('refs/heads/master', 'new1', 1),
            self.make_ref_updated_event('refs/meta/foo', 'foo', 2),
            self.make_change_merged_event(1, 'new1', 3),
        ]
        q.append(orig[0])
        q.run()
        self.assertEqual([], handled)

        q.append(orig[1])
        q.run()
        self.assertEqual([], handled)

        q.append(orig[2])
        q.run()
        expected = [orig[2], orig[0], orig[1]]
        self.assertEqual(expected, handled)
        self.assertEqual(1, handled[0].zuul_event_ltime)
        self.assertEqual(1, handled[1].zuul_event_ltime)
        self.assertEqual(2, handled[2].zuul_event_ltime)

        # Check that other events arrive immediately
        handled.clear()

        q.append(orig[1])
        q.run()
        expected = [orig[1]]
        self.assertEqual(expected, handled)

        # Check missing change-merged event
        handled.clear()
        orig = [
            self.make_ref_updated_event('refs/heads/master', 'new1', 1),
            self.make_ref_updated_event('refs/meta/foo', 'foo', 2),
        ]
        q.append(orig[0])
        q.run()
        self.assertEqual([], handled)

        q.append(orig[1])
        q.run()
        self.assertEqual([], handled)

        # This is what the loop will acutally do
        q.run()
        # Normally we pass the current time in, but in the test we
        # pass in a future time to accelerate the timout (the peek
        # queue never consults the current time on its own).
        q.run(cutoff=time.time() + 10)
        expected = [orig[0], orig[1]]
        self.assertEqual(expected, handled)
        self.assertEqual(1, handled[0].zuul_event_ltime)
        self.assertEqual(2, handled[1].zuul_event_ltime)

        # Check if Gerrit changes the event order; it doesn't do this
        # today, but we want to defend against that.
        handled.clear()
        orig = [
            self.make_change_merged_event(1, 'new1', 1),
            self.make_ref_updated_event('refs/heads/master', 'new1', 2),
            self.make_ref_updated_event('refs/meta/foo', 'foo', 3),
        ]
        q.append(orig[0])
        q.run()
        expected = [orig[0]]
        self.assertEqual(expected, handled)

        q.append(orig[1])
        q.run()
        expected = [orig[0], orig[1]]
        self.assertEqual(expected, handled)

        q.append(orig[2])
        q.run()
        expected = [orig[0], orig[1], orig[2]]
        self.assertEqual(expected, handled)
        self.assertEqual(1, handled[0].zuul_event_ltime)
        self.assertEqual(2, handled[1].zuul_event_ltime)
        self.assertEqual(3, handled[2].zuul_event_ltime)

    def test_peek_queue_busy_timeout(self):
        # Our last line of defense on a busy system is the extra 10
        # second timeout -- if we see an event that's 10 seconds later
        # than the event we're waiting for, then we'll give up.  If we
        # exhaust the event queue in zookeeper though, we'll give up
        # earlier; that's tested in the normal test_peek_queue test.
        handled = []

        def handler(x):
            handled.append(x)

        q = PeekQueue(handler, 0)

        orig = [
            self.make_ref_updated_event('refs/heads/master', 'new1', 1),
        ]
        [q.append(x) for x in orig]
        q.run()
        expected = []
        self.assertEqual(expected, handled)

        q.timeout = 0
        q.run()
        expected = [orig[0]]
        self.assertEqual(expected, handled)
