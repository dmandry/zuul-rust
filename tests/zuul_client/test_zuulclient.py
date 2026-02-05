# Copyright 2020 Red Hat, inc.
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

import time
import os
import subprocess
import tempfile
import textwrap

from kazoo.exceptions import NoNodeError

import zuul.model
from zuul.lib import yamlutil

from tests.base import (
    AnsibleZuulTestCase,
    iterate_timeout,
    simple_layout,
)
from tests.unit.test_web import BaseTestWeb
from tests.unit.test_launcher import LauncherBaseTestCase


def _split_pretty_table(output):
    lines = output.decode().split('\n')
    headers = [x.strip() for x in lines[1].split('|') if x != '']
    # Trim headers and last line of the table
    return [dict(zip(headers,
                     [x.strip() for x in l.split('|') if x != '']))
            for l in lines[3:-2]]


def _split_line_output(output):
    lines = output.decode().split('\n')
    info = {}
    for l in lines:
        if l.startswith('==='):
            continue
        try:
            key, value = l.split(':', 1)
            info[key] = value.strip()
        except ValueError:
            continue
    return info


class TestSmokeZuulClient(BaseTestWeb):
    def test_is_installed(self):
        """Test that the CLI is installed"""
        test_version = subprocess.check_output(
            ['zuul-client', '--version'],
            stderr=subprocess.STDOUT)
        self.assertTrue(b'Zuul-client version:' in test_version)


class TestZuulClientEncrypt(BaseTestWeb):
    """Test using zuul-client to encrypt secrets"""
    tenant_config_file = 'config/secrets/main.yaml'
    config_file = 'zuul-admin-web.conf'
    secret = {'password': 'zuul-client'}
    large_secret = {'key': (('a' * 79 + '\n') * 50)[:-1]}

    def setUp(self):
        super(TestZuulClientEncrypt, self).setUp()
        self.executor_server.hold_jobs_in_build = False

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

    def _getClientCommand(self, *args):
        return ['zuul-client',
                '--zuul-url', self.base_url,
                *args]

    def test_encrypt_large_secret(self):
        """Test that we can use zuul-client to encrypt a large secret"""
        p = subprocess.Popen(
            self._getClientCommand(
                'encrypt',
                '--tenant', 'tenant-one', '--project', 'org/project2',
                '--secret-name', 'my_secret', '--field-name', 'key'),
            stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        p.stdin.write(
            str.encode(self.large_secret['key'])
        )
        output, error = p.communicate()
        p.stdin.close()
        self._test_encrypt(self.large_secret, output, error)

    def test_encrypt(self):
        """Test that we can use zuul-client to generate a project secret"""
        p = subprocess.Popen(
            self._getClientCommand(
                'encrypt',
                '--tenant', 'tenant-one', '--project', 'org/project2',
                '--secret-name', 'my_secret', '--field-name', 'password'),
            stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        p.stdin.write(
            str.encode(self.secret['password'])
        )
        output, error = p.communicate()
        p.stdin.close()
        self._test_encrypt(self.secret, output, error)

    def test_encrypt_outfile(self):
        """Test that we can use zuul-client to generate a project secret to a
        file"""
        outfile = tempfile.NamedTemporaryFile(delete=False)
        p = subprocess.Popen(
            self._getClientCommand(
                'encrypt',
                '--tenant', 'tenant-one', '--project', 'org/project2',
                '--secret-name', 'my_secret', '--field-name', 'password',
                '--outfile', outfile.name),
            stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        p.stdin.write(
            str.encode(self.secret['password'])
        )
        _, error = p.communicate()
        p.stdin.close()
        output = outfile.read()
        self._test_encrypt(self.secret, output, error)

    def test_encrypt_infile(self):
        """Test that we can use zuul-client to generate a project secret from
        a file"""
        infile = tempfile.NamedTemporaryFile(delete=False)
        infile.write(
            str.encode(self.secret['password'])
        )
        infile.close()
        p = subprocess.Popen(
            self._getClientCommand(
                'encrypt',
                '--tenant', 'tenant-one', '--project', 'org/project2',
                '--secret-name', 'my_secret', '--field-name', 'password',
                '--infile', infile.name),
            stdout=subprocess.PIPE)
        output, error = p.communicate()
        os.unlink(infile.name)
        self._test_encrypt(self.secret, output, error)

    def _test_encrypt(self, _secret, output, error):
        self.assertEqual(None, error, error)
        self.assertTrue(b'- secret:' in output, output.decode())
        new_repo_conf = output.decode()
        new_repo_conf += textwrap.dedent(
            """

            - job:
                parent: base
                name: project2-secret
                run: playbooks/secret.yaml
                secrets:
                  - my_secret

            - project:
                check:
                  jobs:
                    - project2-secret
                gate:
                  jobs:
                    - noop
            """
        )
        file_dict = {'zuul.yaml': new_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project2', 'master',
                                           'Add secret',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        # check that the secret is used from there on
        B = self.fake_gerrit.addFakeChange('org/project2', 'master',
                                           'test secret',
                                           files={'newfile': 'xxx'})
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(B.reported, 1, "B should report success")
        self.assertHistory([
            dict(name='project2-secret', result='SUCCESS', changes='2,1'),
        ])
        secrets = self._getSecrets('project2-secret', 'playbooks')
        self.assertEqual(
            secrets,
            [{'my_secret': _secret}],
            secrets)


class TestZuulClientAdmin(BaseTestWeb):
    """Test the admin commands of zuul-client"""
    config_file = 'zuul-admin-web.conf'

    def test_autohold(self):
        """Test that autohold can be set with the Web client"""
        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'autohold', '--reason', 'some reason',
             '--tenant', 'tenant-one', '--project', 'org/project',
             '--job', 'project-test2', '--count', '1'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        # Check result
        resp = self.get_url(
            "api/tenant/tenant-one/autohold",
            headers={'Authorization': 'Bearer %s' % token})
        self.assertEqual(200, resp.status_code, resp.text)
        autohold_requests = resp.json()
        self.assertNotEqual([], autohold_requests)
        self.assertEqual(1, len(autohold_requests))
        request = autohold_requests[0]
        self.assertEqual('tenant-one', request['tenant'])
        self.assertIn('org/project', request['project'])
        self.assertEqual('project-test2', request['job'])
        self.assertEqual(".*", request['ref_filter'])
        self.assertEqual("some reason", request['reason'])
        self.assertEqual(1, request['max_count'])

    def test_enqueue(self):
        """Test that the Web client can enqueue a change"""
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        A.addApproval('Approved', 1)

        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'enqueue', '--tenant', 'tenant-one',
             '--project', 'org/project',
             '--pipeline', 'gate', '--change', '1,1'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        self.waitUntilSettled()
        # Check the build history for our enqueued build
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        # project-merge, project-test1, project-test2 in SUCCESS
        self.assertEqual(self.countJobResults(self.history, 'SUCCESS'), 3)

    def test_enqueue_ref(self):
        """Test that the Web client can enqueue a ref"""
        self.executor_server.hold_jobs_in_build = True
        p = "review.example.com/org/project"
        upstream = self.getUpstreamRepos([p])
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setMerged()
        A_commit = str(upstream[p].commit('master'))
        self.log.debug("A commit: %s" % A_commit)

        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'enqueue-ref', '--tenant', 'tenant-one',
             '--project', 'org/project',
             '--pipeline', 'post', '--ref', 'master',
             '--oldrev', '90f173846e3af9154517b88543ffbd1691f31366',
             '--newrev', A_commit],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        self.waitUntilSettled()
        # Check the build history for our enqueued build
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(self.countJobResults(self.history, 'SUCCESS'), 1)

    def test_dequeue(self):
        """Test that the Web client can dequeue a change"""
        self.executor_server.hold_jobs_in_build = True
        start_builds = len(self.builds)
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.executor_server.hold_jobs_in_build = True
        self.commitConfigUpdate('common-config', 'layouts/timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        for _ in iterate_timeout(30, 'Wait for a build on hold'):
            if len(self.builds) > start_builds:
                break
        self.waitUntilSettled()

        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'dequeue', '--tenant', 'tenant-one', '--project', 'org/project',
             '--pipeline', 'periodic', '--ref', 'refs/heads/stable'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        self.waitUntilSettled()

        self.commitConfigUpdate('common-config',
                                'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(self.countJobResults(self.history, 'ABORTED'), 1)

    def test_promote(self):
        "Test that the Web client can promote a change"
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        C.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))

        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'gate')
        enqueue_times = {}
        for item in items:
            enqueue_times[str(item.changes[0])] = item.enqueue_time

        # Promote B and C using the cli
        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'promote', '--tenant', 'tenant-one',
             '--pipeline', 'gate', '--changes', '2,1', '3,1'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        self.waitUntilSettled()

        # ensure that enqueue times are durable
        items = self.getAllItems('tenant-one', 'gate')
        for item in items:
            self.assertEqual(
                enqueue_times[str(item.changes[0])], item.enqueue_time)

        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 6)
        self.assertEqual(self.builds[0].name, 'project-test1')
        self.assertEqual(self.builds[1].name, 'project-test2')
        self.assertEqual(self.builds[2].name, 'project-test1')
        self.assertEqual(self.builds[3].name, 'project-test2')
        self.assertEqual(self.builds[4].name, 'project-test1')
        self.assertEqual(self.builds[5].name, 'project-test2')

        self.assertTrue(self.builds[0].hasChanges(B))
        self.assertFalse(self.builds[0].hasChanges(A))
        self.assertFalse(self.builds[0].hasChanges(C))

        self.assertTrue(self.builds[2].hasChanges(B))
        self.assertTrue(self.builds[2].hasChanges(C))
        self.assertFalse(self.builds[2].hasChanges(A))

        self.assertTrue(self.builds[4].hasChanges(B))
        self.assertTrue(self.builds[4].hasChanges(C))
        self.assertTrue(self.builds[4].hasChanges(A))

        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.data['status'], 'MERGED')
        self.assertEqual(C.reported, 2)

    def test_manage_events(self):
        self.executor_server.hold_jobs_in_build = False
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'manage-events', '--tenant', 'tenant-one',
             'pause-trigger',
             '--reason', 'test trigger paused'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        time.sleep(1)

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        time.sleep(5)

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(0, len(self.builds))

        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'manage-events', '--tenant', 'tenant-one',
             'pause-result',
             '--reason', 'test result paused'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)

        time.sleep(5)
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(0, len(self.builds))

        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'manage-events', '--all-tenants', 'normal'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)

        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-merge', result='SUCCESS', changes='2,1'),
            dict(name='project-test1', result='SUCCESS', changes='2,1'),
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
        ], ordered=False)

        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token, '-v',
             'manage-events', '--all-tenants', 'discard-trigger'],
            stdout=subprocess.PIPE)
        output = p.communicate()
        self.assertEqual(p.returncode, 0, output)

        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-merge', result='SUCCESS', changes='2,1'),
            dict(name='project-test1', result='SUCCESS', changes='2,1'),
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
        ], ordered=False)


class TestZuulClientQueryData(BaseTestWeb):
    """Test that zuul-client can fetch builds"""
    config_file = 'zuul-sql-driver-mysql.conf'
    tenant_config_file = 'config/sql-driver/main.yaml'

    def setUp(self):
        super(TestZuulClientQueryData, self).setUp()
        self.add_base_changes()

    def add_base_changes(self):
        # change on org/project will run 5 jobs in check
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        # fail project-merge on PS1; its 2 dependent jobs will be skipped
        self.executor_server.failJob('project-merge', B)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = True
        B.addPatchset()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(2))
        # change on org/project1 will run 3 jobs in check
        self.waitUntilSettled()
        # changes on both projects will run 3 jobs in gate each
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()


class TestZuulClientBuilds(TestZuulClientQueryData,
                           AnsibleZuulTestCase):
    """Test that zuul-client can fetch builds"""
    def test_get_builds(self):
        """Test querying builds"""

        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'builds', '--tenant', 'tenant-one', ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        self.assertEqual(17, len(results), results)

        # 5 jobs in check, 3 jobs in gate
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'builds', '--tenant', 'tenant-one', '--project', 'org/project', ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        self.assertEqual(8, len(results), results)
        self.assertTrue(all(x['Project'] == 'org/project' for x in results),
                        results)

        # project-test1 is run 3 times in check, 2 times in gate
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'builds', '--tenant', 'tenant-one', '--job', 'project-test1', ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        self.assertEqual(5, len(results), results)
        self.assertTrue(all(x['Job'] == 'project-test1' for x in results),
                        results)

        # 3 builds in check for 2,1; 3 in check + 3 in gate for 2,2
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'builds', '--tenant', 'tenant-one', '--change', '2', ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        self.assertEqual(9, len(results), results)
        self.assertTrue(all(x['Change or Ref'].startswith('2,')
                            for x in results),
                        results)

        # 1,3 does not exist
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'builds', '--tenant', 'tenant-one', '--change', '1',
             '--ref', '3', ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        self.assertEqual(0, len(results), results)

        for result in ['SUCCESS', 'FAILURE']:
            p = subprocess.Popen(
                ['zuul-client',
                 '--zuul-url', self.base_url,
                 'builds', '--tenant', 'tenant-one', '--result', result, ],
                stdout=subprocess.PIPE)
            job_count = self.countJobResults(self.history, result)
            # noop job not included, must be added
            if result == 'SUCCESS':
                job_count += 1
            output, err = p.communicate()
            self.assertEqual(p.returncode, 0, output)
            results = _split_pretty_table(output)
            self.assertEqual(job_count, len(results), results)
            if len(results) > 0:
                self.assertTrue(all(x['Result'] == result for x in results),
                                results)

        # 6 jobs in gate
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'builds', '--tenant', 'tenant-one', '--pipeline', 'gate', ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        self.assertEqual(6, len(results), results)
        self.assertTrue(all(x['Pipeline'] == 'gate' for x in results),
                        results)


class TestZuulClientBuildInfo(TestZuulClientQueryData,
                              AnsibleZuulTestCase):
    """Test that zuul-client can fetch a build's details"""
    def test_get_build_info(self):
        """Test querying a specific build"""

        test_build = self.history[-1]

        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'build-info', '--tenant', 'tenant-one',
             '--uuid', test_build.uuid],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, (output, err))
        info = _split_line_output(output)
        self.assertEqual(test_build.uuid, info.get('UUID'), test_build)
        self.assertEqual(test_build.result, info.get('Result'), test_build)
        self.assertEqual(test_build.name, info.get('Job'), test_build)

    def test_get_build_artifacts(self):
        """Test querying a specific build's artifacts"""
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'builds', '--tenant', 'tenant-one', '--job', 'project-test1',
             '--limit', '1'],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        uuid = results[0]['ID']
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'build-info', '--tenant', 'tenant-one',
             '--uuid', uuid,
             '--show-artifacts'],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, (output, err))
        artifacts = _split_pretty_table(output)
        self.assertTrue(
            any(x['name'] == 'tarball' and
                x['url'] == 'http://example.com/tarball'
                for x in artifacts),
            output)
        self.assertTrue(
            any(x['name'] == 'docs' and
                x['url'] == 'http://example.com/docs'
                for x in artifacts),
            output)


class TestZuulClientJobGraph(BaseTestWeb):

    def test_job_graph(self):
        """Test the job-graph command"""
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'job-graph',
             '--tenant', 'tenant-one',
             '--pipeline', 'check',
             '--project', 'org/project1',
             '--branch', 'master',
             ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, (output, err))
        results = _split_pretty_table(output)
        expected = [
            {'Job': 'project-merge', 'Dependencies': ''},
            {'Job': 'project-test1', 'Dependencies': 'project-merge'},
            {'Job': 'project-test2', 'Dependencies': 'project-merge'},
            {'Job': 'project1-project2-integration',
             'Dependencies': 'project-merge'}
        ]
        self.assertEqual(results, expected)

    def test_job_graph_dot(self):
        """Test the job-graph command dot output"""
        p = subprocess.Popen(
            ['zuul-client',
             '--format', 'dot',
             '--zuul-url', self.base_url,
             'job-graph',
             '--tenant', 'tenant-one',
             '--pipeline', 'check',
             '--project', 'org/project1',
             '--branch', 'master',
             ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, (output, err))
        expected = textwrap.dedent('''\
        digraph job_graph {
          rankdir=LR;
          node [shape=box];
          "project-merge";
          "project-merge" -> "project-test1" [dir=back];
          "project-merge" -> "project-test2" [dir=back];
          "project-merge" -> "project1-project2-integration" [dir=back];
        }
        ''').encode('utf8')
        self.assertEqual(output.strip(), expected.strip())


class TestZuulClientFreezeJob(BaseTestWeb):
    def test_freeze_job(self):
        """Test the freeze-job command"""
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'freeze-job',
             '--tenant', 'tenant-one',
             '--pipeline', 'check',
             '--project', 'org/project1',
             '--branch', 'master',
             '--job', 'project-test1',
             ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, (output, err))
        output = output.decode('utf8')
        for s in [
                'Job: project-test1',
                'Branch: master',
                'Ansible Version:',
                'Workspace Scheme: golang',
                ('gerrit:common-config:playbooks/project-test1.yaml'
                 '@master [trusted]'),
        ]:
            self.assertIn(s, output)


class TestZuulClientAdminWithAccessRules(TestZuulClientAdmin):
    """Test the admin commands of zuul-client with access rules"""
    config_file = 'zuul-admin-web.conf'
    tenant_config_file = 'config/single-tenant/main-access-rules.yaml'
    default_token_groups = ['users']


class TestZuulClientEncryptWithAccessRules(TestZuulClientEncrypt):
    """Test using zuul-client to encrypt secrets"""
    tenant_config_file = 'config/secrets/main-access-rules.yaml'

    def _getClientCommand(self, *args):
        token = self._getToken()
        return ['zuul-client',
                '--zuul-url', self.base_url,
                '--auth-token', token, '-v',
                *args]


class TestZuulClientNodesets(LauncherBaseTestCase, BaseTestWeb):
    """Test that zuul-client can fetch nodeset info"""
    config_file = 'zuul-connections-nodepool.conf'

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_node_list(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'node-list', '--tenant', 'tenant-one',
             ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        self.assertEqual(1, len(results), results)
        r = results[0]
        self.assertIsNotNone(r['UUID'])
        self.assertEqual("['debian-normal']", r['Labels'])
        self.assertEqual('ssh', r['Connection'])
        self.assertEqual(
            'review.example.com%2Forg%2Fcommon-config/aws-us-east-1-main',
            r['Provider'])
        self.assertEqual('in-use', r['State'])
        self.assertIsNotNone(r['State Time'])

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_nodeset_request_list(self):
        self.hold_nodeset_requests_in_queue = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url,
             'nodeset-request-list', '--tenant', 'tenant-one',
             ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        results = _split_pretty_table(output)
        self.assertEqual(1, len(results), results)
        r = results[0]
        self.assertIsNotNone(r['UUID'])
        self.assertEqual("['debian-normal']", r['Labels'])
        self.assertIsNotNone(r['Buildset'])
        self.assertEqual('test-hold', r['State'])
        self.assertIsNotNone(r['Request Time'])
        self.assertEqual('check', r['Pipeline'])
        self.assertEqual('check-job', r['Job'])

        self.hold_nodeset_requests_in_queue = False
        self.executor_server.hold_jobs_in_build = False
        self.releaseNodesetRequests()
        self.waitUntilSettled()

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_nodes_hold_delete(self):
        # This tests 3 things (since they are lifecycle related):
        # * Setting the node state to hold
        # * Setting the node state to delete
        # * Deleting the request
        self.waitUntilSettled()

        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.state,
                         zuul.model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request.nodes), 1)

        nodes = self.get_url('api/tenant/tenant-one/nodes').json()
        self.assertEqual(len(nodes), 1)

        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token,
             'node-set-state', '--tenant', 'tenant-one',
             nodes[0]['uuid'], 'hold',
             ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        self.waitUntilSettled()

        for _ in iterate_timeout(10, "node to change"):
            node = self.launcher.api.nodes_cache.getItem(nodes[0]['uuid'])
            if node.next_state == node.State.HOLD:
                break

        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token,
             'node-set-state', '--tenant', 'tenant-one',
             nodes[0]['uuid'], 'used',
             ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        self.waitUntilSettled()

        requests = self.get_url(
            'api/tenant/tenant-one/nodeset-requests').json()
        self.assertEqual(len(requests), 1)
        self.assertEqual(request.uuid, requests[0]['uuid'])

        token = self._getToken()
        p = subprocess.Popen(
            ['zuul-client',
             '--zuul-url', self.base_url, '--auth-token', token,
             'nodeset-request-delete', '--tenant', 'tenant-one',
             requests[0]['uuid'],
             ],
            stdout=subprocess.PIPE)
        output, err = p.communicate()
        self.assertEqual(p.returncode, 0, output)
        self.waitUntilSettled()

        ctx = self.createZKContext(None)
        for _ in iterate_timeout(10, "request to be deleted"):
            try:
                request.refresh(ctx)
            except NoNodeError:
                break
        for _ in iterate_timeout(10, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break
