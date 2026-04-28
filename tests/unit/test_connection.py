# Copyright 2014 Rackspace Australia
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
import re
import textwrap
import time
import types

import sqlalchemy as sa

import zuul
from zuul.driver.sql.sqlreporter import (
    DATA_LENGTH_ERROR,
    SQL_MAX_STRING_LENGTH
)
from zuul.lib import yamlutil
from tests.base import (
    AnsibleZuulTestCase,
    BaseTestCase,
    FIXTURE_DIR,
    MySQLSchemaFixture,
    PostgresqlSchemaFixture,
    ZuulTestCase,
    iterate_timeout,
    okay_tracebacks,
)


class TestConnections(ZuulTestCase):
    config_file = 'zuul-connections-same-gerrit.conf'
    tenant_config_file = 'config/zuul-connections-same-gerrit/main.yaml'

    def test_multiple_gerrit_connections(self):
        "Test multiple connections to the one gerrit"

        A = self.fake_review_gerrit.addFakeChange('org/project', 'master', 'A')
        self.addEvent('review_gerrit', A.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]['approvals']), 1)
        self.assertEqual(A.patchsets[-1]['approvals'][0]['type'], 'Verified')
        self.assertEqual(A.patchsets[-1]['approvals'][0]['value'], '1')
        self.assertEqual(A.patchsets[-1]['approvals'][0]['by']['username'],
                         'jenkins')

        B = self.fake_review_gerrit.addFakeChange('org/project', 'master', 'B')
        self.executor_server.failJob('project-test2', B)
        self.addEvent('review_gerrit', B.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        self.assertEqual(len(B.patchsets[-1]['approvals']), 1)
        self.assertEqual(B.patchsets[-1]['approvals'][0]['type'], 'Verified')
        self.assertEqual(B.patchsets[-1]['approvals'][0]['value'], '-1')
        self.assertEqual(B.patchsets[-1]['approvals'][0]['by']['username'],
                         'civoter')


class TestSQLConnectionMysql(ZuulTestCase):
    config_file = 'zuul-sql-driver-mysql.conf'
    tenant_config_file = 'config/sql-driver/main.yaml'
    expected_table_prefix = ''

    def _sql_tables_created(self, connection_name):
        connection = self.scheds.first.connections.connections[connection_name]
        insp = sa.inspect(connection.engine)

        table_prefix = connection.table_prefix
        self.assertEqual(self.expected_table_prefix, table_prefix)

        ref_table = table_prefix + 'zuul_ref'
        buildset_table = table_prefix + 'zuul_buildset'
        buildset_ref_table = table_prefix + 'zuul_buildset_ref'
        build_table = table_prefix + 'zuul_build'
        artifact_table = table_prefix + 'zuul_artifact'
        provides_table = table_prefix + 'zuul_provides'
        build_event_table = table_prefix + 'zuul_build_event'
        buildset_event_table = table_prefix + 'zuul_buildset_event'

        self.assertEqual(9, len(insp.get_columns(ref_table)))
        self.assertEqual(14, len(insp.get_columns(buildset_table)))
        self.assertEqual(2, len(insp.get_columns(buildset_ref_table)))
        self.assertEqual(15, len(insp.get_columns(build_table)))
        self.assertEqual(5, len(insp.get_columns(artifact_table)))
        self.assertEqual(3, len(insp.get_columns(provides_table)))
        self.assertEqual(5, len(insp.get_columns(build_event_table)))
        self.assertEqual(5, len(insp.get_columns(buildset_event_table)))

    def test_sql_tables_created(self):
        "Test the tables for storing results are created properly"
        self._sql_tables_created('database')

    def _sql_indexes_created(self, connection_name):
        connection = self.scheds.first.connections.connections[connection_name]
        insp = sa.inspect(connection.engine)

        table_prefix = connection.table_prefix
        self.assertEqual(self.expected_table_prefix, table_prefix)

        ref_table = table_prefix + 'zuul_ref'
        buildset_table = table_prefix + 'zuul_buildset'
        buildset_ref_table = table_prefix + 'zuul_buildset_ref'
        build_table = table_prefix + 'zuul_build'
        artifact_table = table_prefix + 'zuul_artifact'
        provides_table = table_prefix + 'zuul_provides'
        build_event_table = table_prefix + 'zuul_build_event'
        buildset_event_table = table_prefix + 'zuul_buildset_event'

        indexes_ref = insp.get_indexes(ref_table)
        indexes_buildset = insp.get_indexes(buildset_table)
        indexes_buildset_ref = insp.get_indexes(buildset_ref_table)
        indexes_build = insp.get_indexes(build_table)
        indexes_artifact = insp.get_indexes(artifact_table)
        indexes_provides = insp.get_indexes(provides_table)
        indexes_build_event = insp.get_indexes(build_event_table)
        indexes_buildset_event = insp.get_indexes(buildset_event_table)

        self.assertEqual(8, len(indexes_ref))
        self.assertEqual(3, len(indexes_buildset))
        self.assertEqual(2, len(indexes_buildset_ref))
        self.assertEqual(5, len(indexes_build))
        self.assertEqual(1, len(indexes_artifact))
        self.assertEqual(1, len(indexes_provides))
        self.assertEqual(1, len(indexes_build_event))
        self.assertEqual(1, len(indexes_buildset_event))

        # check if all indexes are prefixed
        if table_prefix:
            indexes = (indexes_ref + indexes_buildset + indexes_buildset_ref +
                       indexes_build + indexes_artifact + indexes_provides +
                       indexes_build_event + indexes_buildset_event)
            for index in indexes:
                self.assertTrue(index['name'].startswith(table_prefix))

    def test_sql_indexes_created(self):
        "Test the indexes are created properly"
        self._sql_indexes_created('database')

    def test_sql_results(self):
        "Test results are entered into an sql table"

        def check_results():
            # Grab the sa tables
            connection = self.scheds.first.connections.getSqlConnection()
            with connection.getSession() as db:
                buildsets = list(reversed(db.getBuildsets()))
                self.assertEqual(5, len(buildsets))

                self.assertEqual('check', buildsets[0].pipeline)
                self.assertEqual('org/project', buildsets[0].refs[0].project)
                self.assertEqual(1, buildsets[0].refs[0].change)
                self.assertEqual('1', buildsets[0].refs[0].patchset)
                self.assertEqual('SUCCESS', buildsets[0].result)
                self.assertEqual('Build succeeded.', buildsets[0].message)
                self.assertEqual('tenant-one', buildsets[0].tenant)
                self.assertEqual(
                    'https://review.example.com/%d' %
                    buildsets[0].refs[0].change,
                    buildsets[0].refs[0].ref_url)
                self.assertNotEqual(None, buildsets[0].event_id)
                self.assertNotEqual(None, buildsets[0].event_timestamp)
                self.assertNotEqual(None, buildsets[0].queue_item_uuid)

                # Check the first result, which should be the project-merge job
                self.assertEqual(
                    'project-merge', buildsets[0].builds[0].job_name)
                self.assertEqual("SUCCESS", buildsets[0].builds[0].result)
                self.assertEqual(None, buildsets[0].builds[0].log_url)
                self.assertEqual('check', buildsets[1].pipeline)
                self.assertEqual('master', buildsets[1].refs[0].branch)
                self.assertEqual('org/project', buildsets[1].refs[0].project)
                self.assertEqual(2, buildsets[1].refs[0].change)
                self.assertEqual('1', buildsets[1].refs[0].patchset)
                self.assertEqual('FAILURE', buildsets[1].result)
                self.assertEqual('Build failed.', buildsets[1].message)

                # Check the second result, which should be the project-test1
                # job which failed
                self.assertEqual(
                    'project-test1', buildsets[1].builds[1].job_name)
                self.assertEqual("FAILURE", buildsets[1].builds[1].result)
                self.assertEqual(None, buildsets[1].builds[1].log_url)

                # Check the first result, which should be the project-publish
                # job
                self.assertEqual('project-publish',
                                 buildsets[2].builds[0].job_name)
                self.assertEqual("SUCCESS", buildsets[2].builds[0].result)

                self.assertEqual(
                    'project-test1', buildsets[3].builds[1].job_name)
                self.assertEqual('NODE_FAILURE', buildsets[3].builds[1].result)
                self.assertEqual(None, buildsets[3].builds[1].log_url)
                self.assertIsNotNone(buildsets[3].builds[1].start_time)
                self.assertIsNotNone(buildsets[3].builds[1].end_time)
                self.assertGreaterEqual(
                    buildsets[3].builds[1].end_time,
                    buildsets[3].builds[1].start_time)

                # Check the paused build result
                paused_build_events = buildsets[4].builds[0].build_events

                self.assertEqual(len(paused_build_events), 2)
                pause_event = paused_build_events[0]
                resume_event = paused_build_events[1]
                self.assertEqual(
                    pause_event.event_type, "paused")
                self.assertIsNotNone(pause_event.event_time)
                self.assertIsNone(pause_event.description)
                self.assertEqual(
                    resume_event.event_type, "resumed")
                self.assertIsNotNone(resume_event.event_time)
                self.assertIsNone(resume_event.description)

                self.assertGreater(
                    resume_event.event_time, pause_event.event_time)

        self.executor_server.hold_jobs_in_build = True

        # Add a success result
        self.log.debug("Adding success FakeChange")
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.orderedRelease()
        self.waitUntilSettled()

        # Add a failed result
        self.log.debug("Adding failed FakeChange")
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')

        self.executor_server.failJob('project-test1', B)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.orderedRelease()
        self.waitUntilSettled()

        # Add a tag result
        self.log.debug("Adding FakeTag event")
        C = self.fake_gerrit.addFakeTag('org/project', 'master', 'foo')
        self.fake_gerrit.addEvent(C)
        self.waitUntilSettled()
        self.orderedRelease()
        self.waitUntilSettled()

        # Add a node_failure result
        self.fake_nodepool.pause()
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.orderedRelease()
        self.waitUntilSettled()
        req = self.fake_nodepool.getNodeRequests()[0]
        self.fake_nodepool.addFailRequest(req)
        self.fake_nodepool.unpause()
        self.waitUntilSettled()
        self.orderedRelease()
        self.waitUntilSettled()

        # Add a paused build result
        self.log.debug("Adding paused build result")
        D = self.fake_gerrit.addFakeChange("org/project", "master", "D")
        self.executor_server.returnData(
            "project-merge", D, {"zuul": {"pause": True}})
        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        build = self.getBuildByName('project-merge')
        self.executor_server.release('project-merge')
        for _ in iterate_timeout(10, "Wait for project-merge to pause"):
            if build.paused:
                break
        # Build has paused we wait at least one second so that the database
        # record can increment.
        time.sleep(1)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        check_results()

    def test_sql_results_retry_builds(self):
        "Test that retry results are entered into an sql table correctly"

        # Check the results
        def check_results():
            # Grab the sa tables
            connection = self.scheds.first.connections.getSqlConnection()
            with connection.getSession() as db:
                buildsets = db.getBuildsets()

                self.assertEqual(1, len(buildsets))
                buildset0 = buildsets[0]

                self.assertEqual('check', buildset0.pipeline)
                self.assertEqual('org/project', buildset0.refs[0].project)
                self.assertEqual(1, buildset0.refs[0].change)
                self.assertEqual('1', buildset0.refs[0].patchset)
                self.assertEqual('SUCCESS', buildset0.result)
                self.assertEqual('Build succeeded.', buildset0.message)
                self.assertEqual('tenant-one', buildset0.tenant)
                self.assertEqual(
                    'https://review.example.com/%d' % buildset0.refs[0].change,
                    buildset0.refs[0].ref_url)

                # Check the retry results
                self.assertEqual('project-merge', buildset0.builds[0].job_name)
                self.assertEqual('SUCCESS', buildset0.builds[0].result)
                self.assertTrue(buildset0.builds[0].final)

                self.assertEqual('project-test1', buildset0.builds[1].job_name)
                self.assertEqual('RETRY', buildset0.builds[1].result)
                self.assertFalse(buildset0.builds[1].final)
                self.assertEqual('project-test2', buildset0.builds[2].job_name)
                self.assertEqual('RETRY', buildset0.builds[2].result)
                self.assertFalse(buildset0.builds[2].final)

                self.assertEqual('project-test1', buildset0.builds[3].job_name)
                self.assertEqual('SUCCESS', buildset0.builds[3].result)
                self.assertTrue(buildset0.builds[3].final)
                self.assertEqual('project-test2', buildset0.builds[4].job_name)
                self.assertEqual('SUCCESS', buildset0.builds[4].result)
                self.assertTrue(buildset0.builds[4].final)

        self.executor_server.hold_jobs_in_build = True

        # Add a retry result
        self.log.debug("Adding retry FakeChange")
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        # Release the merge job (which is the dependency for the other jobs)
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        # Let both test jobs fail on the first run, so they are both run again.
        self.builds[0].requeue = True
        self.builds[1].requeue = True
        self.orderedRelease()
        self.waitUntilSettled()

        check_results()

    def test_sql_intermittent_failure(self):
        # Test that if we fail to create the buildset at the start of
        # a build, we still create it at the end.
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Delete the buildset
        with self.scheds.first.connections.getSqlConnection().\
             engine.connect() as conn:

            # We don't actually need to delete the zuul_ref entry
            result = conn.execute(sa.text(
                f"delete from {self.expected_table_prefix}zuul_buildset_ref;"))
            result = conn.execute(sa.text(
                f"delete from {self.expected_table_prefix}zuul_build;"))
            result = conn.execute(sa.text(
                f"delete from {self.expected_table_prefix}"
                "zuul_buildset_event;"))
            result = conn.execute(sa.text(
                f"delete from {self.expected_table_prefix}zuul_buildset;"))
            result = conn.execute(sa.text("commit;"))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # Check the results
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        manager = tenant.layout.pipeline_managers['check']
        reporter = self.scheds.first.connections.getSqlReporter(
            manager.pipeline)

        with self.scheds.first.connections.getSqlConnection().\
             engine.connect() as conn:

            result = conn.execute(
                sa.sql.select(reporter.connection.zuul_buildset_table)
            )

            buildsets = result.fetchall()
            self.assertEqual(1, len(buildsets))
            buildset0 = buildsets[0]

            buildset0_builds = conn.execute(
                sa.sql.select(
                    reporter.connection.zuul_build_table
                ).where(
                    reporter.connection.zuul_build_table.c.buildset_id ==
                    buildset0.id
                )
            ).fetchall()

            self.assertEqual(len(buildset0_builds), 5)

    def test_sql_retry(self):
        # Exercise the SQL retry code
        reporter = self.scheds.first.sched.sql
        reporter.test_buildset_retries = 0
        reporter.test_build_retries = 0
        reporter.retry_delay = 0

        orig_createBuildset = reporter._createBuildset
        orig_createBuild = reporter._createBuild

        def _createBuildset(*args, **kw):
            ret = orig_createBuildset(*args, **kw)
            if reporter.test_buildset_retries == 0:
                reporter.test_buildset_retries += 1
                raise sa.exc.DBAPIError(None, None, None)
            return ret

        def _createBuild(*args, **kw):
            ret = orig_createBuild(*args, **kw)
            if reporter.test_build_retries == 0:
                reporter.test_build_retries += 1
                raise sa.exc.DBAPIError(None, None, None)
            return ret

        reporter._createBuildset = _createBuildset
        reporter._createBuild = _createBuild

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Check the results

        self.assertEqual(reporter.test_buildset_retries, 1)
        self.assertEqual(reporter.test_build_retries, 1)

        with self.scheds.first.connections.getSqlConnection().\
             engine.connect() as conn:

            result = conn.execute(
                sa.sql.select(reporter.connection.zuul_buildset_table)
            )

            buildsets = result.fetchall()
            self.assertEqual(1, len(buildsets))
            buildset0 = buildsets[0]

            buildset0_builds = conn.execute(
                sa.sql.select(
                    reporter.connection.zuul_build_table
                ).where(
                    reporter.connection.zuul_build_table.c.buildset_id ==
                    buildset0.id
                )
            ).fetchall()

            self.assertEqual(len(buildset0_builds), 5)


class TestSQLReporterLongValues(AnsibleZuulTestCase):
    config_file = 'zuul-sql-driver-mysql.conf'
    tenant_config_file = 'config/sql-reporter-long-values/main.yaml'

    def test_sql_results_log_url_too_long(self):
        "Test builds with a long log url are stored in db"

        def check_results():
            looong_log = "http://logs.example.com/l" + ("o" * 271) + "ng"
            looong_artifact_name = "tarball_l" + ("o" * 239) + "ong_name"
            # Grab the sa tables
            connection = self.scheds.first.connections.getSqlConnection()
            with connection.getSession() as db:
                buildsets = list(db.getBuildsets())
                jobs = []
                for build in buildsets[0].builds:
                    if build.job_name == 'project-test1':
                        self.assertTrue(
                            DATA_LENGTH_ERROR % ("log URL", looong_log) in
                            build.error_detail,
                            build.error_detail)
                        self.assertEqual(
                            None,
                            build.log_url)
                        # Check if the artifact name was correctly truncated
                        self.assertEqual(
                            (looong_artifact_name[:SQL_MAX_STRING_LENGTH - 15]
                             + '... (truncated)'),
                            build.artifacts[0].name)
                        # Also check if the error was recorded with the
                        # original artifact name
                        self.assertTrue(
                            DATA_LENGTH_ERROR % ("artifact name",
                                                 looong_artifact_name) in
                            build.error_detail,
                            build.error_detail)
                        return
                    jobs.append(build.job_name)
                raise Exception(
                    'The following jobs were run: "%s"' % " ".join(jobs))

        self.executor_server.hold_jobs_in_build = True

        self.log.debug("Adding success FakeChange")
        X = self.fake_gerrit.addFakeChange('org/project', 'master', 'X')

        self.fake_gerrit.addEvent(X.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.orderedRelease()
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False

        check_results()


class TestSQLConnectionPostgres(TestSQLConnectionMysql):
    config_file = 'zuul-sql-driver-postgres.conf'


class TestSQLConnectionPrefixMysql(TestSQLConnectionMysql):
    config_file = 'zuul-sql-driver-prefix-mysql.conf'
    expected_table_prefix = 'prefix_'


class TestSQLConnectionPrefixPostgres(TestSQLConnectionMysql):
    config_file = 'zuul-sql-driver-prefix-postgres.conf'
    expected_table_prefix = 'prefix_'


class TestRequiredSQLConnection(BaseTestCase):
    config = None
    connections = None

    def setUp(self):
        super().setUp()
        self.addCleanup(self.stop_connection)

    def setup_connection(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(FIXTURE_DIR, config_file))

        # Setup databases
        for section_name in self.config.sections():
            con_match = re.match(r'^connection ([\'\"]?)(.*)(\1)$',
                                 section_name, re.I)
            if not con_match:
                continue

            if self.config.get(section_name, 'driver') == 'sql':
                if (self.config.get(section_name, 'dburi') ==
                        '$MYSQL_FIXTURE_DBURI$'):
                    f = MySQLSchemaFixture(self.id(), self.random_databases,
                                           self.delete_databases)
                    self.useFixture(f)
                    self.config.set(section_name, 'dburi', f.dburi)
                elif (self.config.get(section_name, 'dburi') ==
                      '$POSTGRESQL_FIXTURE_DBURI$'):
                    f = PostgresqlSchemaFixture(self.id(),
                                                self.random_databases,
                                                self.delete_databases)
                    self.useFixture(f)
                    self.config.set(section_name, 'dburi', f.dburi)
        self.connections = zuul.lib.connections.ConnectionRegistry()

    def stop_connection(self):
        self.connections.stop()


class TestMultipleGerrits(ZuulTestCase):
    config_file = 'zuul-connections-multiple-gerrits.conf'
    tenant_config_file = 'config/zuul-connections-multiple-gerrits/main.yaml'

    def test_multiple_project_separate_gerrits(self):
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_another_gerrit.addFakeChange(
            'org/project1', 'master', 'A')
        self.fake_another_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        self.assertBuilds([dict(name='project-test2',
                                changes='1,1',
                                project='org/project1',
                                pipeline='another_check')])

        # NOTE(jamielennox): the tests back the git repo for both connections
        # onto the same git repo on the file system. If we just create another
        # fake change the fake_review_gerrit will try to create another 1,1
        # change and git will fail to create the ref. Arbitrarily set it to get
        # around the problem.
        self.fake_review_gerrit.change_number = 50

        B = self.fake_review_gerrit.addFakeChange(
            'org/project1', 'master', 'B')
        self.fake_review_gerrit.addEvent(B.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        self.assertBuilds([
            dict(name='project-test2',
                 changes='1,1',
                 project='org/project1',
                 pipeline='another_check'),
            dict(name='project-test1',
                 changes='51,1',
                 project='org/project1',
                 pipeline='review_check'),
        ])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def test_multiple_project_separate_gerrits_common_pipeline(self):
        self.executor_server.hold_jobs_in_build = True

        self.create_branch('org/project2', 'develop')
        self.fake_another_gerrit.addEvent(
            self.fake_another_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'develop'))

        self.fake_another_gerrit.addEvent(
            self.fake_review_gerrit.getFakeBranchCreatedEvent(
                'org/project2', 'develop'))
        self.waitUntilSettled()

        A = self.fake_another_gerrit.addFakeChange(
            'org/project2', 'master', 'A')
        self.fake_another_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        self.assertBuilds([dict(name='project-test2',
                                changes='1,1',
                                project='org/project2',
                                pipeline='common_check')])

        # NOTE(jamielennox): the tests back the git repo for both connections
        # onto the same git repo on the file system. If we just create another
        # fake change the fake_review_gerrit will try to create another 1,1
        # change and git will fail to create the ref. Arbitrarily set it to get
        # around the problem.
        self.fake_review_gerrit.change_number = 50

        B = self.fake_review_gerrit.addFakeChange(
            'org/project2', 'develop', 'B')
        self.fake_review_gerrit.addEvent(B.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        self.assertBuilds([
            dict(name='project-test2',
                 changes='1,1',
                 project='org/project2',
                 pipeline='common_check'),
            dict(name='project-test1',
                 changes='51,1',
                 project='org/project2',
                 pipeline='common_check'),
        ])

        # NOTE(avass): This last change should not trigger any pipelines since
        # common_check is configured to only run on master for another_gerrit
        C = self.fake_another_gerrit.addFakeChange(
            'org/project2', 'develop', 'C')
        self.fake_another_gerrit.addEvent(C.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        self.assertBuilds([
            dict(name='project-test2',
                 changes='1,1',
                 project='org/project2',
                 pipeline='common_check'),
            dict(name='project-test1',
                 changes='51,1',
                 project='org/project2',
                 pipeline='common_check'),
        ])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()


class TestConnectionsMerger(ZuulTestCase):
    config_file = 'zuul-connections-merger.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_connections_merger(self):
        "Test merger only configures source connections"

        self.assertIn("gerrit", self.executor_server.connections.connections)
        self.assertIn("github", self.executor_server.connections.connections)
        self.assertNotIn("smtp", self.executor_server.connections.connections)
        self.assertNotIn("sql", self.executor_server.connections.connections)
        self.assertNotIn("timer", self.executor_server.connections.connections)
        self.assertNotIn("zuul", self.executor_server.connections.connections)


class TestConnectionsCgit(ZuulTestCase):
    config_file = 'zuul-connections-cgit.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_cgit_web_url(self):
        self.assertIn("gerrit", self.scheds.first.connections.connections)
        conn = self.scheds.first.connections.connections['gerrit']
        source = conn.source
        proj = source.getProject('foo/bar')
        url = conn._getWebUrl(proj, '1')
        self.assertEqual(url,
                         'https://cgit.example.com/cgit/foo/bar/commit/?id=1')


class TestConnectionsGitweb(ZuulTestCase):
    config_file = 'zuul-connections-gitweb.conf'
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_gitweb_url(self):
        self.assertIn("gerrit", self.scheds.first.connections.connections)
        conn = self.scheds.first.connections.connections['gerrit']
        source = conn.source
        proj = source.getProject('foo/bar')
        url = conn._getWebUrl(proj, '1')
        url_should_be = 'https://review.example.com/' \
                        'gitweb?p=foo/bar.git;a=commitdiff;h=1'
        self.assertEqual(url, url_should_be)


class TestMQTTConnection(ZuulTestCase):
    config_file = 'zuul-mqtt-driver.conf'
    tenant_config_file = 'config/mqtt-driver/main.yaml'

    @okay_tracebacks('Connection refused')
    def test_mqtt_reporter(self):
        "Test the MQTT reporter"
        # Add a success result
        A = self.fake_gerrit.addFakeChange('org/project',
                                           'master',
                                           'A',
                                           topic='fake/topic')
        artifact = {'name': 'image',
                    'url': 'http://example.com/image',
                    'metadata': {
                        'type': 'container_image'
                    }}
        self.executor_server.returnData(
            "test", A, {
                "zuul": {
                    "log_url": "some-log-url",
                    'artifacts': [artifact],
                },
                'foo': 'bar',
            }
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        success_event_check = self.mqtt_messages.pop()
        start_event_check = self.mqtt_messages.pop()

        self.assertEqual(start_event_check.get('topic'),
                         'tenant-one/zuul_start/check/org/project/master')
        mqtt_payload = start_event_check['msg']
        self.assertEqual(mqtt_payload['project'], 'org/project')
        self.assertEqual(len(mqtt_payload['commit_id']), 40)
        self.assertEqual(mqtt_payload['owner'], 'username')
        self.assertEqual(mqtt_payload['branch'], 'master')
        self.assertEqual(mqtt_payload['buildset']['result'], None)
        self.assertEqual(mqtt_payload['buildset']['builds'][0]['job_name'],
                         'test')
        self.assertNotIn('result', mqtt_payload['buildset']['builds'][0])
        self.assertNotIn('artifacts', mqtt_payload['buildset']['builds'][0])
        builds = mqtt_payload['buildset']['builds']
        test_job = [b for b in builds if b['job_name'] == 'test'][0]
        self.assertNotIn('returned_data', test_job)

        self.assertEqual(success_event_check.get('topic'),
                         'tenant-one/zuul_buildset/check/org/project/master')
        mqtt_payload = success_event_check['msg']
        self.assertEqual(mqtt_payload['project'], 'org/project')
        self.assertEqual(mqtt_payload['pipeline'], 'check')
        self.assertEqual(mqtt_payload['queue'], '')
        self.assertEqual(mqtt_payload['branch'], 'master')
        self.assertEqual(mqtt_payload['buildset']['result'], 'SUCCESS')
        builds = mqtt_payload['buildset']['builds']
        test_job = [b for b in builds if b['job_name'] == 'test'][0]
        dependent_test_job = [
            b for b in builds if b['job_name'] == 'dependent-test'
        ][0]
        self.assertEqual(test_job['job_name'], 'test')
        self.assertEqual(test_job['result'], 'SUCCESS')
        self.assertEqual(test_job['dependencies'], [])
        self.assertEqual(test_job['job_dependencies'], {})
        self.assertEqual(test_job['artifacts'], [artifact])
        self.assertEqual(test_job['log_url'], 'some-log-url/')
        self.assertEqual(test_job['returned_data'], {'foo': 'bar'})
        build_id = test_job["uuid"]
        self.assertEqual(
            test_job["web_url"],
            "https://tenant.example.com/t/tenant-one/build/{}".format(
                build_id
            ),
        )
        self.assertIn('execute_time', test_job)
        self.assertIn('timestamp', mqtt_payload)
        self.assertIn('enqueue_time', mqtt_payload)
        self.assertIn('trigger_time', mqtt_payload)
        self.assertIn('zuul_event_id', mqtt_payload)
        self.assertIn('uuid', mqtt_payload)
        self.assertEqual(dependent_test_job['dependencies'], ['test'])
        self.assertIn('test', dependent_test_job['job_dependencies'])
        self.assertEqual(mqtt_payload['tenant_web_root'],
                         'https://tenant.example.com/t/tenant-one/')

        changes = mqtt_payload['changes']
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]['topic'], 'fake/topic')

        A.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        success_event_gate = self.mqtt_messages.pop()
        mqtt_payload = success_event_gate['msg']
        self.assertEqual(mqtt_payload['project'], 'org/project')
        self.assertEqual(mqtt_payload['pipeline'], 'gate')
        self.assertEqual(mqtt_payload['queue'], 'integrated')

    @okay_tracebacks('Connection refused')
    def test_mqtt_paused_job(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        # Let the job being paused via the executor
        self.executor_server.returnData("test", A, {"zuul": {"pause": True}})
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        build = self.getBuildByName('test')
        self.executor_server.release('test')
        for _ in iterate_timeout(10, "Wait for test to pause"):
            if build.paused:
                break
        # Build has paused we wait at least one second so that the timestamp
        # record can increment.
        time.sleep(1)
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        success_event = self.mqtt_messages.pop()

        mqtt_payload = success_event["msg"]
        self.assertEqual(mqtt_payload["project"], "org/project")
        builds = mqtt_payload["buildset"]["builds"]
        paused_job = [b for b in builds if b["job_name"] == "test"][0]
        self.assertEqual(len(paused_job["events"]), 2)

        pause_event = paused_job["events"][0]
        self.assertEqual(pause_event["event_type"], "paused")
        self.assertGreater(
            pause_event["event_time"], paused_job["start_time"])
        self.assertLess(pause_event["event_time"], paused_job["end_time"])

        resume_event = paused_job["events"][1]
        self.assertEqual(resume_event["event_type"], "resumed")
        self.assertGreater(
            resume_event["event_time"], paused_job["start_time"])
        self.assertLess(resume_event["event_time"], paused_job["end_time"])

        self.assertGreater(
            resume_event["event_time"], pause_event["event_time"])

    @okay_tracebacks('Connection refused')
    def test_mqtt_retried_builds(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 1)
        build = self.builds[0]
        build.requeue = True
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        success_event = self.mqtt_messages.pop()
        mqtt_payload = success_event["msg"]
        retry_builds = mqtt_payload["buildset"]["retry_builds"]
        self.assertEqual(len(retry_builds), 1)
        retry_build = retry_builds[0]

        self.assertEqual(build.job.name, retry_build["job_name"])
        self.assertEqual(build.job.uuid, retry_build["job_uuid"])
        self.assertEqual(build.job.voting, retry_build["voting"])
        self.assertEqual(build.uuid, retry_build["uuid"])
        self.assertIn(build.uuid, retry_build["web_url"])
        self.assertEqual("RETRY", retry_build["result"])

        self.assertIn("start_time", retry_build)
        self.assertIn("end_time", retry_build)
        self.assertIn("execute_time", retry_build)
        self.assertIn("log_url", retry_build)

    @okay_tracebacks('Connection refused')
    def test_mqtt_invalid_topic(self):
        in_repo_conf = textwrap.dedent(
            """
            - pipeline:
                name: test-pipeline
                manager: independent
                trigger:
                  gerrit:
                    - event: comment-added
                start:
                  mqtt:
                    topic: "{bad}/{topic}"
            """)
        file_dict = {'zuul.d/test.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('common-config', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertIn("topic component 'bad' is invalid", A.messages[0],
                      "A should report a syntax error")

    @okay_tracebacks('Connection refused')
    def test_topic_replace_invalid_chars(self):
        # Test that special characters in the topic are replaced
        self.create_branch('org/project', 'weird#+')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'weird#+'))
        self.waitUntilSettled()

        A = self.fake_gerrit.addFakeChange('org/project', 'weird#+', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        success_event = self.mqtt_messages.pop()
        self.assertEqual(success_event['topic'],
                         'tenant-one/zuul_buildset/check/org/project/weird__')


class TestElasticsearchConnection(AnsibleZuulTestCase):
    config_file = 'zuul-elastic-driver.conf'
    tenant_config_file = 'config/elasticsearch-driver/main.yaml'

    # These tests are storing the reported index on the fake
    # elasticsearch backend which is a different instance for each
    # scheduler. Thus, depending on which scheduler reports the
    # item, the assertions in these test might pass or fail.
    scheduler_count = 1

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

    def test_elastic_reporter(self):
        "Test the Elasticsearch reporter"
        # Add a success result
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        indexed_docs = self.scheds.first.connections.connections[
            'elasticsearch'].source_it
        index = self.scheds.first.connections.connections[
            'elasticsearch'].index

        self.assertEqual(len(indexed_docs), 2)
        self.assertEqual(index, ('zuul-index.tenant-one-%s' %
                                 time.strftime("%Y.%m.%d")))
        buildset_doc = [doc for doc in indexed_docs if
                        doc['build_type'] == 'buildset'][0]
        self.assertEqual(buildset_doc['tenant'], 'tenant-one')
        self.assertEqual(buildset_doc['pipeline'], 'check')
        self.assertEqual(buildset_doc['result'], 'SUCCESS')
        build_doc = [doc for doc in indexed_docs if
                     doc['build_type'] == 'build'][0]
        self.assertEqual(build_doc['buildset_uuid'], buildset_doc['uuid'])
        self.assertEqual(build_doc['result'], 'SUCCESS')
        self.assertEqual(build_doc['job_name'], 'test')
        self.assertEqual(build_doc['tenant'], 'tenant-one')
        self.assertEqual(build_doc['pipeline'], 'check')

        self.assertIn('job_vars', build_doc)
        self.assertDictEqual(
            build_doc['job_vars'], {'bar': 'foo', 'bar2': 'foo2'})

        self.assertIn('job_returned_vars', build_doc)
        self.assertDictEqual(
            build_doc['job_returned_vars'], {'foo': 'bar'})

        self.assertEqual(self.history[0].uuid, build_doc['uuid'])
        self.assertIn('duration', build_doc)
        self.assertTrue(type(build_doc['duration']) is int)

        doc_gen = self.scheds.first.connections.connections[
            'elasticsearch'].gen(indexed_docs, index)
        self.assertIsInstance(doc_gen, types.GeneratorType)
        self.assertTrue('@timestamp' in list(doc_gen)[0]['_source'])

    def test_elasticsearch_secret_leak(self):
        expected_secret = [{
            'test_secret': {
                'username': 'test-username',
                'password': 'test-password'
            }
        }]

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        indexed_docs = self.scheds.first.connections.connections[
            'elasticsearch'].source_it

        build_doc = [doc for doc in indexed_docs if
                     doc['build_type'] == 'build'][0]

        # Ensure that job include secret
        self.assertEqual(
            self._getSecrets('test', 'playbooks'),
            expected_secret)

        # Check if there is a secret leak
        self.assertFalse('test_secret' in build_doc['job_vars'])


class TestConnectionsBranchCache(ZuulTestCase):
    config_file = "zuul-gerrit-github.conf"
    tenant_config_file = 'config/multi-driver/main.yaml'

    def test_branch_cache_fetch_error(self):
        # Test that a fetch error stores the right value in the branch cache
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        connection = self.scheds.first.connections.connections['github']
        source = connection.source
        project = source.getProject('org/project1')

        # Patch the fetch method so that it fails
        orig = connection._fetchProjectBranches

        def fail(*args, **kw):
            raise Exception("Unable to fetch branches")
        self.patch(connection, '_fetchProjectBranches', fail)

        # Clear the branch cache so we start with nothing
        connection.clearBranchCache()

        # Verify that we raise an error when we try to get branches
        # for a missing project
        self.assertRaises(
            Exception,
            lambda: connection.getProjectBranches(project, tenant))
        # This should happen again (ie, we should retry since we don't
        # have an entry)
        self.assertRaises(
            Exception,
            lambda: connection.getProjectBranches(project, tenant))

        # Restore the normal fetch method and verify that the cache
        # works as expected
        self.patch(connection, '_fetchProjectBranches', orig)
        branches = connection.getProjectBranches(project, tenant)
        self.assertEqual(['master'], branches)

        # Ensure that the empty list of branches is valid and is not
        # seen as an error
        self.init_repo("org/newproject")
        newproject = source.getProject('org/newproject')
        connection.addProject(newproject)
        tpc = zuul.model.TenantProjectConfig(newproject)
        tpc.exclude_unprotected_branches = True
        tenant.addTPC(tpc)
        branches = connection.getProjectBranches(newproject, tenant)
        self.assertEqual([], branches)
