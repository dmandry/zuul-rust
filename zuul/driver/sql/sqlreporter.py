# Copyright 2015 Rackspace Australia
# Copyright 2024 Acme Gating, LLC
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

import datetime
import json
import logging
import time
import voluptuous as v

import sqlalchemy.exc

from zuul.driver.sql.sqlconnection import SQL_MAX_STRING_LENGTH
from zuul.exceptions import MissingBuildsetError
from zuul.lib.result_data import get_artifacts_from_result_data
from zuul.reporter import BaseReporter


DATA_LENGTH_ERROR = """
%s's length exceeds DB storage capacity.
Original value: %s"""


class SQLReporter(BaseReporter):
    """Sends off reports to a database."""

    name = 'sql'
    log = logging.getLogger("zuul.SQLReporter")
    retry_count = 3
    retry_delay = 5

    def _getBuildData(self, item, job, build):
        (result, _) = item.formatJobResult(job, build)
        start = end = None
        if build.start_time:
            start = datetime.datetime.fromtimestamp(
                build.start_time,
                tz=datetime.timezone.utc)
        if build.end_time:
            end = datetime.datetime.fromtimestamp(
                build.end_time,
                tz=datetime.timezone.utc)
        return result, build.log_url, start, end

    def _createBuildset(self, db, buildset):
        event_id = None
        event_timestamp = None
        event_user_id = None
        item = buildset.item
        if item.event is not None:
            event_id = getattr(item.event, "zuul_event_id", None)
            event_timestamp = datetime.datetime.fromtimestamp(
                item.event.timestamp, tz=datetime.timezone.utc)
            event_user_id = getattr(item.event, "user_id", None)

        db_buildset = db.createBuildSet(
            uuid=buildset.uuid,
            tenant=item.manager.tenant.name,
            pipeline=item.manager.pipeline.name,
            event_id=event_id,
            event_timestamp=event_timestamp,
            queue_item_uuid=item.uuid,
            updated=datetime.datetime.utcnow(),
            user_id=event_user_id,
            parameters=item.parameters,
        )
        for change in item.changes:
            ref = db.getOrCreateRef(
                project=change.project.name,
                change=getattr(change, 'number', None),
                patchset=getattr(change, 'patchset', None),
                ref_url=change.url,
                ref=getattr(change, 'ref', ''),
                oldrev=getattr(change, 'oldrev', ''),
                newrev=getattr(change, 'newrev', ''),
                branch=getattr(change, 'branch', ''),
            )
            db_buildset.refs.append(ref)
        event_change = item.getEventChange()
        if event_change:
            db_buildset.createBuildSetEvent(
                event_time=datetime.datetime.fromtimestamp(
                    item.event.timestamp, tz=datetime.timezone.utc),
                event_type='triggered',
                description=f'Triggered by {event_change.toString()}',
            )
        return db_buildset

    def reportBuildsetStart(self, buildset):
        """Create the initial buildset entry in the db"""
        if not buildset.uuid:
            return

        for retry_count in range(self.retry_count):
            try:
                with self.connection.getSession() as db:
                    return self._createBuildset(db, buildset)
            except sqlalchemy.exc.DBAPIError:
                if retry_count < self.retry_count - 1:
                    self.log.error("Unable to create buildset, will retry")
                    time.sleep(self.retry_delay)
                else:
                    self.log.exception("Unable to create buildset")

    def reportBuildsetEnd(self, buildset, action, final, result=None):
        if not buildset.uuid:
            return
        if final:
            message = self._formatItemReport(
                buildset.item, with_jobs=False, action=action)
        else:
            message = None
        for retry_count in range(self.retry_count):
            try:
                with self.connection.getSession() as db:
                    db_buildset = db.getBuildset(
                        tenant=buildset.item.manager.tenant.name,
                        uuid=buildset.uuid)
                    if not db_buildset:
                        db_buildset = self._createBuildset(db, buildset)
                    db_buildset.result = buildset.result or result
                    db_buildset.message = message
                    end_time = db_buildset.first_build_start_time
                    for build in db_buildset.builds:
                        if (build.end_time and end_time
                            and build.end_time > end_time):
                            end_time = build.end_time
                    db_buildset.last_build_end_time = end_time
                    db_buildset.updated = datetime.datetime.utcnow()
                    return
            except sqlalchemy.exc.DBAPIError:
                if retry_count < self.retry_count - 1:
                    self.log.error("Unable to update buildset, will retry")
                    time.sleep(self.retry_delay)
                else:
                    self.log.exception("Unable to update buildset")

    def reportBuildStart(self, build):
        for retry_count in range(self.retry_count):
            try:
                with self.connection.getSession() as db:
                    db_build = self._createBuild(db, build)
                    return db_build
            except sqlalchemy.exc.DBAPIError:
                if retry_count < self.retry_count - 1:
                    self.log.error("Unable to create build, will retry")
                    time.sleep(self.retry_delay)
                else:
                    self.log.exception("Unable to create build")

    def reportBuildEnd(self, build, tenant, final):
        return self.reportBuildEnds([build], tenant, final)

    def reportBuildEnds(self, builds, tenant, final):
        for retry_count in range(self.retry_count):
            try:
                with self.connection.getSession() as db:
                    buildset = builds[0].build_set
                    try:
                        db_buildset = self._getBuildset(db, builds[0])
                    except MissingBuildsetError:
                        # let _createBuild handle if necessary
                        pass
                    for build in builds:
                        if build.build_set is not buildset:
                            raise Exception("All batch reported builds "
                                            "must be for the same buildset")
                        self._reportBuildEnd(
                            db, db_buildset, build, tenant, final)
                # Exit retry loop
                return
            except sqlalchemy.exc.DBAPIError:
                if retry_count < self.retry_count - 1:
                    self.log.error("Unable to update build, will retry")
                    time.sleep(self.retry_delay)
                else:
                    self.log.exception("Unable to update build")

    def _reportBuildEnd(self, db, db_buildset, build, tenant, final):
        db_build = db.getBuild(tenant=tenant, uuid=build.uuid)
        if not db_build:
            db_build = self._createBuild(db, build, db_buildset=db_buildset)
        end_time = build.end_time or time.time()
        end = datetime.datetime.fromtimestamp(
            end_time, tz=datetime.timezone.utc)

        db_build.result = build.result
        db_build.end_time = end
        db_build.error_detail = build.error_detail
        db_build.log_url = build.log_url
        if build.log_url is not None:
            if len(build.log_url) >= SQL_MAX_STRING_LENGTH:
                db_build.log_url = None
                if db_build.error_detail is not None:
                    db_build.error_detail += (
                        DATA_LENGTH_ERROR % ("log URL", build.log_url)
                    )
                else:
                    db_build.error_detail = (
                        DATA_LENGTH_ERROR % ("log URL", build.log_url)
                    )
        db_build.final = final
        db_build.held = build.held

        for provides in build.job.provides:
            db_build.createProvides(name=provides)

        for artifact in get_artifacts_from_result_data(
                build.result_data,
                logger=self.log):
            if 'metadata' in artifact:
                artifact['metadata'] = json.dumps(
                    artifact['metadata'])
            if artifact['name'] is not None:
                if len(artifact['name']) > SQL_MAX_STRING_LENGTH:
                    db_build.error_detail = (db_build.error_detail or "") + (
                        DATA_LENGTH_ERROR % ("artifact name", artifact['name'])
                    )

                    artifact['name'] = (
                        artifact['name'][:SQL_MAX_STRING_LENGTH - 15] +
                        '... (truncated)'
                    )
            db_build.createArtifact(**artifact)

        for event in build.events:
            # Reformat the event_time so it's compatible to SQL.
            # Don't update the event object in place, but only
            # the generated dict representation to not alter the
            # datastructure for other reporters.
            ev = event.toDict()
            ev["event_time"] = datetime.datetime.fromtimestamp(
                event.event_time, tz=datetime.timezone.utc)
            db_build.createBuildEvent(**ev)
        return db_build

    def _getBuildset(self, db, build):
        buildset = build.build_set
        if not buildset:
            return None
        db_buildset = db.getBuildset(
            tenant=buildset.item.manager.tenant.name, uuid=buildset.uuid)
        if not db_buildset:
            self.log.warning("Creating missing buildset %s", buildset.uuid)
            db_buildset = self._createBuildset(db, buildset)
        return db_buildset

    def _createBuild(self, db, build, db_buildset=None):
        start_time = build.start_time or time.time()
        start = datetime.datetime.fromtimestamp(start_time,
                                                tz=datetime.timezone.utc)
        execute_time = datetime.datetime.fromtimestamp(
            build.execute_time or time.time(), tz=datetime.timezone.utc)
        if db_buildset is None:
            db_buildset = self._getBuildset(db, build)
        if db_buildset is None:
            raise MissingBuildsetError()
        if db_buildset.first_build_start_time is None:
            db_buildset.first_build_start_time = start
        item = build.build_set.item
        change = item.getChangeForJob(build.job)
        ref = db.getOrCreateRef(
            project=change.project.name,
            change=getattr(change, 'number', None),
            patchset=getattr(change, 'patchset', None),
            ref_url=change.url,
            ref=getattr(change, 'ref', ''),
            oldrev=getattr(change, 'oldrev', ''),
            newrev=getattr(change, 'newrev', ''),
            branch=getattr(change, 'branch', ''),
        )

        db_build = db_buildset.createBuild(
            ref=ref,
            uuid=build.uuid,
            job_name=build.job.name,
            execute_time=execute_time,
            start_time=start,
            voting=build.job.voting,
            nodeset=build.job.nodeset.name,
        )
        return db_build

    def getBuilds(self, *args, **kw):
        """Return a list of Build objects"""
        return self.connection.getBuilds(*args, **kw)

    def reportSystemEvent(self, tenant, **kw):
        if 'event_time' in kw:
            kw['event_time'] = datetime.datetime.fromtimestamp(
                kw['event_time'], tz=datetime.timezone.utc)

        for retry_count in range(self.retry_count):
            try:
                with self.connection.getSession() as db:
                    db_event = db.createSystemEvent(tenant=tenant, **kw)
                    return db_event
            except sqlalchemy.exc.DBAPIError:
                if retry_count < self.retry_count - 1:
                    self.log.error("Unable to create event, will retry")
                    time.sleep(self.retry_delay)
                else:
                    self.log.exception("Unable to create event")

    def report(self, item):
        # We're not a real reporter, but we use _formatItemReport, so
        # we inherit from the reporters.
        raise NotImplementedError()


def getSchema():
    sql_reporter = v.Schema(None)
    return sql_reporter
