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

import logging
import re
import time
from urllib.parse import quote_plus

import alembic
import alembic.command
import alembic.config
import alembic.migration
import sqlalchemy as sa
from sqlalchemy import orm
import sqlalchemy.pool

from zuul.connection import BaseConnection
from zuul.zk.locks import CONNECTION_LOCK_ROOT, locked, SessionAwareLock


BUILDSET_TABLE = 'zuul_buildset'
REF_TABLE = 'zuul_ref'
BUILDSET_REF_TABLE = 'zuul_buildset_ref'
BUILDSET_EVENT_TABLE = 'zuul_buildset_event'
BUILD_TABLE = 'zuul_build'
BUILD_EVENT_TABLE = 'zuul_build_event'
ARTIFACT_TABLE = 'zuul_artifact'
PROVIDES_TABLE = 'zuul_provides'
SYSTEM_EVENT_TABLE = 'zuul_system_event'

POSTGRES_STATEMENT_TIMEOUT_RE = re.compile(
    r'/\* postgres_statement_timeout=(\d+) \*/')
MARIADB_STATEMENT_TIMEOUT_RE = re.compile(
    r'/\* mariadb_statement_timeout=(\d+) \*/')

SQL_MAX_STRING_LENGTH = 255


# In Postgres we can set a per-transaction (which for us is
# effectively per-query) execution timeout by executing "SET LOCAL
# statement_timeout" before our query.  There isn't a great way to do
# this using the SQLAlchemy query API, so instead, we add a comment as
# a hint, and here we parse that comment and execute the "SET".  The
# comment remains in the query sent to the server, but that's okay --
# it may even help an operator in debugging.  The same is true for
# mariadb, except that we need to actually alter the statement instead
# of executing another one.
@sa.event.listens_for(sa.Engine, "before_cursor_execute", retval=True)
def _set_timeout(conn, cursor, stmt, params, context, executemany):
    match = POSTGRES_STATEMENT_TIMEOUT_RE.search(stmt)
    if match:
        cursor.execute("SET LOCAL statement_timeout=%s" % match.groups())
    else:
        match = MARIADB_STATEMENT_TIMEOUT_RE.search(stmt)
        if match:
            stmt = ("SET STATEMENT "
                    f"max_statement_time={float(match.groups()[0]) / 1000} "
                    f"for {stmt}")
    return (stmt, params)


class ChangeType(sa.TypeDecorator):
    """Coerces NULL/None values to/from the integer 0 to facilitate use in
    non-nullable columns

    """
    # Underlying implementation
    impl = sa.Integer
    # We produce consistent values, so can be cached
    cache_ok = True
    # Don't use "IS NULL" when comparing to None values
    coerce_to_is_types = ()

    def process_bind_param(self, value, dialect):
        if value is None:
            return 0
        return value

    def process_result_value(self, value, dialect):
        if value == 0:
            return None
        return value


class SHAType(sa.TypeDecorator):
    """Coerces NULL/None values to/from the empty string to facilitate use
    in non-nullable columns

    """
    # Underlying implementation
    impl = sa.String
    # We produce consistent values, so can be cached
    cache_ok = True
    # Don't use "IS NULL" when comparing to None values
    coerce_to_is_types = ()

    def process_bind_param(self, value, dialect):
        if value is None:
            return ''
        return value

    def process_result_value(self, value, dialect):
        if value == '':
            return None
        return value


class DatabaseSession(object):
    log = logging.getLogger("zuul.DatabaseSession")

    def __init__(self, connection):
        self.connection = connection
        self.session = connection.session

    def __enter__(self):
        return self

    def __exit__(self, etype, value, tb):
        if etype:
            self.session().rollback()
        else:
            self.session().commit()
        self.session().close()
        self.session = None

    def _sanitizeSubstringQuery(self, value):
        if isinstance(value, str):
            escape_char = "$"
            for c in (escape_char, "%", "_"):
                value = value.replace(c, escape_char + c)
            return value
        return value

    def _getFuzzyFilterOp(self, column, value):
        if isinstance(value, str) and "*" in value:
            value = self._sanitizeSubstringQuery(value)
            return column.like(value.replace("*", "%"), escape="$")
        else:
            return column == value

    def listFilterFuzzy(self, query, column, value):
        if value is None:
            return query
        elif isinstance(value, list) or isinstance(value, tuple):
            return query.filter(
                sa.or_(*[self._getFuzzyFilterOp(column, v) for v in value])
            )
        return query.filter(self._getFuzzyFilterOp(column, value))

    def listFilter(self, query, column, value):
        if value is None:
            return query
        if isinstance(value, list) or isinstance(value, tuple):
            return query.filter(column.in_(value))
        return query.filter(column == value)

    def exListFilter(self, query, column, value):
        # Exclude values in list
        if value is None:
            return query
        if isinstance(value, list) or isinstance(value, tuple):
            return query.filter(column.not_in(value))
        return query.filter(column != value)

    def getBuilds(self, tenant=None, project=None, pipeline=None,
                  change=None, branch=None, patchset=None, ref=None,
                  newrev=None, event_id=None, event_timestamp=None,
                  first_build_start_time=None, last_build_end_time=None,
                  queue_item_uuid=None, uuid=None, job_name=None,
                  voting=None, nodeset=None,
                  result=None, provides=None, final=None, held=None,
                  complete=None, sort_by_buildset=False, limit=50,
                  offset=0, idx_min=None, idx_max=None,
                  exclude_result=None, query_timeout=None):

        ref_table = self.connection.zuul_ref_table
        build_table = self.connection.zuul_build_table
        buildset_table = self.connection.zuul_buildset_table
        provides_table = self.connection.zuul_provides_table

        q = self.session().query(
            self.connection.buildModel.id).\
            join(self.connection.buildSetModel).\
            join(self.connection.refModel).\
            group_by(self.connection.buildModel.id)

        # If the query planner isn't able to reduce either the number
        # of rows returned by the buildset or build tables, then it
        # tends to produce a very slow query.  This hint produces
        # better results, but only in those cases.  When we can narrow
        # things down with indexes, it's better to omit the hint.
        # job_name is a tricky one.  It is indexed, but if there are a
        # lot of rows, it is better to include the hint, but if there
        # are few, it is better to not include it.  We include the hint
        # regardless of whether job_name is specified (optimizing for
        # the more common case).
        if not (change or ref or uuid):
            q = q.with_hint(build_table, 'USE INDEX (PRIMARY)', 'mysql')
            q = q.with_hint(build_table, 'USE INDEX (PRIMARY)', 'mariadb')

        # Avoid joining the provides table unless necessary; postgres
        # has created some poor query plans in that case.  Currently
        # the only time this gets called with provides is from the
        # scheduler which has several other criteria which narrow down
        # the query.
        if provides:
            q = q.outerjoin(self.connection.providesModel)

        q = self.listFilter(q, buildset_table.c.tenant, tenant)
        q = self.listFilterFuzzy(q, buildset_table.c.pipeline, pipeline)
        q = self.listFilterFuzzy(q, ref_table.c.project, project)
        q = self.listFilter(q, ref_table.c.change, change)
        q = self.listFilterFuzzy(q, ref_table.c.branch, branch)
        q = self.listFilter(q, ref_table.c.patchset, patchset)
        q = self.listFilter(q, ref_table.c.ref, ref)
        q = self.listFilter(q, ref_table.c.newrev, newrev)
        q = self.listFilter(q, buildset_table.c.event_id, event_id)
        q = self.listFilter(
            q, buildset_table.c.event_timestamp, event_timestamp)
        q = self.listFilter(
            q, buildset_table.c.first_build_start_time, first_build_start_time)
        q = self.listFilter(
            q, buildset_table.c.last_build_end_time, last_build_end_time)
        q = self.listFilter(
            q, buildset_table.c.queue_item_uuid, queue_item_uuid)
        q = self.listFilter(q, build_table.c.uuid, uuid)
        q = self.listFilterFuzzy(q, build_table.c.job_name, job_name)
        q = self.listFilter(q, build_table.c.voting, voting)
        q = self.listFilter(q, build_table.c.nodeset, nodeset)
        q = self.listFilter(q, build_table.c.result, result)
        q = self.exListFilter(q, build_table.c.result, exclude_result)
        q = self.listFilter(q, build_table.c.final, final)
        if complete is True:
            q = q.filter(build_table.c.result != None)  # noqa
        elif complete is False:
            q = q.filter(build_table.c.result == None)  # noqa
        q = self.listFilter(q, provides_table.c.name, provides)
        q = self.listFilter(q, build_table.c.held, held)
        if idx_min:
            q = q.filter(build_table.c.id >= idx_min)
        if idx_max:
            q = q.filter(build_table.c.id <= idx_max)

        if sort_by_buildset:
            # If we don't need the builds to be strictly ordered, this
            # query can be much faster as it may avoid the use of a
            # temporary table.
            q = q.order_by(buildset_table.c.id.desc())
        else:
            q = q.order_by(build_table.c.id.desc())

        q = q.limit(limit).offset(offset)

        if query_timeout:
            # For MySQL, we can add a query hint directly.
            q = q.prefix_with(
                f'/*+ MAX_EXECUTION_TIME({query_timeout}) */',
                dialect='mysql')
            # For Postgres and mariadb, we add a comment that we parse
            # in our event handler.
            q = q.with_statement_hint(
                f'/* postgres_statement_timeout={query_timeout} */',
                dialect_name='postgresql')
            q = q.with_statement_hint(
                f'/* mariadb_statement_timeout={query_timeout} */',
                dialect_name='mariadb')

        ids = [x[0] for x in q.all()]
        # We use two queries here, principally because mariadb does
        # not produce a reasonable plan if we combine these into one
        # query with a join, and neither mysql nor mariadb support
        # using limit inside a subquery that is used with the "in"
        # operator.  Therefore, we run a query to get the ids, then
        # run a second to load the data.
        # contains_eager allows us to perform eager loading on the
        # buildset *and* use that table in filters (unlike
        # joinedload).
        bq = self.session().query(self.connection.buildModel).\
            join(self.connection.buildSetModel).\
            join(self.connection.refModel).\
            options(orm.contains_eager(self.connection.buildModel.buildset),
                    orm.contains_eager(self.connection.buildModel.ref),
                    orm.selectinload(self.connection.buildModel.provides),
                    orm.selectinload(self.connection.buildModel.artifacts))

        bq = bq.filter(self.connection.buildModel.id.in_(ids))

        if sort_by_buildset:
            # If we don't need the builds to be strictly ordered, this
            # query can be much faster as it may avoid the use of a
            # temporary table.
            bq = bq.order_by(buildset_table.c.id.desc())
        else:
            bq = bq.order_by(build_table.c.id.desc())

        try:
            return bq.all()
        except sqlalchemy.orm.exc.NoResultFound:
            return []

    def getBuild(self, tenant, uuid):
        build_table = self.connection.zuul_build_table
        buildset_table = self.connection.zuul_buildset_table

        # contains_eager allows us to perform eager loading on the
        # buildset *and* use that table in filters (unlike
        # joinedload).
        q = self.session().query(self.connection.buildModel).\
            join(self.connection.buildSetModel).\
            join(self.connection.refModel).\
            outerjoin(self.connection.providesModel).\
            options(orm.contains_eager(self.connection.buildModel.buildset).
                    subqueryload(self.connection.buildSetModel.refs),
                    orm.selectinload(self.connection.buildModel.provides),
                    orm.selectinload(self.connection.buildModel.artifacts),
                    orm.selectinload(self.connection.buildModel.ref))

        q = self.listFilter(q, buildset_table.c.tenant, tenant)
        q = self.listFilter(q, build_table.c.uuid, uuid)

        try:
            return q.one()
        except sqlalchemy.orm.exc.NoResultFound:
            return None
        except sqlalchemy.orm.exc.MultipleResultsFound:
            raise Exception("Multiple builds found with uuid %s", uuid)

    def getBuildTimes(self, tenant=None, project=None, pipeline=None,
                      branch=None,
                      ref=None,
                      start_time=None,
                      end_time=None,
                      job_name=None,
                      final=None,
                      sort_by_buildset=False,
                      limit=50,
                      offset=0,
                      result=None,
                      exclude_result=None,
                      query_timeout=None):

        ref_table = self.connection.zuul_ref_table
        build_table = self.connection.zuul_build_table
        buildset_table = self.connection.zuul_buildset_table

        q = self.session().query(
            self.connection.buildModel.id,
            self.connection.buildModel.end_time).\
            join(self.connection.buildSetModel).\
            join(self.connection.refModel).\
            group_by(self.connection.buildModel.id)

        # See note above about the hint.
        if not (project):
            q = q.with_hint(build_table, 'USE INDEX (PRIMARY)', 'mysql')
            q = q.with_hint(build_table, 'USE INDEX (PRIMARY)', 'mariadb')

        q = self.listFilter(q, buildset_table.c.tenant, tenant)
        q = self.listFilter(q, buildset_table.c.pipeline, pipeline)
        q = self.listFilter(q, ref_table.c.project, project)
        q = self.listFilter(q, ref_table.c.branch, branch)
        q = self.listFilter(q, ref_table.c.ref, ref)
        q = self.listFilter(q, build_table.c.job_name, job_name)
        q = self.listFilter(q, build_table.c.result, result)
        q = self.exListFilter(q, build_table.c.result, exclude_result)
        q = self.listFilter(q, build_table.c.final, final)
        if start_time:
            # Intentionally using end_time here
            q = q.filter(build_table.c.end_time >= start_time)
        if end_time:
            q = q.filter(build_table.c.end_time <= end_time)
        # Only complete builds (but if we specify a result, then it's
        # obviously complete and this extra filter is not necessary)
        if result is None:
            q = q.filter(build_table.c.result != None)  # noqa

        q = q.order_by(build_table.c.end_time.desc()).\
            limit(limit).\
            offset(offset)

        if query_timeout:
            # For MySQL, we can add a query hint directly.
            q = q.prefix_with(
                f'/*+ MAX_EXECUTION_TIME({query_timeout}) */',
                dialect='mysql')
            # For Postgres and mariadb, we add a comment that we parse
            # in our event handler.
            q = q.with_statement_hint(
                f'/* postgres_statement_timeout={query_timeout} */',
                dialect_name='postgresql')
            q = q.with_statement_hint(
                f'/* mariadb_statement_timeout={query_timeout} */',
                dialect_name='mariadb')

        ids = [x[0] for x in q.all()]
        # contains_eager allows us to perform eager loading on the
        # buildset *and* use that table in filters (unlike
        # joinedload).
        bq = self.session().query(self.connection.buildModel).\
            join(self.connection.buildSetModel).\
            join(self.connection.refModel).\
            options(orm.contains_eager(self.connection.buildModel.buildset),
                    orm.contains_eager(self.connection.buildModel.ref)).\
            filter(self.connection.buildModel.id.in_(ids)).\
            order_by(build_table.c.end_time.desc())

        try:
            return bq.all()
        except sqlalchemy.orm.exc.NoResultFound:
            return []

    def createBuildSet(self, *args, **kw):
        bs = self.connection.buildSetModel(*args, **kw)
        self.session().add(bs)
        self.session().flush()
        return bs

    def getOrCreateRef(self, project, ref, ref_url,
                       change=None, patchset=None, branch=None,
                       oldrev=None, newrev=None):
        ref_table = self.connection.zuul_ref_table
        q = self.session().query(self.connection.refModel)
        # We only query for the columns that we include in the unique
        # constraint (i.e., we omit branch and ref_url which should be
        # guaranteed to be unique by ref).
        q = q.filter(ref_table.c.project == project,
                     ref_table.c.ref == ref,
                     ref_table.c.change == change,
                     ref_table.c.patchset == patchset,
                     ref_table.c.oldrev == oldrev,
                     ref_table.c.newrev == newrev)
        ret = q.all()
        if ret:
            return ret[0]
        ret = self.connection.refModel(
            project=project, ref=ref, ref_url=ref_url,
            change=change, patchset=patchset, branch=branch,
            oldrev=oldrev, newrev=newrev)
        self.session().add(ret)
        self.session().flush()
        return ret

    def getBuildsets(self, tenant=None, project=None, pipeline=None,
                     change=None, branch=None, patchset=None, ref=None,
                     newrev=None, uuid=None, queue_item_uuid=None,
                     result=None, complete=None, updated_max=None,
                     limit=50, offset=0, idx_min=None, idx_max=None,
                     exclude_result=None, query_timeout=None):

        buildset_table = self.connection.zuul_buildset_table
        buildset_ref_table = self.connection.zuul_buildset_ref_table
        ref_table = self.connection.zuul_ref_table

        q = self.session().query(
            self.connection.buildSetModel.id).\
            join(self.connection.buildSetRefModel).\
            join(self.connection.refModel).\
            group_by(self.connection.buildSetModel.id)

        # See note above about the hint.
        if not (change or ref or uuid):
            q = q.with_hint(buildset_table, 'USE INDEX (PRIMARY)', 'mysql')
            q = q.with_hint(buildset_table, 'USE INDEX (PRIMARY)', 'mariadb')
            q = q.with_hint(buildset_ref_table, 'USE INDEX (PRIMARY)',
                            'mysql')
            q = q.with_hint(buildset_ref_table, 'USE INDEX (PRIMARY)',
                            'mariadb')

        q = self.listFilter(q, buildset_table.c.tenant, tenant)
        q = self.listFilterFuzzy(q, buildset_table.c.pipeline, pipeline)
        q = self.listFilterFuzzy(q, ref_table.c.project, project)
        q = self.listFilter(q, ref_table.c.change, change)
        q = self.listFilterFuzzy(q, ref_table.c.branch, branch)
        q = self.listFilter(q, ref_table.c.patchset, patchset)
        q = self.listFilter(q, ref_table.c.ref, ref)
        q = self.listFilter(q, ref_table.c.newrev, newrev)
        q = self.listFilter(q, buildset_table.c.uuid, uuid)
        q = self.listFilter(
            q, buildset_table.c.queue_item_uuid, queue_item_uuid)
        q = self.listFilter(q, buildset_table.c.result, result)
        q = self.exListFilter(q, buildset_table.c.result, exclude_result)
        if idx_min:
            q = q.filter(buildset_table.c.id >= idx_min)
        if idx_max:
            q = q.filter(buildset_table.c.id <= idx_max)

        if complete is True:
            q = q.filter(buildset_table.c.result != None)  # noqa
        elif complete is False:
            q = q.filter(buildset_table.c.result == None)  # noqa

        if updated_max:
            q = q.filter(buildset_table.c.updated < updated_max)
        q = q.order_by(buildset_table.c.id.desc()).\
            limit(limit).\
            offset(offset)

        if query_timeout:
            # For MySQL, we can add a query hint directly.
            q = q.prefix_with(
                f'/*+ MAX_EXECUTION_TIME({query_timeout}) */',
                dialect='mysql')
            # For Postgres and mariadb, we add a comment that we parse
            # in our event handler.
            q = q.with_statement_hint(
                f'/* postgres_statement_timeout={query_timeout} */',
                dialect_name='postgresql')
            q = q.with_statement_hint(
                f'/* mariadb_statement_timeout={query_timeout} */',
                dialect_name='mariadb')

        ids = [x[0] for x in q.all()]
        bq = self.session().query(self.connection.buildSetModel).\
            join(self.connection.buildSetRefModel).\
            join(self.connection.refModel).\
            options(orm.contains_eager(self.connection.buildSetModel.refs)).\
            filter(self.connection.buildSetModel.id.in_(ids)).\
            order_by(buildset_table.c.id.desc())

        try:
            return bq.all()
        except sqlalchemy.orm.exc.NoResultFound:
            return []

    def getBuildset(self, tenant, uuid):
        """Get one buildset with its builds"""

        buildset_table = self.connection.zuul_buildset_table

        q = self.session().query(self.connection.buildSetModel).\
            options(orm.joinedload(self.connection.buildSetModel.refs)).\
            options(orm.joinedload(
                self.connection.buildSetModel.buildset_events)).\
            options(orm.joinedload(self.connection.buildSetModel.builds).
                    subqueryload(self.connection.buildModel.artifacts)).\
            options(orm.joinedload(self.connection.buildSetModel.builds).
                    subqueryload(self.connection.buildModel.provides)).\
            options(orm.joinedload(self.connection.buildSetModel.builds).
                    subqueryload(self.connection.buildModel.ref))

        q = self.listFilter(q, buildset_table.c.tenant, tenant)
        q = self.listFilter(q, buildset_table.c.uuid, uuid)

        try:
            return q.one()
        except sqlalchemy.orm.exc.NoResultFound:
            return None
        except sqlalchemy.orm.exc.MultipleResultsFound:
            raise Exception("Multiple buildset found with uuid %s", uuid)

    def _getBuildsetsForDelete(self, updated_max, limit):
        # This does a query for buildsets, then a "select in" query
        # for each adjoining table.  This is used by the
        # deleteBuildsets method.
        buildset_table = self.connection.zuul_buildset_table
        q = self.session().query(self.connection.buildSetModel).\
            options(
                orm.selectinload(
                    self.connection.buildSetModel.builds,
                ),
                orm.selectinload(
                    self.connection.buildSetModel.refs,
                ),
                orm.selectinload(
                    self.connection.buildSetModel.buildset_events,
                ),
                orm.selectinload(
                    self.connection.buildSetModel.builds,
                    self.connection.buildModel.provides,
                ),
                orm.selectinload(
                    self.connection.buildSetModel.builds,
                    self.connection.buildModel.artifacts,
                ),
                orm.selectinload(
                    self.connection.buildSetModel.builds,
                    self.connection.buildModel.build_events,
                ))

        q = q.filter(buildset_table.c.updated < updated_max)
        q = q.order_by(buildset_table.c.id.desc()).limit(limit)
        try:
            return q.all()
        except sqlalchemy.orm.exc.NoResultFound:
            return []

    def deleteBuildsets(self, cutoff, batch_size):
        """Delete buildsets before the cutoff"""

        deleted = True
        while deleted:
            deleted = False
            oldest = None
            for buildset in self._getBuildsetsForDelete(
                    updated_max=cutoff, limit=batch_size):
                deleted = True
                if oldest is None:
                    oldest = buildset.updated
                else:
                    oldest = min(oldest, buildset.updated)
                self.session().delete(buildset)
            self.session().commit()
            if deleted:
                self.log.info("Deleted from %s to %s", oldest, cutoff)

    def createSystemEvent(self, *args, **kw):
        """Create a new system event"""
        e = self.connection.systemEventModel(*args, **kw)
        self.session().add(e)
        self.session().flush()
        return e

    def getSystemEvents(self, tenant=None, event_type=None,
                        limit=50, offset=0, idx_min=None, idx_max=None,
                        query_timeout=None):

        event_table = self.connection.zuul_system_event_table

        q = self.session().query(self.connection.systemEventModel)
        q = self.listFilter(q, event_table.c.tenant, tenant)
        q = self.listFilter(q, event_table.c.event_type, event_type)
        if idx_min:
            q = q.filter(event_table.c.id >= idx_min)
        if idx_max:
            q = q.filter(event_table.c.id <= idx_max)

        q = q.order_by(event_table.c.id.desc()).\
            limit(limit).\
            offset(offset)

        if query_timeout:
            # For MySQL, we can add a query hint directly.
            q = q.prefix_with(
                f'/*+ MAX_EXECUTION_TIME({query_timeout}) */',
                dialect='mysql')
            # For Postgres and mariadb, we add a comment that we parse
            # in our event handler.
            q = q.with_statement_hint(
                f'/* postgres_statement_timeout={query_timeout} */',
                dialect_name='postgresql')
            q = q.with_statement_hint(
                f'/* mariadb_statement_timeout={query_timeout} */',
                dialect_name='mariadb')

        try:
            return q.all()
        except sqlalchemy.orm.exc.NoResultFound:
            return []

    def deleteSystemEvents(self, cutoff):
        """Delete system events before the cutoff"""

        event_table = self.connection.zuul_system_event_table
        q = sa.delete(self.connection.systemEventModel)\
              .where(event_table.c.event_time < cutoff)
        result = self.session().execute(q)
        self.session().commit()
        if result.rowcount:
            self.log.info("Deleted %s events until %s",
                          result.rowcount, cutoff)


class SQLConnection(BaseConnection):
    driver_name = 'sql'
    log = logging.getLogger("zuul.SQLConnection")
    # This is used by tests only
    force_migrations = False

    def __init__(self, driver, connection_name, connection_config):

        super(SQLConnection, self).__init__(driver, connection_name,
                                            connection_config)

        self.dburi = None
        self.engine = None
        self.connection = None
        self.table_prefix = self.connection_config.get('table_prefix', '')
        self.log.info("Initializing SQL connection {} (prefix: {})".format(
            connection_name, self.table_prefix))

        try:
            self.dburi = self.connection_config.get('dburi')
            self.metadata = sa.MetaData()

            # Recycle connections if they've been idle for more than 1 second.
            # MySQL connections are lightweight and thus keeping long-lived
            # connections around is not valuable.
            self.engine = sa.create_engine(
                self.dburi,
                poolclass=sqlalchemy.pool.QueuePool,
                pool_recycle=self.connection_config.get('pool_recycle', 1),
                future=True)

            self._setup_models()

            # If we want the objects returned from query() to be
            # usable outside of the session, we need to expunge them
            # from the session, and since the DatabaseSession always
            # calls commit() on the session when the context manager
            # exits, we need to inform the session not to expire
            # objects when it does so.
            self.session_factory = orm.sessionmaker(bind=self.engine,
                                                    expire_on_commit=False,
                                                    autoflush=False,
                                                    future=True)
            self.session = orm.scoped_session(self.session_factory)
        except sa.exc.NoSuchModuleError:
            self.log.error(
                "The required module for the dburi dialect isn't available.")
            raise

    def getSession(self):
        return DatabaseSession(self)

    def _migrate(self, revision='head'):
        """Perform the alembic migrations for this connection"""
        # Note that this method needs to be called with an external lock held.
        # The reason for this is we retrieve the alembic version and run the
        # alembic migrations in different database transactions which opens
        # us to races without an external lock.
        with self.engine.begin() as conn:
            context = alembic.migration.MigrationContext.configure(conn)
            current_rev = context.get_current_revision()
        self.log.debug('Current migration revision: %s' % current_rev)

        config = alembic.config.Config()
        config.set_main_option("script_location",
                               "zuul:driver/sql/alembic")
        config.set_main_option("sqlalchemy.url",
                               self.connection_config.get('dburi').
                               replace('%', '%%'))

        # Alembic lets us add arbitrary data in the tag argument. We can
        # leverage that to tell the upgrade scripts about the table prefix.
        tag = {'table_prefix': self.table_prefix}

        if current_rev is None and not self.force_migrations:
            self.metadata.create_all(self.engine)
            alembic.command.stamp(config, revision, tag=tag)
        else:
            alembic.command.upgrade(config, revision, tag=tag)

    def onLoad(self, zk_client, component_registry=None):
        safe_connection = quote_plus(self.connection_name)
        while True:
            try:
                with locked(
                    SessionAwareLock(
                        zk_client.client,
                        f"{CONNECTION_LOCK_ROOT}/{safe_connection}/migration")
                ):
                    self._migrate()
                break
            except sa.exc.OperationalError as e:
                self.log.error(
                    "Unable to connect to the database or establish the "
                    "required tables: %s", e)
            except Exception:
                self.log.exception("Error setting up database:")
                raise
            time.sleep(10)

    def _setup_models(self):
        Base = orm.declarative_base(metadata=self.metadata)

        class RefModel(Base):
            __tablename__ = self.table_prefix + REF_TABLE
            id = sa.Column(sa.Integer, primary_key=True)
            project = sa.Column(sa.String(SQL_MAX_STRING_LENGTH),
                                nullable=False)
            ref = sa.Column(sa.String(SQL_MAX_STRING_LENGTH),
                            nullable=False)
            ref_url = sa.Column(sa.String(SQL_MAX_STRING_LENGTH),
                                nullable=False)
            change = sa.Column(ChangeType, nullable=False)
            patchset = sa.Column(SHAType(40), nullable=False)
            oldrev = sa.Column(SHAType(40), nullable=False)
            newrev = sa.Column(SHAType(40), nullable=False)
            branch = sa.Column(sa.String(SQL_MAX_STRING_LENGTH),
                               nullable=False)

            sa.Index(self.table_prefix + 'zuul_ref_project_idx', project)
            sa.Index(self.table_prefix + 'zuul_ref_ref_idx', ref)
            sa.Index(self.table_prefix + 'zuul_ref_change_idx', change)
            sa.Index(self.table_prefix + 'zuul_ref_patchset_idx', patchset)
            sa.Index(self.table_prefix + 'zuul_ref_oldrev_idx', oldrev)
            sa.Index(self.table_prefix + 'zuul_ref_newrev_idx', newrev)
            sa.Index(self.table_prefix + 'zuul_ref_branch_idx', branch)
            sa.UniqueConstraint(
                project, ref, change, patchset, oldrev, newrev,
                name=self.table_prefix + 'zuul_ref_unique')

        class BuildSetModel(Base):
            __tablename__ = self.table_prefix + BUILDSET_TABLE
            id = sa.Column(sa.Integer, primary_key=True)
            pipeline = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            message = sa.Column(sa.TEXT())
            tenant = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            result = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            uuid = sa.Column(sa.String(36))
            event_id = sa.Column(sa.String(SQL_MAX_STRING_LENGTH),
                                 nullable=True)
            event_timestamp = sa.Column(sa.DateTime, nullable=True)
            first_build_start_time = sa.Column(sa.DateTime, nullable=True)
            last_build_end_time = sa.Column(sa.DateTime, nullable=True)
            updated = sa.Column(sa.DateTime, nullable=True)
            queue_item_uuid = sa.Column(sa.String(36), nullable=True)
            parameters = sa.Column(sa.JSON(), nullable=True)
            user_id = sa.Column(sa.String(255), nullable=True)

            refs = orm.relationship(
                RefModel,
                secondary=self.table_prefix + BUILDSET_REF_TABLE)
            sa.Index(self.table_prefix + 'zuul_buildset_uuid_idx', uuid)
            sa.Index(self.table_prefix + 'zuul_buildset_tenant_idx', tenant)
            sa.Index(self.table_prefix + 'zuul_buildset_queue_item_uuid_idx',
                     queue_item_uuid)

            def createBuild(self, ref, *args, **kw):
                session = orm.session.Session.object_session(self)
                b = BuildModel(*args, **kw)
                b.buildset_id = self.id
                b.ref_id = ref.id
                self.builds.append(b)
                session.add(b)
                session.flush()
                return b

            def createBuildSetEvent(self, *args, **kw):
                session = orm.session.Session.object_session(self)
                e = BuildSetEventModel(*args, **kw)
                e.buildset_id = self.id
                self.buildset_events.append(e)
                session.add(e)
                session.flush()
                return e

        class BuildSetRefModel(Base):
            __tablename__ = self.table_prefix + BUILDSET_REF_TABLE
            __table_args__ = (
                sa.PrimaryKeyConstraint('buildset_id', 'ref_id'),
            )
            buildset_id = sa.Column(sa.Integer, sa.ForeignKey(
                self.table_prefix + BUILDSET_TABLE + ".id",
                name=self.table_prefix + 'zuul_buildset_ref_buildset_id_fkey',
            ))
            ref_id = sa.Column(sa.Integer, sa.ForeignKey(
                self.table_prefix + REF_TABLE + ".id",
                name=self.table_prefix + 'zuul_buildset_ref_ref_id_fkey',
            ))
            sa.Index(self.table_prefix + 'zuul_buildset_ref_buildset_id_idx',
                     buildset_id)
            sa.Index(self.table_prefix + 'zuul_buildset_ref_ref_id_idx',
                     ref_id)

        class BuildSetEventModel(Base):
            __tablename__ = self.table_prefix + BUILDSET_EVENT_TABLE
            id = sa.Column(sa.Integer, primary_key=True)
            buildset_id = sa.Column(sa.Integer, sa.ForeignKey(
                self.table_prefix + BUILDSET_TABLE + ".id",
                name=(self.table_prefix +
                      'zuul_buildset_event_buildset_id_fkey'),
            ))
            event_time = sa.Column(sa.DateTime)
            event_type = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            description = sa.Column(sa.TEXT())
            buildset = orm.relationship(BuildSetModel,
                                        backref=orm.backref(
                                            "buildset_events",
                                            cascade="all, delete-orphan"))
            sa.Index(self.table_prefix + 'zuul_buildset_event_buildset_id_idx',
                     buildset_id)

        class BuildModel(Base):
            __tablename__ = self.table_prefix + BUILD_TABLE
            id = sa.Column(sa.Integer, primary_key=True)
            buildset_id = sa.Column(sa.Integer, sa.ForeignKey(
                self.table_prefix + BUILDSET_TABLE + ".id",
                name=self.table_prefix + 'zuul_build_buildset_id_fkey',
            ))
            uuid = sa.Column(sa.String(36))
            job_name = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            result = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            start_time = sa.Column(sa.DateTime)
            end_time = sa.Column(sa.DateTime)
            voting = sa.Column(sa.Boolean)
            log_url = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            error_detail = sa.Column(sa.TEXT())
            final = sa.Column(sa.Boolean)
            held = sa.Column(sa.Boolean)
            nodeset = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            ref_id = sa.Column(sa.Integer, sa.ForeignKey(
                self.table_prefix + REF_TABLE + ".id",
                name=self.table_prefix + 'zuul_build_ref_id_fkey',
            ))
            execute_time = sa.Column(sa.DateTime)

            buildset = orm.relationship(BuildSetModel,
                                        backref=orm.backref(
                                            "builds",
                                            order_by=id,
                                            cascade="all, delete-orphan"))
            ref = orm.relationship(RefModel)

            sa.Index(self.table_prefix + 'zuul_build_job_name_buildset_id_idx',
                     job_name, buildset_id)
            sa.Index(self.table_prefix + 'zuul_build_uuid_buildset_id_idx',
                     uuid, buildset_id)
            sa.Index(self.table_prefix + 'zuul_build_buildset_id_idx',
                     buildset_id)
            sa.Index(self.table_prefix + 'zuul_build_ref_id_idx',
                     ref_id)
            sa.Index(self.table_prefix + 'zuul_build_end_time_idx',
                     end_time)

            @property
            def duration(self):
                if self.start_time and self.end_time:
                    return max(0.0,
                               (self.end_time -
                                self.start_time).total_seconds())
                else:
                    return None

            def createArtifact(self, *args, **kw):
                session = orm.session.Session.object_session(self)
                # SQLAlchemy reserves the 'metadata' attribute on
                # object models, so our model and table names use
                # 'meta', but here we accept data directly from
                # zuul_return where it's called 'metadata'.  Transform
                # the attribute name.
                if 'metadata' in kw:
                    kw['meta'] = kw['metadata']
                    del kw['metadata']
                a = ArtifactModel(*args, **kw)
                a.build_id = self.id
                self.artifacts.append(a)
                session.add(a)
                session.flush()
                return a

            def createProvides(self, *args, **kw):
                session = orm.session.Session.object_session(self)
                p = ProvidesModel(*args, **kw)
                p.build_id = self.id
                self.provides.append(p)
                session.add(p)
                session.flush()
                return p

            def createBuildEvent(self, *args, **kw):
                session = orm.session.Session.object_session(self)
                e = BuildEventModel(*args, **kw)
                e.build_id = self.id
                self.build_events.append(e)
                session.add(e)
                session.flush()
                return e

        class ArtifactModel(Base):
            __tablename__ = self.table_prefix + ARTIFACT_TABLE
            id = sa.Column(sa.Integer, primary_key=True)
            build_id = sa.Column(sa.Integer, sa.ForeignKey(
                self.table_prefix + BUILD_TABLE + ".id",
                name=self.table_prefix + 'zuul_artifact_build_id_fkey',
            ))
            name = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            url = sa.Column(sa.TEXT())
            meta = sa.Column('metadata', sa.TEXT())
            build = orm.relationship(BuildModel,
                                     backref=orm.backref(
                                         "artifacts",
                                         cascade="all, delete-orphan"))
            sa.Index(self.table_prefix + 'zuul_artifact_build_id_idx',
                     build_id)

        class ProvidesModel(Base):
            __tablename__ = self.table_prefix + PROVIDES_TABLE
            id = sa.Column(sa.Integer, primary_key=True)
            build_id = sa.Column(sa.Integer, sa.ForeignKey(
                self.table_prefix + BUILD_TABLE + ".id",
                name=self.table_prefix + 'zuul_provides_build_id_fkey',
            ))
            name = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            build = orm.relationship(BuildModel,
                                     backref=orm.backref(
                                         "provides",
                                         cascade="all, delete-orphan"))
            sa.Index(self.table_prefix + 'zuul_provides_build_id_idx',
                     build_id)

        class BuildEventModel(Base):
            __tablename__ = self.table_prefix + BUILD_EVENT_TABLE
            id = sa.Column(sa.Integer, primary_key=True)
            build_id = sa.Column(sa.Integer, sa.ForeignKey(
                self.table_prefix + BUILD_TABLE + ".id",
                name=self.table_prefix + 'zuul_build_event_build_id_fkey',
            ))
            event_time = sa.Column(sa.DateTime)
            event_type = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            description = sa.Column(sa.TEXT())
            build = orm.relationship(BuildModel,
                                     backref=orm.backref(
                                         "build_events",
                                         cascade="all, delete-orphan"))
            sa.Index(self.table_prefix + 'zuul_build_event_build_id_idx',
                     build_id)

        class SystemEventModel(Base):
            __tablename__ = self.table_prefix + SYSTEM_EVENT_TABLE
            id = sa.Column(sa.Integer, primary_key=True)
            tenant = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            event_id = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            event_time = sa.Column(sa.DateTime)
            event_type = sa.Column(sa.String(SQL_MAX_STRING_LENGTH))
            description = sa.Column(sa.TEXT())

            sa.Index(self.table_prefix + 'zuul_system_event_tenant_idx',
                     tenant)
            sa.Index(self.table_prefix + 'zuul_system_event_event_id_idx',
                     event_id)
            sa.Index(self.table_prefix + 'zuul_system_event_event_time_idx',
                     event_time)
            sa.Index(self.table_prefix + 'zuul_system_event_event_type_idx',
                     event_type)

        self.buildEventModel = BuildEventModel
        self.zuul_build_event_table = self.buildEventModel.__table__

        self.buildSetEventModel = BuildSetEventModel
        self.zuul_buildset_event_table = self.buildSetEventModel.__table__

        self.providesModel = ProvidesModel
        self.zuul_provides_table = self.providesModel.__table__

        self.artifactModel = ArtifactModel
        self.zuul_artifact_table = self.artifactModel.__table__

        self.buildModel = BuildModel
        self.zuul_build_table = self.buildModel.__table__

        self.buildSetModel = BuildSetModel
        self.zuul_buildset_table = self.buildSetModel.__table__

        self.refModel = RefModel
        self.zuul_ref_table = self.refModel.__table__

        self.buildSetRefModel = BuildSetRefModel
        self.zuul_buildset_ref_table = self.buildSetRefModel.__table__

        self.systemEventModel = SystemEventModel
        self.zuul_system_event_table = self.systemEventModel.__table__

    def onStop(self):
        self.log.debug("Stopping SQL connection %s" % self.connection_name)
        self.engine.dispose()

    def getBuilds(self, *args, **kw):
        """Return a list of Build objects"""
        with self.getSession() as db:
            return db.getBuilds(*args, **kw)

    def getBuild(self, *args, **kw):
        """Return a Build object"""
        with self.getSession() as db:
            return db.getBuild(*args, **kw)

    def getBuildsets(self, *args, **kw):
        """Return a list of BuildSet objects"""
        with self.getSession() as db:
            return db.getBuildsets(*args, **kw)

    def getBuildset(self, *args, **kw):
        """Return a BuildSet objects"""
        with self.getSession() as db:
            return db.getBuildset(*args, **kw)

    def deleteBuildsets(self, *args, **kw):
        """Delete buildsets"""
        with self.getSession() as db:
            return db.deleteBuildsets(*args, **kw)

    def getBuildTimes(self, *args, **kw):
        """Return a list of Build objects"""
        with self.getSession() as db:
            return db.getBuildTimes(*args, **kw)

    def getSystemEvents(self, *args, **kw):
        """Return a list of SystemEvent objects"""
        with self.getSession() as db:
            return db.getSystemEvents(*args, **kw)

    def deleteSystemEvents(self, *args, **kw):
        """Delete system events"""
        with self.getSession() as db:
            return db.deleteSystemEvents(*args, **kw)
