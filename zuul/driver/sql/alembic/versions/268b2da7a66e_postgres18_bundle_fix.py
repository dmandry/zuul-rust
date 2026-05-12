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

"""postgres18_bundle_fix

Revision ID: 268b2da7a66e
Revises: 21d30f0bffef
Create Date: 2026-05-12 08:17:16.765238

"""

# revision identifiers, used by Alembic.
revision = '268b2da7a66e'
down_revision = '21d30f0bffef'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


BUILD_TABLE = 'zuul_build'
BUILDSET_TABLE = 'zuul_buildset'


def upgrade(table_prefix=''):
    connection = op.get_bind()
    dialect_name = connection.engine.dialect.name
    prefixed_build = table_prefix + BUILD_TABLE
    prefixed_build_new = table_prefix + BUILD_TABLE + '_new'
    prefixed_buildset = table_prefix + BUILDSET_TABLE
    prefixed_buildset_new = table_prefix + BUILDSET_TABLE + '_new'

    if dialect_name == 'postgresql':
        # Postgres 18 does not rename the hidden internal not_null
        # constraint:
        statement = f"""
            alter table {prefixed_build} rename constraint
            {prefixed_build_new}_id_not_null to
            {prefixed_build}_id_not_null;
        """
        with op.get_bind().begin_nested() as nested:
            try:
                connection.execute(sa.text(statement))
            except sa.exc.ProgrammingError as err:
                if "does not exist" not in str(err):
                    raise
                nested.rollback()

        statement = f"""
            alter table {prefixed_buildset} rename constraint
            {prefixed_buildset_new}_id_not_null to
            {prefixed_buildset}_id_not_null;
        """
        with op.get_bind().begin_nested() as nested:
            try:
                connection.execute(sa.text(statement))
            except sa.exc.ProgrammingError as err:
                if "does not exist" not in str(err):
                    raise
                nested.rollback()


def downgrade():
    raise Exception("Downgrades not supported")
