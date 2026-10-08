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

"""build_time_index

Revision ID: e95335c00117
Revises: 1385a2593eb2
Create Date: 2026-10-08 12:58:00.772058

"""

# revision identifiers, used by Alembic.
revision = 'e95335c00117'
down_revision = '1385a2593eb2'
branch_labels = None
depends_on = None

from alembic import op

BUILD_TABLE = 'zuul_build'


def upgrade(table_prefix=''):
    prefixed_build = table_prefix + BUILD_TABLE
    op.create_index(f'{prefixed_build}_time_idx',
                    prefixed_build,
                    ['job_name', 'result', 'final', 'end_time'])


def downgrade():
    raise Exception("Downgrades not supported")
