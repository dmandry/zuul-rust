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

"""Add execute_time column to build table

Revision ID: 21d30f0bffef
Revises: 8e35d000c786
Create Date: 2026-01-15 09:13:57.507315

"""

# revision identifiers, used by Alembic.
revision = '21d30f0bffef'
down_revision = '8e35d000c786'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(table_prefix=''):
    op.add_column(
        table_prefix + 'zuul_build', sa.Column('execute_time', sa.DateTime)
    )


def downgrade():
    raise Exception("Downgrades not supported")
