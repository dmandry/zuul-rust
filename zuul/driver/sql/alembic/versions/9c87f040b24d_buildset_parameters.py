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

"""buildset_parameters

Revision ID: 9c87f040b24d
Revises: 21d30f0bffef
Create Date: 2026-04-30 07:42:34.022768

"""

# revision identifiers, used by Alembic.
revision = '9c87f040b24d'
down_revision = '21d30f0bffef'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(table_prefix=''):
    op.add_column(
        table_prefix + 'zuul_buildset', sa.Column('parameters', sa.JSON()))


def downgrade():
    raise Exception("Downgrades not supported")
