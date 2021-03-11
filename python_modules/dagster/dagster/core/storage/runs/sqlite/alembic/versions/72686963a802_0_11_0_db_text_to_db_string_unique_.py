"""0.11.0 db.Text to db.String(UNIQUE_VARCHAR_LEN) for MySQL Support

Revision ID: 72686963a802
Revises: 0da417ae1b81
Create Date: 2021-03-11 15:02:29.174707

"""
import sqlalchemy as sa
from alembic import op
from dagster.core.storage.migration.utils import has_table
from dagster.core.storage.sql import UNIQUE_VARCHAR_LEN

# pylint: disable=no-member

# revision identifiers, used by Alembic.
revision = "72686963a802"
down_revision = "521d4caca7ad"
branch_labels = None
depends_on = None


def upgrade():
    if has_table("secondary_indexes"):
        with op.batch_alter_table("secondary_indexes") as batch_op:
            batch_op.alter_column(
                "name",
                type_=sa.String(UNIQUE_VARCHAR_LEN),
                existing_type=sa.Text,
            )


def downgrade():
    if has_table("secondary_indexes"):
        with op.batch_alter_table("secondary_indexes") as batch_op:
            batch_op.alter_column(
                "name",
                type_=sa.Text,
                existing_type=sa.String(UNIQUE_VARCHAR_LEN),
            )
