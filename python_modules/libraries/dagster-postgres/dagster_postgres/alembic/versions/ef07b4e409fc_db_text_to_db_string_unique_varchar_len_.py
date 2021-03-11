"""db.Text to db.String(UNIQUE_VARCHAR_LEN) for MySQL Support

Revision ID: ef07b4e409fc
Revises: 3e71cf573ba6
Create Date: 2021-03-11 11:00:47.809760

"""
import sqlalchemy as sa
from alembic import op
from dagster.core.storage.migration.utils import has_table
from dagster.core.storage.sql import UNIQUE_VARCHAR_LEN

# revision identifiers, used by Alembic.
revision = "ef07b4e409fc"
down_revision = "3e71cf573ba6"
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
    if has_table("asset_keys"):
        with op.batch_alter_table("asset_keys") as batch_op:
            batch_op.alter_column(
                "asset_key", type_=sa.String(UNIQUE_VARCHAR_LEN), existing_type=sa.Text
            )


def downgrade():
    if has_table("secondary_indexes"):
        with op.batch_alter_table("secondary_indexes") as batch_op:
            batch_op.alter_column(
                "name",
                type_=sa.Text,
                existing_type=sa.String(UNIQUE_VARCHAR_LEN),
            )
    if has_table("asset_keys"):
        with op.batch_alter_table("asset_keys") as batch_op:
            batch_op.alter_column(
                "asset_key",
                type_=sa.Text,
                existing_type=sa.String(UNIQUE_VARCHAR_LEN),
            )
