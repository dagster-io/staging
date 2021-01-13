"""change varchar to text

Revision ID: 0da417ae1b81
Revises: 375e95bad550
Create Date: 2021-01-12 12:29:33.410870

"""
import sqlalchemy as sa
from alembic import op
from dagster.core.storage.migration.utils import has_table

# pylint: disable=no-member

# revision identifiers, used by Alembic.
revision = "0da417ae1b81"
down_revision = "375e95bad550"
branch_labels = None
depends_on = None


def upgrade():
    if has_table("runs"):
        with op.batch_alter_table("runs") as batch_op:
            batch_op.alter_column(
                sa.Column("pipeline_name"), type_=sa.Text, existing_type=sa.String,
            )
            batch_op.alter_column(sa.Column("run_body"), type_=sa.Text, existing_type=sa.String)
            batch_op.alter_column(sa.Column("partition"), type_=sa.Text, existing_type=sa.String)
            batch_op.alter_column(
                sa.Column("partition_set"), type_=sa.Text, existing_type=sa.String,
            )

    if has_table("secondary_indexes"):
        with op.batch_alter_table(
            table_name="secondary_indexes", sa.Column("secondary_indexes")
        ) as batch_op:
            batch_op.alter_column(
                sa.Column("name"), type_=sa.Text, existing_type=sa.String,
            )

    if has_table("run_tags"):
        with op.batch_alter_table("run_tags") as batch_op:
            batch_op.alter_column(sa.Column("key"), type_=sa.Text, existing_type=sa.String)
            batch_op.alter_column(sa.Column("value"), type_=sa.Text, existing_type=sa.String)


def downgrade():
    if has_table("runs"):
        with op.batch_alter_table("runs") as batch_op:
            batch_op.alter_column(
                sa.Column("pipeline_name"), type_=sa.String, existing_type=sa.Text,
            )
            batch_op.alter_column(sa.Column("run_body"), type_=sa.String, existing_type=sa.Text)
            batch_op.alter_column(sa.Column("partition"), type_=sa.String, existing_type=sa.Text)
            batch_op.alter_column(
                sa.Column("partition_set"), type_=sa.String, existing_type=sa.Text,
            )

    if has_table("secondary_indexes"):
        with op.batch_alter_table(
            table_name="secondary_indexes", sa.Column("secondary_indexes")
        ) as batch_op:
            batch_op.alter_column(
                sa.Column("name"), type_=sa.String, existing_type=sa.Text,
            )

    if has_table("run_tags"):
        with op.batch_alter_table(sa.Column("run_tags")) as batch_op:
            batch_op.alter_column(sa.Column("key"), type_=sa.String, existing_type=sa.Text)
            batch_op.alter_column(sa.Column("value"), type_=sa.String, existing_type=sa.Text)
