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
                table_name="runs",
                column_name="pipeline_name",
                type_=sa.Text,
                existing_type=sa.String,
            )
            batch_op.alter_column(
                table_name="runs", column_name="run_body", type_=sa.Text, existing_type=sa.String
            )
            batch_op.alter_column(
                table_name="runs", column_name="partition", type_=sa.Text, existing_type=sa.String
            )
            batch_op.alter_column(
                table_name="runs",
                column_name="partition_set",
                type_=sa.Text,
                existing_type=sa.String,
            )

    if has_table("secondary_indexes"):
        with op.batch_alter_table(
            table_name="secondary_indexes", column_name="secondary_indexes"
        ) as batch_op:
            batch_op.alter_column(
                table_name="secondary_indexes",
                column_name="name",
                type_=sa.Text,
                existing_type=sa.String,
            )

    if has_table("run_tags"):
        with op.batch_alter_table("run_tags") as batch_op:
            batch_op.alter_column(
                table_name="run_tags", column_name="key", type_=sa.Text, existing_type=sa.String
            )
            batch_op.alter_column(
                table_name="run_tags", column_name="value", type_=sa.Text, existing_type=sa.String
            )


def downgrade():
    if has_table("runs"):
        with op.batch_alter_table("runs") as batch_op:
            batch_op.alter_column(
                table_name="runs",
                column_name="pipeline_name",
                type_=sa.String,
                existing_type=sa.Text,
            )
            batch_op.alter_column(
                table_name="runs", column_name="run_body", type_=sa.String, existing_type=sa.Text
            )
            batch_op.alter_column(
                table_name="runs", column_name="partition", type_=sa.String, existing_type=sa.Text
            )
            batch_op.alter_column(
                table_name="runs",
                column_name="partition_set",
                type_=sa.String,
                existing_type=sa.Text,
            )

    if has_table("secondary_indexes"):
        with op.batch_alter_table(
            table_name="secondary_indexes", column_name="secondary_indexes"
        ) as batch_op:
            batch_op.alter_column(
                table_name="secondary_indexes",
                column_name="name",
                type_=sa.String,
                existing_type=sa.Text,
            )

    if has_table("run_tags"):
        with op.batch_alter_table(column_name="run_tags") as batch_op:
            batch_op.alter_column(
                table_name="run_tags", column_name="key", type_=sa.String, existing_type=sa.Text
            )
            batch_op.alter_column(
                table_name="run_tags", column_name="value", type_=sa.String, existing_type=sa.Text
            )
