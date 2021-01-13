"""change varchar to text

Revision ID: 0da417ae1b81
Revises: 375e95bad550
Create Date: 2021-01-12 12:29:33.410870

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import reflection

# pylint: disable=no-member

# revision identifiers, used by Alembic.
revision = "0da417ae1b81"
down_revision = "375e95bad550"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()

    if "runs" in has_tables:
        with op.batch_alter_table("runs") as batch_op:
            batch_op.alter_column(
                sa.Column("pipeline_name"), type_=sa.Text, existing_type=sa.String
            )
            batch_op.alter_column(sa.Column("run_body"), type_=sa.Text, existing_type=sa.String)
            batch_op.alter_column(sa.Column("partition"), type_=sa.Text, existing_type=sa.String)
            batch_op.alter_column(
                sa.Column("partition_set"), type_=sa.Text, existing_type=sa.String
            )

    if "secondary_indexes" in has_tables:
        with op.batch_alter_table("secondary_indexes") as batch_op:
            batch_op.alter_column(sa.Column("name"), type_=sa.Text, existing_type=sa.String)

    if "run_tags" in has_tables:
        with op.batch_alter_table("run_tags") as batch_op:
            batch_op.alter_column(sa.Column("key"), type_=sa.Text, existing_type=sa.String)
            batch_op.alter_column(sa.Column("value"), type_=sa.Text, existing_type=sa.String)


def downgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()

    if "runs" in has_tables:
        with op.batch_alter_table("runs") as batch_op:
            batch_op.alter_column(
                sa.Column("pipeline_name"), type_=sa.String, existing_type=sa.Text
            )
            batch_op.alter_column(sa.Column("run_body"), type_=sa.String, existing_type=sa.Text)
            batch_op.alter_column(sa.Column("partition"), type_=sa.String, existing_type=sa.Text)
            batch_op.alter_column(
                sa.Column("partition_set"), type_=sa.String, existing_type=sa.Text
            )

    if "secondary_indexes" in has_tables:
        with op.batch_alter_table("secondary_indexes") as batch_op:
            batch_op.alter_column(sa.Column("name"), type_=sa.String, existing_type=sa.Text)

    if "run_tags" in has_tables:
        with op.batch_alter_table("run_tags") as batch_op:
            batch_op.alter_column(sa.Column("key"), type_=sa.String, existing_type=sa.Text)
            batch_op.alter_column(sa.Column("value"), type_=sa.String, existing_type=sa.Text)
