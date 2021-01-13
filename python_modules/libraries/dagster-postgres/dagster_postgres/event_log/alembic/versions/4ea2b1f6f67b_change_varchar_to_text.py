"""change varchar to text

Revision ID: 4ea2b1f6f67b
Revises: b32a4f3036d2
Create Date: 2021-01-12 12:39:53.493651

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import reflection

# pylint: disable=no-member

# revision identifiers, used by Alembic.
revision = "4ea2b1f6f67b"
down_revision = "b32a4f3036d2"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()

    if "event_logs" in has_tables:
        with op.batch_alter_table("event_logs") as batch_op:
            batch_op.alter_column(sa.Column("step_key"), type_=sa.Text, existing_type=sa.String)
            batch_op.alter_column(sa.Column("asset_key"), type_=sa.Text, existing_type=sa.String)
            batch_op.alter_column(sa.Column("partition"), type_=sa.Text, existing_type=sa.String)

    if "secondary_indexes" in has_tables:
        with op.batch_alter_table("secondary_indexes") as batch_op:
            op.alter_column(sa.Column("name", type_=sa.Text, existing_type=sa.String))

    if "asset_keys" in has_tables:
        with op.batch_alter_table("asset_keys") as batch_op:
            op.alter_column(sa.Column("asset_key", type_=sa.Text, existing_type=sa.String))


def downgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()

    if "event_logs" in has_tables:
        with op.batch_alter_table("event_logs") as batch_op:
            batch_op.alter_column(sa.Column("step_key"), type_=sa.String, existing_type=sa.Text)
            batch_op.alter_column(sa.Column("asset_key"), type_=sa.String, existing_type=sa.Text)
            batch_op.alter_column(sa.Column("partition"), type_=sa.String, existing_type=sa.Text)

    if "secondary_indexes" in has_tables:
        with op.batch_alter_table("secondary_indexes") as batch_op:
            op.alter_column(sa.Column("name", type_=sa.String, existing_type=sa.Text))

    if "asset_keys" in has_tables:
        with op.batch_alter_table("asset_keys") as batch_op:
            op.alter_column(sa.Column("asset_key", type_=sa.String, existing_type=sa.Text))
