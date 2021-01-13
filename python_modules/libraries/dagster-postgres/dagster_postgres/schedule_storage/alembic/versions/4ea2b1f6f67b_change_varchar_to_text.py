"""change varchar to text

Revision ID: 4ea2b1f6f67b
Revises: f3e43ff66603
Create Date: 2021-01-12 12:39:53.493651

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import reflection

# pylint: disable=no-member

# revision identifiers, used by Alembic.
revision = "4ea2b1f6f67b"
down_revision = "f3e43ff66603"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()

    if "jobs" in has_tables:
        with op.batch_alter_table("jobs") as batch_op:
            batch_op.alter_column(sa.Column("job_body"), type_=sa.Text, existing_type=sa.String)

    if "job_ticks" in has_tables:
        with op.batch_alter_table("job_ticks") as batch_op:
            batch_op.alter_column(sa.Column("tick_body"), type_=sa.Text, existing_type=sa.String)


def downgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()

    if "jobs" in has_tables:
        with op.batch_alter_table("jobs") as batch_op:
            batch_op.alter_column(sa.Column("job_body"), type_=sa.String, existing_type=sa.Text)

    if "job_ticks" in has_tables:
        with op.batch_alter_table("job_ticks") as batch_op:
            batch_op.alter_column(sa.Column("tick_body"), type_=sa.String, existing_type=sa.Text)
