"""add backfill table

Revision ID: 6d366a41b4be
Revises: 0da417ae1b81
Create Date: 2021-02-10 14:45:29.022887

"""
import sqlalchemy as db
from alembic import op
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = "6d366a41b4be"
down_revision = "0da417ae1b81"
branch_labels = None
depends_on = None

# pylint: disable=no-member


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()
    if "backfills" not in has_tables:
        op.create_table(
            "backfills",
            db.Column("id", db.Integer, primary_key=True, autoincrement=True),
            db.Column("backfill_id", db.String(32), unique=True, nullable=False),
            db.Column("partition_set_origin_id", db.String(255), nullable=False),
            db.Column("status", db.String(255), nullable=False),
            db.Column("timestamp", db.types.TIMESTAMP, nullable=False),
            db.Column("body", db.Text),
        )


def downgrade():
    pass
