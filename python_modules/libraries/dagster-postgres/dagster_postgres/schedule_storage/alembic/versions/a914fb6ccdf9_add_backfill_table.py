"""add backfill table

Revision ID: a914fb6ccdf9
Revises: 4ea2b1f6f67b
Create Date: 2021-02-10 14:58:02.954242

"""
import sqlalchemy as db
from alembic import op
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = "a914fb6ccdf9"
down_revision = "4ea2b1f6f67b"
branch_labels = None
depends_on = None

# pylint: disable=no-member


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()
    if "runs" in has_tables and "backfills" not in has_tables:
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
