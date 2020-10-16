"""create_secondary_index_table

Revision ID: 6c0d40c3d559
Revises: 07f83cc13695
Create Date: 2020-10-16 09:41:03.068648

"""
import sqlalchemy as db
from alembic import op
from sqlalchemy.engine import reflection

# pylint: disable=no-member

# revision identifiers, used by Alembic.
revision = "6c0d40c3d559"
down_revision = "07f83cc13695"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()
    if (
        "event_logs" in has_tables or "runs" in has_tables
    ) and "secondary_indexes" not in has_tables:
        # create table
        op.create_table(
            "secondary_indexes",
            db.Column("id", db.Integer, primary_key=True, autoincrement=True),
            db.Column("name", db.String, unique=True),
            db.Column("create_timestamp", db.DateTime, server_default=db.text("CURRENT_TIMESTAMP")),
            db.Column("migration_completed", db.DateTime),
        )


def downgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()
    if (
        "event_logs" in has_tables or "runs" in has_tables
    ) and "secondary_indexes" not in has_tables:
        op.drop_table("secondary_indexes")
