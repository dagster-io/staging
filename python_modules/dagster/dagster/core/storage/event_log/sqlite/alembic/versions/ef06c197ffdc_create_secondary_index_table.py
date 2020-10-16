"""create_secondary_index_table

Revision ID: ef06c197ffdc
Revises: c34498c29964
Create Date: 2020-10-16 09:35:25.369956

"""
import sqlalchemy as db
from alembic import op
from sqlalchemy.engine import reflection

# pylint: disable=no-member

# revision identifiers, used by Alembic.
revision = "ef06c197ffdc"
down_revision = "c34498c29964"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()
    if "event_logs" in has_tables and "secondary_indexes" not in has_tables:
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
    if "event_logs" in has_tables and "secondary_indexes" in has_tables:
        op.drop_table("secondary_indexes")
