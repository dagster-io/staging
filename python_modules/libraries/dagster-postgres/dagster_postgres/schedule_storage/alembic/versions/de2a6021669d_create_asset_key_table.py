"""create_asset_key_table

Revision ID: de2a6021669d
Revises: 07f83cc13695
Create Date: 2020-10-13 10:43:07.011877

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import reflection

# pylint: disable=no-member


# revision identifiers, used by Alembic.
revision = "de2a6021669d"
down_revision = "07f83cc13695"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()
    if "event_logs" in has_tables and "asset_keys" not in has_tables:
        # create table
        op.create_table(
            "asset_keys",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("asset_key", sa.String, unique=True),
            sa.Column("create_timestamp", sa.DateTime, server_default=sa.text("CURRENT_TIMESTAMP")),
        )


def downgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()
    if "event_logs" in has_tables and "asset_keys" in has_tables:
        op.drop_table("asset_keys")
