"""0.11.0 db.Text to db.String(UNIQUE_VARCHAR_LEN) for MySQL Support

Revision ID: 8c8467830471
Revises: ba4050312958
Create Date: 2021-03-11 15:13:19.779216

"""
# revision identifiers, used by Alembic.
revision = "8c8467830471"
down_revision = "ba4050312958"
branch_labels = None
depends_on = None

# this revision is no-op since the underlying schema of the MySQL tables
# already reflected the VARCHAR(LEN) + UNIQUE requirement (via compiler directives)


def upgrade():
    pass


def downgrade():
    pass
