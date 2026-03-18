"""add first-class data_scope to initiative_versions

Revision ID: 0008_initiative_scope
Revises: 0007_x5_scope
Create Date: 2026-03-10 10:20:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0008_initiative_scope"
down_revision = "0007_x5_scope"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "initiative_versions",
        sa.Column("data_scope", sa.String(length=64), nullable=True, server_default="prod"),
    )
    op.execute(
        sa.text(
            """
            UPDATE initiative_versions
            SET data_scope = COALESCE(NULLIF(assumptions_json ->> 'data_scope', ''), 'prod')
            WHERE data_scope IS NULL
            """
        )
    )
    op.alter_column("initiative_versions", "data_scope", nullable=False, server_default=None)


def downgrade() -> None:
    op.drop_column("initiative_versions", "data_scope")
