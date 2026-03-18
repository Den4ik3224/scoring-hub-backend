"""add config dimensions for screens and segments

Revision ID: 0005_config_dimensions
Revises: 0004_learning_layer_v12
Create Date: 2026-03-09 14:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0005_config_dimensions"
down_revision = "0004_learning_layer_v12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "config_screens",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.String(length=4096), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("ix_config_screens_slug", "config_screens", ["slug"], unique=True)
    op.create_index("ix_config_screens_active_updated", "config_screens", ["is_active", "updated_at"])

    op.create_table(
        "config_segments",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.String(length=4096), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("ix_config_segments_slug", "config_segments", ["slug"], unique=True)
    op.create_index("ix_config_segments_active_updated", "config_segments", ["is_active", "updated_at"])


def downgrade() -> None:
    op.drop_index("ix_config_segments_active_updated", table_name="config_segments")
    op.drop_index("ix_config_segments_slug", table_name="config_segments")
    op.drop_table("config_segments")

    op.drop_index("ix_config_screens_active_updated", table_name="config_screens")
    op.drop_index("ix_config_screens_slug", table_name="config_screens")
    op.drop_table("config_screens")
