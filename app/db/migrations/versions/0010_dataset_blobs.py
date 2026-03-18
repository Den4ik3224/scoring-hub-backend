"""add dataset_blobs table and make file_path nullable

Revision ID: 0010_dataset_blobs
Revises: 0009_metric_tree_v3
Create Date: 2026-03-18 18:00:00.000000
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision = "0010_dataset_blobs"
down_revision = "0009_metric_tree_v3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset_blobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.String(36),
            sa.ForeignKey("datasets.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("data", sa.LargeBinary, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_dataset_blobs_dataset_id", "dataset_blobs", ["dataset_id"])

    op.alter_column("datasets", "file_path", existing_type=sa.String(1024), nullable=True)


def downgrade() -> None:
    op.alter_column("datasets", "file_path", existing_type=sa.String(1024), nullable=False)

    op.drop_index("ix_dataset_blobs_dataset_id", table_name="dataset_blobs")
    op.drop_table("dataset_blobs")
