"""replace dataset_blobs with typed dataset tables

Revision ID: 0011_typed_dataset_tables
Revises: 0010_dataset_blobs
Create Date: 2026-03-18 20:00:00.000000
"""

from __future__ import annotations

import io
import uuid

import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import sqlalchemy as sa
from alembic import op

revision = "0011_typed_dataset_tables"
down_revision = "0010_dataset_blobs"
branch_labels = None
depends_on = None

ACTIVE_SCHEMA_TYPES = {"baseline_metrics", "baseline_funnel_steps", "cannibalization_matrix"}


def _parse_blob(data: bytes, fmt: str):
    buf = io.BytesIO(data)
    if fmt == "parquet":
        return pq.read_table(buf).to_pylist()
    return pa_csv.read_csv(buf).to_pylist()


def _to_date_str(val) -> str:
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def upgrade() -> None:
    # 1. Create typed tables
    op.create_table(
        "baseline_metric_rows",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("segment_id", sa.String(255), nullable=False),
        sa.Column("date_start", sa.Date, nullable=False),
        sa.Column("date_end", sa.Date, nullable=False),
        sa.Column("active_users", sa.Float, nullable=False),
        sa.Column("ordering_users", sa.Float, nullable=False),
        sa.Column("orders", sa.Float, nullable=False),
        sa.Column("items", sa.Float, nullable=False),
        sa.Column("rto", sa.Float, nullable=False),
        sa.Column("fm", sa.Float, nullable=False),
        sa.UniqueConstraint("dataset_id", "segment_id", "date_start", "date_end", name="uq_baseline_metric_natural_key"),
        sa.CheckConstraint("active_users >= 0", name="ck_bm_active_users_nonneg"),
        sa.CheckConstraint("ordering_users >= 0", name="ck_bm_ordering_users_nonneg"),
        sa.CheckConstraint("orders >= 0", name="ck_bm_orders_nonneg"),
        sa.CheckConstraint("items >= 0", name="ck_bm_items_nonneg"),
        sa.CheckConstraint("rto >= 0", name="ck_bm_rto_nonneg"),
        sa.CheckConstraint("fm >= 0", name="ck_bm_fm_nonneg"),
    )
    op.create_index("ix_baseline_metric_rows_dataset_id", "baseline_metric_rows", ["dataset_id"])

    op.create_table(
        "funnel_step_rows",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("segment_id", sa.String(255), nullable=False),
        sa.Column("screen", sa.String(255), nullable=False),
        sa.Column("step_id", sa.String(255), nullable=False),
        sa.Column("step_name", sa.String(255), nullable=False),
        sa.Column("step_order", sa.Integer, nullable=False),
        sa.Column("date_start", sa.Date, nullable=False),
        sa.Column("date_end", sa.Date, nullable=False),
        sa.Column("entered_users", sa.Float, nullable=False),
        sa.Column("advanced_users", sa.Float, nullable=False),
        sa.UniqueConstraint(
            "dataset_id", "segment_id", "screen", "step_id", "date_start", "date_end",
            name="uq_funnel_step_natural_key",
        ),
        sa.CheckConstraint("step_order >= 1", name="ck_fs_step_order_min"),
        sa.CheckConstraint("entered_users >= 0", name="ck_fs_entered_users_nonneg"),
        sa.CheckConstraint("advanced_users >= 0", name="ck_fs_advanced_users_nonneg"),
    )
    op.create_index("ix_funnel_step_rows_dataset_id", "funnel_step_rows", ["dataset_id"])

    op.create_table(
        "cannibalization_rows",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("from_screen", sa.String(255), nullable=False),
        sa.Column("to_screen", sa.String(255), nullable=False),
        sa.Column("segment_id", sa.String(255), nullable=False),
        sa.Column("cannibalization_rate", sa.Float, nullable=False),
        sa.UniqueConstraint(
            "dataset_id", "from_screen", "to_screen", "segment_id",
            name="uq_cannibalization_natural_key",
        ),
        sa.CheckConstraint("cannibalization_rate >= 0 AND cannibalization_rate <= 1", name="ck_cr_rate_range"),
    )
    op.create_index("ix_cannibalization_rows_dataset_id", "cannibalization_rows", ["dataset_id"])

    # 2. Data migration: blobs → typed rows
    conn = op.get_bind()
    datasets = conn.execute(
        sa.text("SELECT id, schema_type, format FROM datasets WHERE schema_type IN :types"),
        {"types": tuple(ACTIVE_SCHEMA_TYPES)},
    ).fetchall()

    for ds_id, schema_type, fmt in datasets:
        blob_row = conn.execute(
            sa.text("SELECT data FROM dataset_blobs WHERE dataset_id = :did"),
            {"did": ds_id},
        ).fetchone()
        if not blob_row:
            continue

        rows = _parse_blob(blob_row[0], fmt)
        if not rows:
            continue

        if schema_type == "baseline_metrics":
            table_name = "baseline_metric_rows"
            for row in rows:
                conn.execute(
                    sa.text(
                        f"INSERT INTO {table_name} (id, dataset_id, segment_id, date_start, date_end, "
                        f"active_users, ordering_users, orders, items, rto, fm) "
                        f"VALUES (:id, :did, :segment_id, :date_start, :date_end, "
                        f":active_users, :ordering_users, :orders, :items, :rto, :fm)"
                    ),
                    {
                        "id": str(uuid.uuid4()), "did": ds_id,
                        "segment_id": row["segment_id"],
                        "date_start": _to_date_str(row["date_start"]),
                        "date_end": _to_date_str(row["date_end"]),
                        "active_users": float(row["active_users"]),
                        "ordering_users": float(row["ordering_users"]),
                        "orders": float(row["orders"]),
                        "items": float(row["items"]),
                        "rto": float(row["rto"]),
                        "fm": float(row["fm"]),
                    },
                )
        elif schema_type == "baseline_funnel_steps":
            table_name = "funnel_step_rows"
            for row in rows:
                conn.execute(
                    sa.text(
                        f"INSERT INTO {table_name} (id, dataset_id, segment_id, screen, step_id, step_name, "
                        f"step_order, date_start, date_end, entered_users, advanced_users) "
                        f"VALUES (:id, :did, :segment_id, :screen, :step_id, :step_name, "
                        f":step_order, :date_start, :date_end, :entered_users, :advanced_users)"
                    ),
                    {
                        "id": str(uuid.uuid4()), "did": ds_id,
                        "segment_id": row["segment_id"],
                        "screen": row["screen"],
                        "step_id": row["step_id"],
                        "step_name": row["step_name"],
                        "step_order": int(row["step_order"]),
                        "date_start": _to_date_str(row["date_start"]),
                        "date_end": _to_date_str(row["date_end"]),
                        "entered_users": float(row["entered_users"]),
                        "advanced_users": float(row["advanced_users"]),
                    },
                )
        elif schema_type == "cannibalization_matrix":
            table_name = "cannibalization_rows"
            for row in rows:
                conn.execute(
                    sa.text(
                        f"INSERT INTO {table_name} (id, dataset_id, from_screen, to_screen, segment_id, "
                        f"cannibalization_rate) "
                        f"VALUES (:id, :did, :from_screen, :to_screen, :segment_id, :cannibalization_rate)"
                    ),
                    {
                        "id": str(uuid.uuid4()), "did": ds_id,
                        "from_screen": row["from_screen"],
                        "to_screen": row["to_screen"],
                        "segment_id": row["segment_id"],
                        "cannibalization_rate": float(row["cannibalization_rate"]),
                    },
                )

    # 3. Drop blob table and file_path column
    op.drop_index("ix_dataset_blobs_dataset_id", table_name="dataset_blobs")
    op.drop_table("dataset_blobs")
    op.drop_column("datasets", "file_path")


def downgrade() -> None:
    op.add_column("datasets", sa.Column("file_path", sa.String(1024), nullable=True))

    op.create_table(
        "dataset_blobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("data", sa.LargeBinary, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_dataset_blobs_dataset_id", "dataset_blobs", ["dataset_id"])

    # Note: data migration from typed rows back to blobs is not implemented (lossy downgrade)

    op.drop_index("ix_cannibalization_rows_dataset_id", table_name="cannibalization_rows")
    op.drop_table("cannibalization_rows")
    op.drop_index("ix_funnel_step_rows_dataset_id", table_name="funnel_step_rows")
    op.drop_table("funnel_step_rows")
    op.drop_index("ix_baseline_metric_rows_dataset_id", table_name="baseline_metric_rows")
    op.drop_table("baseline_metric_rows")
