"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-03-05 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "datasets",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("dataset_name", sa.String(length=255), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("schema_type", sa.String(length=64), nullable=False),
        sa.Column("format", sa.String(length=16), nullable=False),
        sa.Column("file_path", sa.String(length=1024), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("columns_json", sa.JSON(), nullable=False),
        sa.Column("schema_version", sa.String(length=32), nullable=False),
        sa.Column("uploaded_by", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("dataset_name", "version", name="uq_dataset_name_version"),
    )
    op.create_index("ix_datasets_name_created", "datasets", ["dataset_name", "created_at"])
    op.create_index("ix_datasets_schema_created", "datasets", ["schema_type", "created_at"])

    op.create_table(
        "metric_tree_templates",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("template_name", sa.String(length=255), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("definition_json", sa.JSON(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("template_name", "version", name="uq_metric_tree_name_version"),
    )
    op.create_index("ix_metric_tree_name_created", "metric_tree_templates", ["template_name", "created_at"])

    op.create_table(
        "evidence_priors_sets",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("priors_json", sa.JSON(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("name", "version", name="uq_evidence_priors_name_version"),
    )
    op.create_index("ix_evidence_priors_name_created", "evidence_priors_sets", ["name", "created_at"])

    op.create_table(
        "initiatives",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("tags_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_initiatives_external_id", "initiatives", ["external_id"])

    op.create_table(
        "scoring_runs",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("initiative_id", sa.String(length=36), sa.ForeignKey("initiatives.id"), nullable=True),
        sa.Column("initiative_name", sa.String(length=255), nullable=False),
        sa.Column("request_payload_json", sa.JSON(), nullable=False),
        sa.Column("resolved_inputs_json", sa.JSON(), nullable=False),
        sa.Column("assumptions_snapshot_hash", sa.String(length=64), nullable=False),
        sa.Column("rng_seed", sa.Integer(), nullable=False),
        sa.Column("monte_carlo_n", sa.Integer(), nullable=False),
        sa.Column("code_version", sa.String(length=255), nullable=False),
        sa.Column("deterministic_output_json", sa.JSON(), nullable=False),
        sa.Column("probabilistic_output_json", sa.JSON(), nullable=False),
        sa.Column("segment_breakdown_json", sa.JSON(), nullable=False),
        sa.Column("node_contributions_json", sa.JSON(), nullable=False),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recompute_of_run_id", sa.String(length=36), sa.ForeignKey("scoring_runs.id"), nullable=True),
    )
    op.create_index("ix_scoring_runs_created", "scoring_runs", ["created_at"])
    op.create_index("ix_scoring_runs_initiative", "scoring_runs", ["initiative_id"])
    op.create_index("ix_scoring_runs_snapshot_hash", "scoring_runs", ["assumptions_snapshot_hash"])


def downgrade() -> None:
    op.drop_index("ix_scoring_runs_snapshot_hash", table_name="scoring_runs")
    op.drop_index("ix_scoring_runs_initiative", table_name="scoring_runs")
    op.drop_index("ix_scoring_runs_created", table_name="scoring_runs")
    op.drop_table("scoring_runs")

    op.drop_index("ix_initiatives_external_id", table_name="initiatives")
    op.drop_table("initiatives")

    op.drop_index("ix_evidence_priors_name_created", table_name="evidence_priors_sets")
    op.drop_table("evidence_priors_sets")

    op.drop_index("ix_metric_tree_name_created", table_name="metric_tree_templates")
    op.drop_table("metric_tree_templates")

    op.drop_index("ix_datasets_schema_created", table_name="datasets")
    op.drop_index("ix_datasets_name_created", table_name="datasets")
    op.drop_table("datasets")
