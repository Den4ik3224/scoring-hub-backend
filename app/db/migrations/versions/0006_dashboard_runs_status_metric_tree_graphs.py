"""add dashboard-alignment entities and run status fields

Revision ID: 0006_dashboard_alignment
Revises: 0005_config_dimensions
Create Date: 2026-03-09 19:30:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0006_dashboard_alignment"
down_revision = "0005_config_dimensions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scoring_runs",
        sa.Column("run_status", sa.String(length=16), nullable=False, server_default="success"),
    )
    op.add_column(
        "scoring_runs",
        sa.Column("error_message", sa.String(length=2048), nullable=True),
    )
    op.create_index("ix_scoring_runs_status_created", "scoring_runs", ["run_status", "created_at"])

    op.create_table(
        "metric_tree_graphs",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("template_name", sa.String(length=255), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("graph_json", sa.JSON(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("template_name", "version", name="uq_metric_tree_graph_name_version"),
    )
    op.create_index("ix_metric_tree_graph_name_created", "metric_tree_graphs", ["template_name", "created_at"])

    op.execute(sa.text("UPDATE scoring_runs SET run_status = 'success' WHERE run_status IS NULL"))
    op.alter_column("scoring_runs", "run_status", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_metric_tree_graph_name_created", table_name="metric_tree_graphs")
    op.drop_table("metric_tree_graphs")

    op.drop_index("ix_scoring_runs_status_created", table_name="scoring_runs")
    op.drop_column("scoring_runs", "error_message")
    op.drop_column("scoring_runs", "run_status")
