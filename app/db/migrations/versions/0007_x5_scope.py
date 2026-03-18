"""add x5 test scope isolation and config metrics

Revision ID: 0007_x5_scope
Revises: 0006_dashboard_alignment
Create Date: 2026-03-09 22:10:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0007_x5_scope"
down_revision = "0006_dashboard_alignment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("datasets", sa.Column("scope", sa.String(length=64), nullable=True, server_default="prod"))
    op.execute(sa.text("UPDATE datasets SET scope = 'prod' WHERE scope IS NULL"))
    op.drop_constraint("uq_dataset_name_version", "datasets", type_="unique")
    op.create_unique_constraint("uq_dataset_name_version_scope", "datasets", ["dataset_name", "version", "scope"])
    op.create_index("ix_datasets_scope_schema_created", "datasets", ["scope", "schema_type", "created_at"])
    op.alter_column("datasets", "scope", nullable=False, server_default=None)

    op.add_column(
        "ab_experiment_results",
        sa.Column("scope", sa.String(length=64), nullable=True, server_default="prod"),
    )
    op.execute(sa.text("UPDATE ab_experiment_results SET scope = 'prod' WHERE scope IS NULL"))
    op.create_index(
        "ix_ab_results_scope_screen_metric_end",
        "ab_experiment_results",
        ["scope", "screen", "metric_driver", "end_at"],
    )
    op.alter_column("ab_experiment_results", "scope", nullable=False, server_default=None)

    op.create_table(
        "config_metrics",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("driver_key", sa.String(length=255), nullable=False),
        sa.Column("unit", sa.String(length=64), nullable=True),
        sa.Column("description", sa.String(length=4096), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("ix_config_metrics_slug", "config_metrics", ["slug"], unique=True)
    op.create_index("ix_config_metrics_active_updated", "config_metrics", ["is_active", "updated_at"])


def downgrade() -> None:
    op.drop_index("ix_config_metrics_active_updated", table_name="config_metrics")
    op.drop_index("ix_config_metrics_slug", table_name="config_metrics")
    op.drop_table("config_metrics")

    op.drop_index("ix_ab_results_scope_screen_metric_end", table_name="ab_experiment_results")
    op.drop_column("ab_experiment_results", "scope")

    op.drop_index("ix_datasets_scope_schema_created", table_name="datasets")
    op.drop_constraint("uq_dataset_name_version_scope", "datasets", type_="unique")
    op.create_unique_constraint("uq_dataset_name_version", "datasets", ["dataset_name", "version"])
    op.drop_column("datasets", "scope")
