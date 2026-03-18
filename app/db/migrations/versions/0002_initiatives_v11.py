"""initiative history and team ownership v1.1

Revision ID: 0002_initiatives_v11
Revises: 0001_initial
Create Date: 2026-03-06 12:00:00.000000
"""

from __future__ import annotations

import uuid

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0002_initiatives_v11"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teams",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.String(length=4096), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("ix_teams_slug", "teams", ["slug"], unique=True)
    op.create_index("ix_teams_active_updated", "teams", ["is_active", "updated_at"])

    unassigned_team_id = str(uuid.uuid4())
    op.execute(
        sa.text(
            "INSERT INTO teams (id, slug, name, description, is_active, created_at, updated_at) "
            "VALUES (:id, 'unassigned', 'Unassigned', 'System default team for legacy initiatives', true, NOW(), NOW())"
        ).bindparams(id=unassigned_team_id)
    )

    op.drop_index("ix_initiatives_external_id", table_name="initiatives")
    op.alter_column("initiatives", "external_id", new_column_name="external_key")
    op.create_index("ix_initiatives_external_key", "initiatives", ["external_key"])

    op.add_column("initiatives", sa.Column("description", sa.String(length=4096), nullable=True))
    op.add_column(
        "initiatives",
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'draft'")),
    )
    op.add_column("initiatives", sa.Column("owner_team_id", sa.String(length=36), nullable=True))
    op.add_column("initiatives", sa.Column("created_by_user_id", sa.String(length=255), nullable=True))
    op.add_column("initiatives", sa.Column("created_by_email", sa.String(length=255), nullable=True))
    op.add_column(
        "initiatives",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )
    op.add_column("initiatives", sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True))

    op.execute(
        sa.text("UPDATE initiatives SET owner_team_id = :team_id WHERE owner_team_id IS NULL").bindparams(
            team_id=unassigned_team_id
        )
    )
    op.alter_column("initiatives", "owner_team_id", nullable=False)
    op.create_foreign_key("fk_initiatives_owner_team_id", "initiatives", "teams", ["owner_team_id"], ["id"])
    op.create_index("ix_initiatives_owner_status_updated", "initiatives", ["owner_team_id", "status", "updated_at"])

    op.create_table(
        "initiative_versions",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("initiative_id", sa.String(length=36), sa.ForeignKey("initiatives.id"), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("title_override", sa.String(length=255), nullable=True),
        sa.Column("description_override", sa.String(length=4096), nullable=True),
        sa.Column("screens_json", sa.JSON(), nullable=False),
        sa.Column("segments_json", sa.JSON(), nullable=False),
        sa.Column("metric_targets_json", sa.JSON(), nullable=False),
        sa.Column("assumptions_json", sa.JSON(), nullable=False),
        sa.Column("p_success", sa.Float(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("evidence_type", sa.String(length=255), nullable=True),
        sa.Column("effort_cost", sa.Float(), nullable=True),
        sa.Column("strategic_weight", sa.Float(), nullable=True),
        sa.Column("learning_value", sa.Float(), nullable=True),
        sa.Column("horizon_weeks", sa.Integer(), nullable=True),
        sa.Column("decay_json", sa.JSON(), nullable=True),
        sa.Column("discount_rate_annual", sa.Float(), nullable=True),
        sa.Column("cannibalization_json", sa.JSON(), nullable=True),
        sa.Column("interactions_json", sa.JSON(), nullable=True),
        sa.Column("created_by_user_id", sa.String(length=255), nullable=True),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("change_comment", sa.String(length=4096), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index(
        "ix_initiative_versions_initiative_version",
        "initiative_versions",
        ["initiative_id", "version_number"],
        unique=True,
    )
    op.create_index("ix_initiative_versions_initiative_created", "initiative_versions", ["initiative_id", "created_at"])

    op.add_column("scoring_runs", sa.Column("initiative_version_id", sa.String(length=36), nullable=True))
    op.add_column("scoring_runs", sa.Column("triggered_by_user_id", sa.String(length=255), nullable=True))
    op.add_column("scoring_runs", sa.Column("triggered_by_email", sa.String(length=255), nullable=True))
    op.add_column("scoring_runs", sa.Column("triggered_by_role", sa.String(length=64), nullable=True))
    op.add_column("scoring_runs", sa.Column("run_label", sa.String(length=255), nullable=True))
    op.add_column("scoring_runs", sa.Column("run_purpose", sa.String(length=32), nullable=True))

    op.create_foreign_key(
        "fk_scoring_runs_initiative_version_id",
        "scoring_runs",
        "initiative_versions",
        ["initiative_version_id"],
        ["id"],
    )
    op.create_index(
        "ix_scoring_runs_initiative_version_created",
        "scoring_runs",
        ["initiative_version_id", "created_at"],
    )
    op.create_index(
        "ix_scoring_runs_triggered_by_created",
        "scoring_runs",
        ["triggered_by_user_id", "created_at"],
    )
    op.create_index("ix_scoring_runs_run_purpose_created", "scoring_runs", ["run_purpose", "created_at"])

    op.execute("UPDATE scoring_runs SET triggered_by_user_id = created_by WHERE triggered_by_user_id IS NULL")


def downgrade() -> None:
    op.drop_index("ix_scoring_runs_run_purpose_created", table_name="scoring_runs")
    op.drop_index("ix_scoring_runs_triggered_by_created", table_name="scoring_runs")
    op.drop_index("ix_scoring_runs_initiative_version_created", table_name="scoring_runs")
    op.drop_constraint("fk_scoring_runs_initiative_version_id", "scoring_runs", type_="foreignkey")
    op.drop_column("scoring_runs", "run_purpose")
    op.drop_column("scoring_runs", "run_label")
    op.drop_column("scoring_runs", "triggered_by_role")
    op.drop_column("scoring_runs", "triggered_by_email")
    op.drop_column("scoring_runs", "triggered_by_user_id")
    op.drop_column("scoring_runs", "initiative_version_id")

    op.drop_index("ix_initiative_versions_initiative_created", table_name="initiative_versions")
    op.drop_index("ix_initiative_versions_initiative_version", table_name="initiative_versions")
    op.drop_table("initiative_versions")

    op.drop_index("ix_initiatives_owner_status_updated", table_name="initiatives")
    op.drop_constraint("fk_initiatives_owner_team_id", "initiatives", type_="foreignkey")
    op.drop_column("initiatives", "archived_at")
    op.drop_column("initiatives", "updated_at")
    op.drop_column("initiatives", "created_by_email")
    op.drop_column("initiatives", "created_by_user_id")
    op.drop_column("initiatives", "owner_team_id")
    op.drop_column("initiatives", "status")
    op.drop_column("initiatives", "description")
    op.drop_index("ix_initiatives_external_key", table_name="initiatives")
    op.alter_column("initiatives", "external_key", new_column_name="external_id")
    op.create_index("ix_initiatives_external_id", "initiatives", ["external_id"])

    op.drop_index("ix_teams_active_updated", table_name="teams")
    op.drop_index("ix_teams_slug", table_name="teams")
    op.drop_table("teams")
