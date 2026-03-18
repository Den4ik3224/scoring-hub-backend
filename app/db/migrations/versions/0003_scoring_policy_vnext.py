"""add scoring policies table for vnext formulas

Revision ID: 0003_scoring_policy_vnext
Revises: 0002_initiatives_v11
Create Date: 2026-03-06 14:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0003_scoring_policy_vnext"
down_revision = "0002_initiatives_v11"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scoring_policies",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("policy_json", sa.JSON(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("name", "version", name="uq_scoring_policy_name_version"),
    )
    op.create_index("ix_scoring_policy_name_created", "scoring_policies", ["name", "created_at"])
    op.create_index("ix_scoring_policy_default_created", "scoring_policies", ["is_default", "created_at"])

    op.execute(
        sa.text(
            """
            INSERT INTO scoring_policies (id, name, version, policy_json, is_default, created_by, created_at)
            VALUES (
                '2f473975-99df-49ec-b0f8-2cf6657b0f3b',
                'ev_policy_vnext',
                '1',
                CAST(:policy_json AS JSON),
                true,
                'system',
                NOW()
            )
            """
        ).bindparams(
            policy_json=(
                '{"primitive_metrics":["mau","penetration","conversion","frequency_monthly","aoq","aiv","fm_pct"],'
                '"derived_metrics":["orders","items","gmv","aov","margin","rto_orders"],'
                '"translator_enabled":true,'
                '"translations":{"aov":{"to":["aoq","aiv"],"weights":{"aoq":0.5,"aiv":0.5}}},'
                '"default_horizons":[4,13,26,52],'
                '"validation":{"reject_derived_targets_without_translation":true}}'
            )
        )
    )


def downgrade() -> None:
    op.drop_index("ix_scoring_policy_default_created", table_name="scoring_policies")
    op.drop_index("ix_scoring_policy_name_created", table_name="scoring_policies")
    op.drop_table("scoring_policies")
