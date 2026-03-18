"""add historical ab learning layer and policy defaults

Revision ID: 0004_learning_layer_v12
Revises: 0003_scoring_policy_vnext
Create Date: 2026-03-06 18:30:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0004_learning_layer_v12"
down_revision = "0003_scoring_policy_vnext"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ab_experiment_results",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("experiment_id", sa.String(length=255), nullable=False),
        sa.Column("initiative_id", sa.String(length=36), nullable=True),
        sa.Column("screen", sa.String(length=128), nullable=False),
        sa.Column("segment_id", sa.String(length=128), nullable=True),
        sa.Column("metric_driver", sa.String(length=128), nullable=False),
        sa.Column("observed_uplift", sa.Float(), nullable=False),
        sa.Column("ci_low", sa.Float(), nullable=True),
        sa.Column("ci_high", sa.Float(), nullable=True),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("significance_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("quality_score", sa.Float(), nullable=False),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("ix_ab_results_screen_metric_end", "ab_experiment_results", ["screen", "metric_driver", "end_at"])
    op.create_index("ix_ab_results_segment_created", "ab_experiment_results", ["segment_id", "created_at"])
    op.create_index("ix_ab_results_initiative_created", "ab_experiment_results", ["initiative_id", "created_at"])
    op.create_index("ix_ab_results_quality_created", "ab_experiment_results", ["quality_score", "created_at"])

    op.execute(sa.text("UPDATE scoring_policies SET is_default = false"))
    op.execute(
        sa.text(
            """
            INSERT INTO scoring_policies (id, name, version, policy_json, is_default, created_by, created_at)
            VALUES (
                '8a3c8f9e-01d7-4f68-931e-9a56afc467b3',
                'ev_policy_vnext_learning',
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
                '"learning_defaults":{"mode":"bayesian","lookback_days":730,"half_life_days":180,"min_quality":0.6,"min_sample_size":500}}'
            )
        )
    )


def downgrade() -> None:
    op.execute(sa.text("DELETE FROM scoring_policies WHERE name = 'ev_policy_vnext_learning' AND version = '1'"))
    op.execute(sa.text("UPDATE scoring_policies SET is_default = true WHERE name = 'ev_policy_vnext' AND version = '1'"))

    op.drop_index("ix_ab_results_quality_created", table_name="ab_experiment_results")
    op.drop_index("ix_ab_results_initiative_created", table_name="ab_experiment_results")
    op.drop_index("ix_ab_results_segment_created", table_name="ab_experiment_results")
    op.drop_index("ix_ab_results_screen_metric_end", table_name="ab_experiment_results")
    op.drop_table("ab_experiment_results")
