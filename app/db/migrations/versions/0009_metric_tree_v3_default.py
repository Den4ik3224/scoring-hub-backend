"""seed canonical metric tree v3 and make it default

Revision ID: 0009_metric_tree_v3
Revises: 0008_initiative_scope
Create Date: 2026-03-11 13:10:00.000000
"""

from __future__ import annotations
from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0009_metric_tree_v3"
down_revision = "0008_initiative_scope"
branch_labels = None
depends_on = None


_GRAPH_ID = "a81e6998-c932-4e6d-9f1d-cfd8ff7ff1ce"
_TEMPLATE_NAME = "x5_retail_test_tree"
_VERSION = "v3"
_GRAPH_JSON = {
    "nodes": [
        {"node_id": "mau", "label": "MAU", "formula": None, "is_targetable": True, "unit": "users", "description": None},
        {"node_id": "penetration", "label": "Penetration", "formula": None, "is_targetable": True, "unit": "ratio", "description": None},
        {"node_id": "conversion", "label": "Conversion", "formula": None, "is_targetable": True, "unit": "ratio", "description": None},
        {"node_id": "frequency", "label": "Frequency", "formula": None, "is_targetable": True, "unit": "orders_per_user_per_week", "description": None},
        {"node_id": "aoq", "label": "AOQ", "formula": None, "is_targetable": True, "unit": "items_per_order", "description": None},
        {"node_id": "aiv", "label": "AIV", "formula": None, "is_targetable": True, "unit": "rub_per_item", "description": None},
        {"node_id": "fm_pct", "label": "FM%", "formula": None, "is_targetable": True, "unit": "ratio", "description": None},
        {
            "node_id": "mau_effective",
            "label": "MAU effective",
            "formula": "mau * penetration",
            "is_targetable": False,
            "unit": "users",
            "description": "Effective addressable audience after segment penetration.",
        },
        {
            "node_id": "orders",
            "label": "Orders",
            "formula": "mau_effective * conversion * frequency",
            "is_targetable": False,
            "unit": "orders_per_week",
            "description": None,
        },
        {
            "node_id": "items",
            "label": "Items",
            "formula": "orders * aoq",
            "is_targetable": False,
            "unit": "items_per_week",
            "description": None,
        },
        {
            "node_id": "aov",
            "label": "AOV",
            "formula": "aoq * aiv",
            "is_targetable": False,
            "unit": "rub_per_order",
            "description": None,
        },
        {
            "node_id": "rto",
            "label": "RTO",
            "formula": "orders * aov",
            "is_targetable": False,
            "unit": "rub_per_week",
            "description": None,
        },
        {
            "node_id": "fm",
            "label": "FM",
            "formula": "rto * fm_pct",
            "is_targetable": False,
            "unit": "rub_per_week",
            "description": None,
        },
    ],
    "edges": [
        {"from_node": "mau", "to_node": "mau_effective"},
        {"from_node": "penetration", "to_node": "mau_effective"},
        {"from_node": "mau_effective", "to_node": "orders"},
        {"from_node": "conversion", "to_node": "orders"},
        {"from_node": "frequency", "to_node": "orders"},
        {"from_node": "orders", "to_node": "items"},
        {"from_node": "aoq", "to_node": "items"},
        {"from_node": "aoq", "to_node": "aov"},
        {"from_node": "aiv", "to_node": "aov"},
        {"from_node": "orders", "to_node": "rto"},
        {"from_node": "aov", "to_node": "rto"},
        {"from_node": "rto", "to_node": "fm"},
        {"from_node": "fm_pct", "to_node": "fm"},
    ],
}


def upgrade() -> None:
    bind = op.get_bind()
    metric_tree_graphs = sa.table(
        "metric_tree_graphs",
        sa.column("id", sa.String),
        sa.column("template_name", sa.String),
        sa.column("version", sa.String),
        sa.column("graph_json", sa.JSON),
        sa.column("is_default", sa.Boolean),
        sa.column("created_by", sa.String),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    now = datetime.now(timezone.utc)
    bind.execute(sa.text("UPDATE metric_tree_graphs SET is_default = false"))
    exists = bind.execute(
        sa.text(
            """
            SELECT id
            FROM metric_tree_graphs
            WHERE template_name = :template_name AND version = :version
            """
        ).bindparams(template_name=_TEMPLATE_NAME, version=_VERSION)
    ).scalar()

    if exists:
        bind.execute(
            metric_tree_graphs.update()
            .where(
                sa.and_(
                    metric_tree_graphs.c.template_name == _TEMPLATE_NAME,
                    metric_tree_graphs.c.version == _VERSION,
                )
            )
            .values(
                graph_json=_GRAPH_JSON,
                is_default=True,
                created_by="system",
                updated_at=now,
            )
        )
        return

    bind.execute(
        metric_tree_graphs.insert().values(
            id=_GRAPH_ID,
            template_name=_TEMPLATE_NAME,
            version=_VERSION,
            graph_json=_GRAPH_JSON,
            is_default=True,
            created_by="system",
            created_at=now,
            updated_at=now,
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            DELETE FROM metric_tree_graphs
            WHERE template_name = :template_name AND version = :version
            """
        ).bindparams(template_name=_TEMPLATE_NAME, version=_VERSION)
    )
