import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.schemas.score import ScoreRunRequest
from app.core.settings import Settings, get_settings
from app.db.base import Base
from app.db.models import Dataset, MetricTreeGraph
from app.db.session import get_session
from app.main import app
from app.services.version_resolver import resolve_scoring_inputs


def _graph(version: str, *, is_default: bool) -> MetricTreeGraph:
    graph_json = {
        "nodes": [
            {"node_id": "mau", "label": "MAU", "is_targetable": True},
            {"node_id": "penetration", "label": "Penetration", "is_targetable": True},
            {"node_id": "conversion", "label": "Conversion", "is_targetable": True},
            {"node_id": "frequency", "label": "Frequency", "is_targetable": True},
            {"node_id": "aoq", "label": "AOQ", "is_targetable": True},
            {"node_id": "aiv", "label": "AIV", "is_targetable": True},
            {"node_id": "fm_pct", "label": "FM%", "is_targetable": True},
            {"node_id": "mau_effective", "label": "MAU effective", "formula": "mau * penetration"},
            {"node_id": "orders", "label": "Orders", "formula": "mau_effective * conversion * frequency"},
            {"node_id": "items", "label": "Items", "formula": "orders * aoq"},
            {"node_id": "aov", "label": "AOV", "formula": "aoq * aiv"},
            {"node_id": "rto", "label": "RTO", "formula": "orders * aov"},
            {"node_id": "fm", "label": "FM", "formula": "rto * fm_pct"},
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
    if version == "v1":
        graph_json = {
            "nodes": [
                {"node_id": "mau_effective", "label": "MAU effective"},
                {"node_id": "orders", "label": "Orders", "formula": "mau_effective * conversion", "is_targetable": True},
                {"node_id": "items", "label": "Items", "formula": "orders * aoq"},
                {"node_id": "gmv", "label": "GMV", "formula": "items * aiv"},
                {"node_id": "margin", "label": "Margin", "formula": "gmv * fm_pct"},
            ],
            "edges": [
                {"from_node": "mau_effective", "to_node": "orders"},
                {"from_node": "orders", "to_node": "items"},
                {"from_node": "items", "to_node": "gmv"},
                {"from_node": "gmv", "to_node": "margin"},
            ],
        }
    elif version == "v2":
        graph_json = {
            "nodes": [
                {"node_id": "mau_effective", "label": "MAU effective"},
                {"node_id": "orders", "label": "Orders", "formula": "mau_effective * conversion * frequency"},
                {"node_id": "items", "label": "Items", "formula": "orders * aoq"},
                {"node_id": "aov", "label": "AOV", "formula": "aoq * aiv"},
                {"node_id": "rto", "label": "RTO", "formula": "items * aiv"},
                {"node_id": "fm", "label": "FM", "formula": "rto * fm_pct"},
                {"node_id": "gmv", "label": "GMV", "formula": "rto"},
                {"node_id": "margin", "label": "Margin", "formula": "fm"},
            ],
            "edges": [
                {"from_node": "mau_effective", "to_node": "orders"},
                {"from_node": "orders", "to_node": "items"},
                {"from_node": "orders", "to_node": "aov"},
                {"from_node": "items", "to_node": "rto"},
                {"from_node": "aov", "to_node": "rto"},
                {"from_node": "rto", "to_node": "fm"},
                {"from_node": "rto", "to_node": "gmv"},
                {"from_node": "fm", "to_node": "margin"},
            ],
        }
    return MetricTreeGraph(
        template_name="x5_retail_test_tree",
        version=version,
        graph_json=graph_json,
        is_default=is_default,
        created_by="tester",
    )


def _setup_client(tmp_path: Path) -> tuple[TestClient, async_sessionmaker[AsyncSession], object]:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    session_maker = asyncio.run(_setup())

    async def _session_override() -> AsyncGenerator[AsyncSession, None]:
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_settings] = lambda: Settings(
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        max_upload_mb=10,
        auth_mode="disabled",
    )
    return TestClient(app), session_maker, engine


def _teardown(engine: object) -> None:
    app.dependency_overrides = {}
    asyncio.run(engine.dispose())


def test_metric_tree_versions_endpoint_marks_legacy_v1_v2_and_default_v3(tmp_path: Path) -> None:
    client, session_maker, engine = _setup_client(tmp_path)
    try:
        async def _seed() -> None:
            async with session_maker() as session:
                session.add_all([_graph("v1", is_default=False), _graph("v2", is_default=False), _graph("v3", is_default=True)])
                await session.commit()

        asyncio.run(_seed())

        response = client.get("/config/metric-tree-graphs/x5_retail_test_tree/versions")
        assert response.status_code == 200, response.text
        items = {row["version"]: row for row in response.json()["items"]}
        assert items["v1"]["is_legacy"] is True
        assert items["v2"]["is_legacy"] is True
        assert items["v3"]["is_default"] is True
        assert items["v3"]["is_legacy"] is False
    finally:
        _teardown(engine)


def test_resolve_scoring_inputs_uses_default_metric_tree_v3(tmp_path: Path) -> None:
    baseline_csv_bytes = (
        b"segment_id,date_start,date_end,active_users,ordering_users,orders,items,rto,fm\n"
        b"s1,2025-01-01,2025-01-31,1000,100,200,400,40000,12000\n"
        b"s1,2025-02-01,2025-02-28,1000,100,200,400,40000,12000\n"
        b"s1,2025-03-01,2025-03-31,1000,100,200,400,40000,12000\n"
    )
    client, session_maker, engine = _setup_client(tmp_path)
    del client
    try:
        async def _seed_and_resolve() -> str:
            from app.db.repositories import datasets as dataset_repo
            async with session_maker() as session:
                session.add_all([_graph("v1", is_default=False), _graph("v2", is_default=False), _graph("v3", is_default=True)])
                ds = Dataset(
                    dataset_name="baseline",
                    version="v1",
                    schema_type="baseline_metrics",
                    format="csv",
                    file_path=None,
                    checksum_sha256="x" * 64,
                    row_count=1,
                    columns_json={"columns": []},
                    schema_version="v1",
                    uploaded_by="tester",
                    scope="prod",
                )
                session.add(ds)
                await session.flush()
                await dataset_repo.store_dataset_blob(session, ds.id, baseline_csv_bytes)
                await session.commit()
                payload = ScoreRunRequest(
                    initiative_name="default-graph",
                    segments=[{"id": "s1", "penetration": 0.5, "screen_penetration": {"home": 1.0}, "uplifts": {"aoq": 0.1}}],
                    screens=["home"],
                    metric_targets=[],
                    p_success=0.8,
                    confidence=0.8,
                    effort_cost=1000,
                    strategic_weight=1.0,
                    learning_value=1.0,
                    horizon_weeks=26,
                    monte_carlo={"enabled": False, "n": 1000, "seed": 1},
                )
                resolved = await resolve_scoring_inputs(session, payload)
                return resolved.metric_tree_source or ""

        metric_tree_source = asyncio.run(_seed_and_resolve())
        assert metric_tree_source == "graph:x5_retail_test_tree:v3"
    finally:
        _teardown(engine)
