import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

import jwt
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.core.settings import Settings, get_settings
from app.db.base import Base
from app.db.models import ABExperimentResult, Initiative, InitiativeVersion, Team
from app.db.session import get_session
from app.main import app

from .helpers_monthly import monthly_baseline_csv


def _override_app(session_maker: async_sessionmaker[AsyncSession], settings: Settings) -> TestClient:
    async def _session_override() -> AsyncGenerator[AsyncSession, None]:
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_settings] = lambda: settings
    return TestClient(app)


def _setup_client(tmp_path: Path, *, auth_mode: str = "disabled", secret: str | None = None) -> tuple[TestClient, object]:
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
    settings = Settings(
        database_url="sqlite+aiosqlite://",
        data_dir=tmp_path,
        max_upload_mb=10,
        auth_mode=auth_mode,
        supabase_jwt_secret=secret,
    )
    return _override_app(session_maker, settings), engine


def _teardown(engine: object) -> None:
    app.dependency_overrides = {}
    asyncio.run(engine.dispose())


def _insert_initiative_version_scope(engine: object, *, scope: str, created_at: datetime) -> None:
    async def _insert() -> None:
        session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        async with session_maker() as session:
            team = Team(slug=f"team-{scope}", name=f"Team {scope}")
            session.add(team)
            await session.flush()
            initiative = Initiative(
                name=f"Initiative {scope}",
                owner_team_id=team.id,
                status="draft",
                tags_json={},
            )
            session.add(initiative)
            await session.flush()
            version = InitiativeVersion(
                initiative_id=initiative.id,
                version_number=1,
                data_scope=scope,
                screens_json=["home"],
                segments_json=[{"id": "s1", "penetration": 0.5}],
                metric_targets_json=[],
                assumptions_json={"data_scope": scope},
                p_success=0.5,
                effort_cost=100.0,
                strategic_weight=1.0,
                learning_value=1.0,
                horizon_weeks=13,
                created_at=created_at,
            )
            session.add(version)
            await session.commit()

    asyncio.run(_insert())


def _insert_ab_scope(engine: object, *, scope: str, created_at: datetime) -> None:
    async def _insert() -> None:
        session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        async with session_maker() as session:
            record = ABExperimentResult(
                experiment_id=f"exp-{scope}",
                scope=scope,
                screen="home",
                segment_id="s1",
                metric_driver="conversion",
                observed_uplift=0.1,
                ci_low=0.05,
                ci_high=0.15,
                sample_size=1000,
                significance_flag=True,
                quality_score=0.8,
                source="test",
                created_by="tester",
                start_at=created_at,
                end_at=created_at,
                created_at=created_at,
            )
            session.add(record)
            await session.commit()

    asyncio.run(_insert())


def test_config_screens_segments_crud_and_soft_delete(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        create_screen = client.post("/config/screens", json={"slug": "home", "name": "Home"})
        assert create_screen.status_code == 200, create_screen.text
        screen_id = create_screen.json()["id"]

        create_segment = client.post("/config/segments", json={"slug": "new_users", "name": "New Users"})
        assert create_segment.status_code == 200, create_segment.text
        segment_id = create_segment.json()["id"]

        list_screens = client.get("/config/screens")
        assert list_screens.status_code == 200
        assert len(list_screens.json()["items"]) == 1

        list_segments = client.get("/config/segments")
        assert list_segments.status_code == 200
        assert len(list_segments.json()["items"]) == 1

        patch_screen = client.patch(f"/config/screens/{screen_id}", json={"description": "Homepage"})
        assert patch_screen.status_code == 200
        assert patch_screen.json()["description"] == "Homepage"

        delete_screen = client.delete(f"/config/screens/{screen_id}")
        assert delete_screen.status_code == 200
        assert delete_screen.json()["is_active"] is False

        delete_segment = client.delete(f"/config/segments/{segment_id}")
        assert delete_segment.status_code == 200
        assert delete_segment.json()["is_active"] is False
    finally:
        _teardown(engine)


def test_config_endpoints_auth_and_admin_enforcement(tmp_path: Path) -> None:
    secret = "secret"
    client, engine = _setup_client(tmp_path, auth_mode="supabase_jwt", secret=secret)
    try:
        user_token = jwt.encode({"sub": "u1", "role": "user"}, secret, algorithm="HS256")
        admin_token = jwt.encode({"sub": "a1", "role": "admin"}, secret, algorithm="HS256")
        user_headers = {"Authorization": f"Bearer {user_token}"}
        admin_headers = {"Authorization": f"Bearer {admin_token}"}

        no_auth = client.get("/config/screens")
        assert no_auth.status_code == 401

        user_read = client.get("/config/screens", headers=user_headers)
        assert user_read.status_code == 200

        user_write = client.post("/config/screens", json={"slug": "s", "name": "S"}, headers=user_headers)
        assert user_write.status_code == 403

        admin_write = client.post("/config/screens", json={"slug": "s", "name": "S"}, headers=admin_headers)
        assert admin_write.status_code == 200
    finally:
        _teardown(engine)


def test_json_schemas_endpoints_and_text_duplicate(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        machine = client.get("/config/json-schemas")
        assert machine.status_code == 200, machine.text
        body = machine.json()
        assert body["schema_version"] == "v2"
        assert "metric_targets_json" in body
        assert "assumptions_json" in body
        assert "screens_json" in body
        assert "segments_json" in body
        assert "cannibalization_json" in body
        assert "interactions_json" in body
        assert "dataset_schemas" in body
        assert "baseline_metrics" in body["dataset_schemas"]
        assert "baseline_aoq_components" not in body["dataset_schemas"]
        assert "conventions" in body
        assert "deprecated_aliases" in body["conventions"]
        assert body["conventions"]["canonical_metric_tree"]["current_default_version"] == "v3"

        text = client.get("/config/json-schemas/text")
        assert text.status_code == 200
        assert "# Backlog Scoring JSON Shapes" in text.text
        assert "## metric_targets_json" in text.text
        assert "## assumptions_json" in text.text
        assert "## screens_json" in text.text
        assert "## segments_json" in text.text
        assert "## cannibalization_json" in text.text
        assert "## interactions_json" in text.text
        assert "## dataset_schemas" in text.text
        assert "## conventions" in text.text
        assert "baseline_aoq_components" not in text.text
    finally:
        _teardown(engine)


def test_config_metrics_crud(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        create_metric = client.post(
            "/config/metrics",
            json={
                "slug": "incremental_rto",
                "name": "Incremental RTO",
                "kind": "output",
                "driver_key": "incremental_rto",
                "unit": "rub",
            },
        )
        assert create_metric.status_code == 200, create_metric.text
        metric_id = create_metric.json()["id"]

        list_metrics = client.get("/config/metrics")
        assert list_metrics.status_code == 200
        assert len(list_metrics.json()["items"]) == 1

        patch_metric = client.patch(
            f"/config/metrics/{metric_id}",
            json={"description": "Alias for GMV impact"},
        )
        assert patch_metric.status_code == 200
        assert patch_metric.json()["description"] == "Alias for GMV impact"

        delete_metric = client.delete(f"/config/metrics/{metric_id}")
        assert delete_metric.status_code == 200
        assert delete_metric.json()["is_active"] is False
    finally:
        _teardown(engine)


def test_config_scopes_discovery_metadata_and_ordering(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        csv = monthly_baseline_csv(active_users=1000, ordering_users=100, orders=200, items=300, rto=6000, fm=1500)
        dataset_response = client.post(
            "/datasets/upload",
            params={
                "dataset_name": "baseline_main",
                "format": "csv",
                "schema_type": "baseline_metrics",
                "dataset_version": "v1",
                "scope": "x5_retail_test_v2",
            },
            files={"file": ("baseline.csv", csv, "text/csv")},
        )
        assert dataset_response.status_code == 200, dataset_response.text

        legacy_dataset_response = client.post(
            "/datasets/upload",
            params={
                "dataset_name": "baseline_legacy",
                "format": "csv",
                "schema_type": "baseline_metrics",
                "dataset_version": "v1",
                "scope": "x5_retail_test_v1",
            },
            files={"file": ("baseline.csv", csv, "text/csv")},
        )
        assert legacy_dataset_response.status_code == 200, legacy_dataset_response.text

        validation_timestamp = datetime(2026, 3, 12, 10, 0, tzinfo=UTC)
        custom_timestamp = datetime(2026, 3, 10, 9, 0, tzinfo=UTC)
        initiative_timestamp = datetime(2026, 3, 11, 8, 0, tzinfo=UTC)

        _insert_ab_scope(engine, scope="x5_validation_20260312", created_at=validation_timestamp)
        _insert_initiative_version_scope(engine, scope="custom_scope_alpha", created_at=custom_timestamp)
        _insert_initiative_version_scope(engine, scope="x5_retail_test_v2", created_at=initiative_timestamp)

        response = client.get("/config/scopes")
        assert response.status_code == 200, response.text
        body = response.json()
        items = body["items"]
        ids = [item["id"] for item in items]
        assert ids == [
            "prod",
            "x5_retail_test_v2",
            "x5_retail_test_v1",
            "x5_validation_20260312",
            "custom_scope_alpha",
        ]

        prod = items[0]
        assert prod["label"] == "Production"
        assert prod["kind"] == "prod"
        assert prod["is_default"] is True
        assert prod["is_legacy"] is False
        assert prod["read_only"] is False

        active_test = items[1]
        assert active_test["label"] == "X5 Retail Test v2"
        assert active_test["kind"] == "test"
        assert active_test["is_legacy"] is False
        assert active_test["read_only"] is False
        assert active_test["source_counts"]["datasets"] == 1
        assert active_test["source_counts"]["initiative_versions"] == 1

        legacy_test = items[2]
        assert legacy_test["label"] == "X5 Retail Test v1"
        assert legacy_test["kind"] == "test"
        assert legacy_test["is_legacy"] is True
        assert legacy_test["read_only"] is True
        assert legacy_test["source_counts"]["datasets"] == 1

        validation = items[3]
        assert validation["kind"] == "validation"
        assert validation["source_counts"]["ab_results"] == 1
        assert validation["last_seen_at"].startswith("2026-03-12T10:00:00")

        custom = items[4]
        assert custom["kind"] == "custom"
        assert custom["source_counts"]["initiative_versions"] == 1
        assert custom["last_seen_at"].startswith("2026-03-10T09:00:00")
    finally:
        _teardown(engine)


def test_config_scopes_auth_and_prod_when_empty(tmp_path: Path) -> None:
    secret = "secret"
    client, engine = _setup_client(tmp_path, auth_mode="supabase_jwt", secret=secret)
    try:
        user_token = jwt.encode({"sub": "u1", "role": "user"}, secret, algorithm="HS256")
        admin_token = jwt.encode({"sub": "a1", "role": "admin"}, secret, algorithm="HS256")
        user_headers = {"Authorization": f"Bearer {user_token}"}
        admin_headers = {"Authorization": f"Bearer {admin_token}"}

        no_auth = client.get("/config/scopes")
        assert no_auth.status_code == 401

        user_response = client.get("/config/scopes", headers=user_headers)
        assert user_response.status_code == 200, user_response.text
        assert user_response.json()["items"] == [
            {
                "id": "prod",
                "label": "Production",
                "kind": "prod",
                "is_default": True,
                "is_legacy": False,
                "read_only": False,
                "source_counts": {
                    "datasets": 0,
                    "ab_results": 0,
                    "initiative_versions": 0,
                },
                "last_seen_at": None,
            }
        ]

        admin_response = client.get("/config/scopes", headers=admin_headers)
        assert admin_response.status_code == 200
        assert admin_response.json()["items"][0]["id"] == "prod"
    finally:
        _teardown(engine)


def test_metric_tree_graph_create_list_get_validate(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        payload = {
            "template_name": "core_tree",
            "version": "v3",
            "is_default": True,
            "graph": {
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
            },
        }
        created = client.post("/config/metric-tree-graphs", json=payload)
        assert created.status_code == 200, created.text

        listed = client.get("/config/metric-tree-graphs", params={"template_name": "core_tree"})
        assert listed.status_code == 200
        assert len(listed.json()["items"]) == 1

        versions = client.get("/config/metric-tree-graphs/core_tree/versions")
        assert versions.status_code == 200
        assert versions.json()["items"][0]["version"] == "v3"
        assert versions.json()["items"][0]["is_default"] is True
        assert versions.json()["items"][0]["is_legacy"] is False

        fetched = client.get("/config/metric-tree-graphs/core_tree/v3")
        assert fetched.status_code == 200
        assert fetched.json()["template_name"] == "core_tree"
        assert fetched.json()["is_legacy"] is False

        validated = client.post("/config/metric-tree-graphs/core_tree/v3/validate")
        assert validated.status_code == 200, validated.text
        assert validated.json()["valid"] is True
        assert validated.json()["warnings"] == []
    finally:
        _teardown(engine)


def test_metric_tree_graph_validation_rejects_cycles(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        payload = {
            "template_name": "broken_tree",
            "version": "1",
            "graph": {
                "nodes": [
                    {"node_id": "mau_effective", "label": "MAU effective"},
                    {"node_id": "orders", "label": "Orders", "is_targetable": True},
                    {"node_id": "items", "label": "Items"},
                    {"node_id": "gmv", "label": "GMV"},
                    {"node_id": "margin", "label": "Margin"},
                ],
                "edges": [
                    {"from_node": "mau_effective", "to_node": "orders"},
                    {"from_node": "orders", "to_node": "items"},
                    {"from_node": "items", "to_node": "orders"},
                ],
            },
        }
        created = client.post("/config/metric-tree-graphs", json=payload)
        assert created.status_code == 422
    finally:
        _teardown(engine)


def test_metric_tree_graph_validation_rejects_extra_edge_not_in_formula(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        payload = {
            "template_name": "broken_tree",
            "version": "v3",
            "graph": {
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
                    {"from_node": "orders", "to_node": "aov"},
                    {"from_node": "rto", "to_node": "fm"},
                    {"from_node": "fm_pct", "to_node": "fm"},
                ],
            },
        }
        created = client.post("/config/metric-tree-graphs", json=payload)
        assert created.status_code == 422
        assert "inbound edges must match formula refs exactly" in created.text
    finally:
        _teardown(engine)


def test_metric_tree_graph_validation_rejects_forbidden_legacy_nodes(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        payload = {
            "template_name": "legacy_tree",
            "version": "v3",
            "graph": {
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
                    {"node_id": "gmv", "label": "GMV", "formula": "rto"},
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
                    {"from_node": "rto", "to_node": "gmv"},
                ],
            },
        }
        created = client.post("/config/metric-tree-graphs", json=payload)
        assert created.status_code == 422
        assert "deprecated for current metric trees" in created.text
    finally:
        _teardown(engine)


def test_dataset_upload_with_column_mapping_success(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        csv = (
            "Segment,Start,End,ActiveUsers,OrderingUsers,Orders,Items,RTO,FM\n"
            "s1,2025-01-01,2025-01-31,1000,100,200,200,4000,1200\n"
            "s1,2025-02-01,2025-02-28,1000,100,200,200,4000,1200\n"
            "s1,2025-03-01,2025-03-31,1000,100,200,200,4000,1200\n"
        )
        mapping = {
            "segment_id": "Segment",
            "date_start": "Start",
            "date_end": "End",
            "active_users": "ActiveUsers",
            "ordering_users": "OrderingUsers",
            "orders": "Orders",
            "items": "Items",
            "rto": "RTO",
            "fm": "FM",
        }
        response = client.post(
            "/datasets/upload",
            params={
                "dataset_name": "baseline_mapped",
                "format": "csv",
                "schema_type": "baseline_metrics",
                "dataset_version": "v1",
                "column_mapping": json.dumps(mapping),
            },
            files={"file": ("baseline.csv", csv, "text/csv")},
        )
        assert response.status_code == 200, response.text
        metadata = response.json()["dataset"]["columns"]
        assert metadata == [
            "segment_id",
            "date_start",
            "date_end",
            "active_users",
            "ordering_users",
            "orders",
            "items",
            "rto",
            "fm",
        ]
    finally:
        _teardown(engine)


def test_dataset_upload_with_column_mapping_errors(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        csv_with_extra = (
            "Segment,Start,End,ActiveUsers,OrderingUsers,Orders,Items,RTO,FM,Extra\n"
            "s1,2025-01-01,2025-01-31,1000,100,200,200,4000,1200,1\n"
            "s1,2025-02-01,2025-02-28,1000,100,200,200,4000,1200,1\n"
            "s1,2025-03-01,2025-03-31,1000,100,200,200,4000,1200,1\n"
        )
        valid_mapping = {
            "segment_id": "Segment",
            "date_start": "Start",
            "date_end": "End",
            "active_users": "ActiveUsers",
            "ordering_users": "OrderingUsers",
            "orders": "Orders",
            "items": "Items",
            "rto": "RTO",
            "fm": "FM",
        }
        extra_column_error = client.post(
            "/datasets/upload",
            params={
                "dataset_name": "baseline_extra",
                "format": "csv",
                "schema_type": "baseline_metrics",
                "dataset_version": "v1",
                "column_mapping": json.dumps(valid_mapping),
            },
            files={"file": ("baseline.csv", csv_with_extra, "text/csv")},
        )
        assert extra_column_error.status_code == 422

        unknown_key_error = client.post(
            "/datasets/upload",
            params={
                "dataset_name": "baseline_unknown_key",
                "format": "csv",
                "schema_type": "baseline_metrics",
                "dataset_version": "v1",
                "column_mapping": json.dumps({"not_a_canonical_key": "Segment"}),
            },
            files={
                "file": (
                    "baseline.csv",
                    "Segment,Start,End,ActiveUsers,OrderingUsers,Orders,Items,RTO,FM\ns1,2025-01-01,2025-01-31,1000,100,200,200,4000,1200\ns1,2025-02-01,2025-02-28,1000,100,200,200,4000,1200\ns1,2025-03-01,2025-03-31,1000,100,200,200,4000,1200\n",
                    "text/csv",
                )
            },
        )
        assert unknown_key_error.status_code == 422
    finally:
        _teardown(engine)


def test_dataset_scope_listing_and_version_filters(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        csv = monthly_baseline_csv(active_users=1000, ordering_users=100, orders=200, items=200, rto=4000, fm=1200)
        for scope, version in (("prod", "v1"), ("x5_retail_test_v1", "v2")):
            response = client.post(
                "/datasets/upload",
                params={
                    "dataset_name": "baseline_main",
                    "format": "csv",
                    "schema_type": "baseline_metrics",
                    "dataset_version": version,
                    "scope": scope,
                },
                files={"file": ("baseline.csv", csv, "text/csv")},
            )
            assert response.status_code == 200, response.text

        scoped_list = client.get("/datasets", params={"scope": "x5_retail_test_v1"})
        assert scoped_list.status_code == 200
        assert len(scoped_list.json()["items"]) == 1
        assert scoped_list.json()["items"][0]["scope"] == "x5_retail_test_v1"

        scoped_versions = client.get("/datasets/baseline_main/versions", params={"scope": "prod"})
        assert scoped_versions.status_code == 200
        assert [item["version"] for item in scoped_versions.json()["items"]] == ["v1"]
    finally:
        _teardown(engine)


def test_baseline_aoq_components_upload_rejected_by_schema_enum(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        legacy_csv = (
            "segment_id,component_id,component_name,baseline_aoq_value\n"
            "s1,favorites,Favorites,0.4\n"
            "s1,listing,Listing,0.6\n"
        )
        response = client.post(
            "/datasets/upload",
            params={
                "dataset_name": "aoq_components",
                "format": "csv",
                "schema_type": "baseline_aoq_components",
                "dataset_version": "v1",
            },
            files={"file": ("aoq.csv", legacy_csv, "text/csv")},
        )
        assert response.status_code == 422
        assert "baseline_aoq_components" in response.text
    finally:
        _teardown(engine)


def test_scoring_methodology_endpoints(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        machine = client.get("/config/scoring-methodology")
        assert machine.status_code == 200, machine.text
        body = machine.json()
        assert body["version"] == "v3"
        assert body["canonical_metrics"]["primary"] == ["rto", "fm"]
        assert body["canonical_metrics"]["deprecated_aliases"]["incremental_gmv"] == "incremental_rto"
        assert any(item["metric"] == "rto" and item["formula"] == "orders * aov" for item in body["causal_chain"])
        assert any(item["case"] == "Рост AOQ" for item in body["examples"])
        assert "Monte Carlo" in body["monte_carlo"]["physical_distribution"]

        text = client.get("/config/scoring-methodology/text")
        assert text.status_code == 200
        assert "# Scoring Methodology" in text.text
        assert "Primary metrics: RTO, FM" in text.text
        assert "GMV -> RTO" in text.text
        assert "Historical A/B learning" in text.text
        assert "screen + metric_driver + segment_id" in text.text
        assert "AOQ component" not in text.text
        assert "## Monte Carlo semantics" in text.text
        assert "Bernoulli success/failure gate" in text.text
        assert "## Practical examples" in text.text
    finally:
        _teardown(engine)
