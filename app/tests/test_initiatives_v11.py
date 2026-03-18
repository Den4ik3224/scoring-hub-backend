import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path

import jwt
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.core.settings import Settings, get_settings
from app.db.base import Base
from app.db.session import get_session
from app.main import app

from .helpers_monthly import monthly_baseline_csv


def _baseline_csv() -> str:
    return monthly_baseline_csv(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=200.0,
        rto=4000.0,
        fm=1200.0,
    )


def _version_payload(comment: str | None = None) -> dict:
    return {
        "change_comment": comment,
        "title_override": "Checkout uplift",
        "description_override": "v",
        "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.1}}],
        "screens": ["home"],
        "metric_targets": [],
        "p_success": 0.7,
        "confidence": 0.8,
        "effort_cost": 1000,
        "strategic_weight": 1.0,
        "learning_value": 1.0,
        "horizon_weeks": 26,
        "cannibalization": {"mode": "off"},
        "interactions": [],
        "monte_carlo": {"enabled": True, "n": 1000, "seed": 42},
    }


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


def _upload_baseline(client: TestClient, headers: dict | None = None) -> None:
    resp = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
        headers=headers or {},
    )
    assert resp.status_code == 200, resp.text


def test_team_crud_and_uniqueness(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        create = client.post("/teams", json={"slug": "growth", "name": "Growth"})
        assert create.status_code == 200, create.text
        team_id = create.json()["id"]

        duplicate = client.post("/teams", json={"slug": "growth", "name": "Duplicate"})
        assert duplicate.status_code == 409

        patch = client.patch(f"/teams/{team_id}", json={"description": "updated", "is_active": False})
        assert patch.status_code == 200
        assert patch.json()["is_active"] is False

        listed = client.get("/teams")
        assert listed.status_code == 200
        assert len(listed.json()["items"]) == 1
    finally:
        _teardown(engine)


def test_create_initiative_with_versions_and_immutable_history(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        team_id = client.post("/teams", json={"slug": "core", "name": "Core"}).json()["id"]
        create = client.post(
            "/initiatives",
            json={
                "external_key": "checkout-v1",
                "name": "Checkout",
                "owner_team_id": team_id,
                "initial_version": _version_payload("initial"),
            },
        )
        assert create.status_code == 200, create.text
        initiative_id = create.json()["id"]

        second = client.post(
            f"/initiatives/{initiative_id}/versions",
            json=_version_payload("second"),
        )
        assert second.status_code == 200, second.text

        versions = client.get(f"/initiatives/{initiative_id}/versions")
        assert versions.status_code == 200
        data = versions.json()["items"]
        assert len(data) == 2
        assert data[0]["version_number"] == 2
        assert data[1]["version_number"] == 1

        unsupported_update = client.patch(
            f"/initiatives/{initiative_id}/versions/{data[0]['id']}",
            json={"change_comment": "mutate"},
        )
        assert unsupported_update.status_code == 405
    finally:
        _teardown(engine)


def test_score_run_links_latest_version_and_recompute_keeps_linkage(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        _upload_baseline(client)
        team_id = client.post("/teams", json={"slug": "checkout", "name": "Checkout"}).json()["id"]
        created = client.post(
            "/initiatives",
            json={
                "name": "Checkout",
                "owner_team_id": team_id,
                "initial_version": _version_payload("v1"),
            },
        )
        initiative_id = created.json()["id"]
        v1_id = created.json()["latest_version_summary"]["id"]

        v2 = client.post(f"/initiatives/{initiative_id}/versions", json=_version_payload("v2"))
        assert v2.status_code == 200
        v2_id = v2.json()["id"]

        run = client.post("/score/run", json={"initiative_id": initiative_id, "run_purpose": "refresh"})
        assert run.status_code == 200, run.text
        run_id = run.json()["run_id"]

        detail = client.get(f"/score/runs/{run_id}")
        assert detail.status_code == 200
        assert detail.json()["initiative_version_id"] == v2_id

        compare = client.get(f"/initiatives/{initiative_id}/compare", params={"version_a": v1_id, "version_b": v2_id})
        assert compare.status_code == 200, compare.text
        assert compare.json()["outputs_available"] is False

        recompute = client.post(f"/score/runs/{run_id}/recompute")
        assert recompute.status_code == 200, recompute.text
        recompute_id = recompute.json()["run_id"]

        recompute_detail = client.get(f"/score/runs/{recompute_id}")
        assert recompute_detail.status_code == 200
        assert recompute_detail.json()["initiative_version_id"] == detail.json()["initiative_version_id"]
        assert recompute.json()["deterministic"] == run.json()["deterministic"]
    finally:
        _teardown(engine)


def test_jwt_triggered_by_attribution_and_run_filters(tmp_path: Path) -> None:
    secret = "secret"
    client, engine = _setup_client(tmp_path, auth_mode="supabase_jwt", secret=secret)
    try:
        admin_token = jwt.encode({"sub": "admin-1", "role": "admin", "email": "admin@example.com"}, secret, algorithm="HS256")
        user_token = jwt.encode({"sub": "user-1", "role": "user", "email": "user@example.com"}, secret, algorithm="HS256")
        admin_headers = {"Authorization": f"Bearer {admin_token}"}
        user_headers = {"Authorization": f"Bearer {user_token}"}

        _upload_baseline(client, headers=admin_headers)
        team = client.post("/teams", json={"slug": "team-a", "name": "Team A"}, headers=admin_headers)
        team_id = team.json()["id"]

        initiative = client.post(
            "/initiatives",
            json={
                "name": "Initiative A",
                "owner_team_id": team_id,
                "initial_version": _version_payload("v1"),
            },
            headers=user_headers,
        )
        initiative_id = initiative.json()["id"]

        run = client.post(
            "/score/run",
            json={"initiative_id": initiative_id, "run_purpose": "review", "run_label": "weekly review"},
            headers=user_headers,
        )
        assert run.status_code == 200, run.text
        run_id = run.json()["run_id"]

        detail = client.get(f"/score/runs/{run_id}", headers=user_headers)
        assert detail.status_code == 200
        assert detail.json()["triggered_by_user_id"] == "user-1"
        assert detail.json()["triggered_by_email"] == "user@example.com"
        assert detail.json()["triggered_by_role"] == "user"

        filtered = client.get(
            "/score/runs",
            params={"owner_team_id": team_id, "triggered_by_user_id": "user-1", "run_purpose": "review"},
            headers=user_headers,
        )
        assert filtered.status_code == 200
        assert len(filtered.json()["items"]) >= 1
    finally:
        _teardown(engine)


def test_compare_versions_with_stored_outputs(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        _upload_baseline(client)
        team_id = client.post("/teams", json={"slug": "cmp", "name": "Compare"}).json()["id"]
        created = client.post(
            "/initiatives",
            json={
                "name": "Compare Initiative",
                "owner_team_id": team_id,
                "initial_version": _version_payload("v1"),
            },
        )
        initiative_id = created.json()["id"]
        version_1 = created.json()["latest_version_summary"]["id"]
        version_2 = client.post(f"/initiatives/{initiative_id}/versions", json=_version_payload("v2")).json()["id"]

        run_v1 = client.post("/score/run", json={"initiative_version_id": version_1, "run_purpose": "baseline"})
        run_v2 = client.post("/score/run", json={"initiative_version_id": version_2, "run_purpose": "refresh"})
        assert run_v1.status_code == 200, run_v1.text
        assert run_v2.status_code == 200, run_v2.text

        compare = client.get(f"/initiatives/{initiative_id}/compare", params={"version_a": version_1, "version_b": version_2})
        assert compare.status_code == 200, compare.text
        body = compare.json()
        assert body["outputs_available"] is True
        assert "incremental_margin" in body["outputs_delta"]
    finally:
        _teardown(engine)


def test_backward_compatible_ad_hoc_score_run(tmp_path: Path) -> None:
    client, engine = _setup_client(tmp_path)
    try:
        _upload_baseline(client)
        run = client.post(
            "/score/run",
            json={
                "initiative_name": "Old style request",
                "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.1}}],
                "screens": ["home"],
                "metric_targets": [],
                "p_success": 0.7,
                "confidence": 0.8,
                "effort_cost": 1000,
                "strategic_weight": 1.0,
                "learning_value": 1.0,
                "horizon_weeks": 26,
                "monte_carlo": {"enabled": True, "n": 1000, "seed": 123},
                "input_versions": {"baseline_metrics": "v1"},
            },
        )
        assert run.status_code == 200, run.text
        detail = client.get(f"/score/runs/{run.json()['run_id']}")
        assert detail.status_code == 200
        assert detail.json()["initiative_name"] == "Old style request"
    finally:
        _teardown(engine)
