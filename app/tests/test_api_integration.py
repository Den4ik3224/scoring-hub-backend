import json
from collections.abc import AsyncGenerator
from pathlib import Path

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi.testclient import TestClient
from jwt.algorithms import ECAlgorithm
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.core.settings import Settings, get_settings
from app.core.security import _reset_jwks_cache_for_tests
from app.db.base import Base
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


def _baseline_csv() -> str:
    return monthly_baseline_csv(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=200.0,
        rto=4000.0,
        fm=1200.0,
    )


def test_upload_preview_and_score_happy_path(tmp_path: Path) -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    import asyncio

    session_maker = asyncio.run(_setup())
    settings = Settings(database_url="sqlite+aiosqlite://", data_dir=None, max_upload_mb=10)

    client = _override_app(session_maker, settings)

    upload_resp = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
    )
    assert upload_resp.status_code == 200, upload_resp.text

    preview_resp = client.get("/datasets/baseline_main/v1/preview", params={"limit": 10})
    assert preview_resp.status_code == 200
    assert len(preview_resp.json()["rows"]) == 3

    score_resp = client.post(
        "/score/run",
        json={
            "initiative_name": "Test initiative",
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
    assert score_resp.status_code == 200, score_resp.text
    body = score_resp.json()
    assert body["run_id"]
    assert body["deterministic"]["incremental_margin"] > 0
    assert body["per_screen_breakdown"]

    detail_resp = client.get(f"/score/runs/{body['run_id']}")
    assert detail_resp.status_code == 200
    assert detail_resp.json()["run_status"] == "success"
    assert detail_resp.json()["per_screen_breakdown"]

    recompute_resp = client.post(f"/score/runs/{body['run_id']}/recompute")
    assert recompute_resp.status_code == 200, recompute_resp.text
    recompute_body = recompute_resp.json()
    assert recompute_body["run_id"] != body["run_id"]
    assert recompute_body["seed"] == body["seed"]
    assert recompute_body["deterministic"] == body["deterministic"]
    assert recompute_body["probabilistic"] == body["probabilistic"]

    app.dependency_overrides = {}
    asyncio.run(engine.dispose())


def test_auth_and_admin_enforcement(tmp_path: Path) -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    import asyncio

    session_maker = asyncio.run(_setup())
    settings = Settings(
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        max_upload_mb=10,
        auth_mode="supabase_jwt",
        supabase_jwt_secret="secret",
    )

    client = _override_app(session_maker, settings)

    no_auth = client.get("/datasets")
    assert no_auth.status_code == 401

    user_token = jwt.encode({"sub": "u1", "role": "user"}, "secret", algorithm="HS256")
    admin_token = jwt.encode({"sub": "a1", "role": "admin"}, "secret", algorithm="HS256")

    user_list = client.get("/datasets", headers={"Authorization": f"Bearer {user_token}"})
    assert user_list.status_code == 200

    user_upload = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert user_upload.status_code == 403

    admin_upload = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert admin_upload.status_code == 200

    app.dependency_overrides = {}
    asyncio.run(engine.dispose())


def test_auth_with_supabase_jwks_es256(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    import asyncio

    session_maker = asyncio.run(_setup())
    _reset_jwks_cache_for_tests()
    settings = Settings(
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        max_upload_mb=10,
        auth_mode="supabase_jwt",
        supabase_url="https://example.supabase.co",
        supabase_jwt_secret=None,
    )
    client = _override_app(session_maker, settings)

    admin_private = ec.generate_private_key(ec.SECP256R1())
    user_private = ec.generate_private_key(ec.SECP256R1())
    admin_jwk = json.loads(ECAlgorithm.to_jwk(admin_private.public_key()))
    admin_jwk.update({"kid": "admin-kid", "alg": "ES256", "use": "sig"})
    user_jwk = json.loads(ECAlgorithm.to_jwk(user_private.public_key()))
    user_jwk.update({"kid": "user-kid", "alg": "ES256", "use": "sig"})

    monkeypatch.setattr(
        "app.core.security._fetch_jwks",
        lambda _: {"admin-kid": admin_jwk, "user-kid": user_jwk},
    )

    user_token = jwt.encode(
        {"sub": "u1", "role": "user", "iss": settings.expected_jwt_issuer},
        user_private,
        algorithm="ES256",
        headers={"kid": "user-kid"},
    )
    admin_token = jwt.encode(
        {"sub": "a1", "role": "admin", "iss": settings.expected_jwt_issuer},
        admin_private,
        algorithm="ES256",
        headers={"kid": "admin-kid"},
    )

    user_list = client.get("/datasets", headers={"Authorization": f"Bearer {user_token}"})
    assert user_list.status_code == 200

    user_upload = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert user_upload.status_code == 403

    admin_upload = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert admin_upload.status_code == 200

    app.dependency_overrides = {}
    asyncio.run(engine.dispose())


def test_learning_api_auth_and_create_flow(tmp_path: Path) -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    import asyncio

    session_maker = asyncio.run(_setup())
    settings = Settings(
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        auth_mode="supabase_jwt",
        supabase_jwt_secret="secret",
    )
    client = _override_app(session_maker, settings)

    user_token = jwt.encode({"sub": "u1", "role": "user"}, "secret", algorithm="HS256")
    admin_token = jwt.encode({"sub": "a1", "role": "admin"}, "secret", algorithm="HS256")

    unauth_list = client.get("/learning/ab-results")
    assert unauth_list.status_code == 401

    user_list = client.get("/learning/ab-results", headers={"Authorization": f"Bearer {user_token}"})
    assert user_list.status_code == 200

    payload = {
        "experiment_id": "exp-001",
        "initiative_id": None,
        "screen": "home",
        "segment_id": "s1",
        "metric_driver": "conversion",
        "observed_uplift": 0.08,
        "ci_low": 0.04,
        "ci_high": 0.11,
        "sample_size": 2500,
        "significance_flag": True,
        "quality_score": 0.9,
        "source": "ab_platform",
        "start_at": "2025-01-01T00:00:00Z",
        "end_at": "2025-01-31T00:00:00Z",
    }

    user_create = client.post("/learning/ab-results", json=payload, headers={"Authorization": f"Bearer {user_token}"})
    assert user_create.status_code == 403

    admin_create = client.post("/learning/ab-results", json=payload, headers={"Authorization": f"Bearer {admin_token}"})
    assert admin_create.status_code == 200, admin_create.text
    result_id = admin_create.json()["id"]

    detail = client.get(f"/learning/ab-results/{result_id}", headers={"Authorization": f"Bearer {user_token}"})
    assert detail.status_code == 200
    assert detail.json()["experiment_id"] == "exp-001"

    app.dependency_overrides = {}
    asyncio.run(engine.dispose())


def test_learning_scope_filters_and_score_scope_default_prod(tmp_path: Path) -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    import asyncio

    session_maker = asyncio.run(_setup())
    settings = Settings(database_url="sqlite+aiosqlite://", data_dir=None, max_upload_mb=10)
    client = _override_app(session_maker, settings)

    upload_resp = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
            "scope": "prod",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
    )
    assert upload_resp.status_code == 200, upload_resp.text

    ab_prod = client.post(
        "/learning/ab-results",
        json={
            "experiment_id": "exp-prod",
            "scope": "prod",
            "screen": "home",
            "segment_id": "s1",
            "metric_driver": "conversion",
            "observed_uplift": 0.05,
            "ci_low": 0.01,
            "ci_high": 0.09,
            "sample_size": 1000,
            "significance_flag": True,
            "quality_score": 0.8,
            "source": "seed:prod",
            "start_at": "2025-01-01T00:00:00Z",
            "end_at": "2025-02-01T00:00:00Z",
        },
    )
    assert ab_prod.status_code == 200, ab_prod.text
    ab_test = client.post(
        "/learning/ab-results",
        json={
            "experiment_id": "exp-test",
            "scope": "x5_retail_test_v1",
            "screen": "home",
            "segment_id": "s1",
            "metric_driver": "conversion",
            "observed_uplift": 0.15,
            "ci_low": 0.1,
            "ci_high": 0.2,
            "sample_size": 1000,
            "significance_flag": True,
            "quality_score": 0.9,
            "source": "seed:test",
            "start_at": "2025-01-01T00:00:00Z",
            "end_at": "2025-02-01T00:00:00Z",
        },
    )
    assert ab_test.status_code == 200, ab_test.text

    filtered = client.get("/learning/ab-results", params={"scope": "x5_retail_test_v1"})
    assert filtered.status_code == 200
    assert len(filtered.json()["items"]) == 1
    assert filtered.json()["items"][0]["scope"] == "x5_retail_test_v1"

    score_resp = client.post(
        "/score/run",
        json={
            "initiative_name": "Prod scope default",
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
    assert score_resp.status_code == 200, score_resp.text
    detail = client.get(f"/score/runs/{score_resp.json()['run_id']}")
    assert detail.status_code == 200
    assert detail.json()["request_payload"]["data_scope"] == "prod"

    app.dependency_overrides = {}
    asyncio.run(engine.dispose())


def test_dashboard_summary_and_initiatives_latest_run_metrics(tmp_path: Path) -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    import asyncio

    session_maker = asyncio.run(_setup())
    settings = Settings(database_url="sqlite+aiosqlite://", data_dir=None, max_upload_mb=10)
    client = _override_app(session_maker, settings)

    upload_resp = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
    )
    assert upload_resp.status_code == 200, upload_resp.text

    team = client.post("/teams", json={"slug": "growth", "name": "Growth"})
    assert team.status_code == 200
    initiative = client.post(
        "/initiatives",
        json={
            "name": "Dash Initiative",
            "owner_team_id": team.json()["id"],
            "initial_version": {
                "screens": ["home"],
                "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.1}}],
                "metric_targets": [],
                "p_success": 0.7,
                "confidence": 0.8,
                "effort_cost": 1000,
                "strategic_weight": 1.0,
                "learning_value": 1.0,
                "horizon_weeks": 26,
            },
        },
    )
    assert initiative.status_code == 200, initiative.text
    initiative_id = initiative.json()["id"]

    run_resp = client.post(
        "/score/run",
        json={
            "initiative_id": initiative_id,
            "run_purpose": "baseline",
            "input_versions": {"baseline_metrics": "v1"},
        },
    )
    assert run_resp.status_code == 200, run_resp.text

    list_resp = client.get("/initiatives")
    assert list_resp.status_code == 200
    latest_metrics = list_resp.json()["items"][0]["latest_run_metrics"]
    assert latest_metrics is not None
    assert latest_metrics["run_id"]
    assert latest_metrics["expected_margin"] is not None

    dashboard = client.get("/dashboard/summary")
    assert dashboard.status_code == 200, dashboard.text
    body = dashboard.json()
    assert body["kpi_cards"]["initiatives_total"] >= 1
    assert body["kpi_cards"]["initiatives_with_runs"] >= 1
    assert isinstance(body["top_initiatives"], list)
    assert body["review_queue_counts"]["available"] is False

    app.dependency_overrides = {}
    asyncio.run(engine.dispose())


def test_failed_run_status_persistence_and_filtering(tmp_path: Path) -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    import asyncio

    session_maker = asyncio.run(_setup())
    settings = Settings(database_url="sqlite+aiosqlite://", data_dir=None, max_upload_mb=10)
    client = _override_app(session_maker, settings)

    upload_resp = client.post(
        "/datasets/upload",
        params={
            "dataset_name": "baseline_main",
            "format": "csv",
            "schema_type": "baseline_metrics",
            "dataset_version": "v1",
        },
        files={"file": ("baseline.csv", _baseline_csv(), "text/csv")},
    )
    assert upload_resp.status_code == 200, upload_resp.text

    failed = client.post(
        "/score/run",
        json={
            "initiative_name": "Invalid step target",
            "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {}}],
            "screens": ["home"],
            "metric_targets": [{"node": "checkout_to_payment", "node_type": "funnel_step", "target_id": "checkout_to_payment", "uplift_dist": 0.1}],
            "p_success": 0.7,
            "confidence": 0.8,
            "effort_cost": 1000,
            "strategic_weight": 1.0,
            "learning_value": 1.0,
            "horizon_weeks": 26,
            "input_versions": {"baseline_metrics": "v1"},
        },
    )
    assert failed.status_code == 422

    failed_runs = client.get("/score/runs", params={"run_status": "failed"})
    assert failed_runs.status_code == 200
    assert len(failed_runs.json()["items"]) >= 1
    row = failed_runs.json()["items"][0]
    assert row["run_status"] == "failed"
    assert row["error_message"]

    app.dependency_overrides = {}
    asyncio.run(engine.dispose())
