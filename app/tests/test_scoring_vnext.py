import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path

from pyarrow import table
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.schemas.score import ScoreRunRequest
from app.core.errors import ValidationError
from app.core.settings import Settings, get_settings
from app.db.base import Base
from app.db.session import get_session
from app.main import app
from app.services.scoring_engine import run_scoring

from .helpers_monthly import (
    monthly_baseline_csv,
    monthly_baseline_table,
    monthly_funnel_table,
    resolved_inputs_stub,
)


def _base_payload(**overrides) -> ScoreRunRequest:
    data = {
        "initiative_name": "vnext",
        "segments": [{"id": "s1", "penetration": 0.5, "screen_penetration": {"home": 0.8}, "uplifts": {}}],
        "screens": ["home"],
        "metric_targets": [],
        "p_success": 0.8,
        "confidence": 0.8,
        "effort_cost": 1000,
        "strategic_weight": 1.0,
        "learning_value": 1.0,
        "horizon_weeks": 26,
        "monte_carlo": {"enabled": True, "n": 2000, "seed": 42},
    }
    data.update(overrides)
    return ScoreRunRequest.model_validate(data)


def test_funnel_step_scoring_and_aggregate_fallback() -> None:
    baseline = monthly_baseline_table(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=200.0,
        rto=4000.0,
        fm=1200.0,
    )
    funnel = monthly_funnel_table(
        steps=(
            ("home_to_catalog", "Home to catalog", 1, 600.0, 300.0),
            ("catalog_to_cart", "Catalog to cart", 2, 300.0, 120.0),
        )
    )
    payload = _base_payload(
        metric_targets=[
            {
                "node": "catalog_to_cart",
                "node_type": "funnel_step",
                "target_id": "catalog_to_cart",
                "uplift_dist": 0.25,
            }
        ]
    )
    with_funnel = run_scoring(payload, resolved_inputs_stub(baseline=baseline, funnel=funnel), mc_max_n=50_000)

    fallback_payload = _base_payload(
        segments=[{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.25}}],
        metric_targets=[],
    )
    without_funnel = run_scoring(fallback_payload, resolved_inputs_stub(baseline=baseline, funnel=None), mc_max_n=50_000)

    assert with_funnel.deterministic["incremental_fm"] > 0
    assert without_funnel.deterministic["incremental_fm"] > 0


def test_derived_conflict_policy_translation_and_rejection() -> None:
    baseline = monthly_baseline_table()
    payload = _base_payload(
        metric_targets=[
            {"node": "aov", "node_type": "metric", "uplift_dist": 0.1},
            {"node": "aoq", "node_type": "metric", "uplift_dist": 0.1},
        ]
    )

    with_translation = run_scoring(payload, resolved_inputs_stub(baseline=baseline), mc_max_n=50_000)
    assert any("AOV" in warning for warning in with_translation.validation_warnings)

    reject_policy = {
        "primitive_metrics": ["mau", "penetration", "conversion", "frequency", "frequency_monthly", "aoq", "aiv", "fm_pct"],
        "derived_metrics": ["orders", "items", "aov", "rto", "fm"],
        "translator_enabled": False,
    }
    with pytest.raises(ValidationError):
        run_scoring(payload, resolved_inputs_stub(baseline=baseline, scoring_policy_snapshot=reject_policy), mc_max_n=50_000)


def test_gross_vs_net_multihorizon_scenarios_sensitivity_explainability() -> None:
    baseline = monthly_baseline_table()
    funnel = monthly_funnel_table(
        steps=(
            ("home_to_catalog", "Home to catalog", 1, 600.0, 300.0),
            ("catalog_to_cart", "Catalog to cart", 2, 300.0, 120.0),
        )
    )
    cannibalization = table(
        {
            "from_screen": ["home"],
            "to_screen": ["catalog"],
            "segment_id": ["s1"],
            "cannibalization_rate": [0.4],
        }
    )
    payload = _base_payload(
        segments=[{"id": "s1", "penetration": 0.5, "screen_penetration": {"home": 0.8}, "uplifts": {"conversion": 0.2}}],
        horizons_weeks=[4, 13, 26, 52],
        cannibalization={"mode": "matrix", "matrix_id": "v1", "conservative_shrink": 0.2},
        scenarios={
            "conservative": {"p_success": 0.5, "confidence": 0.6},
            "upside": {"p_success": 0.95, "confidence": 0.9},
        },
        sensitivity={"enabled": True, "epsilon": 0.1, "top_n": 5, "target_metric": "net_margin"},
    )
    result = run_scoring(
        payload,
        resolved_inputs_stub(baseline=baseline, funnel=funnel, cannibalization=cannibalization),
        mc_max_n=50_000,
    )

    assert result.gross_impact["fm"] >= result.net_incremental_impact["fm"]
    assert set(result.horizon_results.keys()) == {"4", "13", "26", "52"}
    assert "conservative" in result.scenarios and "upside" in result.scenarios
    assert "base_vs_upside" in result.scenario_comparison
    assert len(result.sensitivity["top_sensitive_inputs"]) > 0
    assert result.explainability["summary_text"]
    assert "home" in result.per_screen_breakdown
    assert "net_delta_fm" in result.per_screen_breakdown["home"]
    assert result.explainability["primary_driver"] in {
        "conversion-driven",
        "frequency-driven",
        "basket-driven",
        "margin-driven",
    }


def test_scenarios_base_override_is_rejected() -> None:
    baseline = monthly_baseline_table()
    payload = _base_payload(
        scenarios={
            "base": {"p_success": 0.9},
            "upside": {"p_success": 0.95, "confidence": 0.9},
        }
    )

    with pytest.raises(ValidationError, match="Scenario `base` is reserved"):
        run_scoring(payload, resolved_inputs_stub(baseline=baseline), mc_max_n=50_000)


def test_aoq_component_targets_are_rejected_for_new_runs() -> None:
    baseline = monthly_baseline_table()
    payload = _base_payload(
        segments=[{"id": "s1", "penetration": 0.5, "screen_penetration": {"home": 0.8}, "uplifts": {"aoq_component:favorites": 0.2}}],
    )

    with pytest.raises(ValidationError, match="AOQ component targets are no longer supported"):
        run_scoring(payload, resolved_inputs_stub(baseline=baseline), mc_max_n=50_000)


def _override_app(session_maker: async_sessionmaker[AsyncSession], settings: Settings) -> TestClient:
    async def _session_override() -> AsyncGenerator[AsyncSession, None]:
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_settings] = lambda: settings
    return TestClient(app)


def test_scoring_policy_persistence_in_run_artifacts(tmp_path: Path) -> None:
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
    settings = Settings(database_url="sqlite+aiosqlite://", data_dir=None, max_upload_mb=10)
    client = _override_app(session_maker, settings)

    try:
        upload_resp = client.post(
            "/datasets/upload",
            params={
                "dataset_name": "baseline_main",
                "format": "csv",
                "schema_type": "baseline_metrics",
                "dataset_version": "v1",
            },
            files={"file": ("baseline.csv", monthly_baseline_csv(), "text/csv")},
        )
        assert upload_resp.status_code == 200, upload_resp.text

        funnel_upload = client.post(
            "/datasets/upload",
            params={
                "dataset_name": "funnel_main",
                "format": "csv",
                "schema_type": "baseline_funnel_steps",
                "dataset_version": "v1",
            },
            files={
                "file": (
                    "funnel.csv",
                    "segment_id,screen,step_id,step_name,step_order,date_start,date_end,entered_users,advanced_users\n"
                    "s1,home,home_to_catalog,Home to catalog,1,2025-01-01,2025-01-31,600,300\n"
                    "s1,home,catalog_to_cart,Catalog to cart,2,2025-01-01,2025-01-31,300,120\n"
                    "s1,home,home_to_catalog,Home to catalog,1,2025-02-01,2025-02-28,600,300\n"
                    "s1,home,catalog_to_cart,Catalog to cart,2,2025-02-01,2025-02-28,300,120\n"
                    "s1,home,home_to_catalog,Home to catalog,1,2025-03-01,2025-03-31,600,300\n"
                    "s1,home,catalog_to_cart,Catalog to cart,2,2025-03-01,2025-03-31,300,120\n",
                    "text/csv",
                )
            },
        )
        assert funnel_upload.status_code == 200, funnel_upload.text

        policy_create = client.post(
            "/config/scoring-policies",
            json={
                "name": "ev_policy_vnext",
                "version": "99",
                "is_default": True,
                "policy": {
                    "primitive_metrics": ["mau", "penetration", "conversion", "frequency", "frequency_monthly", "aoq", "aiv", "fm_pct"],
                    "derived_metrics": ["orders", "items", "aov", "rto", "fm"],
                    "translator_enabled": True,
                    "translations": {"aov": {"to": ["aoq", "aiv"], "weights": {"aoq": 0.5, "aiv": 0.5}}},
                },
            },
        )
        assert policy_create.status_code == 200, policy_create.text

        run = client.post(
            "/score/run",
            json={
                "initiative_name": "Policy run",
                "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.1}}],
                "screens": ["home"],
                "metric_targets": [],
                "p_success": 0.7,
                "confidence": 0.8,
                "effort_cost": 1000,
                "strategic_weight": 1.0,
                "learning_value": 1.0,
                "horizon_weeks": 26,
                "baseline_window": "quarter",
                "input_versions": {"baseline_metrics": "v1", "baseline_funnel_steps": "v1"},
                "scoring_policy": {"name": "ev_policy_vnext", "version": "99"},
            },
        )
        assert run.status_code == 200, run.text
        body = run.json()
        assert body["scoring_policy_version"] == "99"
        assert body["scoring_policy_source"].startswith("config:")

        detail = client.get(f"/score/runs/{body['run_id']}")
        assert detail.status_code == 200
        assert detail.json()["resolved_inputs"]["scoring_policy_source"].endswith(":99")

        recompute = client.post(f"/score/runs/{body['run_id']}/recompute")
        assert recompute.status_code == 200, recompute.text
        assert recompute.json()["deterministic"] == body["deterministic"]
    finally:
        app.dependency_overrides = {}
        asyncio.run(engine.dispose())
