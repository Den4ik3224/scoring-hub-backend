import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.schemas.score import ScoreRunCreateV11, ScoreRunRequest
from app.core.errors import ValidationError
from app.core.settings import Settings, get_settings
from app.db.base import Base
from app.db.session import get_session
from app.main import app
from app.services.scoring_engine import run_scoring

from .helpers_monthly import monthly_baseline_table, monthly_funnel_table, resolved_inputs_stub


def _resolved_inputs(*, baseline=None, funnel=None, cannibalization=None):
    return resolved_inputs_stub(
        baseline=baseline or monthly_baseline_table(),
        funnel=funnel or monthly_funnel_table(),
        cannibalization=cannibalization,
    )


def _payload(
    uplifts: dict[str, float],
    *,
    p_success: float = 0.8,
    confidence: float = 0.8,
    penetration: float = 0.5,
) -> ScoreRunRequest:
    return ScoreRunRequest(
        initiative_name="hardening",
        segments=[{"id": "s1", "penetration": penetration, "screen_penetration": {"home": 1.0}, "uplifts": uplifts}],
        screens=["home"],
        metric_targets=[],
        p_success=p_success,
        confidence=confidence,
        effort_cost=1000,
        strategic_weight=1.0,
        learning_value=1.0,
        horizon_weeks=1,
        monte_carlo={"enabled": False, "n": 1000, "seed": 1},
    )


def _oracle(
    *,
    mau: float = 1000.0,
    penetration: float = 0.5,
    conversion: float = 0.1,
    frequency_monthly: float = 2.0,
    aoq: float = 2.0,
    aiv: float = 100.0,
    fm_pct: float = 0.3,
    uplifts: dict[str, float] | None = None,
    p_success: float = 0.8,
    confidence: float = 0.8,
) -> dict[str, float]:
    uplifts = uplifts or {}
    weekly_factor = 12.0 / 52.0
    pen_base = penetration
    pen_new = min(1.0, max(0.0, penetration * (1.0 + uplifts.get("penetration", 0.0))))
    conv_base = min(1.0, max(0.0, conversion))
    conv_new = min(1.0, max(0.0, conversion * (1.0 + uplifts.get("conversion", 0.0))))
    mau_base = mau
    mau_new = max(0.0, mau * (1.0 + uplifts.get("mau", 0.0)))
    freq_base = frequency_monthly * weekly_factor
    freq_new = max(
        0.0,
        frequency_monthly * (1.0 + uplifts.get("frequency", uplifts.get("frequency_monthly", 0.0))) * weekly_factor,
    )
    aoq_base = aoq
    aoq_new = max(0.0, aoq * (1.0 + uplifts.get("aoq", 0.0)))
    aiv_base = aiv
    aiv_new = max(0.0, aiv * (1.0 + uplifts.get("aiv", 0.0)))
    fm_base = fm_pct
    fm_new = min(1.0, max(0.0, fm_pct * (1.0 + uplifts.get("fm_pct", 0.0))))

    orders_base = mau_base * pen_base * conv_base * freq_base
    items_base = orders_base * aoq_base
    aov_base = aoq_base * aiv_base
    rto_base = orders_base * aov_base
    fm_base_value = rto_base * fm_base

    orders_new = mau_new * pen_new * conv_new * freq_new
    items_new = orders_new * aoq_new
    aov_new = aoq_new * aiv_new
    rto_new = orders_new * aov_new
    fm_new_value = rto_new * fm_new

    incremental_orders = orders_new - orders_base
    incremental_items = items_new - items_base
    incremental_rto = rto_new - rto_base
    incremental_fm = fm_new_value - fm_base_value
    incremental_aoq = (items_new / orders_new if orders_new else 0.0) - (items_base / orders_base if orders_base else 0.0)
    incremental_aov = (rto_new / orders_new if orders_new else 0.0) - (rto_base / orders_base if orders_base else 0.0)
    expected_fm = incremental_fm * p_success * confidence

    return {
        "incremental_orders": incremental_orders,
        "incremental_items": incremental_items,
        "incremental_rto": incremental_rto,
        "incremental_fm": incremental_fm,
        "incremental_aoq": incremental_aoq,
        "incremental_aov": incremental_aov,
        "expected_fm": expected_fm,
    }


@pytest.mark.parametrize(
    ("uplifts", "label"),
    [
        ({"conversion": 0.1}, "conversion only"),
        ({"frequency_monthly": 0.1}, "frequency only"),
        ({"aoq": 0.1}, "aoq only"),
        ({"aiv": 0.1}, "aiv only"),
        ({"fm_pct": 0.1}, "fm only"),
        ({"conversion": 0.1, "frequency_monthly": 0.1}, "conversion + frequency"),
        ({"aoq": 0.1, "aiv": 0.1}, "aoq + aiv"),
        ({"conversion": 0.1, "aoq": 0.1}, "conversion + aoq"),
        ({"conversion": 0.1, "frequency_monthly": 0.1, "aoq": 0.1, "aiv": 0.1, "fm_pct": 0.1}, "all drivers"),
    ],
)
def test_formula_truth_table_matches_oracle(uplifts: dict[str, float], label: str) -> None:
    payload = _payload(uplifts, p_success=0.7, confidence=0.6)
    result = run_scoring(payload, _resolved_inputs(), mc_max_n=50_000)
    expected = _oracle(uplifts=uplifts, p_success=0.7, confidence=0.6)

    assert result.deterministic["incremental_gmv"] == pytest.approx(result.deterministic["incremental_rto"]), label
    assert result.deterministic["incremental_margin"] == pytest.approx(result.deterministic["incremental_fm"]), label
    assert result.deterministic["incremental_orders"] == pytest.approx(expected["incremental_orders"]), label
    assert result.deterministic["incremental_items"] == pytest.approx(expected["incremental_items"]), label
    assert result.deterministic["incremental_rto"] == pytest.approx(expected["incremental_rto"]), label
    assert result.deterministic["incremental_fm"] == pytest.approx(expected["incremental_fm"]), label
    assert result.deterministic["incremental_aoq"] == pytest.approx(expected["incremental_aoq"]), label
    assert result.deterministic["incremental_aov"] == pytest.approx(expected["incremental_aov"]), label
    assert result.deterministic["expected_fm"] == pytest.approx(expected["expected_fm"]), label


@pytest.mark.parametrize("p_success", [0.0, 0.2, 0.5, 0.8, 1.0])
@pytest.mark.parametrize("confidence", [0.0, 0.3, 0.6, 0.9, 1.0])
def test_probability_matrix_scales_expected_layer_only(p_success: float, confidence: float) -> None:
    payload = _payload({"conversion": 0.1}, p_success=p_success, confidence=confidence)
    result = run_scoring(payload, _resolved_inputs(), mc_max_n=50_000)
    physical_fm = result.deterministic["incremental_fm"]
    physical_rto = result.deterministic["incremental_rto"]

    control = run_scoring(_payload({"conversion": 0.1}, p_success=1.0, confidence=1.0), _resolved_inputs(), mc_max_n=50_000)
    assert physical_fm == pytest.approx(control.deterministic["incremental_fm"])
    assert physical_rto == pytest.approx(control.deterministic["incremental_rto"])
    assert result.deterministic["expected_fm"] == pytest.approx(physical_fm * p_success * confidence)
    assert result.deterministic["expected_margin"] == pytest.approx(physical_fm * p_success * confidence)
    if p_success == 0.0 or confidence == 0.0:
        assert result.deterministic["expected_fm"] == pytest.approx(0.0)


def test_ad_hoc_create_maps_baseline_window_and_dates() -> None:
    create = ScoreRunCreateV11.model_validate(
        {
            "initiative_name": "window-mapping",
            "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.1}}],
            "screens": ["home"],
            "metric_targets": [],
            "p_success": 0.7,
            "confidence": 0.8,
            "effort_cost": 1000,
            "strategic_weight": 1.0,
            "learning_value": 1.0,
            "baseline_window": "month",
            "baseline_date_start": "2025-02-01",
            "baseline_date_end": "2025-02-28",
            "horizon_weeks": 13,
        }
    )

    request = create.to_score_run_request()
    assert request.baseline_window == "month"
    assert str(request.baseline_date_start) == "2025-02-01"
    assert str(request.baseline_date_end) == "2025-02-28"


def test_aoq_uplift_propagates_to_rto_and_fm_regression() -> None:
    payload = _payload({"aoq": 0.12}, p_success=0.74, confidence=0.76)
    result = run_scoring(payload, _resolved_inputs(), mc_max_n=50_000)

    assert result.deterministic["incremental_orders"] == pytest.approx(0.0)
    assert result.deterministic["incremental_items"] > 0
    assert result.deterministic["incremental_aov"] > 0
    assert result.deterministic["incremental_rto"] > 0
    assert result.deterministic["incremental_fm"] > 0


def test_matrix_cannibalization_mc_mean_matches_deterministic_expectation() -> None:
    cannibalization = pa.table(
        {
            "from_screen": ["home"],
            "to_screen": ["search"],
            "segment_id": ["s1"],
            "cannibalization_rate": [0.25],
        }
    )
    payload = ScoreRunRequest.model_validate(
        {
            "initiative_name": "matrix-mc-parity",
            "segments": [{"id": "s1", "penetration": 0.5, "screen_penetration": {"home": 1.0, "search": 1.0}, "uplifts": {"conversion": 0.15}}],
            "screens": ["home", "search"],
            "metric_targets": [],
            "p_success": 0.8,
            "confidence": 0.7,
            "effort_cost": 1000,
            "strategic_weight": 1.0,
            "learning_value": 1.0,
            "horizon_weeks": 1,
            "cannibalization": {"mode": "matrix", "conservative_shrink": 0.0},
            "monte_carlo": {"enabled": True, "n": 20000, "seed": 123},
        }
    )
    result = run_scoring(payload, _resolved_inputs(cannibalization=cannibalization), mc_max_n=50_000)

    deterministic_margin = result.deterministic["incremental_fm"]
    mc_mean = result.probabilistic["mean"]
    assert mc_mean == pytest.approx(deterministic_margin * payload.p_success, rel=0.05)


def test_monte_carlo_same_seed_is_reproducible_and_different_seed_varies() -> None:
    mc_payload = ScoreRunRequest.model_validate(
        {
            "initiative_name": "mc-seed",
            "segments": [
                {
                    "id": "s1",
                    "penetration": 0.5,
                    "screen_penetration": {"home": 1.0},
                    "uplifts": {
                        "conversion": {"type": "normal", "mean": 0.1, "sd": 0.03},
                        "aoq": {"type": "triangular", "low": 0.02, "mode": 0.06, "high": 0.12},
                    },
                }
            ],
            "screens": ["home"],
            "metric_targets": [],
            "p_success": 0.8,
            "confidence": 0.7,
            "effort_cost": 1000,
            "strategic_weight": 1.0,
            "learning_value": 1.0,
            "horizon_weeks": 13,
            "monte_carlo": {"enabled": True, "n": 8000, "seed": 123},
        }
    )

    first = run_scoring(mc_payload, _resolved_inputs(), mc_max_n=50_000)
    second = run_scoring(mc_payload, _resolved_inputs(), mc_max_n=50_000)
    different_seed = run_scoring(
        mc_payload.model_copy(update={"monte_carlo": mc_payload.monte_carlo.model_copy(update={"seed": 124})}),
        _resolved_inputs(),
        mc_max_n=50_000,
    )

    assert first.probabilistic == second.probabilistic
    assert different_seed.probabilistic["mean"] != first.probabilistic["mean"]
    assert different_seed.probabilistic["median"] != first.probabilistic["median"]
    assert different_seed.probabilistic["p5"] < different_seed.probabilistic["median"] < different_seed.probabilistic["p95"]


def test_monte_carlo_p_success_gates_physical_distribution_and_confidence_does_not() -> None:
    base_payload = ScoreRunRequest.model_validate(
        {
            "initiative_name": "mc-p-success",
            "segments": [
                {
                    "id": "s1",
                    "penetration": 0.5,
                    "screen_penetration": {"home": 1.0},
                    "uplifts": {"conversion": {"type": "normal", "mean": 0.12, "sd": 0.04}},
                }
            ],
            "screens": ["home"],
            "metric_targets": [],
            "p_success": 1.0,
            "confidence": 0.9,
            "effort_cost": 1000,
            "strategic_weight": 1.0,
            "learning_value": 1.0,
            "horizon_weeks": 13,
            "monte_carlo": {"enabled": True, "n": 12000, "seed": 321},
        }
    )

    full_success = run_scoring(base_payload, _resolved_inputs(), mc_max_n=50_000)
    partial_success = run_scoring(base_payload.model_copy(update={"p_success": 0.2}), _resolved_inputs(), mc_max_n=50_000)
    low_confidence = run_scoring(base_payload.model_copy(update={"confidence": 0.3}), _resolved_inputs(), mc_max_n=50_000)

    assert full_success.probabilistic["stddev"] > 0
    assert partial_success.probabilistic["stddev"] > 0
    assert partial_success.probabilistic["mean"] < full_success.probabilistic["mean"]
    assert partial_success.probabilistic["median"] <= full_success.probabilistic["median"]
    assert partial_success.probabilistic["p5"] <= 1e-9
    assert low_confidence.probabilistic == full_success.probabilistic
    assert low_confidence.deterministic["expected_fm"] == pytest.approx(
        low_confidence.deterministic["incremental_fm"] * 1.0 * 0.3
    )


def test_segment_uplifts_feed_explainability_and_sensitivity() -> None:
    payload = _payload({"conversion": 0.2, "aoq": 0.1}, p_success=0.8, confidence=0.7)
    payload = ScoreRunRequest.model_validate(
        {
            **payload.model_dump(mode="json"),
            "sensitivity": {"enabled": True, "epsilon": 0.1, "top_n": 5, "target_metric": "net_margin"},
        }
    )
    result = run_scoring(payload, _resolved_inputs(), mc_max_n=50_000)

    top_node_names = [item["node"] for item in result.explainability["top_nodes"]]
    assert any(name.startswith("segment_uplift:s1:conversion") for name in top_node_names)
    assert any(name.startswith("segment_uplift:s1:aoq") for name in top_node_names)

    sensitive_inputs = [item["input"] for item in result.sensitivity["top_sensitive_inputs"]]
    assert any(name == "segment_uplift:s1:conversion" for name in sensitive_inputs)
    assert any(name == "segment_uplift:s1:aoq" for name in sensitive_inputs)
    assert result.explainability["primary_driver"] in {
        "conversion-driven",
        "frequency-driven",
        "basket-driven",
        "margin-driven",
    }


def test_segment_level_aov_conflict_is_rejected() -> None:
    payload = _payload({"aov": 0.1, "aoq": 0.1})
    with pytest.raises(ValidationError, match="simultaneous AOV"):
        run_scoring(payload, _resolved_inputs(), mc_max_n=50_000)


@settings(max_examples=50)
@given(
    mau=st.floats(min_value=100.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
    penetration=st.floats(min_value=0.05, max_value=1.0, allow_nan=False, allow_infinity=False),
    conversion=st.floats(min_value=0.001, max_value=0.9, allow_nan=False, allow_infinity=False),
    frequency_monthly=st.floats(min_value=0.05, max_value=3.0, allow_nan=False, allow_infinity=False),
    aoq=st.floats(min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False),
    aiv=st.floats(min_value=1.0, max_value=2_000.0, allow_nan=False, allow_infinity=False),
    fm_pct=st.floats(min_value=0.01, max_value=0.9, allow_nan=False, allow_infinity=False),
)
def test_property_invariants_hold_for_random_positive_inputs(
    mau: float,
    penetration: float,
    conversion: float,
    frequency_monthly: float,
    aoq: float,
    aiv: float,
    fm_pct: float,
) -> None:
    baseline = monthly_baseline_table(
        active_users=mau,
        ordering_users=mau * conversion,
        orders=mau * conversion * frequency_monthly,
        items=mau * conversion * frequency_monthly * aoq,
        rto=mau * conversion * frequency_monthly * aoq * aiv,
        fm=mau * conversion * frequency_monthly * aoq * aiv * fm_pct,
    )
    result = run_scoring(
        _payload({"conversion": 0.05}, p_success=0.7, confidence=0.8, penetration=penetration),
        _resolved_inputs(baseline=baseline),
        mc_max_n=50_000,
    )
    base = _oracle(
        mau=mau,
        penetration=penetration,
        conversion=conversion,
        frequency_monthly=frequency_monthly,
        aoq=aoq,
        aiv=aiv,
        fm_pct=fm_pct,
        uplifts={"conversion": 0.05},
        p_success=0.7,
        confidence=0.8,
    )
    assert result.deterministic["incremental_rto"] == pytest.approx(base["incremental_rto"])
    assert result.deterministic["incremental_fm"] == pytest.approx(base["incremental_fm"])
    assert result.deterministic["incremental_margin"] == pytest.approx(result.deterministic["incremental_fm"])


@settings(max_examples=50)
@given(
    aoq=st.floats(min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False),
    aiv=st.floats(min_value=1.0, max_value=2_000.0, allow_nan=False, allow_infinity=False),
    uplift=st.floats(min_value=0.01, max_value=0.5, allow_nan=False, allow_infinity=False),
)
def test_property_aoq_positive_delta_implies_positive_rto_delta(aoq: float, aiv: float, uplift: float) -> None:
    baseline = monthly_baseline_table(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=200.0 * aoq,
        rto=200.0 * aoq * aiv,
        fm=200.0 * aoq * aiv * 0.3,
    )
    result = run_scoring(_payload({"aoq": uplift}, p_success=0.9, confidence=0.9), _resolved_inputs(baseline=baseline), mc_max_n=50_000)
    assert result.deterministic["incremental_orders"] == pytest.approx(0.0)
    assert result.deterministic["incremental_rto"] > 0


def test_initiative_runtime_overrides_are_merged_and_version_scope_is_first_class(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    async def _setup() -> async_sessionmaker[AsyncSession]:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    session_maker = asyncio.run(_setup())

    async def _session_override() -> AsyncGenerator[AsyncSession, None]:
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_settings] = lambda: Settings(database_url="sqlite+aiosqlite://", data_dir=tmp_path, max_upload_mb=10)
    client = TestClient(app)
    try:
        team = client.post("/teams", json={"slug": "core", "name": "Core"})
        assert team.status_code == 200

        baseline_csv = (
            "segment_id,date_start,date_end,active_users,ordering_users,orders,items,rto,fm\n"
            "s1,2025-01-01,2025-01-31,1000,100,200,400,40000,12000\n"
            "s1,2025-02-01,2025-02-28,1000,100,200,400,40000,12000\n"
            "s1,2025-03-01,2025-03-31,1000,100,200,400,40000,12000\n"
        )
        upload = client.post(
            "/datasets/upload",
            params={"dataset_name": "baseline_main", "format": "csv", "schema_type": "baseline_metrics", "dataset_version": "v1"},
            files={"file": ("baseline.csv", baseline_csv, "text/csv")},
        )
        assert upload.status_code == 200, upload.text

        create = client.post(
            "/initiatives",
            json={
                "name": "Checkout",
                "owner_team_id": team.json()["id"],
                "initial_version": {
                    "title_override": "v1",
                    "data_scope": "x5_retail_test_v2",
                    "baseline_window": "quarter",
                    "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.1}}],
                    "screens": ["home"],
                    "metric_targets": [],
                    "p_success": 0.4,
                    "confidence": 0.5,
                    "effort_cost": 1000,
                    "strategic_weight": 1.0,
                    "learning_value": 1.0,
                    "horizon_weeks": 26,
                    "monte_carlo": {"enabled": False, "n": 1000, "seed": 7},
                },
            },
        )
        assert create.status_code == 200, create.text
        initiative_id = create.json()["id"]
        version_id = create.json()["latest_version_summary"]["id"]

        detail = client.get(f"/initiatives/{initiative_id}/versions/{version_id}")
        assert detail.status_code == 200
        assert detail.json()["data_scope"] == "x5_retail_test_v2"
        assert detail.json()["baseline_window"] == "quarter"

        run = client.post(
            "/score/run",
            json={
                "initiative_id": initiative_id,
                "data_scope": "prod",
                "learning": {"mode": "off"},
                "scenarios": {"upside": {"p_success": 0.9}},
            },
        )
        assert run.status_code == 200, run.text
        run_detail = client.get(f"/score/runs/{run.json()['run_id']}")
        assert run_detail.status_code == 200
        resolved = run_detail.json()["resolved_inputs"]
        assert resolved["data_scope"] == "prod"
        assert resolved["baseline_window"]["name"] == "quarter"
        assert run_detail.json()["request_payload"]["learning"]["mode"] == "off"
        assert "upside" in run_detail.json()["request_payload"]["scenarios"]
    finally:
        app.dependency_overrides = {}
        asyncio.run(engine.dispose())
