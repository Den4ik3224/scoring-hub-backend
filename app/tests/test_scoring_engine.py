import pytest

from app.api.schemas.score import ScoreRunRequest
from app.services.scoring_engine import run_scoring

from .helpers_monthly import monthly_baseline_table, resolved_inputs_stub


def test_deterministic_scoring_correctness_and_monthly_normalization() -> None:
    baseline = monthly_baseline_table(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=200.0,
        rto=4000.0,
        fm=1200.0,
    )

    payload = ScoreRunRequest(
        initiative_name="Checkout uplift",
        segments=[{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.1}}],
        screens=["home"],
        metric_targets=[],
        p_success=0.5,
        confidence=0.8,
        effort_cost=1000,
        strategic_weight=1.0,
        learning_value=1.0,
        horizon_weeks=52,
        monte_carlo={"enabled": False, "n": 1000, "seed": 1},
    )

    result = run_scoring(payload, resolved_inputs_stub(baseline=baseline), mc_max_n=50_000)

    assert round(result.deterministic["incremental_orders"], 3) == 120.0
    assert round(result.deterministic["incremental_rto"], 2) == 2400.0
    assert round(result.deterministic["incremental_fm"], 2) == 720.0
    assert round(result.deterministic["incremental_gmv"], 2) == 2400.0
    assert round(result.deterministic["incremental_margin"], 2) == 720.0
    assert round(result.deterministic["expected_fm"], 2) == 288.0
    assert round(result.deterministic["expected_margin"], 2) == 288.0


def test_aov_precedence_with_aoq_aiv_fallback() -> None:
    baseline = monthly_baseline_table(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=400.0,
        rto=8000.0,
        fm=2400.0,
    )

    payload = ScoreRunRequest(
        initiative_name="Basket size uplift",
        segments=[{"id": "s1", "penetration": 0.5, "uplifts": {"aoq": 0.5}}],
        screens=["home"],
        metric_targets=[],
        p_success=1.0,
        confidence=1.0,
        effort_cost=1,
        strategic_weight=1.0,
        learning_value=1.0,
        horizon_weeks=1,
        monte_carlo={"enabled": False, "n": 1000, "seed": 1},
    )

    result = run_scoring(payload, resolved_inputs_stub(baseline=baseline), mc_max_n=50_000)

    assert result.deterministic["incremental_rto"] > 0
    assert result.deterministic["incremental_gmv"] > 0
    assert result.deterministic["incremental_aov"] > 0


def test_monte_carlo_reproducibility_with_seed() -> None:
    baseline = monthly_baseline_table(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=200.0,
        rto=4000.0,
        fm=1200.0,
    )

    payload = ScoreRunRequest(
        initiative_name="MC reproducibility",
        segments=[
            {
                "id": "s1",
                "penetration": 0.5,
                "uplifts": {"conversion": {"type": "normal", "mean": 0.1, "sd": 0.05}},
            }
        ],
        screens=["home"],
        metric_targets=[],
        p_success=1.0,
        confidence=1.0,
        effort_cost=1,
        strategic_weight=1.0,
        learning_value=1.0,
        horizon_weeks=12,
        monte_carlo={"enabled": True, "n": 5000, "seed": 42},
    )

    first = run_scoring(payload, resolved_inputs_stub(baseline=baseline), mc_max_n=50_000)
    second = run_scoring(payload, resolved_inputs_stub(baseline=baseline), mc_max_n=50_000)

    assert first.probabilistic == second.probabilistic

    payload2 = payload.model_copy(deep=True)
    assert payload2.monte_carlo is not None
    payload2.monte_carlo.seed = 43
    third = run_scoring(payload2, resolved_inputs_stub(baseline=baseline), mc_max_n=50_000)

    assert first.probabilistic["mean"] != third.probabilistic["mean"]


def test_cannibalization_conservative_shrink_reduces_impact() -> None:
    baseline = monthly_baseline_table(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=200.0,
        rto=4000.0,
        fm=1200.0,
    )
    cannibalization = monthly_baseline_table  # type: ignore[assignment]
    del cannibalization
    from pyarrow import table

    cannibalization_table = table(
        {
            "from_screen": ["home"],
            "to_screen": ["catalog"],
            "segment_id": ["s1"],
            "cannibalization_rate": [0.4],
        }
    )

    base_payload = {
        "initiative_name": "Cannibalization test",
        "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.2}}],
        "screens": ["home"],
        "metric_targets": [],
        "p_success": 1.0,
        "confidence": 1.0,
        "effort_cost": 1,
        "strategic_weight": 1.0,
        "learning_value": 1.0,
        "horizon_weeks": 12,
        "monte_carlo": {"enabled": False, "n": 1000, "seed": 1},
    }

    payload_no_shrink = ScoreRunRequest(
        **base_payload,
        cannibalization={"mode": "matrix", "conservative_shrink": 0.0, "matrix_id": "v1"},
    )
    payload_with_shrink = ScoreRunRequest(
        **base_payload,
        cannibalization={"mode": "matrix", "conservative_shrink": 0.7, "matrix_id": "v1"},
    )

    result_no_shrink = run_scoring(
        payload_no_shrink,
        resolved_inputs_stub(baseline=baseline, cannibalization=cannibalization_table),
        mc_max_n=50_000,
    )
    result_with_shrink = run_scoring(
        payload_with_shrink,
        resolved_inputs_stub(baseline=baseline, cannibalization=cannibalization_table),
        mc_max_n=50_000,
    )

    assert result_with_shrink.deterministic["incremental_margin"] < result_no_shrink.deterministic["incremental_margin"]
