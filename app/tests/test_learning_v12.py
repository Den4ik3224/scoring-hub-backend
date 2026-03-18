from datetime import datetime, timedelta, timezone

from app.api.schemas.score import ScoreRunRequest
from app.services.scoring_engine import run_scoring

from .helpers_monthly import monthly_baseline_table, monthly_funnel_table, resolved_inputs_stub


def _payload(**overrides) -> ScoreRunRequest:
    data = {
        "initiative_name": "learning",
        "segments": [{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.02}}],
        "screens": ["home"],
        "metric_targets": [{"node": "conversion", "node_type": "metric", "uplift_dist": 0.02}],
        "p_success": 0.7,
        "confidence": 0.8,
        "effort_cost": 1000,
        "strategic_weight": 1.0,
        "learning_value": 1.0,
        "horizon_weeks": 26,
        "monte_carlo": {"enabled": True, "n": 1000, "seed": 123},
    }
    data.update(overrides)
    return ScoreRunRequest.model_validate(data)


def _resolved_inputs(
    *,
    learning_evidence: list[dict] | None = None,
    learning_mode: str = "bayesian",
):
    return resolved_inputs_stub(
        baseline=monthly_baseline_table(
            active_users=2000.0,
            ordering_users=200.0,
            orders=400.0,
            items=400.0,
            rto=8000.0,
            fm=2400.0,
        ),
        funnel=monthly_funnel_table(
            steps=(
                ("home_to_catalog", "Home to catalog", 1, 900.0, 400.0),
                ("catalog_to_cart", "Catalog to cart", 2, 400.0, 200.0),
            ),
        ),
        learning_config={
            "mode": learning_mode,
            "lookback_days": 730,
            "half_life_days": 180,
            "min_quality": 0.6,
            "min_sample_size": 500,
        },
        learning_evidence=learning_evidence or [],
    )


def test_learning_bayesian_applies_evidence() -> None:
    evidence = [
        {
            "id": "ev1",
            "metric_driver": "conversion",
            "screen": "home",
            "segment_id": "s1",
            "observed_uplift": 0.12,
            "ci_low": 0.08,
            "ci_high": 0.16,
            "sample_size": 2500,
            "quality_score": 0.9,
            "significance_flag": True,
            "end_at": datetime.now(timezone.utc) - timedelta(days=30),
        }
    ]
    baseline_result = run_scoring(_payload(learning={"mode": "off"}), _resolved_inputs(), mc_max_n=50_000)
    learned_result = run_scoring(
        _payload(learning={"mode": "bayesian"}),
        _resolved_inputs(learning_evidence=evidence),
        mc_max_n=50_000,
    )

    assert learned_result.learning_applied is True
    assert learned_result.learning_summary is not None
    assert learned_result.learning_summary["evidence_count"] == 1
    assert "ev1" in learned_result.learning_summary["evidence_ids"]
    assert learned_result.deterministic["incremental_fm"] > baseline_result.deterministic["incremental_fm"]


def test_learning_metric_target_exact_segment_match_applies() -> None:
    evidence = [
        {
            "id": "ev_metric_target_exact",
            "metric_driver": "conversion",
            "screen": "home",
            "segment_id": "s1",
            "observed_uplift": 0.11,
            "ci_low": 0.07,
            "ci_high": 0.15,
            "sample_size": 2800,
            "quality_score": 0.9,
            "significance_flag": True,
            "end_at": datetime.now(timezone.utc) - timedelta(days=10),
        }
    ]
    baseline = run_scoring(
        _payload(
            learning={"mode": "off"},
            segments=[{"id": "s1", "penetration": 0.5, "uplifts": {}}],
            metric_targets=[{"node": "conversion", "node_type": "metric", "uplift_dist": 0.02}],
        ),
        _resolved_inputs(),
        mc_max_n=50_000,
    )
    learned = run_scoring(
        _payload(
            learning={"mode": "bayesian"},
            segments=[{"id": "s1", "penetration": 0.5, "uplifts": {}}],
            metric_targets=[{"node": "conversion", "node_type": "metric", "uplift_dist": 0.02}],
        ),
        _resolved_inputs(learning_evidence=evidence),
        mc_max_n=50_000,
    )

    assert learned.learning_applied is True
    assert learned.learning_summary is not None
    assert learned.learning_summary["evidence_count"] == 1
    assert learned.deterministic["incremental_fm"] > baseline.deterministic["incremental_fm"]


def test_learning_advisory_keeps_calculation_but_returns_warnings() -> None:
    evidence = [
        {
            "id": "ev2",
            "metric_driver": "conversion",
            "screen": "home",
            "segment_id": None,
            "observed_uplift": 0.1,
            "ci_low": 0.06,
            "ci_high": 0.14,
            "sample_size": 3000,
            "quality_score": 0.8,
            "significance_flag": True,
            "end_at": datetime.now(timezone.utc) - timedelta(days=20),
        }
    ]
    advisory_result = run_scoring(
        _payload(learning={"mode": "advisory"}),
        _resolved_inputs(learning_evidence=evidence, learning_mode="advisory"),
        mc_max_n=50_000,
    )
    off_result = run_scoring(
        _payload(learning={"mode": "off"}),
        _resolved_inputs(learning_evidence=evidence, learning_mode="off"),
        mc_max_n=50_000,
    )

    assert advisory_result.learning_applied is False
    assert advisory_result.learning_warnings
    assert advisory_result.deterministic["incremental_fm"] == off_result.deterministic["incremental_fm"]


def test_learning_applies_after_aov_translation_to_aoq_and_aiv() -> None:
    evidence = [
        {
            "id": "ev_aov_translation",
            "metric_driver": "aoq",
            "screen": "home",
            "segment_id": None,
            "observed_uplift": 0.18,
            "ci_low": 0.1,
            "ci_high": 0.26,
            "sample_size": 4000,
            "quality_score": 0.9,
            "significance_flag": True,
            "end_at": datetime.now(timezone.utc) - timedelta(days=15),
        }
    ]
    baseline = run_scoring(
        _payload(metric_targets=[{"node": "aov", "node_type": "metric", "uplift_dist": 0.02}], learning={"mode": "off"}),
        _resolved_inputs(),
        mc_max_n=50_000,
    )
    learned = run_scoring(
        _payload(metric_targets=[{"node": "aov", "node_type": "metric", "uplift_dist": 0.02}], learning={"mode": "bayesian"}),
        _resolved_inputs(learning_evidence=evidence),
        mc_max_n=50_000,
    )

    assert learned.learning_applied is True
    assert learned.learning_summary is not None
    assert learned.learning_summary["evidence_count"] == 1
    assert learned.deterministic["incremental_fm"] > baseline.deterministic["incremental_fm"]


def test_learning_does_not_mix_evidence_between_screens() -> None:
    evidence = [
        {
            "id": "ev_home_only",
            "metric_driver": "conversion",
            "screen": "home",
            "segment_id": "s1",
            "observed_uplift": 0.25,
            "ci_low": 0.18,
            "ci_high": 0.32,
            "sample_size": 5000,
            "quality_score": 0.95,
            "significance_flag": True,
            "end_at": datetime.now(timezone.utc) - timedelta(days=20),
        }
    ]
    payload = _payload(
        screens=["home", "search"],
        learning={"mode": "bayesian"},
        segments=[{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.02}}],
        metric_targets=[{"node": "conversion", "node_type": "metric", "uplift_dist": 0.02}],
    )
    result = run_scoring(payload, _resolved_inputs(learning_evidence=evidence), mc_max_n=50_000)

    assert result.learning_applied is False
    assert any("screen-specific" in warning for warning in result.learning_warnings)


def test_learning_falls_back_to_same_screen_when_segment_is_null() -> None:
    evidence = [
        {
            "id": "ev_screen_only",
            "metric_driver": "conversion",
            "screen": "home",
            "segment_id": None,
            "observed_uplift": 0.14,
            "ci_low": 0.09,
            "ci_high": 0.19,
            "sample_size": 3500,
            "quality_score": 0.82,
            "significance_flag": True,
            "end_at": datetime.now(timezone.utc) - timedelta(days=25),
        }
    ]
    result = run_scoring(
        _payload(learning={"mode": "bayesian"}),
        _resolved_inputs(learning_evidence=evidence),
        mc_max_n=50_000,
    )

    assert result.learning_applied is True
    assert result.learning_summary is not None
    assert result.learning_summary["evidence_count"] == 1
