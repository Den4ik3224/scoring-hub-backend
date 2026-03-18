import hashlib
import json

from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.score import ScoreRunRequest
from app.core.settings import Settings
from app.db.models import ScoringRun
from app.services.scoring_engine import ScoringResult
from app.services.version_resolver import ResolvedScoringInputs

MODEL_VERSION = "ev_uncertainty_vnext"


def build_assumptions_snapshot_hash(
    payload: ScoreRunRequest,
    resolved_inputs: ResolvedScoringInputs,
    code_version: str,
) -> str:
    canonical = {
        "model_version": MODEL_VERSION,
        "code_version": code_version,
        "payload": payload.model_dump(mode="json", exclude_none=True),
        "resolved_inputs": resolved_inputs.resolved_inputs_json,
        "seed": payload.monte_carlo.seed,
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def persist_scoring_run(
    session: AsyncSession,
    *,
    payload: ScoreRunRequest,
    request_payload_override: dict | None = None,
    resolved_inputs: ResolvedScoringInputs,
    scoring_result: ScoringResult,
    settings: Settings,
    created_by: str,
    initiative_db_id: str | None,
    initiative_version_id: str | None = None,
    triggered_by_user_id: str | None = None,
    triggered_by_email: str | None = None,
    triggered_by_role: str | None = None,
    run_label: str | None = None,
    run_purpose: str | None = None,
    recompute_of_run_id: str | None = None,
) -> ScoringRun:
    snapshot_hash = build_assumptions_snapshot_hash(payload, resolved_inputs, settings.code_version)

    deterministic_payload = dict(scoring_result.deterministic)
    deterministic_payload.update(
        {
            "gross_impact": scoring_result.gross_impact,
            "net_incremental_impact": scoring_result.net_incremental_impact,
            "horizon_results": scoring_result.horizon_results,
            "scenarios": scoring_result.scenarios,
            "scenario_comparison": scoring_result.scenario_comparison,
            "sensitivity": scoring_result.sensitivity,
            "explainability": scoring_result.explainability,
            "effective_input_metrics": scoring_result.effective_input_metrics,
            "derived_output_metrics": scoring_result.derived_output_metrics,
            "validation_warnings": scoring_result.validation_warnings,
            "learning_applied": scoring_result.learning_applied,
            "learning_summary": scoring_result.learning_summary,
            "learning_warnings": scoring_result.learning_warnings,
            "scoring_policy_version": scoring_result.scoring_policy_version,
            "scoring_policy_source": scoring_result.scoring_policy_source,
            "per_screen_breakdown": scoring_result.per_screen_breakdown,
        }
    )
    probabilistic_payload = dict(scoring_result.probabilistic)
    probabilistic_payload["horizon_results"] = {
        horizon: data.get("probabilistic")
        for horizon, data in scoring_result.horizon_results.items()
    }
    probabilistic_payload["scenarios"] = {
        scenario_name: data.get("probabilistic")
        for scenario_name, data in scoring_result.scenarios.items()
    }

    run = ScoringRun(
        initiative_id=initiative_db_id,
        initiative_version_id=initiative_version_id,
        initiative_name=payload.initiative_name,
        request_payload_json=request_payload_override or payload.model_dump(mode="json", exclude_none=True),
        resolved_inputs_json=resolved_inputs.resolved_inputs_json,
        assumptions_snapshot_hash=snapshot_hash,
        rng_seed=payload.monte_carlo.seed,
        monte_carlo_n=payload.monte_carlo.n,
        code_version=settings.code_version,
        deterministic_output_json=deterministic_payload,
        probabilistic_output_json=probabilistic_payload,
        segment_breakdown_json=scoring_result.per_segment,
        node_contributions_json=scoring_result.per_metric_node,
        created_by=created_by,
        triggered_by_user_id=triggered_by_user_id or created_by,
        triggered_by_email=triggered_by_email,
        triggered_by_role=triggered_by_role,
        run_label=run_label,
        run_purpose=run_purpose,
        run_status="success",
        error_message=None,
        recompute_of_run_id=recompute_of_run_id,
    )

    session.add(run)
    await session.commit()
    await session.refresh(run)
    return run


def build_failed_snapshot_hash(
    payload: ScoreRunRequest,
    code_version: str,
    *,
    resolved_inputs_json: dict | None = None,
) -> str:
    canonical = {
        "model_version": MODEL_VERSION,
        "code_version": code_version,
        "payload": payload.model_dump(mode="json", exclude_none=True),
        "resolved_inputs": resolved_inputs_json or {},
        "seed": payload.monte_carlo.seed,
        "failed": True,
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def persist_failed_scoring_run(
    session: AsyncSession,
    *,
    payload: ScoreRunRequest,
    request_payload_override: dict | None,
    resolved_inputs_json: dict | None,
    settings: Settings,
    created_by: str,
    initiative_db_id: str | None,
    initiative_version_id: str | None,
    triggered_by_user_id: str | None,
    triggered_by_email: str | None,
    triggered_by_role: str | None,
    run_label: str | None,
    run_purpose: str | None,
    error_message: str,
    recompute_of_run_id: str | None = None,
) -> ScoringRun:
    snapshot_hash = build_failed_snapshot_hash(
        payload,
        settings.code_version,
        resolved_inputs_json=resolved_inputs_json,
    )
    run = ScoringRun(
        initiative_id=initiative_db_id,
        initiative_version_id=initiative_version_id,
        initiative_name=payload.initiative_name,
        request_payload_json=request_payload_override or payload.model_dump(mode="json", exclude_none=True),
        resolved_inputs_json=resolved_inputs_json or {},
        assumptions_snapshot_hash=snapshot_hash,
        rng_seed=payload.monte_carlo.seed,
        monte_carlo_n=payload.monte_carlo.n,
        code_version=settings.code_version,
        deterministic_output_json={},
        probabilistic_output_json={},
        segment_breakdown_json={},
        node_contributions_json={},
        created_by=created_by,
        triggered_by_user_id=triggered_by_user_id or created_by,
        triggered_by_email=triggered_by_email,
        triggered_by_role=triggered_by_role,
        run_label=run_label,
        run_purpose=run_purpose,
        run_status="failed",
        error_message=error_message[:2048],
        recompute_of_run_id=recompute_of_run_id,
    )
    session.add(run)
    await session.commit()
    await session.refresh(run)
    return run
