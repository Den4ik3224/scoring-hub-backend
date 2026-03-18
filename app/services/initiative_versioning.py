from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.initiative import InitiativeVersionCreate
from app.api.schemas.score import (
    CannibalizationInput,
    InputVersions,
    MonteCarloInput,
    ScoreRunRequest,
)
from app.core.errors import ValidationError
from app.db.models import Initiative, InitiativeVersion
from app.db.repositories import initiatives as initiative_repo


def build_assumptions_json(payload: ScoreRunRequest) -> dict[str, Any]:
    return {
        "data_scope": payload.data_scope,
        "p_success": payload.p_success,
        "confidence": payload.confidence,
        "evidence_type": payload.evidence_type,
        "effort_cost": payload.effort_cost,
        "strategic_weight": payload.strategic_weight,
        "learning_value": payload.learning_value,
        "baseline_window": payload.baseline_window,
        "baseline_date_start": payload.baseline_date_start.isoformat() if payload.baseline_date_start else None,
        "baseline_date_end": payload.baseline_date_end.isoformat() if payload.baseline_date_end else None,
        "horizon_weeks": payload.horizon_weeks,
        "horizons_weeks": payload.horizons_weeks,
        "decay": payload.decay.model_dump(mode="json") if payload.decay else None,
        "discount_rate_annual": payload.discount_rate_annual,
        "cannibalization": payload.cannibalization.model_dump(mode="json"),
        "interactions": [item.model_dump(mode="json") for item in payload.interactions],
        "monte_carlo": payload.monte_carlo.model_dump(mode="json"),
        "scenarios": (
            {name: override.model_dump(mode="json", exclude_none=True) for name, override in payload.scenarios.items()}
            if payload.scenarios
            else None
        ),
        "sensitivity": payload.sensitivity.model_dump(mode="json"),
        "learning": payload.learning.model_dump(mode="json") if payload.learning else None,
        "input_versions": payload.input_versions.model_dump(mode="json", exclude_none=True) if payload.input_versions else None,
        "metric_tree": payload.metric_tree.model_dump(mode="json", exclude_none=True) if payload.metric_tree else None,
        "scoring_policy": payload.scoring_policy.model_dump(mode="json", exclude_none=True) if payload.scoring_policy else None,
    }


def validate_non_legacy_metric_inputs(
    *,
    segments: list,
    metric_targets: list,
) -> None:
    violations: list[str] = []
    for segment in segments:
        for key in segment.uplifts.keys():
            if key.startswith("aoq_component:"):
                violations.append(f"segment={segment.id}, key={key}")
    for target in metric_targets:
        metric_key = target.metric_key or target.node
        if metric_key.startswith("aoq_component:"):
            violations.append(f"metric_target={metric_key}")
    if violations:
        raise ValidationError(
            "AOQ component inputs are deprecated for new runs and versions. "
            "Use primitive `aoq` uplift instead. Violations: " + "; ".join(violations)
        )


def version_to_score_run_request(version: InitiativeVersion, initiative: Initiative) -> ScoreRunRequest:
    assumptions = version.assumptions_json or {}
    return ScoreRunRequest(
        initiative_id=initiative.id,
        initiative_name=version.title_override or initiative.name,
        segments=version.segments_json,
        screens=version.screens_json,
        metric_targets=version.metric_targets_json,
        p_success=version.p_success if version.p_success is not None else float(assumptions.get("p_success", 0.0)),
        confidence=version.confidence if version.confidence is not None else assumptions.get("confidence"),
        evidence_type=version.evidence_type or assumptions.get("evidence_type"),
        effort_cost=version.effort_cost if version.effort_cost is not None else float(assumptions.get("effort_cost", 1.0)),
        strategic_weight=(
            version.strategic_weight
            if version.strategic_weight is not None
            else float(assumptions.get("strategic_weight", 1.0))
        ),
        learning_value=(
            version.learning_value
            if version.learning_value is not None
            else float(assumptions.get("learning_value", 1.0))
        ),
        baseline_window=str(assumptions.get("baseline_window") or "quarter"),
        baseline_date_start=assumptions.get("baseline_date_start"),
        baseline_date_end=assumptions.get("baseline_date_end"),
        horizon_weeks=version.horizon_weeks if version.horizon_weeks is not None else int(assumptions.get("horizon_weeks", 1)),
        horizons_weeks=assumptions.get("horizons_weeks"),
        decay=assumptions.get("decay"),
        discount_rate_annual=(
            version.discount_rate_annual
            if version.discount_rate_annual is not None
            else assumptions.get("discount_rate_annual")
        ),
        cannibalization=version.cannibalization_json or assumptions.get("cannibalization") or CannibalizationInput(),
        interactions=version.interactions_json or assumptions.get("interactions") or [],
        monte_carlo=assumptions.get("monte_carlo") or MonteCarloInput(),
        scenarios=assumptions.get("scenarios"),
        sensitivity=assumptions.get("sensitivity") or {"enabled": False},
        learning=assumptions.get("learning"),
        input_versions=assumptions.get("input_versions") or InputVersions(),
        metric_tree=assumptions.get("metric_tree") or None,
        scoring_policy=assumptions.get("scoring_policy"),
        data_scope=version.data_scope or str(assumptions.get("data_scope") or "prod"),
    )


async def create_version_from_score_request(
    session: AsyncSession,
    initiative_id: str,
    payload: ScoreRunRequest,
    *,
    created_by_user_id: str | None,
    created_by_email: str | None,
    change_comment: str | None,
    title_override: str | None = None,
    description_override: str | None = None,
) -> InitiativeVersion:
    validate_non_legacy_metric_inputs(segments=payload.segments, metric_targets=payload.metric_targets)
    assumptions_json = build_assumptions_json(payload)
    return await initiative_repo.create_initiative_version(
        session,
        initiative_id=initiative_id,
        title_override=title_override,
        description_override=description_override,
        data_scope=payload.data_scope,
        screens_json=payload.screens,
        segments_json=[segment.model_dump(mode="json") for segment in payload.segments],
        metric_targets_json=[target.model_dump(mode="json") for target in payload.metric_targets],
        assumptions_json=assumptions_json,
        p_success=payload.p_success,
        confidence=payload.confidence,
        evidence_type=payload.evidence_type,
        effort_cost=payload.effort_cost,
        strategic_weight=payload.strategic_weight,
        learning_value=payload.learning_value,
        horizon_weeks=payload.horizon_weeks,
        decay_json=payload.decay.model_dump(mode="json") if payload.decay else None,
        discount_rate_annual=payload.discount_rate_annual,
        cannibalization_json=payload.cannibalization.model_dump(mode="json"),
        interactions_json=[item.model_dump(mode="json") for item in payload.interactions],
        created_by_user_id=created_by_user_id,
        created_by_email=created_by_email,
        change_comment=change_comment,
    )


async def create_version_from_payload(
    session: AsyncSession,
    initiative_id: str,
    payload: InitiativeVersionCreate,
    *,
    created_by_user_id: str | None,
    created_by_email: str | None,
) -> InitiativeVersion:
    validate_non_legacy_metric_inputs(segments=payload.segments, metric_targets=payload.metric_targets)
    assumptions_json = {
        "data_scope": payload.data_scope,
        "p_success": payload.p_success,
        "confidence": payload.confidence,
        "evidence_type": payload.evidence_type,
        "effort_cost": payload.effort_cost,
        "strategic_weight": payload.strategic_weight,
        "learning_value": payload.learning_value,
        "baseline_window": payload.baseline_window,
        "baseline_date_start": payload.baseline_date_start.isoformat() if payload.baseline_date_start else None,
        "baseline_date_end": payload.baseline_date_end.isoformat() if payload.baseline_date_end else None,
        "horizon_weeks": payload.horizon_weeks,
        "horizons_weeks": payload.horizons_weeks,
        "decay": payload.decay.model_dump(mode="json") if payload.decay else None,
        "discount_rate_annual": payload.discount_rate_annual,
        "cannibalization": payload.cannibalization.model_dump(mode="json"),
        "interactions": [item.model_dump(mode="json") for item in payload.interactions],
        "monte_carlo": payload.monte_carlo.model_dump(mode="json"),
        "scenarios": (
            {name: override.model_dump(mode="json", exclude_none=True) for name, override in payload.scenarios.items()}
            if payload.scenarios
            else None
        ),
        "sensitivity": payload.sensitivity.model_dump(mode="json"),
        "learning": payload.learning.model_dump(mode="json") if payload.learning else None,
        "input_versions": payload.input_versions.model_dump(mode="json", exclude_none=True) if payload.input_versions else None,
        "metric_tree": payload.metric_tree.model_dump(mode="json", exclude_none=True) if payload.metric_tree else None,
        "scoring_policy": payload.scoring_policy.model_dump(mode="json", exclude_none=True) if payload.scoring_policy else None,
    }
    return await initiative_repo.create_initiative_version(
        session,
        initiative_id=initiative_id,
        title_override=payload.title_override,
        description_override=payload.description_override,
        data_scope=payload.data_scope,
        screens_json=payload.screens,
        segments_json=[segment.model_dump(mode="json") for segment in payload.segments],
        metric_targets_json=[target.model_dump(mode="json") for target in payload.metric_targets],
        assumptions_json=assumptions_json,
        p_success=payload.p_success,
        confidence=payload.confidence,
        evidence_type=payload.evidence_type,
        effort_cost=payload.effort_cost,
        strategic_weight=payload.strategic_weight,
        learning_value=payload.learning_value,
        horizon_weeks=payload.horizon_weeks,
        decay_json=payload.decay.model_dump(mode="json") if payload.decay else None,
        discount_rate_annual=payload.discount_rate_annual,
        cannibalization_json=payload.cannibalization.model_dump(mode="json"),
        interactions_json=[item.model_dump(mode="json") for item in payload.interactions],
        created_by_user_id=created_by_user_id,
        created_by_email=created_by_email,
        change_comment=payload.change_comment,
    )


def compare_versions(version_a: InitiativeVersion, version_b: InitiativeVersion) -> dict[str, dict[str, Any]]:
    field_pairs = {
        "title_override": (version_a.title_override, version_b.title_override),
        "description_override": (version_a.description_override, version_b.description_override),
        "data_scope": (version_a.data_scope, version_b.data_scope),
        "screens": (version_a.screens_json, version_b.screens_json),
        "segments": (version_a.segments_json, version_b.segments_json),
        "metric_targets": (version_a.metric_targets_json, version_b.metric_targets_json),
        "assumptions": (version_a.assumptions_json, version_b.assumptions_json),
        "p_success": (version_a.p_success, version_b.p_success),
        "confidence": (version_a.confidence, version_b.confidence),
        "evidence_type": (version_a.evidence_type, version_b.evidence_type),
        "effort_cost": (version_a.effort_cost, version_b.effort_cost),
        "strategic_weight": (version_a.strategic_weight, version_b.strategic_weight),
        "learning_value": (version_a.learning_value, version_b.learning_value),
        "horizon_weeks": (version_a.horizon_weeks, version_b.horizon_weeks),
        "decay": (version_a.decay_json, version_b.decay_json),
        "discount_rate_annual": (version_a.discount_rate_annual, version_b.discount_rate_annual),
        "cannibalization": (version_a.cannibalization_json, version_b.cannibalization_json),
        "interactions": (version_a.interactions_json, version_b.interactions_json),
    }
    return {
        field: {"a": value_a, "b": value_b, "changed": value_a != value_b}
        for field, (value_a, value_b) in field_pairs.items()
    }
