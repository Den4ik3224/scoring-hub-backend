from copy import deepcopy
from datetime import date, datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.score import (
    InputVersions,
    MetricTreeSelector,
    ScoreRunCreateV11,
    ScoreRunRequest,
    ScoreRunResponse,
    ScoringRunDetailResponse,
    ScoringRunListResponse,
    ScoringRunRecord,
)
from app.core.errors import NotFoundError, ValidationError
from app.core.security import Principal, get_current_principal
from app.core.settings import Settings, get_settings
from app.db.models import Initiative, InitiativeVersion, ScoringRun
from app.db.repositories import initiatives as initiative_repo
from app.db.repositories import scoring_runs as scoring_repo
from app.db.session import get_session
from app.services.audit import persist_failed_scoring_run, persist_scoring_run
from app.services.initiative_versioning import create_version_from_score_request, version_to_score_run_request
from app.services.scoring_engine import run_scoring
from app.services.version_resolver import resolve_scoring_inputs

router = APIRouter(prefix="/score", tags=["score"])


def _build_score_response(run_id: str, snapshot_hash: str, resolved_versions: dict[str, str], code_version: str, seed: int, result) -> ScoreRunResponse:
    return ScoreRunResponse(
        run_id=run_id,
        assumptions_snapshot_hash=snapshot_hash,
        resolved_versions=resolved_versions,
        code_version=code_version,
        seed=seed,
        deterministic=result.deterministic,
        probabilistic=result.probabilistic,
        per_segment=result.per_segment,
        per_metric_node=result.per_metric_node,
        gross_impact=result.gross_impact,
        net_incremental_impact=result.net_incremental_impact,
        horizon_results=result.horizon_results,
        scenarios=result.scenarios,
        scenario_comparison=result.scenario_comparison,
        sensitivity=result.sensitivity,
        explainability=result.explainability,
        effective_input_metrics=result.effective_input_metrics,
        derived_output_metrics=result.derived_output_metrics,
        validation_warnings=result.validation_warnings,
        learning_applied=result.learning_applied,
        learning_summary=result.learning_summary,
        learning_warnings=result.learning_warnings,
        scoring_policy_version=result.scoring_policy_version,
        scoring_policy_source=result.scoring_policy_source,
        per_screen_breakdown=result.per_screen_breakdown,
    )


def _scenario_names_from_record(record: ScoringRun) -> list[str]:
    deterministic = record.deterministic_output_json or {}
    scenarios = deterministic.get("scenarios")
    if isinstance(scenarios, dict):
        return sorted(str(name) for name in scenarios.keys())
    payload = record.request_payload_json or {}
    payload_scenarios = payload.get("scenarios")
    if isinstance(payload_scenarios, dict):
        names = set(payload_scenarios.keys())
        names.add("base")
        return sorted(str(name) for name in names)
    return ["base"]


def _record_to_api(record: ScoringRun) -> ScoringRunRecord:
    return ScoringRunRecord(
        id=record.id,
        initiative_id=record.initiative_id,
        initiative_version_id=record.initiative_version_id,
        initiative_name=record.initiative_name,
        assumptions_snapshot_hash=record.assumptions_snapshot_hash,
        rng_seed=record.rng_seed,
        monte_carlo_n=record.monte_carlo_n,
        code_version=record.code_version,
        created_by=record.created_by,
        triggered_by_user_id=record.triggered_by_user_id,
        triggered_by_email=record.triggered_by_email,
        triggered_by_role=record.triggered_by_role,
        run_label=record.run_label,
        run_purpose=record.run_purpose,
        run_status=record.run_status,
        error_message=record.error_message,
        scenario_names=_scenario_names_from_record(record),
        created_at=record.created_at,
        recompute_of_run_id=record.recompute_of_run_id,
        deterministic_output=record.deterministic_output_json,
        probabilistic_output=record.probabilistic_output_json,
    )


def _resolve_run_actor(principal: Principal, payload: ScoreRunCreateV11) -> tuple[str, str | None, str]:
    if payload.actor_override and not principal.auth_disabled:
        raise ValidationError("actor_override is allowed only when AUTH_MODE=disabled")

    user_id = principal.user_id
    email = principal.email
    role = principal.role

    if principal.auth_disabled and payload.actor_override:
        user_id = payload.actor_override.user_id or user_id
        email = payload.actor_override.email or email
        role = payload.actor_override.role or role

    return user_id, email, role


async def _get_initiative_by_ref(session: AsyncSession, initiative_ref: str) -> Initiative | None:
    initiative = await initiative_repo.get_initiative(session, initiative_ref)
    if initiative:
        return initiative
    return await initiative_repo.get_initiative_by_external_key(session, initiative_ref)


def _hydrate_input_versions_from_resolved(payload: ScoreRunRequest, record: ScoringRun) -> None:
    resolved_datasets = record.resolved_inputs_json.get("datasets", {})
    metric_tree_source = record.resolved_inputs_json.get("metric_tree_source") or ""
    scoring_policy_source = record.resolved_inputs_json.get("scoring_policy_source") or ""

    payload.input_versions = InputVersions(
        baseline_metrics=resolved_datasets.get("baseline_metrics", {}).get("version"),
        baseline_funnel_steps=resolved_datasets.get("baseline_funnel_steps", {}).get("version"),
        cannibalization_matrix=resolved_datasets.get("cannibalization_matrix", {}).get("version"),
        scoring_policy=(scoring_policy_source.split(":")[-1] if scoring_policy_source.startswith("config:") else None),
    )
    if metric_tree_source and ":" in metric_tree_source:
        _, name, version = metric_tree_source.split(":", 2)
        payload.metric_tree = MetricTreeSelector(template_name=name, version=version)
    payload.data_scope = str(
        (record.resolved_inputs_json or {}).get("data_scope")
        or (record.request_payload_json or {}).get("data_scope")
        or "prod"
    )
    baseline_window = (record.resolved_inputs_json or {}).get("baseline_window") or {}
    if baseline_window:
        payload.baseline_window = str(baseline_window.get("name") or "quarter")
        payload.baseline_date_start = (
            date.fromisoformat(str(baseline_window["date_start"])) if baseline_window.get("date_start") else None
        )
        payload.baseline_date_end = (
            date.fromisoformat(str(baseline_window["date_end"])) if baseline_window.get("date_end") else None
        )


_OVERRIDABLE_SCALAR_FIELDS = (
    "data_scope",
    "p_success",
    "confidence",
    "evidence_type",
    "effort_cost",
    "strategic_weight",
    "learning_value",
    "baseline_window",
    "baseline_date_start",
    "baseline_date_end",
    "horizon_weeks",
    "horizons_weeks",
    "decay",
    "discount_rate_annual",
    "cannibalization",
    "interactions",
    "monte_carlo",
    "scenarios",
    "sensitivity",
    "learning",
    "input_versions",
    "metric_tree",
    "scoring_policy",
)


def _merge_runtime_overrides(base_payload: ScoreRunRequest, payload: ScoreRunCreateV11) -> ScoreRunRequest:
    update_data: dict[str, object] = {}
    for field_name in _OVERRIDABLE_SCALAR_FIELDS:
        value = getattr(payload, field_name)
        if value is not None:
            update_data[field_name] = deepcopy(value)
    if payload.initiative_name:
        update_data["initiative_name"] = payload.initiative_name
    return base_payload.model_copy(update=update_data, deep=True)


async def _resolve_score_payload(
    session: AsyncSession,
    payload: ScoreRunCreateV11,
    *,
    actor_user_id: str | None,
    actor_email: str | None,
) -> tuple[ScoreRunRequest, Initiative | None, InitiativeVersion | None]:
    if payload.initiative_version_id:
        initiative_version = await initiative_repo.get_initiative_version_by_id(session, payload.initiative_version_id)
        if not initiative_version:
            raise NotFoundError(f"Initiative version `{payload.initiative_version_id}` not found")
        initiative = await initiative_repo.get_initiative(session, initiative_version.initiative_id)
        if not initiative:
            raise NotFoundError(f"Initiative `{initiative_version.initiative_id}` not found")
        score_payload = version_to_score_run_request(initiative_version, initiative)
        return _merge_runtime_overrides(score_payload, payload), initiative, initiative_version

    if payload.has_ad_hoc_payload:
        score_payload = payload.to_score_run_request()
        initiative: Initiative | None = None
        if payload.initiative_id:
            initiative = await _get_initiative_by_ref(session, payload.initiative_id)
            if not initiative:
                initiative = await initiative_repo.create_or_get_initiative(
                    session,
                    external_id=payload.initiative_id,
                    name=score_payload.initiative_name,
                    created_by_user_id=actor_user_id,
                    created_by_email=actor_email,
                )
        else:
            initiative = await initiative_repo.create_or_get_initiative(
                session,
                external_id=score_payload.initiative_id,
                name=score_payload.initiative_name,
                created_by_user_id=actor_user_id,
                created_by_email=actor_email,
            )

        initiative_version = None
        if initiative:
            initiative_version = await create_version_from_score_request(
                session,
                initiative.id,
                score_payload,
                created_by_user_id=actor_user_id,
                created_by_email=actor_email,
                change_comment=payload.version_change_comment if payload.save_as_new_version else None,
                title_override=score_payload.initiative_name,
            )
            score_payload.initiative_id = initiative.id
        return score_payload, initiative, initiative_version

    if payload.initiative_id:
        initiative = await _get_initiative_by_ref(session, payload.initiative_id)
        if not initiative:
            raise NotFoundError(f"Initiative `{payload.initiative_id}` not found")
        latest_version = await initiative_repo.get_latest_initiative_version(session, initiative.id)
        if not latest_version:
            raise ValidationError("Initiative has no versions. Create an initiative version first.")
        score_payload = version_to_score_run_request(latest_version, initiative)
        return _merge_runtime_overrides(score_payload, payload), initiative, latest_version

    raise ValidationError("Unable to resolve scoring request")


@router.post("/run", response_model=ScoreRunResponse)
async def run_score(
    payload: ScoreRunCreateV11,
    principal: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> ScoreRunResponse:
    actor_user_id, actor_email, actor_role = _resolve_run_actor(principal, payload)
    score_payload, initiative, initiative_version = await _resolve_score_payload(
        session,
        payload,
        actor_user_id=actor_user_id,
        actor_email=actor_email,
    )
    request_payload_override = payload.model_dump(mode="json", exclude_none=True)
    request_payload_override.setdefault("data_scope", score_payload.data_scope)

    resolved_inputs = None
    try:
        resolved_inputs = await resolve_scoring_inputs(session, score_payload)
        result = run_scoring(score_payload, resolved_inputs, mc_max_n=settings.mc_max_n)
    except Exception as exc:
        await persist_failed_scoring_run(
            session,
            payload=score_payload,
            request_payload_override=request_payload_override,
            resolved_inputs_json=resolved_inputs.resolved_inputs_json if resolved_inputs else None,
            settings=settings,
            created_by=actor_user_id,
            initiative_db_id=initiative.id if initiative else None,
            initiative_version_id=initiative_version.id if initiative_version else None,
            triggered_by_user_id=actor_user_id,
            triggered_by_email=actor_email,
            triggered_by_role=actor_role,
            run_label=payload.run_label,
            run_purpose=payload.run_purpose,
            error_message=str(exc),
        )
        raise

    run = await persist_scoring_run(
        session,
        payload=score_payload,
        request_payload_override=request_payload_override,
        resolved_inputs=resolved_inputs,
        scoring_result=result,
        settings=settings,
        created_by=actor_user_id,
        initiative_db_id=initiative.id if initiative else None,
        initiative_version_id=initiative_version.id if initiative_version else None,
        triggered_by_user_id=actor_user_id,
        triggered_by_email=actor_email,
        triggered_by_role=actor_role,
        run_label=payload.run_label,
        run_purpose=payload.run_purpose,
    )

    return _build_score_response(
        run_id=run.id,
        snapshot_hash=run.assumptions_snapshot_hash,
        resolved_versions=resolved_inputs.resolved_versions,
        code_version=run.code_version,
        seed=run.rng_seed,
        result=result,
    )


@router.get("/runs", response_model=ScoringRunListResponse)
async def list_runs(
    initiative_id: str | None = Query(default=None),
    owner_team_id: str | None = Query(default=None),
    triggered_by_user_id: str | None = Query(default=None),
    run_purpose: str | None = Query(default=None),
    run_status: str | None = Query(default=None),
    created_from: datetime | None = Query(default=None, alias="created_from"),
    created_to: datetime | None = Query(default=None, alias="created_to"),
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = Query(default=None, alias="to"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ScoringRunListResponse:
    records = await scoring_repo.list_scoring_runs(
        session,
        initiative_id=initiative_id,
        owner_team_id=owner_team_id,
        triggered_by_user_id=triggered_by_user_id,
        run_purpose=run_purpose,
        run_status=run_status,
        from_dt=created_from or from_,
        to_dt=created_to or to,
        limit=limit,
        offset=offset,
    )
    return ScoringRunListResponse(items=[_record_to_api(record) for record in records])


@router.get("/runs/{run_id}", response_model=ScoringRunDetailResponse)
async def get_run(
    run_id: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ScoringRunDetailResponse:
    record = await scoring_repo.get_scoring_run(session, run_id)
    if not record:
        raise NotFoundError(f"Scoring run `{run_id}` not found")

    return ScoringRunDetailResponse(
        id=record.id,
        initiative_id=record.initiative_id,
        initiative_version_id=record.initiative_version_id,
        initiative_name=record.initiative_name,
        request_payload=record.request_payload_json,
        resolved_inputs=record.resolved_inputs_json,
        assumptions_snapshot_hash=record.assumptions_snapshot_hash,
        rng_seed=record.rng_seed,
        monte_carlo_n=record.monte_carlo_n,
        code_version=record.code_version,
        deterministic_output=record.deterministic_output_json,
        probabilistic_output=record.probabilistic_output_json,
        segment_breakdown=record.segment_breakdown_json,
        node_contributions=record.node_contributions_json,
        created_by=record.created_by,
        triggered_by_user_id=record.triggered_by_user_id,
        triggered_by_email=record.triggered_by_email,
        triggered_by_role=record.triggered_by_role,
        run_label=record.run_label,
        run_purpose=record.run_purpose,
        run_status=record.run_status,
        error_message=record.error_message,
        scenario_names=_scenario_names_from_record(record),
        per_screen_breakdown=(record.deterministic_output_json or {}).get("per_screen_breakdown"),
        created_at=record.created_at,
        recompute_of_run_id=record.recompute_of_run_id,
    )


async def _recompute_payload_from_record(session: AsyncSession, record: ScoringRun) -> ScoreRunRequest:
    try:
        payload_v11 = ScoreRunCreateV11.model_validate(record.request_payload_json)
    except Exception:
        return ScoreRunRequest.model_validate(record.request_payload_json)

    if payload_v11.has_ad_hoc_payload:
        return payload_v11.to_score_run_request()
    if record.initiative_version_id:
        version = await initiative_repo.get_initiative_version_by_id(session, record.initiative_version_id)
        if not version:
            raise NotFoundError(f"Initiative version `{record.initiative_version_id}` not found")
        initiative = await initiative_repo.get_initiative(session, version.initiative_id)
        if not initiative:
            raise NotFoundError(f"Initiative `{version.initiative_id}` not found")
        base_payload = version_to_score_run_request(version, initiative)
        return _merge_runtime_overrides(base_payload, payload_v11)
    if payload_v11.initiative_id:
        initiative = await _get_initiative_by_ref(session, payload_v11.initiative_id)
        if initiative:
            latest_version = await initiative_repo.get_latest_initiative_version(session, initiative.id)
            if latest_version:
                base_payload = version_to_score_run_request(latest_version, initiative)
                return _merge_runtime_overrides(base_payload, payload_v11)
    return ScoreRunRequest.model_validate(record.request_payload_json)


@router.post("/runs/{run_id}/recompute", response_model=ScoreRunResponse)
async def recompute_run(
    run_id: str,
    principal: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> ScoreRunResponse:
    record = await scoring_repo.get_scoring_run(session, run_id)
    if not record:
        raise NotFoundError(f"Scoring run `{run_id}` not found")

    payload = await _recompute_payload_from_record(session, record)
    _hydrate_input_versions_from_resolved(payload, record)

    payload.monte_carlo.seed = record.rng_seed
    payload.monte_carlo.n = record.monte_carlo_n

    resolved_inputs = await resolve_scoring_inputs(session, payload)
    frozen_policy_snapshot = record.resolved_inputs_json.get("scoring_policy_snapshot")
    frozen_policy_source = record.resolved_inputs_json.get("scoring_policy_source")
    frozen_learning = record.resolved_inputs_json.get("learning")
    if frozen_policy_snapshot:
        resolved_inputs.scoring_policy_snapshot = frozen_policy_snapshot
    if frozen_policy_source:
        resolved_inputs.scoring_policy_source = frozen_policy_source
    if isinstance(frozen_learning, dict):
        frozen_config = frozen_learning.get("config")
        frozen_evidence = frozen_learning.get("evidence_items")
        if isinstance(frozen_config, dict):
            resolved_inputs.learning_config = frozen_config
        if isinstance(frozen_evidence, list):
            resolved_inputs.learning_evidence = frozen_evidence
        resolved_inputs.resolved_inputs_json["learning"] = frozen_learning
    try:
        result = run_scoring(payload, resolved_inputs, mc_max_n=settings.mc_max_n)
    except Exception as exc:
        failed = await persist_failed_scoring_run(
            session,
            payload=payload,
            request_payload_override=record.request_payload_json,
            resolved_inputs_json=resolved_inputs.resolved_inputs_json,
            settings=settings,
            created_by=principal.user_id,
            initiative_db_id=record.initiative_id,
            initiative_version_id=record.initiative_version_id,
            triggered_by_user_id=principal.user_id,
            triggered_by_email=principal.email,
            triggered_by_role=principal.role,
            run_label=record.run_label,
            run_purpose=record.run_purpose,
            error_message=str(exc),
            recompute_of_run_id=record.id,
        )
        raise ValidationError(f"Recompute failed, failed run stored as `{failed.id}`: {exc}") from exc

    recomputed = await persist_scoring_run(
        session,
        payload=payload,
        request_payload_override=record.request_payload_json,
        resolved_inputs=resolved_inputs,
        scoring_result=result,
        settings=settings,
        created_by=principal.user_id,
        initiative_db_id=record.initiative_id,
        initiative_version_id=record.initiative_version_id,
        triggered_by_user_id=principal.user_id,
        triggered_by_email=principal.email,
        triggered_by_role=principal.role,
        run_label=record.run_label,
        run_purpose=record.run_purpose,
        recompute_of_run_id=record.id,
    )

    return _build_score_response(
        run_id=recomputed.id,
        snapshot_hash=recomputed.assumptions_snapshot_hash,
        resolved_versions=resolved_inputs.resolved_versions,
        code_version=recomputed.code_version,
        seed=recomputed.rng_seed,
        result=result,
    )
