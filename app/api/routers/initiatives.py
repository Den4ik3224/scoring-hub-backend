from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.initiative import (
    InitiativeCompareResponse,
    InitiativeCreate,
    InitiativeListResponse,
    InitiativeRead,
    InitiativeUpdate,
    InitiativeVersionCreate,
    InitiativeVersionListResponse,
    InitiativeVersionRead,
)
from app.api.schemas.score import ScoringRunListResponse, ScoringRunRecord
from app.core.errors import NotFoundError, ValidationError
from app.core.security import Principal, get_current_principal
from app.db.models import InitiativeVersion
from app.db.repositories import initiatives as initiative_repo
from app.db.repositories import scoring_runs as scoring_repo
from app.db.repositories import teams as team_repo
from app.db.session import get_session
from app.services.initiative_service import build_initiative_read, build_initiative_reads, to_initiative_version_read
from app.services.initiative_versioning import compare_versions, create_version_from_payload

router = APIRouter(prefix="/initiatives", tags=["initiatives"])


def _run_record(record) -> ScoringRunRecord:
    deterministic = record.deterministic_output_json or {}
    scenarios = deterministic.get("scenarios")
    if isinstance(scenarios, dict):
        scenario_names = sorted(str(name) for name in scenarios.keys())
    else:
        scenario_names = ["base"]

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
        scenario_names=scenario_names,
        created_at=record.created_at,
        recompute_of_run_id=record.recompute_of_run_id,
        deterministic_output=record.deterministic_output_json,
        probabilistic_output=record.probabilistic_output_json,
    )


def _outputs_delta(outputs_a: dict, outputs_b: dict) -> dict[str, float]:
    delta: dict[str, float] = {}
    for key, value_a in outputs_a.items():
        value_b = outputs_b.get(key)
        if isinstance(value_a, (int, float)) and isinstance(value_b, (int, float)):
            delta[key] = float(value_b) - float(value_a)
    return delta


async def _resolve_versions_for_compare(
    session: AsyncSession,
    initiative_id: str,
    version_a_id: str | None,
    version_b_id: str | None,
) -> tuple[InitiativeVersion, InitiativeVersion]:
    if bool(version_a_id) != bool(version_b_id):
        raise ValidationError("Provide both version_a and version_b, or neither")

    if version_a_id and version_b_id:
        version_a = await initiative_repo.get_initiative_version(session, initiative_id, version_a_id)
        version_b = await initiative_repo.get_initiative_version(session, initiative_id, version_b_id)
        if not version_a or not version_b:
            raise NotFoundError("One or both initiative versions were not found")
        return version_a, version_b

    versions = await initiative_repo.list_initiative_versions(session, initiative_id)
    if len(versions) < 2:
        raise NotFoundError("At least two initiative versions are required for comparison")
    return versions[1], versions[0]


@router.post("", response_model=InitiativeRead)
async def create_initiative(
    body: InitiativeCreate,
    principal: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> InitiativeRead:
    team = await team_repo.get_team(session, body.owner_team_id)
    if not team:
        raise NotFoundError(f"Team `{body.owner_team_id}` not found")

    initiative = await initiative_repo.create_initiative(
        session,
        external_key=body.external_key,
        name=body.name,
        description=body.description,
        status=body.status,
        owner_team_id=body.owner_team_id,
        created_by_user_id=principal.user_id,
        created_by_email=principal.email,
        tags_json=body.tags,
    )
    if body.initial_version:
        await create_version_from_payload(
            session,
            initiative.id,
            body.initial_version,
            created_by_user_id=principal.user_id,
            created_by_email=principal.email,
        )

    await session.commit()
    await session.refresh(initiative)
    return await build_initiative_read(session, initiative)


@router.get("", response_model=InitiativeListResponse)
async def list_initiatives(
    owner_team_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    created_by_user_id: str | None = Query(default=None),
    q: str | None = Query(default=None),
    updated_from: datetime | None = Query(default=None),
    updated_to: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> InitiativeListResponse:
    initiatives = await initiative_repo.list_initiatives(
        session,
        owner_team_id=owner_team_id,
        status=status,
        created_by_user_id=created_by_user_id,
        query=q,
        updated_from=updated_from,
        updated_to=updated_to,
        limit=limit,
        offset=offset,
    )
    items = await build_initiative_reads(session, initiatives)
    return InitiativeListResponse(items=items)


@router.get("/{initiative_id}", response_model=InitiativeRead)
async def get_initiative(
    initiative_id: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> InitiativeRead:
    initiative = await initiative_repo.get_initiative(session, initiative_id)
    if not initiative:
        raise NotFoundError(f"Initiative `{initiative_id}` not found")
    return await build_initiative_read(session, initiative)


@router.patch("/{initiative_id}", response_model=InitiativeRead)
async def patch_initiative(
    initiative_id: str,
    body: InitiativeUpdate,
    principal: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> InitiativeRead:
    initiative = await initiative_repo.get_initiative(session, initiative_id)
    if not initiative:
        raise NotFoundError(f"Initiative `{initiative_id}` not found")

    if body.owner_team_id:
        team = await team_repo.get_team(session, body.owner_team_id)
        if not team:
            raise NotFoundError(f"Team `{body.owner_team_id}` not found")

    update_data = body.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(initiative, key, value)

    if body.status == "archived":
        initiative.archived_at = datetime.now(timezone.utc)
    elif body.status in {"draft", "active"}:
        initiative.archived_at = None

    if initiative.created_by_user_id is None and principal.user_id:
        initiative.created_by_user_id = principal.user_id
    if initiative.created_by_email is None and principal.email:
        initiative.created_by_email = principal.email

    await session.commit()
    await session.refresh(initiative)
    return await build_initiative_read(session, initiative)


@router.get("/{initiative_id}/versions", response_model=InitiativeVersionListResponse)
async def list_versions(
    initiative_id: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> InitiativeVersionListResponse:
    initiative = await initiative_repo.get_initiative(session, initiative_id)
    if not initiative:
        raise NotFoundError(f"Initiative `{initiative_id}` not found")

    versions = await initiative_repo.list_initiative_versions(session, initiative_id)
    return InitiativeVersionListResponse(items=[to_initiative_version_read(item) for item in versions])


@router.post("/{initiative_id}/versions", response_model=InitiativeVersionRead)
async def create_version(
    initiative_id: str,
    body: InitiativeVersionCreate,
    principal: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> InitiativeVersionRead:
    initiative = await initiative_repo.get_initiative(session, initiative_id)
    if not initiative:
        raise NotFoundError(f"Initiative `{initiative_id}` not found")

    version = await create_version_from_payload(
        session,
        initiative_id,
        body,
        created_by_user_id=principal.user_id,
        created_by_email=principal.email,
    )
    await session.commit()
    await session.refresh(version)
    return to_initiative_version_read(version)


@router.get("/{initiative_id}/versions/{version_id}", response_model=InitiativeVersionRead)
async def get_version(
    initiative_id: str,
    version_id: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> InitiativeVersionRead:
    version = await initiative_repo.get_initiative_version(session, initiative_id, version_id)
    if not version:
        raise NotFoundError(f"Initiative version `{version_id}` not found")
    return to_initiative_version_read(version)


@router.get("/{initiative_id}/runs", response_model=ScoringRunListResponse)
async def list_initiative_runs(
    initiative_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ScoringRunListResponse:
    initiative = await initiative_repo.get_initiative(session, initiative_id)
    if not initiative:
        raise NotFoundError(f"Initiative `{initiative_id}` not found")

    runs = await scoring_repo.list_runs_for_initiative(session, initiative_id, limit=limit, offset=offset)
    return ScoringRunListResponse(items=[_run_record(item) for item in runs])


@router.get("/{initiative_id}/compare", response_model=InitiativeCompareResponse)
async def compare_initiative_versions(
    initiative_id: str,
    version_a: str | None = Query(default=None),
    version_b: str | None = Query(default=None),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> InitiativeCompareResponse:
    initiative = await initiative_repo.get_initiative(session, initiative_id)
    if not initiative:
        raise NotFoundError(f"Initiative `{initiative_id}` not found")

    version_a_model, version_b_model = await _resolve_versions_for_compare(session, initiative_id, version_a, version_b)
    assumptions_diff = compare_versions(version_a_model, version_b_model)

    run_a = await scoring_repo.get_latest_run_for_initiative_version(session, version_a_model.id)
    run_b = await scoring_repo.get_latest_run_for_initiative_version(session, version_b_model.id)
    outputs_available = bool(run_a and run_b)

    outputs_a = run_a.deterministic_output_json if run_a else None
    outputs_b = run_b.deterministic_output_json if run_b else None
    outputs_delta = _outputs_delta(outputs_a, outputs_b) if outputs_a and outputs_b else None

    return InitiativeCompareResponse(
        initiative_id=initiative_id,
        version_a=version_a_model.id,
        version_b=version_b_model.id,
        assumptions_diff=assumptions_diff,
        outputs_available=outputs_available,
        outputs_a=outputs_a,
        outputs_b=outputs_b,
        outputs_delta=outputs_delta,
    )
