from datetime import datetime

from sqlalchemy import Select, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Initiative, InitiativeVersion, Team


UNASSIGNED_TEAM_SLUG = "unassigned"
UNASSIGNED_TEAM_NAME = "Unassigned"
UNASSIGNED_TEAM_DESCRIPTION = "System default team for ad hoc and legacy initiatives"


async def get_or_create_unassigned_team(session: AsyncSession) -> Team:
    stmt: Select[tuple[Team]] = select(Team).where(Team.slug == UNASSIGNED_TEAM_SLUG)
    team = await session.scalar(stmt)
    if team:
        return team

    team = Team(
        slug=UNASSIGNED_TEAM_SLUG,
        name=UNASSIGNED_TEAM_NAME,
        description=UNASSIGNED_TEAM_DESCRIPTION,
        is_active=True,
    )
    session.add(team)
    await session.flush()
    return team


async def get_initiative(session: AsyncSession, initiative_id: str) -> Initiative | None:
    stmt: Select[tuple[Initiative]] = select(Initiative).where(Initiative.id == initiative_id)
    return await session.scalar(stmt)


async def get_initiative_by_external_key(session: AsyncSession, external_key: str) -> Initiative | None:
    stmt: Select[tuple[Initiative]] = select(Initiative).where(Initiative.external_key == external_key)
    return await session.scalar(stmt)


async def list_initiatives(
    session: AsyncSession,
    *,
    owner_team_id: str | None,
    status: str | None,
    created_by_user_id: str | None,
    query: str | None,
    updated_from: datetime | None,
    updated_to: datetime | None,
    limit: int,
    offset: int,
) -> list[Initiative]:
    stmt: Select[tuple[Initiative]] = select(Initiative).order_by(desc(Initiative.updated_at), desc(Initiative.created_at))
    if owner_team_id:
        stmt = stmt.where(Initiative.owner_team_id == owner_team_id)
    if status:
        stmt = stmt.where(Initiative.status == status)
    if created_by_user_id:
        stmt = stmt.where(Initiative.created_by_user_id == created_by_user_id)
    if query:
        pattern = f"%{query}%"
        stmt = stmt.where(or_(Initiative.name.ilike(pattern), Initiative.description.ilike(pattern)))
    if updated_from:
        stmt = stmt.where(Initiative.updated_at >= updated_from)
    if updated_to:
        stmt = stmt.where(Initiative.updated_at <= updated_to)

    stmt = stmt.limit(limit).offset(offset)
    rows = await session.scalars(stmt)
    return list(rows)


async def create_initiative(
    session: AsyncSession,
    *,
    external_key: str | None,
    name: str,
    description: str | None,
    status: str,
    owner_team_id: str,
    created_by_user_id: str | None,
    created_by_email: str | None,
    tags_json: dict | None = None,
) -> Initiative:
    initiative = Initiative(
        external_key=external_key,
        name=name,
        description=description,
        status=status,
        owner_team_id=owner_team_id,
        created_by_user_id=created_by_user_id,
        created_by_email=created_by_email,
        tags_json=tags_json or {},
    )
    session.add(initiative)
    await session.flush()
    return initiative


async def create_or_get_initiative(
    session: AsyncSession,
    external_id: str | None,
    name: str,
    *,
    created_by_user_id: str | None = None,
    created_by_email: str | None = None,
) -> Initiative:
    if external_id:
        existing = await get_initiative_by_external_key(session, external_id)
        if existing:
            return existing

    unassigned = await get_or_create_unassigned_team(session)
    initiative = Initiative(
        external_key=external_id,
        name=name,
        status="draft",
        owner_team_id=unassigned.id,
        created_by_user_id=created_by_user_id,
        created_by_email=created_by_email,
        tags_json={},
    )
    session.add(initiative)
    await session.flush()
    return initiative


async def get_latest_initiative_version(session: AsyncSession, initiative_id: str) -> InitiativeVersion | None:
    stmt: Select[tuple[InitiativeVersion]] = (
        select(InitiativeVersion)
        .where(InitiativeVersion.initiative_id == initiative_id)
        .order_by(desc(InitiativeVersion.version_number), desc(InitiativeVersion.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def list_initiative_versions(session: AsyncSession, initiative_id: str) -> list[InitiativeVersion]:
    stmt: Select[tuple[InitiativeVersion]] = (
        select(InitiativeVersion)
        .where(InitiativeVersion.initiative_id == initiative_id)
        .order_by(desc(InitiativeVersion.version_number), desc(InitiativeVersion.created_at))
    )
    rows = await session.scalars(stmt)
    return list(rows)


async def get_initiative_version(session: AsyncSession, initiative_id: str, version_id: str) -> InitiativeVersion | None:
    stmt: Select[tuple[InitiativeVersion]] = select(InitiativeVersion).where(
        InitiativeVersion.initiative_id == initiative_id,
        InitiativeVersion.id == version_id,
    )
    return await session.scalar(stmt)


async def get_initiative_version_by_id(session: AsyncSession, version_id: str) -> InitiativeVersion | None:
    stmt: Select[tuple[InitiativeVersion]] = select(InitiativeVersion).where(InitiativeVersion.id == version_id)
    return await session.scalar(stmt)


async def create_initiative_version(
    session: AsyncSession,
    *,
    initiative_id: str,
    title_override: str | None,
    description_override: str | None,
    data_scope: str,
    screens_json: list,
    segments_json: list,
    metric_targets_json: list,
    assumptions_json: dict,
    p_success: float | None,
    confidence: float | None,
    evidence_type: str | None,
    effort_cost: float | None,
    strategic_weight: float | None,
    learning_value: float | None,
    horizon_weeks: int | None,
    decay_json: dict | None,
    discount_rate_annual: float | None,
    cannibalization_json: dict | None,
    interactions_json: list | None,
    created_by_user_id: str | None,
    created_by_email: str | None,
    change_comment: str | None,
) -> InitiativeVersion:
    await session.execute(
        select(Initiative.id).where(Initiative.id == initiative_id).with_for_update()
    )
    latest_number_stmt = select(func.max(InitiativeVersion.version_number)).where(
        InitiativeVersion.initiative_id == initiative_id
    )
    latest_number = await session.scalar(latest_number_stmt)
    next_version = int(latest_number or 0) + 1

    version = InitiativeVersion(
        initiative_id=initiative_id,
        version_number=next_version,
        title_override=title_override,
        description_override=description_override,
        data_scope=data_scope,
        screens_json=screens_json,
        segments_json=segments_json,
        metric_targets_json=metric_targets_json,
        assumptions_json=assumptions_json,
        p_success=p_success,
        confidence=confidence,
        evidence_type=evidence_type,
        effort_cost=effort_cost,
        strategic_weight=strategic_weight,
        learning_value=learning_value,
        horizon_weeks=horizon_weeks,
        decay_json=decay_json,
        discount_rate_annual=discount_rate_annual,
        cannibalization_json=cannibalization_json,
        interactions_json=interactions_json,
        created_by_user_id=created_by_user_id,
        created_by_email=created_by_email,
        change_comment=change_comment,
    )
    session.add(version)
    await session.flush()
    return version
