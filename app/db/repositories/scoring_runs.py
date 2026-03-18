from datetime import datetime

from sqlalchemy import Select, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Initiative, ScoringRun


async def list_scoring_runs(
    session: AsyncSession,
    *,
    initiative_id: str | None,
    owner_team_id: str | None,
    triggered_by_user_id: str | None,
    run_purpose: str | None,
    run_status: str | None,
    from_dt: datetime | None,
    to_dt: datetime | None,
    limit: int,
    offset: int,
) -> list[ScoringRun]:
    stmt: Select[tuple[ScoringRun]] = select(ScoringRun).order_by(desc(ScoringRun.created_at))

    if owner_team_id:
        stmt = stmt.join(Initiative, Initiative.id == ScoringRun.initiative_id).where(
            Initiative.owner_team_id == owner_team_id
        )
    if initiative_id:
        stmt = stmt.where(ScoringRun.initiative_id == initiative_id)
    if triggered_by_user_id:
        stmt = stmt.where(ScoringRun.triggered_by_user_id == triggered_by_user_id)
    if run_purpose:
        stmt = stmt.where(ScoringRun.run_purpose == run_purpose)
    if run_status:
        stmt = stmt.where(ScoringRun.run_status == run_status)
    if from_dt:
        stmt = stmt.where(ScoringRun.created_at >= from_dt)
    if to_dt:
        stmt = stmt.where(ScoringRun.created_at <= to_dt)

    stmt = stmt.limit(limit).offset(offset)
    rows = await session.scalars(stmt)
    return list(rows)


async def get_scoring_run(session: AsyncSession, run_id: str) -> ScoringRun | None:
    stmt: Select[tuple[ScoringRun]] = select(ScoringRun).where(ScoringRun.id == run_id)
    return await session.scalar(stmt)


async def list_runs_for_initiative(
    session: AsyncSession,
    initiative_id: str,
    *,
    limit: int,
    offset: int,
) -> list[ScoringRun]:
    stmt: Select[tuple[ScoringRun]] = (
        select(ScoringRun)
        .where(ScoringRun.initiative_id == initiative_id)
        .order_by(desc(ScoringRun.created_at))
        .limit(limit)
        .offset(offset)
    )
    rows = await session.scalars(stmt)
    return list(rows)


async def get_latest_run_for_initiative(session: AsyncSession, initiative_id: str) -> ScoringRun | None:
    stmt: Select[tuple[ScoringRun]] = (
        select(ScoringRun)
        .where(ScoringRun.initiative_id == initiative_id)
        .order_by(desc(ScoringRun.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_latest_run_for_initiative_version(session: AsyncSession, initiative_version_id: str) -> ScoringRun | None:
    stmt: Select[tuple[ScoringRun]] = (
        select(ScoringRun)
        .where(ScoringRun.initiative_version_id == initiative_version_id)
        .order_by(desc(ScoringRun.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_latest_runs_for_initiatives(
    session: AsyncSession,
    initiative_ids: list[str],
) -> dict[str, ScoringRun]:
    if not initiative_ids:
        return {}

    ranked = (
        select(
            ScoringRun.id.label("id"),
            ScoringRun.initiative_id.label("initiative_id"),
            func.row_number()
            .over(partition_by=ScoringRun.initiative_id, order_by=desc(ScoringRun.created_at))
            .label("rn"),
        )
        .where(ScoringRun.initiative_id.in_(initiative_ids))
        .subquery()
    )
    stmt: Select[tuple[ScoringRun]] = (
        select(ScoringRun)
        .join(ranked, ranked.c.id == ScoringRun.id)
        .where(ranked.c.rn == 1)
    )
    rows = await session.scalars(stmt)
    result: dict[str, ScoringRun] = {}
    for row in rows:
        if row.initiative_id:
            result[row.initiative_id] = row
    return result
