from datetime import datetime

from sqlalchemy import Select, desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ABExperimentResult


async def create_ab_result(
    session: AsyncSession,
    *,
    experiment_id: str,
    scope: str,
    initiative_id: str | None,
    screen: str,
    segment_id: str | None,
    metric_driver: str,
    observed_uplift: float,
    ci_low: float | None,
    ci_high: float | None,
    sample_size: int,
    significance_flag: bool,
    quality_score: float,
    source: str,
    created_by: str,
    start_at: datetime,
    end_at: datetime,
) -> ABExperimentResult:
    record = ABExperimentResult(
        experiment_id=experiment_id,
        scope=scope,
        initiative_id=initiative_id,
        screen=screen,
        segment_id=segment_id,
        metric_driver=metric_driver,
        observed_uplift=observed_uplift,
        ci_low=ci_low,
        ci_high=ci_high,
        sample_size=sample_size,
        significance_flag=significance_flag,
        quality_score=quality_score,
        source=source,
        created_by=created_by,
        start_at=start_at,
        end_at=end_at,
    )
    session.add(record)
    await session.commit()
    await session.refresh(record)
    return record


async def get_ab_result(session: AsyncSession, result_id: str) -> ABExperimentResult | None:
    stmt: Select[tuple[ABExperimentResult]] = select(ABExperimentResult).where(ABExperimentResult.id == result_id)
    return await session.scalar(stmt)


async def list_ab_results(
    session: AsyncSession,
    *,
    scope: str | None = None,
    screens: list[str] | None = None,
    segment_id: str | None = None,
    metric_driver: str | None = None,
    from_dt: datetime | None = None,
    to_dt: datetime | None = None,
    min_quality: float | None = None,
    metric_drivers: list[str] | None = None,
    segment_ids: list[str] | None = None,
    min_sample_size: int | None = None,
    limit: int = 200,
    offset: int = 0,
) -> list[ABExperimentResult]:
    stmt: Select[tuple[ABExperimentResult]] = select(ABExperimentResult).order_by(desc(ABExperimentResult.end_at))

    if scope:
        stmt = stmt.where(ABExperimentResult.scope == scope)
    if screens:
        stmt = stmt.where(ABExperimentResult.screen.in_(screens))
    if segment_id:
        stmt = stmt.where(ABExperimentResult.segment_id == segment_id)
    if metric_driver:
        stmt = stmt.where(ABExperimentResult.metric_driver == metric_driver)
    if metric_drivers:
        stmt = stmt.where(ABExperimentResult.metric_driver.in_(metric_drivers))
    if segment_ids:
        stmt = stmt.where(or_(ABExperimentResult.segment_id.is_(None), ABExperimentResult.segment_id.in_(segment_ids)))
    if from_dt:
        stmt = stmt.where(ABExperimentResult.end_at >= from_dt)
    if to_dt:
        stmt = stmt.where(ABExperimentResult.end_at <= to_dt)
    if min_quality is not None:
        stmt = stmt.where(ABExperimentResult.quality_score >= min_quality)
    if min_sample_size is not None:
        stmt = stmt.where(ABExperimentResult.sample_size >= min_sample_size)

    stmt = stmt.limit(limit).offset(offset)
    rows = await session.scalars(stmt)
    return list(rows)


async def list_matching_evidence_for_scoring(
    session: AsyncSession,
    *,
    scope: str,
    screens: list[str],
    metric_drivers: list[str],
    segment_ids: list[str],
    min_quality: float,
    min_sample_size: int,
    lookback_from: datetime,
    limit: int = 2000,
) -> list[ABExperimentResult]:
    if not screens or not metric_drivers:
        return []

    if segment_ids:
        segment_clause = or_(
            ABExperimentResult.segment_id.is_(None),
            ABExperimentResult.segment_id.in_(segment_ids),
        )
    else:
        segment_clause = ABExperimentResult.segment_id.is_(None)

    stmt: Select[tuple[ABExperimentResult]] = (
        select(ABExperimentResult)
        .where(
            ABExperimentResult.scope == scope,
            ABExperimentResult.screen.in_(screens),
            ABExperimentResult.metric_driver.in_(metric_drivers),
            ABExperimentResult.quality_score >= min_quality,
            ABExperimentResult.sample_size >= min_sample_size,
            ABExperimentResult.end_at >= lookback_from,
            segment_clause,
        )
        .order_by(desc(ABExperimentResult.end_at), desc(ABExperimentResult.quality_score))
        .limit(limit)
    )
    rows = await session.scalars(stmt)
    return list(rows)
