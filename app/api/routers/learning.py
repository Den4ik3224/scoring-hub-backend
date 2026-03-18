from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.learning import ABResultCreate, ABResultListResponse, ABResultRead
from app.core.errors import NotFoundError
from app.core.security import Principal, get_current_principal, require_admin
from app.db.models import ABExperimentResult
from app.db.repositories import ab_results as learning_repo
from app.db.session import get_session

router = APIRouter(prefix="/learning", tags=["learning"])


def _to_read(record: ABExperimentResult) -> ABResultRead:
    return ABResultRead(
        id=record.id,
        experiment_id=record.experiment_id,
        scope=record.scope,
        initiative_id=record.initiative_id,
        screen=record.screen,
        segment_id=record.segment_id,
        metric_driver=record.metric_driver,
        observed_uplift=record.observed_uplift,
        ci_low=record.ci_low,
        ci_high=record.ci_high,
        sample_size=record.sample_size,
        significance_flag=record.significance_flag,
        quality_score=record.quality_score,
        source=record.source,
        start_at=record.start_at,
        end_at=record.end_at,
        created_by=record.created_by,
        created_at=record.created_at,
    )


@router.post("/ab-results", response_model=ABResultRead, dependencies=[Depends(require_admin)])
async def create_ab_result(
    body: ABResultCreate,
    principal: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ABResultRead:
    record = await learning_repo.create_ab_result(
        session,
        experiment_id=body.experiment_id,
        scope=body.scope,
        initiative_id=body.initiative_id,
        screen=body.screen,
        segment_id=body.segment_id,
        metric_driver=body.metric_driver,
        observed_uplift=body.observed_uplift,
        ci_low=body.ci_low,
        ci_high=body.ci_high,
        sample_size=body.sample_size,
        significance_flag=body.significance_flag,
        quality_score=body.quality_score,
        source=body.source,
        created_by=principal.user_id,
        start_at=body.start_at,
        end_at=body.end_at,
    )
    return _to_read(record)


@router.get("/ab-results", response_model=ABResultListResponse)
async def list_ab_results(
    screen: str | None = Query(default=None),
    segment_id: str | None = Query(default=None),
    metric: str | None = Query(default=None),
    scope: str | None = Query(default=None, min_length=1, max_length=64),
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = Query(default=None),
    min_quality: float | None = Query(default=None, ge=0.0, le=1.0),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ABResultListResponse:
    records = await learning_repo.list_ab_results(
        session,
        scope=scope,
        screens=[screen] if screen else None,
        segment_id=segment_id,
        metric_driver=metric,
        from_dt=from_,
        to_dt=to,
        min_quality=min_quality,
        limit=limit,
        offset=offset,
    )
    return ABResultListResponse(items=[_to_read(item) for item in records])


@router.get("/ab-results/{result_id}", response_model=ABResultRead)
async def get_ab_result(
    result_id: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ABResultRead:
    record = await learning_repo.get_ab_result(session, result_id)
    if not record:
        raise NotFoundError(f"A/B result `{result_id}` not found")
    return _to_read(record)
