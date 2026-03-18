from fastapi import APIRouter, Depends, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.team import TeamCreate, TeamListResponse, TeamRead, TeamUpdate
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import Principal, get_current_principal, require_admin
from app.db.repositories import teams as team_repo
from app.db.session import get_session
from app.services.initiative_service import to_team_read

router = APIRouter(prefix="/teams", tags=["teams"])


@router.get("", response_model=TeamListResponse)
async def list_teams(
    active_only: bool | None = Query(default=None),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> TeamListResponse:
    items = await team_repo.list_teams(session, active_only=active_only)
    return TeamListResponse(items=[to_team_read(item) for item in items])


@router.post("", response_model=TeamRead)
async def create_team(
    body: TeamCreate,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> TeamRead:
    try:
        model = await team_repo.create_team(
            session,
            slug=body.slug,
            name=body.name,
            description=body.description,
            is_active=body.is_active,
        )
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(f"Team slug `{body.slug}` already exists") from exc

    await session.refresh(model)
    return to_team_read(model)


@router.get("/{team_id}", response_model=TeamRead)
async def get_team(
    team_id: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> TeamRead:
    model = await team_repo.get_team(session, team_id)
    if not model:
        raise NotFoundError(f"Team `{team_id}` not found")
    return to_team_read(model)


@router.patch("/{team_id}", response_model=TeamRead)
async def patch_team(
    team_id: str,
    body: TeamUpdate,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> TeamRead:
    model = await team_repo.get_team(session, team_id)
    if not model:
        raise NotFoundError(f"Team `{team_id}` not found")

    update_data = body.model_dump(exclude_unset=True)
    if not update_data:
        raise ValidationError("At least one field must be provided")

    for key, value in update_data.items():
        setattr(model, key, value)

    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError("Unable to update team due to unique constraint") from exc

    await session.refresh(model)
    return to_team_read(model)
