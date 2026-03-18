from sqlalchemy import Select, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Team


async def list_teams(session: AsyncSession, active_only: bool | None = None) -> list[Team]:
    stmt: Select[tuple[Team]] = select(Team).order_by(desc(Team.is_active), Team.name.asc())
    if active_only is True:
        stmt = stmt.where(Team.is_active.is_(True))
    if active_only is False:
        stmt = stmt.where(Team.is_active.is_(False))
    rows = await session.scalars(stmt)
    return list(rows)


async def get_team(session: AsyncSession, team_id: str) -> Team | None:
    stmt: Select[tuple[Team]] = select(Team).where(Team.id == team_id)
    return await session.scalar(stmt)


async def get_team_by_slug(session: AsyncSession, slug: str) -> Team | None:
    stmt: Select[tuple[Team]] = select(Team).where(Team.slug == slug)
    return await session.scalar(stmt)


async def create_team(
    session: AsyncSession,
    *,
    slug: str,
    name: str,
    description: str | None,
    is_active: bool,
) -> Team:
    team = Team(slug=slug, name=name, description=description, is_active=is_active)
    session.add(team)
    await session.flush()
    return team
