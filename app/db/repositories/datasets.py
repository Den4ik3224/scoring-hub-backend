from sqlalchemy import Select, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Dataset


async def get_dataset_by_name_version(
    session: AsyncSession,
    dataset_name: str,
    version: str,
    *,
    scope: str | None = None,
) -> Dataset | None:
    stmt: Select[tuple[Dataset]] = select(Dataset).where(Dataset.dataset_name == dataset_name, Dataset.version == version)
    if scope:
        stmt = stmt.where(Dataset.scope == scope)
    return await session.scalar(stmt)


async def list_datasets(session: AsyncSession, *, scope: str | None = None) -> list[Dataset]:
    stmt: Select[tuple[Dataset]] = select(Dataset).order_by(Dataset.dataset_name.asc(), Dataset.created_at.desc())
    if scope:
        stmt = stmt.where(Dataset.scope == scope)
    rows = await session.scalars(stmt)
    return list(rows)


async def list_versions_for_dataset(
    session: AsyncSession,
    dataset_name: str,
    *,
    scope: str | None = None,
) -> list[Dataset]:
    stmt: Select[tuple[Dataset]] = (
        select(Dataset)
        .where(Dataset.dataset_name == dataset_name)
        .order_by(desc(Dataset.created_at), desc(Dataset.version))
    )
    if scope:
        stmt = stmt.where(Dataset.scope == scope)
    rows = await session.scalars(stmt)
    return list(rows)


async def get_latest_dataset_by_name(
    session: AsyncSession,
    dataset_name: str,
    *,
    scope: str = "prod",
) -> Dataset | None:
    stmt: Select[tuple[Dataset]] = (
        select(Dataset)
        .where(Dataset.dataset_name == dataset_name, Dataset.scope == scope)
        .order_by(desc(Dataset.created_at), desc(Dataset.version))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_latest_dataset_by_schema_type(
    session: AsyncSession,
    schema_type: str,
    *,
    scope: str = "prod",
) -> Dataset | None:
    stmt: Select[tuple[Dataset]] = (
        select(Dataset)
        .where(Dataset.schema_type == schema_type, Dataset.scope == scope)
        .order_by(desc(Dataset.created_at), desc(Dataset.version))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_dataset_by_schema_type_version(
    session: AsyncSession,
    schema_type: str,
    version: str,
    *,
    scope: str | None = None,
) -> Dataset | None:
    stmt: Select[tuple[Dataset]] = (
        select(Dataset)
        .where(Dataset.schema_type == schema_type, Dataset.version == version)
        .order_by(desc(Dataset.created_at))
        .limit(1)
    )
    if scope:
        stmt = stmt.where(Dataset.scope == scope)
    return await session.scalar(stmt)
