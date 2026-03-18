from collections.abc import AsyncGenerator
from typing import Any

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.settings import get_settings

settings = get_settings()


def _engine_kwargs_for_database_url(database_url: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"pool_pre_ping": True}
    if database_url.startswith("postgresql+asyncpg"):
        # Supabase pooler (PgBouncer transaction mode) is incompatible with prepared statement caching.
        kwargs["poolclass"] = NullPool
        kwargs["connect_args"] = {
            "statement_cache_size": 0,
            "prepared_statement_cache_size": 0,
        }
    return kwargs


def _normalized_database_url(database_url: str) -> str:
    if not database_url.startswith("postgresql+asyncpg"):
        return database_url
    url = make_url(database_url)
    query = dict(url.query)
    if "sslmode" in query and "ssl" not in query:
        query["ssl"] = query.pop("sslmode")
    query.setdefault("prepared_statement_cache_size", "0")
    query.setdefault("statement_cache_size", "0")
    return url.set(query=query).render_as_string(hide_password=False)


engine = create_async_engine(
    _normalized_database_url(settings.database_url),
    **_engine_kwargs_for_database_url(settings.database_url),
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
