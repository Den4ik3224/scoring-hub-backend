from sqlalchemy.pool import NullPool

from app.db.session import _engine_kwargs_for_database_url, _normalized_database_url


def test_engine_kwargs_for_supabase_pooler_postgres() -> None:
    kwargs = _engine_kwargs_for_database_url(
        "postgresql+asyncpg://postgres:secret@aws-1-eu-west-2.pooler.supabase.com:6543/postgres?ssl=require"
    )
    assert kwargs["pool_pre_ping"] is True
    assert kwargs["poolclass"] is NullPool
    assert kwargs["connect_args"]["statement_cache_size"] == 0
    assert kwargs["connect_args"]["prepared_statement_cache_size"] == 0


def test_engine_kwargs_for_sqlite() -> None:
    kwargs = _engine_kwargs_for_database_url("sqlite+aiosqlite://")
    assert kwargs == {"pool_pre_ping": True}


def test_normalized_database_url_sets_pooler_safe_query_params() -> None:
    normalized = str(
        _normalized_database_url(
        "postgresql+asyncpg://postgres:secret@aws-1-eu-west-2.pooler.supabase.com:6543/postgres?ssl=require"
        )
    )
    assert "prepared_statement_cache_size=0" in normalized
    assert "statement_cache_size=0" in normalized


def test_normalized_database_url_converts_sslmode_for_asyncpg() -> None:
    normalized = _normalized_database_url("postgresql+asyncpg://u:p@db.example.com:5432/postgres?sslmode=require")
    assert "ssl=require" in normalized
    assert "sslmode=require" not in normalized


def test_normalized_database_url_keeps_non_postgres_url() -> None:
    assert _normalized_database_url("sqlite+aiosqlite://") == "sqlite+aiosqlite://"
