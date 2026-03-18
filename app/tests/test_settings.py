import pytest
from pydantic import ValidationError as PydanticValidationError

from app.core.settings import Settings


def test_settings_fail_closed_for_supabase_auth_without_secret() -> None:
    with pytest.raises(PydanticValidationError):
        Settings(
            auth_mode="supabase_jwt",
            database_url="sqlite+aiosqlite://",
            data_dir=None,
            supabase_jwt_secret=None,
        )


def test_settings_allow_supabase_auth_with_url_without_secret() -> None:
    settings = Settings(
        auth_mode="supabase_jwt",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_url="https://example.supabase.co",
        supabase_jwt_secret=None,
    )
    assert settings.auth_mode == "supabase_jwt"
    assert settings.effective_supabase_jwks_url == "https://example.supabase.co/auth/v1/.well-known/jwks.json"


def test_settings_allow_supabase_auth_with_explicit_jwks_without_secret() -> None:
    settings = Settings(
        auth_mode="supabase_jwt",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_jwks_url="https://example.supabase.co/auth/v1/.well-known/jwks.json",
        supabase_jwt_secret=None,
    )
    assert settings.auth_mode == "supabase_jwt"
    assert settings.effective_supabase_jwks_url == "https://example.supabase.co/auth/v1/.well-known/jwks.json"


def test_settings_allow_disabled_auth_without_secret() -> None:
    settings = Settings(
        auth_mode="disabled",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_jwt_secret=None,
    )
    assert settings.auth_mode == "disabled"
