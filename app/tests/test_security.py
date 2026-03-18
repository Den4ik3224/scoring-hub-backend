import json

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi.security import HTTPAuthorizationCredentials
from jwt.algorithms import ECAlgorithm

from app.core.errors import AuthError
from app.core.security import _reset_jwks_cache_for_tests, get_current_principal
from app.core.settings import Settings


def _make_es256_material(kid: str) -> tuple[ec.EllipticCurvePrivateKey, dict[str, str]]:
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key = private_key.public_key()
    jwk = json.loads(ECAlgorithm.to_jwk(public_key))
    jwk["kid"] = kid
    jwk["use"] = "sig"
    jwk["alg"] = "ES256"
    return private_key, jwk


@pytest.mark.asyncio
async def test_get_current_principal_supports_es256_with_jwks(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_jwks_cache_for_tests()

    private_key_1, jwk_1 = _make_es256_material("kid-1")
    private_key_2, jwk_2 = _make_es256_material("kid-2")
    settings = Settings(
        auth_mode="supabase_jwt",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_url="https://example.supabase.co",
        supabase_jwt_secret=None,
    )

    fetch_calls = {"count": 0}

    def _fake_fetch(_: str) -> dict[str, dict[str, str]]:
        fetch_calls["count"] += 1
        if fetch_calls["count"] == 1:
            return {"kid-1": jwk_1}
        return {"kid-1": jwk_1, "kid-2": jwk_2}

    monkeypatch.setattr("app.core.security._fetch_jwks", _fake_fetch)

    token_1 = jwt.encode(
        {"sub": "u1", "email": "u1@example.com", "role": "user", "iss": settings.expected_jwt_issuer},
        private_key_1,
        algorithm="ES256",
        headers={"kid": "kid-1"},
    )
    principal_1 = await get_current_principal(
        HTTPAuthorizationCredentials(scheme="Bearer", credentials=token_1),
        settings,
    )
    assert principal_1.user_id == "u1"
    assert principal_1.role == "user"

    token_2 = jwt.encode(
        {"sub": "a1", "email": "a1@example.com", "role": "admin", "iss": settings.expected_jwt_issuer},
        private_key_2,
        algorithm="ES256",
        headers={"kid": "kid-2"},
    )
    principal_2 = await get_current_principal(
        HTTPAuthorizationCredentials(scheme="Bearer", credentials=token_2),
        settings,
    )
    assert principal_2.user_id == "a1"
    assert principal_2.role == "admin"
    assert fetch_calls["count"] == 2


@pytest.mark.asyncio
async def test_get_current_principal_supports_hs256_fallback() -> None:
    settings = Settings(
        auth_mode="supabase_jwt",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_jwt_secret="secret",
    )
    token = jwt.encode({"sub": "u1", "role": "user"}, "secret", algorithm="HS256")

    principal = await get_current_principal(
        HTTPAuthorizationCredentials(scheme="Bearer", credentials=token),
        settings,
    )
    assert principal.user_id == "u1"
    assert principal.role == "user"


@pytest.mark.asyncio
async def test_get_current_principal_rejects_disallowed_algorithm() -> None:
    settings = Settings(
        auth_mode="supabase_jwt",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_jwt_secret="secret",
        jwt_allowed_algs="ES256,RS256",
    )
    token = jwt.encode({"sub": "u1", "role": "user"}, "secret", algorithm="HS256")

    with pytest.raises(AuthError, match="Invalid token"):
        await get_current_principal(
            HTTPAuthorizationCredentials(scheme="Bearer", credentials=token),
            settings,
        )


@pytest.mark.asyncio
async def test_role_resolution_prefers_app_role_claim() -> None:
    settings = Settings(
        auth_mode="supabase_jwt",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_jwt_secret="secret",
    )
    token = jwt.encode(
        {
            "sub": "u1",
            "role": "authenticated",
            "app_role": "admin",
            "app_metadata": {"role": "user"},
        },
        "secret",
        algorithm="HS256",
    )

    principal = await get_current_principal(
        HTTPAuthorizationCredentials(scheme="Bearer", credentials=token),
        settings,
    )
    assert principal.role == "admin"


@pytest.mark.asyncio
async def test_role_resolution_uses_app_metadata_for_authenticated_supabase_role() -> None:
    settings = Settings(
        auth_mode="supabase_jwt",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_jwt_secret="secret",
    )
    token = jwt.encode(
        {
            "sub": "u1",
            "role": "authenticated",
            "app_metadata": {"role": "admin"},
        },
        "secret",
        algorithm="HS256",
    )

    principal = await get_current_principal(
        HTTPAuthorizationCredentials(scheme="Bearer", credentials=token),
        settings,
    )
    assert principal.role == "admin"


@pytest.mark.asyncio
async def test_role_resolution_defaults_to_user_for_non_business_role() -> None:
    settings = Settings(
        auth_mode="supabase_jwt",
        database_url="sqlite+aiosqlite://",
        data_dir=None,
        supabase_jwt_secret="secret",
    )
    token = jwt.encode(
        {
            "sub": "u1",
            "role": "authenticated",
        },
        "secret",
        algorithm="HS256",
    )

    principal = await get_current_principal(
        HTTPAuthorizationCredentials(scheme="Bearer", credentials=token),
        settings,
    )
    assert principal.role == "user"
