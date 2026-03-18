from dataclasses import dataclass
import json
import threading
import time
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import jwt
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.errors import AuthError, PermissionError
from app.core.logging import get_logger
from app.core.settings import Settings, get_settings

bearer_scheme = HTTPBearer(auto_error=False)
logger = get_logger("app.security")
_JWKS_TTL_SECONDS = 600
_JWKS_LOCK = threading.Lock()
_JWKS_BY_KID: dict[str, dict[str, Any]] = {}
_JWKS_EXPIRES_AT = 0.0


@dataclass
class Principal:
    user_id: str
    role: str
    email: str | None
    claims: dict[str, Any]
    auth_disabled: bool = False

    @property
    def sub(self) -> str:
        return self.user_id


def _reset_jwks_cache_for_tests() -> None:
    global _JWKS_EXPIRES_AT
    with _JWKS_LOCK:
        _JWKS_BY_KID.clear()
        _JWKS_EXPIRES_AT = 0.0


def _fetch_jwks(jwks_url: str) -> dict[str, dict[str, Any]]:
    request = Request(jwks_url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))

    keys = payload.get("keys")
    if not isinstance(keys, list):
        raise ValueError("JWKS payload does not contain keys[]")

    keys_by_kid: dict[str, dict[str, Any]] = {}
    for key in keys:
        if isinstance(key, dict) and key.get("kid"):
            keys_by_kid[str(key["kid"])] = key

    if not keys_by_kid:
        raise ValueError("JWKS payload does not contain usable signing keys")
    return keys_by_kid


def _refresh_jwks(settings: Settings, force: bool = False) -> None:
    global _JWKS_EXPIRES_AT
    jwks_url = settings.effective_supabase_jwks_url
    if not jwks_url:
        raise AuthError("Invalid token")

    now = time.monotonic()
    with _JWKS_LOCK:
        if not force and _JWKS_BY_KID and now < _JWKS_EXPIRES_AT:
            return

    try:
        fresh_keys = _fetch_jwks(jwks_url)
    except (URLError, ValueError, OSError, json.JSONDecodeError) as exc:
        logger.warning("auth.jwks_fetch_failed", reason=str(exc), jwks_url=jwks_url)
        raise AuthError("Invalid token") from exc

    with _JWKS_LOCK:
        _JWKS_BY_KID.clear()
        _JWKS_BY_KID.update(fresh_keys)
        _JWKS_EXPIRES_AT = now + _JWKS_TTL_SECONDS


def _resolve_jwk(settings: Settings, kid: str | None) -> dict[str, Any]:
    if not kid:
        logger.warning("auth.jwt_missing_kid")
        raise AuthError("Invalid token")

    now = time.monotonic()
    with _JWKS_LOCK:
        if _JWKS_BY_KID and now < _JWKS_EXPIRES_AT and kid in _JWKS_BY_KID:
            return _JWKS_BY_KID[kid]

    # Force refresh on cache miss or expiry.
    _refresh_jwks(settings, force=True)
    with _JWKS_LOCK:
        jwk = _JWKS_BY_KID.get(kid)
    if not jwk:
        logger.warning("auth.jwt_kid_not_found", kid=kid, jwks_url=settings.effective_supabase_jwks_url)
        raise AuthError("Invalid token")
    return jwk


def _decode_asymmetric_token(token: str, alg: str, kid: str | None, settings: Settings) -> dict[str, Any]:
    jwk = _resolve_jwk(settings, kid)
    try:
        signing_key = jwt.PyJWK.from_dict(jwk).key
        decode_kwargs: dict[str, Any] = {
            "key": signing_key,
            "algorithms": [alg],
            "options": {"verify_aud": False},
        }
        if settings.expected_jwt_issuer:
            decode_kwargs["issuer"] = settings.expected_jwt_issuer
        return jwt.decode(token, **decode_kwargs)
    except Exception as exc:
        logger.warning(
            "auth.jwt_invalid",
            reason=str(exc),
            alg=alg,
            kid=kid,
            issuer=settings.expected_jwt_issuer,
        )
        raise AuthError("Invalid token") from exc


def _decode_hs256_token(token: str, settings: Settings, kid: str | None) -> dict[str, Any]:
    if not settings.supabase_jwt_secret:
        logger.warning("auth.jwt_hs256_secret_missing", alg="HS256", kid=kid)
        raise AuthError("Invalid token")

    try:
        decode_kwargs: dict[str, Any] = {
            "key": settings.supabase_jwt_secret,
            "algorithms": ["HS256"],
            "options": {"verify_aud": False},
        }
        if settings.expected_jwt_issuer:
            decode_kwargs["issuer"] = settings.expected_jwt_issuer
        return jwt.decode(token, **decode_kwargs)
    except jwt.InvalidTokenError as exc:
        logger.warning("auth.jwt_invalid", reason=str(exc), alg="HS256", kid=kid, issuer=settings.expected_jwt_issuer)
        raise AuthError("Invalid token") from exc


def _decode_token(token: str, settings: Settings) -> dict[str, Any]:
    try:
        header = jwt.get_unverified_header(token)
    except jwt.InvalidTokenError as exc:
        logger.warning("auth.jwt_header_invalid", reason=str(exc))
        raise AuthError("Invalid token") from exc

    alg = str(header.get("alg", "")).upper()
    kid = str(header["kid"]) if header.get("kid") else None
    if alg not in settings.jwt_allowed_algorithms:
        logger.warning("auth.jwt_alg_not_allowed", alg=alg, kid=kid)
        raise AuthError("Invalid token")

    if alg in {"ES256", "RS256"}:
        return _decode_asymmetric_token(token, alg, kid, settings)
    if alg == "HS256":
        return _decode_hs256_token(token, settings, kid)

    logger.warning("auth.jwt_alg_unsupported", alg=alg, kid=kid)
    raise AuthError("Invalid token")


def _resolve_principal_role(claims: dict[str, Any]) -> str:
    def _as_business_role(value: Any) -> str | None:
        role = str(value or "").strip().lower()
        return role if role in {"admin", "user"} else None

    app_role_claim = _as_business_role(claims.get("app_role"))
    if app_role_claim:
        return app_role_claim

    app_metadata = claims.get("app_metadata")
    if isinstance(app_metadata, dict):
        metadata_role = _as_business_role(app_metadata.get("role"))
        if metadata_role:
            return metadata_role

    legacy_role = _as_business_role(claims.get("role"))
    if legacy_role:
        return legacy_role

    return "user"


async def get_current_principal(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    settings: Settings = Depends(get_settings),
) -> Principal:
    if settings.auth_mode == "disabled":
        return Principal(user_id="anonymous", role="admin", email=None, claims={}, auth_disabled=True)

    if settings.auth_mode != "supabase_jwt":
        raise AuthError("Unsupported auth mode")

    if credentials is None:
        raise AuthError()

    claims = _decode_token(credentials.credentials, settings)

    return Principal(
        user_id=str(claims.get("sub") or claims.get("user_id") or "unknown"),
        role=_resolve_principal_role(claims),
        email=str(claims.get("email")) if claims.get("email") else None,
        claims=claims,
        auth_disabled=False,
    )


async def require_admin(principal: Principal = Depends(get_current_principal)) -> Principal:
    if principal.role != "admin":
        raise PermissionError()
    return principal
