from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "backlog-scoring-service"
    environment: str = "dev"
    auth_mode: Literal["disabled", "supabase_jwt"] = "disabled"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/scoring"
    migration_database_url: str | None = None
    data_dir: Path | None = None

    max_upload_mb: int = Field(default=100, ge=1, le=4096)
    preview_max_limit: int = Field(default=200, ge=1, le=1000)
    mc_default_n: int = Field(default=10_000, ge=100, le=50_000)
    mc_max_n: int = Field(default=50_000, ge=1_000, le=500_000)

    supabase_url: str | None = None
    supabase_jwks_url: str | None = None
    jwt_allowed_algs: str = "ES256,RS256,HS256"
    supabase_jwt_secret: str | None = None
    cors_allow_origins: str = "*"
    log_level: str = "INFO"
    git_sha: str | None = None
    image_tag: str | None = None

    @model_validator(mode="after")
    def validate_auth_mode(self) -> "Settings":
        if self.auth_mode == "supabase_jwt":
            if not self.jwt_allowed_algorithms:
                raise ValueError("JWT_ALLOWED_ALGS must include at least one algorithm")
            if not (self.supabase_jwt_secret or self.effective_supabase_jwks_url):
                raise ValueError(
                    "AUTH_MODE=supabase_jwt requires one key source: SUPABASE_JWT_SECRET or SUPABASE_URL/SUPABASE_JWKS_URL"
                )
        return self

    @property
    def code_version(self) -> str:
        if self.git_sha:
            return self.git_sha
        if self.image_tag:
            return self.image_tag
        return "unknown"

    @property
    def cors_origins(self) -> list[str]:
        if self.cors_allow_origins.strip() == "*":
            return ["*"]
        return [origin.strip() for origin in self.cors_allow_origins.split(",") if origin.strip()]

    @property
    def jwt_allowed_algorithms(self) -> set[str]:
        return {alg.strip().upper() for alg in self.jwt_allowed_algs.split(",") if alg.strip()}

    @property
    def effective_supabase_jwks_url(self) -> str | None:
        if self.supabase_jwks_url:
            return self.supabase_jwks_url.rstrip("/")
        if self.supabase_url:
            return f"{self.supabase_url.rstrip('/')}/auth/v1/.well-known/jwks.json"
        return None

    @property
    def expected_jwt_issuer(self) -> str | None:
        if self.supabase_url:
            return f"{self.supabase_url.rstrip('/')}/auth/v1"
        return None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
