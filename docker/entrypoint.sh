#!/usr/bin/env bash
set -euo pipefail

runtime_database_url="${DATABASE_URL:-}"

if [[ "${RUN_MIGRATIONS_ON_STARTUP:-1}" == "1" ]]; then
  if [[ -n "${MIGRATION_DATABASE_URL:-}" ]]; then
    export DATABASE_URL="${MIGRATION_DATABASE_URL}"
  fi
  alembic upgrade head
fi

if [[ -n "${runtime_database_url}" ]]; then
  export DATABASE_URL="${runtime_database_url}"
fi

exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
