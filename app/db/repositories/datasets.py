import datetime
import uuid

from sqlalchemy import Select, delete, desc, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Dataset
from app.db.models.baseline_metric_row import BaselineMetricRow
from app.db.models.cannibalization_row import CannibalizationRow
from app.db.models.funnel_step_row import FunnelStepRow

TYPED_TABLE_MODELS = {
    "baseline_metrics": BaselineMetricRow,
    "baseline_funnel_steps": FunnelStepRow,
    "cannibalization_matrix": CannibalizationRow,
}

_DATE_COLUMNS = {"date_start", "date_end"}

_NATURAL_KEY_ORDER = {
    "baseline_metrics": ("segment_id", "date_start", "date_end"),
    "baseline_funnel_steps": ("segment_id", "screen", "step_id", "date_start", "date_end"),
    "cannibalization_matrix": ("from_screen", "to_screen", "segment_id"),
}

_DATA_COLUMNS = {
    "baseline_metrics": (
        "segment_id", "date_start", "date_end",
        "active_users", "ordering_users", "orders", "items", "rto", "fm",
    ),
    "baseline_funnel_steps": (
        "segment_id", "screen", "step_id", "step_name", "step_order",
        "date_start", "date_end", "entered_users", "advanced_users",
    ),
    "cannibalization_matrix": (
        "from_screen", "to_screen", "segment_id", "cannibalization_rate",
    ),
}


def _parse_date(val) -> datetime.date:
    if isinstance(val, datetime.date):
        return val
    return datetime.date.fromisoformat(str(val))


def _row_to_db(row: dict, dataset_id: str, schema_type: str) -> dict:
    out = {"id": str(uuid.uuid4()), "dataset_id": dataset_id}
    for col in _DATA_COLUMNS[schema_type]:
        val = row[col]
        if col in _DATE_COLUMNS:
            val = _parse_date(val)
        out[col] = val
    return out


def _row_from_db(row, schema_type: str) -> dict:
    out = {}
    for col in _DATA_COLUMNS[schema_type]:
        val = getattr(row, col)
        if col in _DATE_COLUMNS and isinstance(val, datetime.date):
            val = val.isoformat()
        out[col] = val
    return out


async def store_dataset_rows(
    session: AsyncSession,
    dataset_id: str,
    schema_type: str,
    rows: list[dict],
) -> int:
    if not rows:
        return 0
    model = TYPED_TABLE_MODELS[schema_type]
    db_rows = [_row_to_db(row, dataset_id, schema_type) for row in rows]
    await session.execute(insert(model), db_rows)
    return len(db_rows)


async def get_dataset_rows(
    session: AsyncSession,
    dataset_id: str,
    schema_type: str,
) -> list[dict]:
    model = TYPED_TABLE_MODELS[schema_type]
    order_cols = [getattr(model, c) for c in _NATURAL_KEY_ORDER[schema_type]]
    stmt = select(model).where(model.dataset_id == dataset_id).order_by(*order_cols)
    result = await session.scalars(stmt)
    return [_row_from_db(row, schema_type) for row in result]


async def get_dataset_rows_preview(
    session: AsyncSession,
    dataset_id: str,
    schema_type: str,
    limit: int,
) -> list[dict]:
    model = TYPED_TABLE_MODELS[schema_type]
    order_cols = [getattr(model, c) for c in _NATURAL_KEY_ORDER[schema_type]]
    stmt = select(model).where(model.dataset_id == dataset_id).order_by(*order_cols).limit(limit)
    result = await session.scalars(stmt)
    return [_row_from_db(row, schema_type) for row in result]


async def delete_dataset_rows(
    session: AsyncSession,
    dataset_id: str,
    schema_type: str,
) -> int:
    model = TYPED_TABLE_MODELS[schema_type]
    result = await session.execute(delete(model).where(model.dataset_id == dataset_id))
    return result.rowcount


# --- Dataset metadata queries (unchanged) ---


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
