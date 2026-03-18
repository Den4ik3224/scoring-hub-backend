import hashlib
import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from fastapi import UploadFile
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.db.models import Dataset
from app.db.repositories import datasets as dataset_repo
from app.services.validators import SCHEMAS, validate_dataset_table


def generate_dataset_version() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def read_dataset_from_bytes(data: bytes, fmt: str) -> pa.Table:
    buf = io.BytesIO(data)
    if fmt == "csv":
        return pa_csv.read_csv(buf)
    if fmt == "parquet":
        return pq.read_table(buf)
    raise ValidationError(f"Unsupported format `{fmt}`")


async def read_upload_to_bytes(upload_file: UploadFile, max_upload_mb: int) -> tuple[bytes, str]:
    max_bytes = max_upload_mb * 1024 * 1024
    hasher = hashlib.sha256()
    chunks: list[bytes] = []
    size = 0

    while True:
        chunk = await upload_file.read(1024 * 1024)
        if not chunk:
            break
        size += len(chunk)
        if size > max_bytes:
            raise ValidationError(f"File exceeds max upload size ({max_upload_mb}MB)")
        hasher.update(chunk)
        chunks.append(chunk)

    return b"".join(chunks), hasher.hexdigest()


def serialize_table(table: pa.Table, fmt: str) -> tuple[bytes, str]:
    buf = io.BytesIO()
    if fmt == "csv":
        pa_csv.write_csv(table, buf)
    elif fmt == "parquet":
        pq.write_table(table, buf)
    else:
        raise ValidationError(f"Unsupported format `{fmt}`")
    data = buf.getvalue()
    checksum = hashlib.sha256(data).hexdigest()
    return data, checksum


MONTHLY_MATERIALIZED_SCHEMA_TYPES = {"baseline_metrics", "baseline_funnel_steps"}
NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "baseline_metrics": ("segment_id", "date_start", "date_end"),
    "baseline_funnel_steps": ("segment_id", "screen", "step_id", "date_start", "date_end"),
}


def _merge_materialized_table(schema_type: str, previous: pa.Table | None, current: pa.Table) -> pa.Table:
    if previous is None or previous.num_rows == 0:
        return current

    key_columns = NATURAL_KEYS[schema_type]
    merged_rows: dict[tuple[object, ...], dict[str, object]] = {
        tuple(row[column] for column in key_columns): row
        for row in previous.to_pylist()
    }
    for row in current.to_pylist():
        merged_rows[tuple(row[column] for column in key_columns)] = row
    ordered_rows = sorted(merged_rows.values(), key=lambda row: tuple(row[column] for column in key_columns))
    return pa.Table.from_pylist(ordered_rows, schema=current.schema)


async def store_and_register_dataset(
    session: AsyncSession,
    *,
    upload_file: UploadFile,
    dataset_name: str,
    version: str,
    schema_type: str,
    fmt: str,
    uploaded_by: str,
    max_upload_mb: int,
    scope: str = "prod",
    column_mapping: dict[str, str] | None = None,
) -> Dataset:
    raw_bytes, _ = await read_upload_to_bytes(upload_file, max_upload_mb=max_upload_mb)
    source_table = read_dataset_from_bytes(raw_bytes, fmt)
    table, source_columns, resolved_mapping = apply_column_mapping(
        source_table,
        schema_type=schema_type,
        column_mapping=column_mapping,
    )

    latest_snapshot: Dataset | None = None
    if schema_type in MONTHLY_MATERIALIZED_SCHEMA_TYPES:
        latest_snapshot = await dataset_repo.get_latest_dataset_by_name(session, dataset_name=dataset_name, scope=scope)
        previous_table = None
        if latest_snapshot:
            previous_blob = await dataset_repo.get_dataset_blob(session, latest_snapshot.id)
            if previous_blob:
                previous_table = read_dataset_from_bytes(previous_blob, latest_snapshot.format)
        table = _merge_materialized_table(schema_type, previous_table, table)

    validate_dataset_table(schema_type=schema_type, table=table)
    final_bytes, checksum = serialize_table(table, fmt)

    row_count = table.num_rows
    columns = list(table.column_names)

    record = Dataset(
        dataset_name=dataset_name,
        version=version,
        scope=scope,
        schema_type=schema_type,
        format=fmt,
        file_path=None,
        checksum_sha256=checksum,
        row_count=row_count,
        columns_json={
            "columns": columns,
            "source_columns": source_columns,
            "column_mapping": resolved_mapping,
            "materialized_from_version": latest_snapshot.version if latest_snapshot else None,
        },
        schema_version="v1",
        uploaded_by=uploaded_by,
    )

    session.add(record)
    try:
        await session.flush()
        await dataset_repo.store_dataset_blob(session, record.id, final_bytes)
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(f"Dataset `{dataset_name}` version `{version}` already exists in scope `{scope}`") from exc

    await session.refresh(record)
    return record


def apply_column_mapping(
    table: pa.Table,
    *,
    schema_type: str,
    column_mapping: dict[str, str] | None,
) -> tuple[pa.Table, list[str], dict[str, str]]:
    source_columns = list(table.column_names)
    if not column_mapping:
        return table, source_columns, {}

    if not isinstance(column_mapping, dict):
        raise ValidationError("`column_mapping` must be a JSON object in canonical->source format")

    expected_columns = [spec.name for spec in SCHEMAS[schema_type]]
    unknown_canonical = sorted(key for key in column_mapping.keys() if key not in expected_columns)
    if unknown_canonical:
        raise ValidationError(
            f"`column_mapping` has unknown canonical keys: {unknown_canonical}. "
            f"Allowed: {expected_columns}"
        )

    for canonical, source in column_mapping.items():
        if not isinstance(source, str) or not source.strip():
            raise ValidationError(f"`column_mapping[{canonical}]` must be a non-empty string")

    used_sources: set[str] = set()
    resolved_mapping: dict[str, str] = {}
    columns_data: dict[str, pa.ChunkedArray] = {}
    missing_source: dict[str, str] = {}

    for canonical in expected_columns:
        source = column_mapping.get(canonical, canonical)
        if source in used_sources:
            raise ValidationError(
                f"`column_mapping` collision: source column `{source}` maps to multiple canonical columns"
            )
        if source not in source_columns:
            missing_source[canonical] = source
            continue
        used_sources.add(source)
        resolved_mapping[canonical] = source
        columns_data[canonical] = table.column(source)

    if missing_source:
        raise ValidationError(
            f"`column_mapping` is incomplete. Missing source columns for canonical keys: {missing_source}"
        )

    extra_source_columns = sorted(column for column in source_columns if column not in used_sources)
    if extra_source_columns:
        raise ValidationError(
            f"Source dataset has extra columns not covered by canonical schema after remap: {extra_source_columns}"
        )

    remapped = pa.table(columns_data)
    return remapped, source_columns, resolved_mapping


def table_preview_from_bytes(data: bytes, fmt: str, limit: int) -> list[dict]:
    table = read_dataset_from_bytes(data, fmt)
    return table.slice(0, limit).to_pylist()


async def fetch_dataset_or_404(
    session: AsyncSession,
    dataset_name: str,
    version: str,
    *,
    scope: str | None = None,
) -> Dataset:
    record = await dataset_repo.get_dataset_by_name_version(session, dataset_name, version, scope=scope)
    if not record:
        suffix = f" in scope `{scope}`" if scope else ""
        raise NotFoundError(f"Dataset `{dataset_name}` version `{version}` not found{suffix}")
    return record
