import csv
import hashlib
import io
import json
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import UploadFile
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.db.models import Dataset
from app.db.repositories import datasets as dataset_repo
from app.services.validators import ColumnKind, SCHEMAS, validate_dataset_table


def generate_dataset_version() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


CSV_SUPPORT_HINT = (
    "Supported CSV uploads: standard comma-delimited CSV, or semicolon-delimited CSV with decimal comma."
)
_CSV_DELIMITER_CANDIDATES = (",", ";")


def _decode_csv_text(data: bytes) -> str:
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"Unable to decode CSV file as UTF-8. {CSV_SUPPORT_HINT}") from exc


def _detect_csv_delimiter(text: str) -> str:
    non_empty_rows = [row for row in csv.reader(io.StringIO(text), delimiter=",") if any(cell.strip() for cell in row)]
    if not non_empty_rows:
        raise ValidationError("CSV file is empty")

    for delimiter in _CSV_DELIMITER_CANDIDATES:
        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        widths: list[int] = []
        for row in reader:
            if not row or not any(cell.strip() for cell in row):
                continue
            widths.append(len(row))
            if len(widths) >= 10:
                break
        if widths and widths[0] > 1 and all(width == widths[0] for width in widths):
            return delimiter

    raise ValidationError(
        "Could not recognize CSV delimiter. "
        f"{CSV_SUPPORT_HINT}"
    )


def _parse_csv_bytes(data: bytes) -> pa.Table:
    try:
        text = _decode_csv_text(data)
        delimiter = _detect_csv_delimiter(text)
        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        rows = [row for row in reader if any(cell.strip() for cell in row)]
        if not rows:
            raise ValidationError("CSV file is empty")

        header = [cell.lstrip("\ufeff").strip() for cell in rows[0]]
        if not header or any(not cell for cell in header):
            raise ValidationError("CSV header contains empty column names")
        if len(set(header)) != len(header):
            raise ValidationError("CSV header contains duplicate column names")

        data_rows = rows[1:]
        for index, row in enumerate(data_rows, start=2):
            if len(row) != len(header):
                raise ValidationError(
                    f"CSV row {index} has {len(row)} columns, expected {len(header)}. "
                    f"{CSV_SUPPORT_HINT}"
                )

        columns = {
            column: pa.array([row[pos].strip() for row in data_rows], type=pa.string())
            for pos, column in enumerate(header)
        }
        return pa.table(columns)
    except csv.Error as exc:
        raise ValidationError(f"CSV parse error. {CSV_SUPPORT_HINT}") from exc


def _parse_upload_bytes(data: bytes, fmt: str) -> pa.Table:
    buf = io.BytesIO(data)
    if fmt == "csv":
        return _parse_csv_bytes(data)
    if fmt == "parquet":
        return pq.read_table(buf)
    raise ValidationError(f"Unsupported format `{fmt}`")


def _normalize_text_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_date_value(value: object, column_name: str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = _normalize_text_value(value)
    if not text:
        raise ValidationError(f"Column `{column_name}` contains empty date values. {CSV_SUPPORT_HINT}")

    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        pass

    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        return datetime.fromisoformat(normalized).date().isoformat()
    except ValueError as exc:
        raise ValidationError(
            f"Column `{column_name}` contains invalid date value `{text}`. "
            "Use `YYYY-MM-DD` or an ISO timestamp. "
            f"{CSV_SUPPORT_HINT}"
        ) from exc


def _normalize_numeric_value(value: object, column_name: str) -> float:
    if isinstance(value, bool):
        raise ValidationError(f"Column `{column_name}` contains invalid numeric value `{value}`. {CSV_SUPPORT_HINT}")
    if isinstance(value, (int, float)):
        return float(value)

    text = _normalize_text_value(value)
    if not text:
        raise ValidationError(f"Column `{column_name}` contains empty numeric values. {CSV_SUPPORT_HINT}")

    compact = (
        text.replace(" ", "")
        .replace("\u00a0", "")
        .replace("\u202f", "")
    )
    if "," in compact and "." in compact:
        raise ValidationError(
            f"Column `{column_name}` contains ambiguous numeric value `{text}`. "
            "Use either dot decimals or decimal comma without thousands separators. "
            f"{CSV_SUPPORT_HINT}"
        )

    normalized = compact.replace(",", ".")
    try:
        return float(normalized)
    except ValueError as exc:
        raise ValidationError(
            f"Column `{column_name}` contains invalid numeric value `{text}`. "
            f"{CSV_SUPPORT_HINT}"
        ) from exc


def _normalize_integer_value(value: object, column_name: str) -> int:
    numeric_value = _normalize_numeric_value(value, column_name)
    if not numeric_value.is_integer():
        raise ValidationError(f"Column `{column_name}` must contain integer values")
    return int(numeric_value)


def _normalize_csv_table(table: pa.Table, *, schema_type: str) -> pa.Table:
    normalized_columns: dict[str, pa.Array] = {}
    for spec in SCHEMAS[schema_type]:
        raw_values = table.column(spec.name).to_pylist()
        if spec.name in {"date_start", "date_end"}:
            normalized_columns[spec.name] = pa.array(
                [_normalize_date_value(value, spec.name) for value in raw_values],
                type=pa.string(),
            )
        elif spec.kind == ColumnKind.string:
            normalized_columns[spec.name] = pa.array(
                [_normalize_text_value(value) for value in raw_values],
                type=pa.string(),
            )
        elif spec.kind == ColumnKind.integer:
            normalized_columns[spec.name] = pa.array(
                [_normalize_integer_value(value, spec.name) for value in raw_values],
                type=pa.int64(),
            )
        elif spec.kind == ColumnKind.float:
            normalized_columns[spec.name] = pa.array(
                [_normalize_numeric_value(value, spec.name) for value in raw_values],
                type=pa.float64(),
            )
        else:
            normalized_columns[spec.name] = pa.array(raw_values)
    return pa.table(normalized_columns)


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


MONTHLY_MATERIALIZED_SCHEMA_TYPES = {"baseline_metrics", "baseline_funnel_steps"}
NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "baseline_metrics": ("segment_id", "date_start", "date_end"),
    "baseline_funnel_steps": ("segment_id", "screen", "step_id", "date_start", "date_end"),
}


def _merge_rows(schema_type: str, previous_rows: list[dict], current_rows: list[dict]) -> list[dict]:
    if not previous_rows:
        return current_rows

    key_columns = NATURAL_KEYS[schema_type]
    merged: dict[tuple, dict] = {
        tuple(row[col] for col in key_columns): row
        for row in previous_rows
    }
    for row in current_rows:
        merged[tuple(row[col] for col in key_columns)] = row
    return sorted(merged.values(), key=lambda row: tuple(str(row[col]) for col in key_columns))


def compute_rows_checksum(rows: list[dict]) -> str:
    sorted_rows = sorted(rows, key=lambda r: tuple(str(r.get(k, "")) for k in sorted(r.keys())))
    content = json.dumps(sorted_rows, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()


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
    source_table = _parse_upload_bytes(raw_bytes, fmt)
    table, source_columns, resolved_mapping = apply_column_mapping(
        source_table,
        schema_type=schema_type,
        column_mapping=column_mapping,
    )
    if fmt == "csv":
        table = _normalize_csv_table(table, schema_type=schema_type)

    rows = table.to_pylist()
    latest_snapshot: Dataset | None = None

    if schema_type in MONTHLY_MATERIALIZED_SCHEMA_TYPES:
        latest_snapshot = await dataset_repo.get_latest_dataset_by_name(session, dataset_name=dataset_name, scope=scope)
        if latest_snapshot:
            previous_rows = await dataset_repo.get_dataset_rows(session, latest_snapshot.id, schema_type)
            rows = _merge_rows(schema_type, previous_rows, rows)
            table = pa.Table.from_pylist(rows, schema=table.schema)

    validate_dataset_table(schema_type=schema_type, table=table)
    checksum = compute_rows_checksum(rows)

    row_count = len(rows)
    columns = list(table.column_names)

    record = Dataset(
        dataset_name=dataset_name,
        version=version,
        scope=scope,
        schema_type=schema_type,
        format=fmt,
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
        await dataset_repo.store_dataset_rows(session, record.id, schema_type, rows)
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
