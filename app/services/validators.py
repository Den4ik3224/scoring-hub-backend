from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from enum import Enum

import pyarrow as pa
import pyarrow.compute as pc

from app.core.errors import ValidationError


class ColumnKind(str, Enum):
    string = "string"
    float = "float"
    integer = "integer"
    boolean = "boolean"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    kind: ColumnKind
    min_value: float | None = None
    max_value: float | None = None
    allow_null: bool = False


SCHEMAS: dict[str, list[ColumnSpec]] = {
    "baseline_metrics": [
        ColumnSpec("segment_id", ColumnKind.string),
        ColumnSpec("date_start", ColumnKind.string),
        ColumnSpec("date_end", ColumnKind.string),
        ColumnSpec("active_users", ColumnKind.float, min_value=0.0),
        ColumnSpec("ordering_users", ColumnKind.float, min_value=0.0),
        ColumnSpec("orders", ColumnKind.float, min_value=0.0),
        ColumnSpec("items", ColumnKind.float, min_value=0.0),
        ColumnSpec("rto", ColumnKind.float, min_value=0.0),
        ColumnSpec("fm", ColumnKind.float, min_value=0.0),
    ],
    "baseline_funnel_steps": [
        ColumnSpec("segment_id", ColumnKind.string),
        ColumnSpec("screen", ColumnKind.string),
        ColumnSpec("step_id", ColumnKind.string),
        ColumnSpec("step_name", ColumnKind.string),
        ColumnSpec("step_order", ColumnKind.integer, min_value=1.0),
        ColumnSpec("date_start", ColumnKind.string),
        ColumnSpec("date_end", ColumnKind.string),
        ColumnSpec("entered_users", ColumnKind.float, min_value=0.0),
        ColumnSpec("advanced_users", ColumnKind.float, min_value=0.0),
    ],
    "cannibalization_matrix": [
        ColumnSpec("from_screen", ColumnKind.string),
        ColumnSpec("to_screen", ColumnKind.string),
        ColumnSpec("segment_id", ColumnKind.string),
        ColumnSpec("cannibalization_rate", ColumnKind.float, min_value=0.0, max_value=1.0),
    ],
}

ALLOWED_DIST_TYPES = {"normal", "lognormal", "triangular", "point"}


def _assert_column_kind(name: str, arr: pa.Array, expected: ColumnKind) -> None:
    dtype = arr.type
    if expected == ColumnKind.string:
        if (
            not pa.types.is_string(dtype)
            and not pa.types.is_large_string(dtype)
            and not (name in {"date_start", "date_end"} and (pa.types.is_date(dtype) or pa.types.is_timestamp(dtype)))
        ):
            raise ValidationError(f"Column `{name}` must be string")
        return

    if expected == ColumnKind.boolean:
        if not pa.types.is_boolean(dtype):
            raise ValidationError(f"Column `{name}` must be boolean")
        return

    if expected == ColumnKind.integer:
        if not pa.types.is_integer(dtype):
            raise ValidationError(f"Column `{name}` must be integer")
        return

    if expected == ColumnKind.float:
        if not (pa.types.is_floating(dtype) or pa.types.is_integer(dtype)):
            raise ValidationError(f"Column `{name}` must be numeric")
        return


def _assert_no_nulls(name: str, arr: pa.Array) -> None:
    if arr.null_count > 0:
        raise ValidationError(f"Column `{name}` contains nulls")


def _assert_bounds(spec: ColumnSpec, arr: pa.Array) -> None:
    if spec.min_value is None and spec.max_value is None:
        return

    numeric_arr = pc.cast(arr, pa.float64())

    if spec.min_value is not None:
        min_value = pc.min(numeric_arr).as_py()
        if min_value is not None and min_value < spec.min_value:
            raise ValidationError(f"Column `{spec.name}` has values below {spec.min_value}")

    if spec.max_value is not None:
        max_value = pc.max(numeric_arr).as_py()
        if max_value is not None and max_value > spec.max_value:
            raise ValidationError(f"Column `{spec.name}` has values above {spec.max_value}")


def _assert_unique_rows(table: pa.Table, columns: tuple[str, ...], label: str) -> None:
    seen: set[tuple] = set()
    for row in table.select(columns).to_pylist():
        key = tuple(row[col] for col in columns)
        if key in seen:
            raise ValidationError(f"`{label}` contains duplicate key {key}")
        seen.add(key)


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValidationError(f"Column `{field_name}` must contain ISO dates YYYY-MM-DD") from exc


def _assert_full_calendar_month(start_value: str, end_value: str, *, label: str) -> None:
    start = _parse_date(start_value, "date_start")
    end = _parse_date(end_value, "date_end")
    if start.day != 1:
        raise ValidationError(f"`{label}` row date_start must be the first day of a calendar month")
    month_end = monthrange(start.year, start.month)[1]
    if start.year != end.year or start.month != end.month or end.day != month_end:
        raise ValidationError(f"`{label}` rows must cover exactly one full calendar month")


def _assert_monthly_ranges(table: pa.Table, *, label: str) -> None:
    for row in table.select(["date_start", "date_end"]).to_pylist():
        _assert_full_calendar_month(row["date_start"], row["date_end"], label=label)


def _assert_baseline_metrics_semantics(table: pa.Table) -> None:
    _assert_unique_rows(table, ("segment_id", "date_start", "date_end"), "baseline_metrics")
    _assert_monthly_ranges(table, label="baseline_metrics")
    for row in table.to_pylist():
        active_users = float(row["active_users"])
        ordering_users = float(row["ordering_users"])
        orders = float(row["orders"])
        items = float(row["items"])
        rto = float(row["rto"])
        fm = float(row["fm"])
        if ordering_users > active_users:
            raise ValidationError("`baseline_metrics` ordering_users must be <= active_users")
        if orders > 0 and ordering_users <= 0:
            raise ValidationError("`baseline_metrics` orders > 0 requires ordering_users > 0")
        if items > 0 and orders <= 0:
            raise ValidationError("`baseline_metrics` items > 0 requires orders > 0")
        if rto > 0 and items <= 0:
            raise ValidationError("`baseline_metrics` rto > 0 requires items > 0")
        if rto > 0 and fm > rto + 1e-9:
            raise ValidationError("`baseline_metrics` fm must be <= rto")


def _assert_baseline_funnel_semantics(table: pa.Table) -> None:
    _assert_unique_rows(
        table,
        ("segment_id", "screen", "step_id", "date_start", "date_end"),
        "baseline_funnel_steps",
    )
    _assert_monthly_ranges(table, label="baseline_funnel_steps")
    for row in table.to_pylist():
        entered = float(row["entered_users"])
        advanced = float(row["advanced_users"])
        if advanced > entered:
            raise ValidationError("`baseline_funnel_steps` advanced_users must be <= entered_users")


def validate_dataset_table(schema_type: str, table: pa.Table) -> None:
    if schema_type not in SCHEMAS:
        raise ValidationError(f"Unsupported schema_type `{schema_type}`")

    expected = SCHEMAS[schema_type]
    expected_names = [col.name for col in expected]
    actual_names = list(table.column_names)

    if actual_names != expected_names:
        missing = [col for col in expected_names if col not in actual_names]
        extra = [col for col in actual_names if col not in expected_names]
        raise ValidationError(
            "Invalid columns. "
            f"Expected exact columns {expected_names}; missing={missing}; extra={extra}"
        )

    for spec in expected:
        arr = table.column(spec.name).combine_chunks()
        if not spec.allow_null:
            _assert_no_nulls(spec.name, arr)
        _assert_column_kind(spec.name, arr, spec.kind)
        _assert_bounds(spec, arr)

    if schema_type == "baseline_metrics":
        _assert_baseline_metrics_semantics(table)
    elif schema_type == "baseline_funnel_steps":
        _assert_baseline_funnel_semantics(table)
