from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from typing import Any

import pyarrow as pa

from app.core.errors import ValidationError


BASELINE_WINDOW_MONTHS = {
    "month": 1,
    "quarter": 3,
    "half_year": 6,
    "year": 12,
}


@dataclass(frozen=True)
class ResolvedBaselineWindow:
    start_date: date
    end_date: date
    month_starts: tuple[date, ...]
    anchor_month: date


@dataclass(frozen=True)
class SegmentMonthlyBaseline:
    segment_id: str
    mau: float
    conversion: float
    frequency_monthly: float
    frequency_weekly: float
    aoq: float
    aiv: float
    aov: float
    fm_pct: float
    base_ordering_users: float
    base_orders: float
    base_items: float
    base_rto: float
    base_fm: float


@dataclass(frozen=True)
class FunnelStepAggregate:
    segment_id: str
    screen: str
    step_id: str
    step_name: str
    step_order: int
    entered_users: float
    advanced_users: float
    baseline_rate: float


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _month_end(value: date) -> date:
    return value.replace(day=monthrange(value.year, value.month)[1])


def _add_months(value: date, delta: int) -> date:
    year = value.year + (value.month - 1 + delta) // 12
    month = (value.month - 1 + delta) % 12 + 1
    return date(year, month, 1)


def _month_sequence(start: date, end: date) -> list[date]:
    months: list[date] = []
    current = _month_start(start)
    while current <= end:
        months.append(current)
        current = _add_months(current, 1)
    return months


def _parse_month_row(row: dict[str, Any], *, label: str) -> date:
    start = date.fromisoformat(str(row["date_start"]))
    end = date.fromisoformat(str(row["date_end"]))
    if start.day != 1 or end != _month_end(start):
        raise ValidationError(f"`{label}` contains non-monthly rows; expected full calendar month coverage")
    return start


def resolve_baseline_window(
    table: pa.Table,
    *,
    baseline_window: str,
    baseline_date_start: date | None,
    baseline_date_end: date | None,
) -> ResolvedBaselineWindow:
    month_starts = sorted({_parse_month_row(row, label="baseline") for row in table.to_pylist()})
    if not month_starts:
        raise ValidationError("baseline dataset is empty")

    if baseline_date_start or baseline_date_end:
        assert baseline_date_start is not None and baseline_date_end is not None
        start = _month_start(baseline_date_start)
        if baseline_date_start != start or baseline_date_end != _month_end(_month_start(baseline_date_end)):
            raise ValidationError("baseline_date_start/date_end must align to full calendar months")
        end = _month_start(baseline_date_end)
    else:
        if baseline_window not in BASELINE_WINDOW_MONTHS:
            raise ValidationError(
                f"Unsupported baseline_window `{baseline_window}`. Allowed: {sorted(BASELINE_WINDOW_MONTHS)}"
            )
        end = max(month_starts)
        start = _add_months(end, -(BASELINE_WINDOW_MONTHS[baseline_window] - 1))

    expected_months = tuple(_month_sequence(start, end))
    missing_months = [month.isoformat() for month in expected_months if month not in month_starts]
    if missing_months:
        raise ValidationError(
            "Baseline period is incomplete. Missing monthly facts for months: " + ", ".join(missing_months)
        )

    return ResolvedBaselineWindow(
        start_date=start,
        end_date=_month_end(end),
        month_starts=expected_months,
        anchor_month=end,
    )


def filter_table_to_window(table: pa.Table, window: ResolvedBaselineWindow) -> pa.Table:
    keep = {month.isoformat() for month in window.month_starts}
    rows = [row for row in table.to_pylist() if str(row["date_start"]) in keep]
    return pa.Table.from_pylist(rows, schema=table.schema)


def aggregate_segment_baselines(
    table: pa.Table,
    *,
    segment_ids: list[str],
    window: ResolvedBaselineWindow,
) -> dict[str, SegmentMonthlyBaseline]:
    by_segment: dict[str, list[dict[str, Any]]] = {segment_id: [] for segment_id in segment_ids}
    expected_months = {month.isoformat() for month in window.month_starts}
    for row in table.to_pylist():
        segment_id = str(row["segment_id"])
        if segment_id not in by_segment:
            continue
        if str(row["date_start"]) not in expected_months:
            continue
        by_segment[segment_id].append(row)

    result: dict[str, SegmentMonthlyBaseline] = {}
    for segment_id in segment_ids:
        rows = by_segment.get(segment_id, [])
        months_present = {str(row["date_start"]) for row in rows}
        missing = sorted(expected_months - months_present)
        if missing:
            raise ValidationError(
                f"baseline_metrics is missing months for segment `{segment_id}`: {missing}"
            )

        active_users = [float(row["active_users"]) for row in rows]
        ordering_users = sum(float(row["ordering_users"]) for row in rows)
        orders = sum(float(row["orders"]) for row in rows)
        items = sum(float(row["items"]) for row in rows)
        rto = sum(float(row["rto"]) for row in rows)
        fm = sum(float(row["fm"]) for row in rows)
        active_sum = sum(active_users)
        mau = active_sum / len(active_users)
        conversion = ordering_users / active_sum if active_sum > 0 else 0.0
        frequency_monthly = orders / ordering_users if ordering_users > 0 else 0.0
        frequency_weekly = frequency_monthly * 12.0 / 52.0
        aoq = items / orders if orders > 0 else 0.0
        aiv = rto / items if items > 0 else 0.0
        aov = rto / orders if orders > 0 else 0.0
        fm_pct = fm / rto if rto > 0 else 0.0
        result[segment_id] = SegmentMonthlyBaseline(
            segment_id=segment_id,
            mau=mau,
            conversion=conversion,
            frequency_monthly=frequency_monthly,
            frequency_weekly=frequency_weekly,
            aoq=aoq,
            aiv=aiv,
            aov=aov,
            fm_pct=fm_pct,
            base_ordering_users=ordering_users,
            base_orders=orders,
            base_items=items,
            base_rto=rto,
            base_fm=fm,
        )
    return result


def aggregate_funnel_steps(
    table: pa.Table | None,
    *,
    segment_ids: list[str],
    screens: list[str],
    window: ResolvedBaselineWindow,
) -> dict[tuple[str, str], list[FunnelStepAggregate]]:
    if table is None:
        return {}
    expected_months = {month.isoformat() for month in window.month_starts}
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in table.to_pylist():
        segment_id = str(row["segment_id"])
        screen = str(row["screen"])
        if segment_id not in segment_ids or screen not in screens:
            continue
        if str(row["date_start"]) not in expected_months:
            continue
        key = (segment_id, screen, str(row["step_id"]))
        grouped.setdefault(key, []).append(row)

    aggregated: dict[tuple[str, str], list[FunnelStepAggregate]] = {}
    for (segment_id, screen, step_id), rows in grouped.items():
        months_present = {str(row["date_start"]) for row in rows}
        missing = sorted(expected_months - months_present)
        if missing:
            raise ValidationError(
                f"baseline_funnel_steps is missing months for segment `{segment_id}` screen `{screen}` step `{step_id}`: {missing}"
            )
        entered = sum(float(row["entered_users"]) for row in rows)
        advanced = sum(float(row["advanced_users"]) for row in rows)
        step = FunnelStepAggregate(
            segment_id=segment_id,
            screen=screen,
            step_id=step_id,
            step_name=str(rows[0]["step_name"]),
            step_order=int(rows[0]["step_order"]),
            entered_users=entered,
            advanced_users=advanced,
            baseline_rate=(advanced / entered if entered > 0 else 0.0),
        )
        aggregated.setdefault((segment_id, screen), []).append(step)

    for key, rows in aggregated.items():
        rows.sort(key=lambda item: item.step_order)
    return aggregated


def screen_exposure_shares(funnel_index: dict[tuple[str, str], list[FunnelStepAggregate]], *, segment_id: str, screens: list[str]) -> dict[str, float]:
    exposures: dict[str, float] = {}
    for screen in screens:
        rows = funnel_index.get((segment_id, screen), [])
        if rows:
            exposures[screen] = max(0.0, rows[0].entered_users)
    total = sum(exposures.values())
    if total <= 0:
        if not screens:
            return {}
        equal = 1.0 / len(screens)
        return {screen: equal for screen in screens}
    return {screen: value / total for screen, value in exposures.items()}
