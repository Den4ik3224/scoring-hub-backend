from __future__ import annotations

from datetime import date

import pyarrow as pa

from app.db.models import Dataset
from app.services.monthly_baselines import (
    aggregate_funnel_steps,
    aggregate_segment_baselines,
    filter_table_to_window,
    resolve_baseline_window,
)
from app.services.version_resolver import ResolvedScoringInputs


def monthly_baseline_table(
    *,
    segment_id: str = "s1",
    active_users: float = 1000.0,
    ordering_users: float = 100.0,
    orders: float = 200.0,
    items: float = 400.0,
    rto: float = 40000.0,
    fm: float = 12000.0,
    months: tuple[str, ...] = ("2025-01-01", "2025-02-01", "2025-03-01"),
) -> pa.Table:
    rows = []
    for month in months:
        start = date.fromisoformat(month)
        end = (
            date(start.year, start.month + 1, 1) if start.month < 12 else date(start.year + 1, 1, 1)
        ) - date.resolution
        rows.append(
            {
                "segment_id": segment_id,
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "active_users": active_users,
                "ordering_users": ordering_users,
                "orders": orders,
                "items": items,
                "rto": rto,
                "fm": fm,
            }
        )
    return pa.Table.from_pylist(rows)


def monthly_funnel_table(
    *,
    segment_id: str = "s1",
    screen: str = "home",
    steps: tuple[tuple[str, str, int, float, float], ...] = (
        ("home_to_catalog", "Home to catalog", 1, 500.0, 250.0),
        ("catalog_to_cart", "Catalog to cart", 2, 250.0, 100.0),
    ),
    months: tuple[str, ...] = ("2025-01-01", "2025-02-01", "2025-03-01"),
) -> pa.Table:
    rows = []
    for month in months:
        start = date.fromisoformat(month)
        end = (
            date(start.year, start.month + 1, 1) if start.month < 12 else date(start.year + 1, 1, 1)
        ) - date.resolution
        for step_id, step_name, step_order, entered_users, advanced_users in steps:
            rows.append(
                {
                    "segment_id": segment_id,
                    "screen": screen,
                    "step_id": step_id,
                    "step_name": step_name,
                    "step_order": step_order,
                    "date_start": start.isoformat(),
                    "date_end": end.isoformat(),
                    "entered_users": entered_users,
                    "advanced_users": advanced_users,
                }
            )
    return pa.Table.from_pylist(rows)


def monthly_baseline_csv(
    *,
    segment_id: str = "s1",
    active_users: float = 1000.0,
    ordering_users: float = 100.0,
    orders: float = 200.0,
    items: float = 400.0,
    rto: float = 40000.0,
    fm: float = 12000.0,
) -> str:
    header = "segment_id,date_start,date_end,active_users,ordering_users,orders,items,rto,fm\n"
    rows = [
        f"{segment_id},2025-01-01,2025-01-31,{active_users},{ordering_users},{orders},{items},{rto},{fm}",
        f"{segment_id},2025-02-01,2025-02-28,{active_users},{ordering_users},{orders},{items},{rto},{fm}",
        f"{segment_id},2025-03-01,2025-03-31,{active_users},{ordering_users},{orders},{items},{rto},{fm}",
    ]
    return header + "\n".join(rows) + "\n"


def monthly_funnel_csv(
    *,
    segment_id: str = "s1",
    screen: str = "home",
    steps: tuple[tuple[str, str, int, float, float], ...] = (
        ("home_to_catalog", "Home to catalog", 1, 500.0, 250.0),
        ("catalog_to_cart", "Catalog to cart", 2, 250.0, 100.0),
    ),
    months: tuple[str, ...] = ("2025-01-01", "2025-02-01", "2025-03-01"),
) -> str:
    header = "segment_id,screen,step_id,step_name,step_order,date_start,date_end,entered_users,advanced_users\n"
    rows: list[str] = []
    for month in months:
        start = date.fromisoformat(month)
        end = (
            date(start.year, start.month + 1, 1) if start.month < 12 else date(start.year + 1, 1, 1)
        ) - date.resolution
        for step_id, step_name, step_order, entered_users, advanced_users in steps:
            rows.append(
                f"{segment_id},{screen},{step_id},{step_name},{step_order},{start.isoformat()},{end.isoformat()},{entered_users},{advanced_users}"
            )
    return header + "\n".join(rows) + "\n"


def dataset_stub(*, schema_type: str = "baseline_metrics", version: str = "v1") -> Dataset:
    return Dataset(
        dataset_name=schema_type,
        version=version,
        schema_type=schema_type,
        format="csv",
        file_path=f"/tmp/{schema_type}.csv",
        checksum_sha256="x" * 64,
        row_count=1,
        columns_json={"columns": []},
        schema_version="v1",
        uploaded_by="tester",
        scope="prod",
    )


def resolved_inputs_stub(
    *,
    baseline: pa.Table,
    funnel: pa.Table | None = None,
    cannibalization: pa.Table | None = None,
    scoring_policy_snapshot: dict | None = None,
    learning_config: dict | None = None,
    learning_evidence: list[dict] | None = None,
    data_scope: str = "prod",
) -> ResolvedScoringInputs:
    window = resolve_baseline_window(baseline, baseline_window="quarter", baseline_date_start=None, baseline_date_end=None)
    baseline_window_table = filter_table_to_window(baseline, window)
    funnel_table = filter_table_to_window(funnel, window) if funnel is not None else None
    return ResolvedScoringInputs(
        baseline_dataset=dataset_stub(schema_type="baseline_metrics"),
        baseline_table=baseline_window_table,
        baseline_window=window,
        segment_baselines=aggregate_segment_baselines(
            baseline_window_table,
            segment_ids=sorted({str(row["segment_id"]) for row in baseline_window_table.to_pylist()}),
            window=window,
        ),
        funnel_dataset=dataset_stub(schema_type="baseline_funnel_steps") if funnel is not None else None,
        funnel_table=funnel_table,
        funnel_index=aggregate_funnel_steps(
            funnel_table,
            segment_ids=sorted({str(row["segment_id"]) for row in baseline_window_table.to_pylist()}),
            screens=sorted({str(row["screen"]) for row in funnel_table.to_pylist()}) if funnel_table is not None else [],
            window=window,
        )
        if funnel_table is not None
        else {},
        cannibalization_dataset=dataset_stub(schema_type="cannibalization_matrix") if cannibalization is not None else None,
        cannibalization_table=cannibalization,
        evidence_priors_source="config:evidence_priors:default",
        evidence_priors={
            "ab_test": {"default_confidence": 0.8, "default_uplift_sd": 0.1, "default_dist_type": "normal"}
        },
        metric_tree_source=None,
        metric_tree_definition=None,
        scoring_policy_source="builtin:ev_policy_vnext_learning:1",
        scoring_policy_snapshot=scoring_policy_snapshot
        or {
            "primitive_metrics": ["mau", "penetration", "conversion", "frequency", "frequency_monthly", "aoq", "aiv", "fm_pct"],
            "derived_metrics": ["orders", "items", "aov", "rto", "fm"],
            "translator_enabled": True,
            "translations": {"aov": {"to": ["aoq", "aiv"], "weights": {"aoq": 0.5, "aiv": 0.5}}},
            "default_horizons": [4, 13, 26, 52],
            "learning_defaults": {
                "mode": "bayesian",
                "lookback_days": 730,
                "half_life_days": 180,
                "min_quality": 0.6,
                "min_sample_size": 500,
            },
        },
        learning_config=learning_config or {},
        learning_evidence=learning_evidence or [],
        data_scope=data_scope,
        resolved_versions={
            "baseline_metrics": "v1",
            **({"baseline_funnel_steps": "v1"} if funnel is not None else {}),
            **({"cannibalization_matrix": "v1"} if cannibalization is not None else {}),
        },
        resolved_inputs_json={
            "data_scope": data_scope,
            "datasets": {
                "baseline_metrics": {"version": "v1", "scope": data_scope},
                **({"baseline_funnel_steps": {"version": "v1", "scope": data_scope}} if funnel is not None else {}),
                **({"cannibalization_matrix": {"version": "v1", "scope": data_scope}} if cannibalization is not None else {}),
            },
            "baseline_window": {
                "name": "quarter",
                "date_start": window.start_date.isoformat(),
                "date_end": window.end_date.isoformat(),
                "anchor_month": window.anchor_month.isoformat(),
            },
        },
    )
