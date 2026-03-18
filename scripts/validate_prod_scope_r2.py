#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
import math
import os
import random
from calendar import monthrange
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx


API_BASE_URL = os.environ.get("API_BASE_URL", "https://backlog-scoring-service-eh3tzmv36q-ew.a.run.app")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kogeqjbjwjcsfyaqoprt.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")
VALIDATION_EMAIL = os.environ.get("VALIDATION_EMAIL")
VALIDATION_PASSWORD = os.environ.get("VALIDATION_PASSWORD")
VALIDATION_SCOPE = os.environ.get("VALIDATION_SCOPE", "x5_validation_20260312_r2")
METRIC_TREE_NAME = "x5_retail_test_tree"
METRIC_TREE_VERSION = "v3"
SCORING_POLICY_NAME = "ev_policy_vnext_learning"
SCORING_POLICY_VERSION = "1"
REPORT_MD = Path(os.environ.get("VALIDATION_REPORT_MD", f"/tmp/{VALIDATION_SCOPE}_report.md"))
REPORT_JSON = Path(os.environ.get("VALIDATION_REPORT_JSON", f"/tmp/{VALIDATION_SCOPE}_report.json"))


SEGMENTS = [
    ("veteran_3plus_plu_6m", "Старички 3+ PLU за полгода"),
    ("dormant_new_users_12m_no_orders", "Новички без заказов за год"),
    ("active_orange_card", "Активная апельсиновая карта"),
    ("loyalty_club_member", "Член клуба лояльности"),
    ("paket_subscriber", "Подписка Пакет"),
]

SCREENS = [
    "home",
    "catalog_listing",
    "search",
    "cart",
    "checkout",
    "loyalty_clubs",
    "notification_center",
    "postorder",
]

PRIMARY_TEAM_SLUG = "search"


def require_env(value: str | None, name: str) -> str:
    if value:
        return value
    raise SystemExit(f"Missing required env: {name}")


def csv_text(columns: list[str], rows: list[dict[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def month_end(value: date) -> date:
    return value.replace(day=monthrange(value.year, value.month)[1])


def month_sequence(start: date, months: int) -> list[date]:
    items: list[date] = []
    year = start.year
    month = start.month
    for _ in range(months):
        items.append(date(year, month, 1))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return items


def as_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def approx_equal(left: float, right: float, *, tolerance: float = 1e-6) -> bool:
    if left == right:
        return True
    denom = max(1.0, abs(left), abs(right))
    return abs(left - right) / denom <= tolerance


def get_nested(data: dict[str, Any], path: str, default: Any = None) -> Any:
    current: Any = data
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    return current


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str


@dataclass
class RunRecord:
    label: str
    run_id: str
    initiative_id: str | None = None
    initiative_version_id: str | None = None
    mode: str = "ad_hoc"
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationContext:
    client: httpx.Client
    scope: str
    dataset_versions: dict[str, str] = field(default_factory=dict)
    evidence_ids: list[str] = field(default_factory=list)
    initiative_ids: list[str] = field(default_factory=list)
    initiative_version_ids: list[str] = field(default_factory=list)
    runs: list[RunRecord] = field(default_factory=list)
    checks: list[CheckResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    team_id: str | None = None

    def check(self, name: str, passed: bool, details: str) -> None:
        self.checks.append(CheckResult(name=name, passed=passed, details=details))


def login_supabase() -> str:
    anon = require_env(SUPABASE_ANON_KEY, "SUPABASE_ANON_KEY")
    email = require_env(VALIDATION_EMAIL, "VALIDATION_EMAIL")
    password = require_env(VALIDATION_PASSWORD, "VALIDATION_PASSWORD")
    response = httpx.post(
        f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
        headers={"apikey": anon, "Content-Type": "application/json"},
        json={"email": email, "password": password},
        timeout=30,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise SystemExit("Supabase login did not return access_token")
    return token


def api_client(token: str) -> httpx.Client:
    return httpx.Client(
        base_url=API_BASE_URL,
        timeout=180,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )


def request_json(client: httpx.Client, method: str, path: str, **kwargs: Any) -> Any:
    response = client.request(method, path, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"{method} {path} failed: {response.status_code} {response.text}")
    return response.json() if response.content else None


def request_expect_status(client: httpx.Client, method: str, path: str, expected_status: int, **kwargs: Any) -> httpx.Response:
    response = client.request(method, path, **kwargs)
    if response.status_code != expected_status:
        raise RuntimeError(
            f"{method} {path} expected {expected_status}, got {response.status_code}: {response.text}"
        )
    return response


def upload_dataset(
    client: httpx.Client,
    *,
    dataset_name: str,
    version: str,
    schema_type: str,
    content: str,
    scope: str,
) -> dict[str, Any]:
    response = httpx.post(
        f"{API_BASE_URL}/datasets/upload",
        params={
            "dataset_name": dataset_name,
            "dataset_version": version,
            "format": "csv",
            "schema_type": schema_type,
            "scope": scope,
        },
        files={"file": (f"{dataset_name}.csv", content, "text/csv")},
        headers={"Authorization": client.headers["Authorization"]},
        timeout=180,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Upload failed for {dataset_name}: {response.status_code} {response.text}")
    return response.json()["dataset"]


def preflight(client: httpx.Client, scope: str) -> None:
    datasets = request_json(client, "GET", "/datasets", params={"scope": scope}).get("items", [])
    if datasets:
        raise SystemExit(f"Scope `{scope}` already has datasets; aborting validation.")
    learning = request_json(client, "GET", "/learning/ab-results", params={"scope": scope, "limit": 5}).get("items", [])
    if learning:
        raise SystemExit(f"Scope `{scope}` already has learning evidence; aborting validation.")


def ensure_team(client: httpx.Client, slug: str) -> str:
    rows = request_json(client, "GET", "/teams").get("items", [])
    existing = next((row for row in rows if row["slug"] == slug), None)
    if existing:
        return existing["id"]
    created = request_json(
        client,
        "POST",
        "/teams",
        json={"slug": slug, "name": slug.title(), "description": "Validation team", "is_active": True},
    )
    return created["id"]


def build_monthly_baseline_rows() -> list[dict[str, Any]]:
    months = month_sequence(month_start(2024, 1), 12)
    params = {
        "veteran_3plus_plu_6m": {"active": 420_000, "conv": 0.165, "freq": 1.95, "aoq": 3.2, "aiv": 188.0, "fm_pct": 0.245},
        "dormant_new_users_12m_no_orders": {"active": 160_000, "conv": 0.055, "freq": 1.05, "aoq": 2.15, "aiv": 143.0, "fm_pct": 0.185},
        "active_orange_card": {"active": 290_000, "conv": 0.115, "freq": 1.68, "aoq": 2.85, "aiv": 171.0, "fm_pct": 0.225},
        "loyalty_club_member": {"active": 250_000, "conv": 0.108, "freq": 1.62, "aoq": 3.05, "aiv": 179.0, "fm_pct": 0.255},
        "paket_subscriber": {"active": 210_000, "conv": 0.098, "freq": 1.88, "aoq": 2.74, "aiv": 167.0, "fm_pct": 0.214},
    }
    rows: list[dict[str, Any]] = []
    for segment_id, cfg in params.items():
        for idx, start in enumerate(months):
            trend = 0.92 + idx * 0.015
            seasonal = 0.98 + (0.02 if start.month in {11, 12} else -0.01 if start.month in {1, 2} else 0.0)
            active_users = int(cfg["active"] * trend * seasonal)
            conversion = cfg["conv"] * (0.97 + idx * 0.003)
            ordering_users = max(1, int(active_users * conversion))
            frequency = cfg["freq"] * (0.98 + idx * 0.004)
            orders = max(1, int(ordering_users * frequency))
            aoq = cfg["aoq"] * (0.985 + idx * 0.002)
            items = max(orders, int(orders * aoq))
            aiv = cfg["aiv"] * (0.99 + idx * 0.0025)
            rto = round(items * aiv, 2)
            fm_pct = min(0.45, max(0.08, cfg["fm_pct"] * (0.995 + idx * 0.001)))
            fm = round(rto * fm_pct, 2)
            rows.append(
                {
                    "segment_id": segment_id,
                    "date_start": start.isoformat(),
                    "date_end": month_end(start).isoformat(),
                    "active_users": active_users,
                    "ordering_users": ordering_users,
                    "orders": orders,
                    "items": items,
                    "rto": rto,
                    "fm": fm,
                }
            )
    return rows


def build_funnel_rows() -> list[dict[str, Any]]:
    months = month_sequence(month_start(2024, 1), 12)
    segment_scales = {
        "veteran_3plus_plu_6m": 1.0,
        "dormant_new_users_12m_no_orders": 0.48,
        "active_orange_card": 0.72,
        "loyalty_club_member": 0.63,
        "paket_subscriber": 0.55,
    }
    screen_defs: dict[str, tuple[tuple[str, str, int, float, float], ...]] = {
        "home": (("home_to_catalog", "Home to catalog", 1, 0.62, 0.34),),
        "catalog_listing": (("catalog_to_cart", "Catalog to cart", 1, 0.46, 0.18),),
        "search": (("search_to_cart", "Search to cart", 1, 0.28, 0.22),),
        "cart": (("cart_to_checkout", "Cart to checkout", 1, 0.12, 0.59),),
        "checkout": (("checkout_to_order", "Checkout to order", 1, 0.1, 0.74),),
        "loyalty_clubs": (("clubs_to_action", "Clubs to action", 1, 0.08, 0.14),),
        "notification_center": (("notification_to_action", "Notification to action", 1, 0.11, 0.1),),
        "postorder": (("postorder_to_action", "Postorder to action", 1, 0.09, 0.12),),
    }
    base_active = {
        "veteran_3plus_plu_6m": 420_000,
        "dormant_new_users_12m_no_orders": 160_000,
        "active_orange_card": 290_000,
        "loyalty_club_member": 250_000,
        "paket_subscriber": 210_000,
    }
    rows: list[dict[str, Any]] = []
    for segment_id, scale in segment_scales.items():
        for idx, start in enumerate(months):
            month_users = base_active[segment_id] * (0.92 + idx * 0.015)
            for screen, defs in screen_defs.items():
                for step_id, step_name, step_order, exposure_share, rate in defs:
                    entered = max(1, int(month_users * exposure_share * scale))
                    advanced = max(0, min(entered, int(entered * rate * (0.99 + idx * 0.0015))))
                    rows.append(
                        {
                            "segment_id": segment_id,
                            "screen": screen,
                            "step_id": step_id,
                            "step_name": step_name,
                            "step_order": step_order,
                            "date_start": start.isoformat(),
                            "date_end": month_end(start).isoformat(),
                            "entered_users": entered,
                            "advanced_users": advanced,
                        }
                    )
    return rows


def build_cannibalization_rows() -> list[dict[str, Any]]:
    pairs = [
        ("search", "catalog_listing", 0.12),
        ("catalog_listing", "cart", 0.08),
        ("cart", "checkout", 0.05),
        ("notification_center", "postorder", 0.04),
    ]
    rows: list[dict[str, Any]] = []
    for segment_id, _name in SEGMENTS:
        for from_screen, to_screen, rate in pairs:
            rows.append(
                {
                    "from_screen": from_screen,
                    "to_screen": to_screen,
                    "segment_id": segment_id,
                    "cannibalization_rate": rate,
                }
            )
    return rows


def seed_datasets(ctx: ValidationContext) -> None:
    datasets = {
        "validation_baseline_metrics": (
            "bm_validation_r2",
            "baseline_metrics",
            csv_text(
                ["segment_id", "date_start", "date_end", "active_users", "ordering_users", "orders", "items", "rto", "fm"],
                build_monthly_baseline_rows(),
            ),
        ),
        "validation_baseline_funnel_steps": (
            "bfs_validation_r2",
            "baseline_funnel_steps",
            csv_text(
                ["segment_id", "screen", "step_id", "step_name", "step_order", "date_start", "date_end", "entered_users", "advanced_users"],
                build_funnel_rows(),
            ),
        ),
        "validation_cannibalization_matrix": (
            "can_validation_r2",
            "cannibalization_matrix",
            csv_text(
                ["from_screen", "to_screen", "segment_id", "cannibalization_rate"],
                build_cannibalization_rows(),
            ),
        ),
    }
    for dataset_name, (version, schema_type, content) in datasets.items():
        uploaded = upload_dataset(
            ctx.client,
            dataset_name=dataset_name,
            version=version,
            schema_type=schema_type,
            content=content,
            scope=ctx.scope,
        )
        ctx.dataset_versions[dataset_name] = uploaded["version"]


def input_versions(ctx: ValidationContext) -> dict[str, str]:
    return {
        "baseline_metrics": ctx.dataset_versions["validation_baseline_metrics"],
        "baseline_funnel_steps": ctx.dataset_versions["validation_baseline_funnel_steps"],
        "cannibalization_matrix": ctx.dataset_versions["validation_cannibalization_matrix"],
    }


def seed_ab_results(ctx: ValidationContext) -> None:
    now = datetime.now(UTC)
    rows = [
        ("exact-aoq-club", "loyalty_clubs", "loyalty_club_member", "aoq", 0.065, 0.03, 0.10, 8200, True, 0.91, 35),
        ("fallback-aoq-club", "loyalty_clubs", None, "aoq", 0.04, 0.01, 0.08, 12000, True, 0.8, 120),
        ("exact-home-funnel", "home", "veteran_3plus_plu_6m", "home_to_catalog", 0.045, 0.02, 0.07, 15000, True, 0.9, 44),
        ("fallback-home-funnel", "home", None, "home_to_catalog", 0.03, 0.005, 0.06, 17000, True, 0.76, 220),
        ("exact-search-conv", "search", "active_orange_card", "conversion", 0.055, 0.025, 0.08, 13500, True, 0.88, 28),
        ("fallback-search-conv", "search", None, "conversion", 0.032, 0.01, 0.05, 18000, False, 0.72, 260),
        ("exact-checkout-funnel", "checkout", "active_orange_card", "checkout_to_order", 0.042, 0.018, 0.066, 9100, True, 0.9, 31),
        ("fallback-checkout-funnel", "checkout", None, "checkout_to_order", 0.028, 0.008, 0.05, 14000, True, 0.77, 190),
        ("exact-notification-freq", "notification_center", "paket_subscriber", "frequency_monthly", 0.06, 0.025, 0.09, 7600, True, 0.86, 25),
        ("fallback-notification-freq", "notification_center", None, "frequency_monthly", 0.035, 0.012, 0.055, 11000, False, 0.68, 320),
        ("exact-postorder-aiv", "postorder", "paket_subscriber", "aiv", 0.04, 0.015, 0.07, 8200, True, 0.83, 63),
        ("fallback-postorder-fm", "postorder", None, "fm_pct", 0.025, 0.01, 0.04, 9500, True, 0.79, 150),
    ]
    for idx, row in enumerate(rows, start=1):
        experiment_id, screen, segment_id, driver, uplift, ci_low, ci_high, sample_size, significant, quality, age_days = row
        end_at = now - timedelta(days=age_days)
        start_at = end_at - timedelta(days=21)
        created = request_json(
            ctx.client,
            "POST",
            "/learning/ab-results",
            json={
                "experiment_id": f"{VALIDATION_SCOPE}-{experiment_id}",
                "scope": ctx.scope,
                "initiative_id": None,
                "screen": screen,
                "segment_id": segment_id,
                "metric_driver": driver,
                "observed_uplift": uplift,
                "ci_low": ci_low,
                "ci_high": ci_high,
                "sample_size": sample_size,
                "significance_flag": significant,
                "quality_score": quality,
                "source": f"validation:{ctx.scope}",
                "start_at": as_iso(start_at),
                "end_at": as_iso(end_at),
            },
        )
        ctx.evidence_ids.append(created["id"])


def create_initiative(client: httpx.Client, team_id: str, *, name: str, external_id: str, version_payload: dict[str, Any]) -> tuple[str, str]:
    created = request_json(
        client,
        "POST",
        "/initiatives",
        json={
            "external_id": external_id,
            "name": name,
            "description": "Validation initiative",
            "status": "active",
            "owner_team_id": team_id,
            "tags": {"scope": VALIDATION_SCOPE, "validation": True},
            "initial_version": version_payload,
        },
    )
    initiative_id = created["id"]
    version = request_json(client, "GET", f"/initiatives/{initiative_id}/versions")
    version_id = version["items"][0]["id"]
    return initiative_id, version_id


def run_score(ctx: ValidationContext, label: str, payload: dict[str, Any], *, mode: str = "ad_hoc", initiative_id: str | None = None, version_id: str | None = None) -> dict[str, Any]:
    response = request_json(ctx.client, "POST", "/score/run", json=payload)
    ctx.runs.append(RunRecord(label=label, run_id=response["run_id"], initiative_id=initiative_id, initiative_version_id=version_id, mode=mode))
    return response


def run_detail(ctx: ValidationContext, run_id: str) -> dict[str, Any]:
    return request_json(ctx.client, "GET", f"/score/runs/{run_id}")


def base_payload(*, name: str, segment_id: str, screens: list[str], metric_targets: list[dict[str, Any]] | None = None, segment_uplifts: dict[str, Any] | None = None, p_success: float = 0.7, confidence: float | None = 0.8, evidence_type: str = "ab_test_high", baseline_window: str = "quarter", explicit_dates: tuple[str, str] | None = None, learning_mode: str = "off", scenarios: dict[str, Any] | None = None, cannibalization_mode: str = "off", mc_seed: int = 123, mc_n: int = 3000) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "initiative_name": name,
        "data_scope": VALIDATION_SCOPE,
        "segments": [
            {
                "id": segment_id,
                "penetration": 0.42,
                "screen_penetration": {screen: 0.75 for screen in screens},
                "uplifts": segment_uplifts or {},
            }
        ],
        "screens": screens,
        "metric_targets": metric_targets or [],
        "p_success": p_success,
        "confidence": confidence,
        "evidence_type": evidence_type,
        "effort_cost": 150000,
        "strategic_weight": 1.1,
        "learning_value": 1.0,
        "baseline_window": baseline_window,
        "horizon_weeks": 26,
        "horizons_weeks": [4, 13, 26, 52],
        "decay": {"type": "exponential", "half_life_weeks": 20},
        "discount_rate_annual": 0.12,
        "cannibalization": {"mode": cannibalization_mode, "matrix_id": input_versions(_CTX)["cannibalization_matrix"], "conservative_shrink": 0.08},
        "interactions": [],
        "monte_carlo": {"enabled": True, "n": mc_n, "seed": mc_seed},
        "scenarios": scenarios,
        "sensitivity": {"enabled": True, "epsilon": 0.1, "top_n": 8, "target_metric": "net_margin"},
        "learning": {"mode": learning_mode, "lookback_days": 730, "half_life_days": 180, "min_quality": 0.6, "min_sample_size": 500},
        "input_versions": input_versions(_CTX),
        "metric_tree": {"template_name": METRIC_TREE_NAME, "version": METRIC_TREE_VERSION},
        "scoring_policy": {"name": SCORING_POLICY_NAME, "version": SCORING_POLICY_VERSION},
    }
    if explicit_dates:
        payload["baseline_date_start"], payload["baseline_date_end"] = explicit_dates
    return payload


_CTX: ValidationContext  # set in main for helper reuse


def validate_baseline_windows(ctx: ValidationContext) -> None:
    cases = {}
    for window in ("month", "quarter", "half_year", "year"):
        resp = run_score(
            ctx,
            f"baseline-{window}",
            base_payload(
                name=f"Validation • Baseline {window}",
                segment_id="active_orange_card",
                screens=["search"],
                segment_uplifts={"conversion": {"type": "normal", "mean": 0.04, "sd": 0.015}},
                baseline_window=window,
                learning_mode="off",
                mc_seed=410 + len(cases),
            ),
        )
        detail = run_detail(ctx, resp["run_id"])
        cases[window] = {
            "rto": resp["deterministic"]["incremental_rto"],
            "fm": resp["deterministic"]["incremental_fm"],
            "resolved_window": detail["resolved_inputs"]["baseline_window"]["name"],
            "start": detail["resolved_inputs"]["baseline_window"]["date_start"],
            "end": detail["resolved_inputs"]["baseline_window"]["date_end"],
        }
    distinct_rto = len({round(v["rto"], 6) for v in cases.values()})
    ctx.check("baseline_window_ad_hoc_changes_results", distinct_rto > 1, json.dumps(cases, ensure_ascii=False))

    explicit = run_score(
        ctx,
        "baseline-explicit",
        base_payload(
            name="Validation • Baseline explicit",
            segment_id="active_orange_card",
            screens=["search"],
            segment_uplifts={"conversion": {"type": "normal", "mean": 0.04, "sd": 0.015}},
            baseline_window="quarter",
            explicit_dates=("2024-07-01", "2024-09-30"),
            learning_mode="off",
            mc_seed=499,
        ),
    )
    detail = run_detail(ctx, explicit["run_id"])
    applied = detail["resolved_inputs"]["baseline_window"]
    ctx.check(
        "baseline_date_range_applied",
        applied["date_start"] == "2024-07-01" and applied["date_end"] == "2024-09-30",
        json.dumps(applied, ensure_ascii=False),
    )


def validate_driver_math(ctx: ValidationContext) -> dict[str, dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    payloads = {
        "aoq": base_payload(
            name="Validation • AOQ only",
            segment_id="loyalty_club_member",
            screens=["loyalty_clubs"],
            segment_uplifts={"aoq": {"type": "normal", "mean": 0.08, "sd": 0.02}},
            p_success=0.8,
            confidence=0.7,
            learning_mode="off",
            mc_seed=601,
        ),
        "conversion": base_payload(
            name="Validation • Conversion only",
            segment_id="active_orange_card",
            screens=["search"],
            segment_uplifts={"conversion": {"type": "triangular", "low": 0.01, "mode": 0.05, "high": 0.09}},
            p_success=0.8,
            confidence=0.7,
            learning_mode="off",
            mc_seed=602,
        ),
        "frequency": base_payload(
            name="Validation • Frequency only",
            segment_id="paket_subscriber",
            screens=["notification_center"],
            segment_uplifts={"frequency_monthly": {"type": "normal", "mean": 0.07, "sd": 0.02}},
            p_success=0.8,
            confidence=0.7,
            learning_mode="off",
            mc_seed=603,
        ),
        "aiv": base_payload(
            name="Validation • AIV only",
            segment_id="paket_subscriber",
            screens=["postorder"],
            segment_uplifts={"aiv": {"type": "lognormal", "mean": 0.05, "sd": 0.02}},
            p_success=0.8,
            confidence=0.7,
            learning_mode="off",
            mc_seed=604,
        ),
        "fm_pct": base_payload(
            name="Validation • FM only",
            segment_id="veteran_3plus_plu_6m",
            screens=["home"],
            segment_uplifts={"fm_pct": {"type": "normal", "mean": 0.06, "sd": 0.015}},
            p_success=0.8,
            confidence=0.7,
            learning_mode="off",
            mc_seed=605,
        ),
        "mixed": base_payload(
            name="Validation • Mixed drivers",
            segment_id="active_orange_card",
            screens=["checkout"],
            segment_uplifts={
                "conversion": {"type": "triangular", "low": 0.01, "mode": 0.04, "high": 0.07},
                "aoq": {"type": "normal", "mean": 0.04, "sd": 0.015},
                "aiv": {"type": "lognormal", "mean": 0.03, "sd": 0.012},
            },
            p_success=0.8,
            confidence=0.7,
            learning_mode="off",
            mc_seed=606,
        ),
    }
    for key, payload in payloads.items():
        resp = run_score(ctx, f"driver-{key}", payload)
        cases[key] = resp
    aoq = cases["aoq"]["deterministic"]
    ctx.check(
        "aoq_only_math",
        aoq["incremental_items"] > 0 and aoq["incremental_aov"] > 0 and aoq["incremental_rto"] > 0 and aoq["incremental_fm"] > 0,
        json.dumps(aoq, ensure_ascii=False),
    )
    conv = cases["conversion"]["deterministic"]
    ctx.check(
        "conversion_only_math",
        conv["incremental_orders"] > 0 and conv["incremental_rto"] > 0 and conv["incremental_fm"] > 0,
        json.dumps(conv, ensure_ascii=False),
    )
    freq = cases["frequency"]["deterministic"]
    ctx.check(
        "frequency_only_math",
        freq["incremental_orders"] > 0 and approx_equal(freq["incremental_aov"], 0.0, tolerance=1e-4),
        json.dumps(freq, ensure_ascii=False),
    )
    aiv = cases["aiv"]["deterministic"]
    ctx.check(
        "aiv_only_math",
        approx_equal(aiv["incremental_orders"], 0.0, tolerance=1e-4) and aiv["incremental_rto"] > 0 and aiv["incremental_fm"] > 0,
        json.dumps(aiv, ensure_ascii=False),
    )
    fm_only = cases["fm_pct"]["deterministic"]
    ctx.check(
        "fm_pct_only_math",
        approx_equal(fm_only["incremental_rto"], 0.0, tolerance=1e-4) and fm_only["incremental_fm"] > 0,
        json.dumps(fm_only, ensure_ascii=False),
    )
    for key, resp in cases.items():
        det = resp["deterministic"]
        ctx.check(
            f"aliases_{key}",
            approx_equal(det["incremental_gmv"], det["incremental_rto"]) and approx_equal(det["incremental_margin"], det["incremental_fm"]),
            json.dumps(det, ensure_ascii=False),
        )
    return cases


def validate_monte_carlo(ctx: ValidationContext) -> None:
    payload = base_payload(
        name="Validation • Monte Carlo AOQ",
        segment_id="loyalty_club_member",
        screens=["loyalty_clubs"],
        segment_uplifts={"aoq": {"type": "normal", "mean": 0.09, "sd": 0.03}},
        p_success=0.5,
        confidence=0.6,
        learning_mode="off",
        mc_seed=777,
        mc_n=6000,
    )
    run_a = run_score(ctx, "mc-same-seed-a", payload)
    run_b = run_score(ctx, "mc-same-seed-b", payload)
    payload_diff_seed = json.loads(json.dumps(payload))
    payload_diff_seed["monte_carlo"]["seed"] = 778
    run_c = run_score(ctx, "mc-different-seed", payload_diff_seed)

    prob_a = run_a["probabilistic"]
    prob_b = run_b["probabilistic"]
    prob_c = run_c["probabilistic"]
    same_seed_equal = prob_a == prob_b
    diff_seed_diff = prob_a != prob_c
    sane = prob_a["stddev"] > 0 and prob_a["p5"] < prob_a["median"] < prob_a["p95"] and len(prob_a["histogram"]) > 0
    no_nans = all(not math.isnan(float(prob_a[k])) and math.isfinite(float(prob_a[k])) for k in ("mean", "median", "p5", "p95", "stddev", "cv", "prob_negative"))
    ctx.check("mc_same_seed_reproducible", same_seed_equal, json.dumps(prob_a, ensure_ascii=False))
    ctx.check("mc_different_seed_varies", diff_seed_diff, json.dumps({"seed_777": prob_a, "seed_778": prob_c}, ensure_ascii=False))
    ctx.check("mc_summary_sane", sane and no_nans, json.dumps(prob_a, ensure_ascii=False))

    p_cases = []
    for p_success in (0.2, 0.5, 0.8):
        payload_p = json.loads(json.dumps(payload))
        payload_p["p_success"] = p_success
        payload_p["confidence"] = 0.6
        payload_p["monte_carlo"]["seed"] = int(800 + p_success * 10)
        resp = run_score(ctx, f"mc-p-{p_success}", payload_p)
        p_cases.append((p_success, resp))
    increasing = p_cases[0][1]["probabilistic"]["mean"] < p_cases[1][1]["probabilistic"]["mean"] < p_cases[2][1]["probabilistic"]["mean"]
    median_monotonic = p_cases[0][1]["probabilistic"]["median"] <= p_cases[1][1]["probabilistic"]["median"] <= p_cases[2][1]["probabilistic"]["median"]
    p95_monotonic = p_cases[0][1]["probabilistic"]["p95"] <= p_cases[1][1]["probabilistic"]["p95"] <= p_cases[2][1]["probabilistic"]["p95"]
    ctx.check(
        "mc_p_success_changes_distribution",
        increasing and median_monotonic and p95_monotonic,
        json.dumps({str(p): resp["probabilistic"] for p, resp in p_cases}, ensure_ascii=False),
    )

    conf_cases = []
    for confidence in (0.3, 0.6, 0.9):
        payload_c = json.loads(json.dumps(payload))
        payload_c["p_success"] = 0.5
        payload_c["confidence"] = confidence
        payload_c["monte_carlo"]["seed"] = 880
        resp = run_score(ctx, f"mc-c-{confidence}", payload_c)
        conf_cases.append((confidence, resp))
    phys_same = all(conf_cases[0][1]["probabilistic"] == item[1]["probabilistic"] for item in conf_cases[1:])
    expected_monotonic = conf_cases[0][1]["deterministic"]["expected_fm"] < conf_cases[1][1]["deterministic"]["expected_fm"] < conf_cases[2][1]["deterministic"]["expected_fm"]
    ctx.check(
        "mc_confidence_only_changes_expected_layer",
        phys_same and expected_monotonic,
        json.dumps({str(c): {"probabilistic": r["probabilistic"], "deterministic": r["deterministic"]} for c, r in conf_cases}, ensure_ascii=False),
    )


def validate_learning(ctx: ValidationContext) -> None:
    off = run_score(
        ctx,
        "learning-off",
        base_payload(
            name="Validation • Learning off",
            segment_id="active_orange_card",
            screens=["search"],
            metric_targets=[{"node": "conversion", "metric_key": "conversion", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.05}}],
            p_success=0.7,
            confidence=0.8,
            learning_mode="off",
            mc_seed=901,
        ),
    )
    advisory = run_score(
        ctx,
        "learning-advisory",
        base_payload(
            name="Validation • Learning advisory",
            segment_id="active_orange_card",
            screens=["search"],
            metric_targets=[{"node": "conversion", "metric_key": "conversion", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.05}}],
            p_success=0.7,
            confidence=0.8,
            learning_mode="advisory",
            mc_seed=902,
        ),
    )
    bayesian_exact = run_score(
        ctx,
        "learning-bayesian-exact",
        base_payload(
            name="Validation • Learning bayesian exact",
            segment_id="active_orange_card",
            screens=["search"],
            metric_targets=[{"node": "conversion", "metric_key": "conversion", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.05}}],
            p_success=0.7,
            confidence=0.8,
            learning_mode="bayesian",
            mc_seed=903,
        ),
    )
    bayesian_fallback = run_score(
        ctx,
        "learning-bayesian-fallback",
        base_payload(
            name="Validation • Learning bayesian fallback",
            segment_id="active_orange_card",
            screens=["home"],
            metric_targets=[{"node": "home_to_catalog", "node_type": "funnel_step", "target_id": "home_to_catalog", "uplift_dist": {"type": "point", "value": 0.05}}],
            p_success=0.7,
            confidence=0.8,
            learning_mode="bayesian",
            mc_seed=904,
        ),
    )
    multi = run_score(
        ctx,
        "learning-multi-screen",
        base_payload(
            name="Validation • Learning multi screen",
            segment_id="active_orange_card",
            screens=["home", "search"],
            metric_targets=[{"node": "conversion", "metric_key": "conversion", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.05}}],
            p_success=0.7,
            confidence=0.8,
            learning_mode="bayesian",
            mc_seed=905,
        ),
    )

    ctx.check(
        "learning_off_does_not_apply",
        not bool(off.get("learning_applied")) and not off.get("learning_summary"),
        json.dumps(off.get("learning_warnings", []), ensure_ascii=False),
    )
    ctx.check(
        "learning_advisory_preserves_physical",
        advisory["deterministic"]["incremental_rto"] == off["deterministic"]["incremental_rto"] and bool(advisory.get("learning_summary")),
        json.dumps({"off": off.get("learning_summary"), "advisory": advisory.get("learning_summary")}, ensure_ascii=False),
    )
    exact_summary = bayesian_exact.get("learning_summary") or {}
    ctx.check(
        "learning_exact_match_applies",
        bool(bayesian_exact.get("learning_applied")) and exact_summary.get("evidence_count", 0) > 0 and exact_summary.get("posterior_mean") != exact_summary.get("prior_mean"),
        json.dumps(exact_summary, ensure_ascii=False),
    )
    fallback_summary = bayesian_fallback.get("learning_summary") or {}
    ctx.check(
        "learning_fallback_applies",
        bool(bayesian_fallback.get("learning_applied")) and fallback_summary.get("evidence_count", 0) > 0,
        json.dumps(fallback_summary, ensure_ascii=False),
    )
    multi_warnings = multi.get("learning_warnings", []) or []
    ctx.check(
        "learning_multi_screen_skips",
        (not multi.get("learning_applied"))
        and any(
            ("multiple screens" in warning.lower())
            or ("screen-specific" in warning.lower())
            or ("multi-screen" in warning.lower())
            for warning in multi_warnings
        ),
        json.dumps(multi_warnings, ensure_ascii=False),
    )


def validate_scenarios_and_cannibalization(ctx: ValidationContext) -> None:
    scenario_resp = run_score(
        ctx,
        "scenarios",
        base_payload(
            name="Validation • Scenarios",
            segment_id="veteran_3plus_plu_6m",
            screens=["checkout"],
            metric_targets=[{"node": "checkout_to_order", "node_type": "funnel_step", "target_id": "checkout_to_order", "uplift_dist": {"type": "point", "value": 0.05}}],
            p_success=0.7,
            confidence=0.8,
            learning_mode="off",
            scenarios={
                "conservative": {"p_success": 0.45, "confidence": 0.55},
                "upside": {"p_success": 0.9, "confidence": 0.92},
            },
            mc_seed=1001,
        ),
    )
    ctx.check(
        "scenario_comparison_present",
        bool(scenario_resp.get("scenario_comparison")) and set((scenario_resp.get("scenarios") or {}).keys()) == {"base", "conservative", "upside"},
        json.dumps(scenario_resp.get("scenario_comparison"), ensure_ascii=False),
    )
    request_expect_status(
        ctx.client,
        "POST",
        "/score/run",
        422,
        json={
            "initiative_name": "Validation • Invalid base scenario",
            "data_scope": ctx.scope,
            "segments": [{"id": "veteran_3plus_plu_6m", "penetration": 0.4, "uplifts": {"conversion": 0.03}}],
            "screens": ["home"],
            "metric_targets": [],
            "p_success": 0.6,
            "confidence": 0.8,
            "effort_cost": 100000,
            "strategic_weight": 1.0,
            "learning_value": 1.0,
            "baseline_window": "quarter",
            "horizon_weeks": 26,
            "cannibalization": {"mode": "off"},
            "monte_carlo": {"enabled": True, "n": 1000, "seed": 1002},
            "scenarios": {"base": {"p_success": 0.5}},
            "input_versions": input_versions(ctx),
            "metric_tree": {"template_name": METRIC_TREE_NAME, "version": METRIC_TREE_VERSION},
            "scoring_policy": {"name": SCORING_POLICY_NAME, "version": SCORING_POLICY_VERSION},
        },
    )
    ctx.check("scenario_base_rejected", True, "scenarios.base correctly returned 422")

    cann = run_score(
        ctx,
        "cannibalization",
        base_payload(
            name="Validation • Cannibalization",
            segment_id="active_orange_card",
            screens=["search", "catalog_listing"],
            metric_targets=[{"node": "conversion", "metric_key": "conversion", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.05}}],
            p_success=0.8,
            confidence=0.7,
            learning_mode="off",
            cannibalization_mode="matrix",
            mc_seed=1003,
        ),
    )
    detail = run_detail(ctx, cann["run_id"])
    gross = cann["gross_impact"]
    net = cann["net_incremental_impact"]
    screen_breakdown = detail.get("per_screen_breakdown") or {}
    sum_net_rto = sum(float(row.get("net_delta_rto", 0.0)) for row in screen_breakdown.values())
    sum_net_fm = sum(float(row.get("net_delta_fm", 0.0)) for row in screen_breakdown.values())
    ctx.check(
        "cannibalization_gross_ge_net",
        gross["rto"] >= net["rto"] and gross["fm"] >= net["fm"],
        json.dumps({"gross": gross, "net": net}, ensure_ascii=False),
    )
    ctx.check(
        "per_screen_breakdown_sums",
        approx_equal(sum_net_rto, net["rto"], tolerance=1e-4) and approx_equal(sum_net_fm, net["fm"], tolerance=1e-4),
        json.dumps(screen_breakdown, ensure_ascii=False),
    )


def validate_initiative_orchestration(ctx: ValidationContext) -> None:
    version_payload = {
        "change_comment": "Validation initial version",
        "title_override": "Validation initiative exact",
        "data_scope": ctx.scope,
        "baseline_window": "quarter",
        "screens": ["search"],
        "segments": [{"id": "active_orange_card", "penetration": 0.42, "screen_penetration": {"search": 0.75}, "uplifts": {"conversion": {"type": "point", "value": 0.04}}}],
        "metric_targets": [],
        "p_success": 0.7,
        "confidence": 0.8,
        "evidence_type": "ab_test_high",
        "effort_cost": 155000,
        "strategic_weight": 1.1,
        "learning_value": 1.0,
        "horizon_weeks": 26,
        "horizons_weeks": [4, 13, 26, 52],
        "decay": {"type": "exponential", "half_life_weeks": 20},
        "discount_rate_annual": 0.12,
        "cannibalization": {"mode": "off"},
        "interactions": [],
        "monte_carlo": {"enabled": True, "n": 3000, "seed": 1101},
        "scenarios": {"conservative": {"p_success": 0.45, "confidence": 0.55}, "upside": {"p_success": 0.9, "confidence": 0.92}},
        "sensitivity": {"enabled": True, "epsilon": 0.1, "top_n": 8, "target_metric": "net_margin"},
        "learning": {"mode": "off", "lookback_days": 730, "half_life_days": 180, "min_quality": 0.6, "min_sample_size": 500},
        "input_versions": input_versions(ctx),
        "metric_tree": {"template_name": METRIC_TREE_NAME, "version": METRIC_TREE_VERSION},
        "scoring_policy": {"name": SCORING_POLICY_NAME, "version": SCORING_POLICY_VERSION},
    }
    initiative_id, version_id = create_initiative(
        ctx.client,
        ctx.team_id or "",
        name="Validation • Initiative exact",
        external_id=f"{ctx.scope}-initiative-exact",
        version_payload=version_payload,
    )
    ctx.initiative_ids.append(initiative_id)
    ctx.initiative_version_ids.append(version_id)

    by_initiative = run_score(
        ctx,
        "initiative-override",
        {
            "initiative_id": initiative_id,
            "run_label": "validation-initiative-override",
            "run_purpose": "what_if",
            "data_scope": ctx.scope,
            "baseline_window": "month",
            "learning": {"mode": "bayesian", "lookback_days": 730, "half_life_days": 180, "min_quality": 0.6, "min_sample_size": 500},
            "scenarios": {"upside": {"p_success": 0.88, "confidence": 0.9}},
        },
        mode="initiative_id",
        initiative_id=initiative_id,
        version_id=version_id,
    )
    detail_i = run_detail(ctx, by_initiative["run_id"])
    resolved_i = detail_i["resolved_inputs"]
    ctx.check(
        "initiative_override_merge",
        resolved_i["data_scope"] == ctx.scope
        and resolved_i["baseline_window"]["name"] == "month"
        and get_nested(resolved_i, "learning.config.mode") == "bayesian",
        json.dumps(resolved_i, ensure_ascii=False),
    )

    by_version = run_score(
        ctx,
        "version-override",
        {
            "initiative_version_id": version_id,
            "run_label": "validation-version-override",
            "run_purpose": "what_if",
            "data_scope": ctx.scope,
            "baseline_date_start": "2024-10-01",
            "baseline_date_end": "2024-12-31",
            "scenarios": {"conservative": {"p_success": 0.4, "confidence": 0.55}},
        },
        mode="initiative_version_id",
        initiative_id=initiative_id,
        version_id=version_id,
    )
    detail_v = run_detail(ctx, by_version["run_id"])
    resolved_v = detail_v["resolved_inputs"]
    ctx.check(
        "version_override_merge",
        resolved_v["baseline_window"]["date_start"] == "2024-10-01" and resolved_v["baseline_window"]["date_end"] == "2024-12-31",
        json.dumps(resolved_v["baseline_window"], ensure_ascii=False),
    )


def write_reports(ctx: ValidationContext) -> None:
    passed = sum(1 for check in ctx.checks if check.passed)
    total = len(ctx.checks)
    payload = {
        "scope": ctx.scope,
        "api_base_url": API_BASE_URL,
        "datasets": ctx.dataset_versions,
        "evidence_ids": ctx.evidence_ids,
        "initiative_ids": ctx.initiative_ids,
        "initiative_version_ids": ctx.initiative_version_ids,
        "runs": [asdict(run) for run in ctx.runs],
        "checks": [asdict(check) for check in ctx.checks],
        "summary": {"passed": passed, "total": total, "failed": total - passed},
        "warnings": ctx.warnings,
    }
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    lines = [
        f"# Validation Report: {ctx.scope}",
        "",
        f"- API: `{API_BASE_URL}`",
        f"- Passed: **{passed}/{total}**",
        f"- Failed: **{total - passed}**",
        "",
        "## Datasets",
    ]
    for name, version in ctx.dataset_versions.items():
        lines.append(f"- `{name}` -> `{version}`")
    lines.extend(["", "## Runs"])
    for run in ctx.runs:
        lines.append(f"- `{run.label}` -> `{run.run_id}` ({run.mode})")
    lines.extend(["", "## Checks"])
    for check in ctx.checks:
        status = "PASS" if check.passed else "FAIL"
        lines.append(f"- **{status}** `{check.name}`")
        lines.append(f"  - {check.details}")
    if ctx.warnings:
        lines.extend(["", "## Warnings"])
        for warning in ctx.warnings:
            lines.append(f"- {warning}")
    REPORT_MD.write_text("\n".join(lines))


def main() -> None:
    global _CTX
    token = login_supabase()
    with api_client(token) as client:
        _CTX = ValidationContext(client=client, scope=VALIDATION_SCOPE)
        preflight(client, VALIDATION_SCOPE)
        _CTX.team_id = ensure_team(client, PRIMARY_TEAM_SLUG)
        seed_datasets(_CTX)
        seed_ab_results(_CTX)
        validate_baseline_windows(_CTX)
        validate_driver_math(_CTX)
        validate_monte_carlo(_CTX)
        validate_learning(_CTX)
        validate_scenarios_and_cannibalization(_CTX)
        validate_initiative_orchestration(_CTX)
        write_reports(_CTX)

    passed = sum(1 for check in _CTX.checks if check.passed)
    total = len(_CTX.checks)
    print(
        json.dumps(
            {
                "scope": VALIDATION_SCOPE,
                "passed": passed,
                "total": total,
                "failed": total - passed,
                "report_md": str(REPORT_MD),
                "report_json": str(REPORT_JSON),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
