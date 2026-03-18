#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import os
import random
import sys
from calendar import monthrange
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

API_BASE_URL = os.environ.get("API_BASE_URL", "https://backlog-scoring-service-eh3tzmv36q-ew.a.run.app")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kogeqjbjwjcsfyaqoprt.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")
SEED_EMAIL = os.environ.get("SEED_EMAIL")
SEED_PASSWORD = os.environ.get("SEED_PASSWORD")
SEED_SCOPE = os.environ.get("SEED_SCOPE", "x5_retail_test_v2")
METRIC_TREE_NAME = "x5_retail_test_tree"
METRIC_TREE_VERSION = "v3"
SCORING_POLICY_NAME = "ev_policy_vnext_learning"
SCORING_POLICY_VERSION = "1"


SEGMENTS = [
    ("veteran_3plus_plu_6m", "Старички 3+ PLU за полгода"),
    ("dormant_new_users_12m_no_orders", "Новички без заказов за год"),
    ("active_orange_card", "Активная апельсиновая карта"),
    ("loyalty_club_member", "Член клуба лояльности"),
    ("paket_subscriber", "Подписка Пакет"),
    ("personal_promo_user_6m", "Использовали персональные акции за полгода"),
]

SCREENS = [
    ("home", "Главная"),
    ("catalog_total_delivery", "Каталог тотал и доставка"),
    ("catalog_store", "Каталог магазин"),
    ("catalog_listing", "Каталог листинг"),
    ("search", "Поиск"),
    ("cart", "Корзина"),
    ("checkout", "Чекаут"),
    ("profile", "Личный кабинет"),
    ("loyalty_clubs", "Клубы лояльности"),
    ("notification_center", "Центр уведомлений"),
    ("reviews", "Отзывы"),
    ("postorder", "Постордер"),
]


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


@dataclass
class SeedContext:
    client: httpx.Client
    scope: str
    dataset_versions: dict[str, str]
    team_ids: dict[str, str]
    initiative_ids: list[str]
    run_ids: list[str]


def login_supabase() -> str:
    supabase_anon_key = require_env(SUPABASE_ANON_KEY, "SUPABASE_ANON_KEY")
    email = require_env(SEED_EMAIL, "SEED_EMAIL")
    password = require_env(SEED_PASSWORD, "SEED_PASSWORD")
    response = httpx.post(
        f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
        headers={"apikey": supabase_anon_key, "Content-Type": "application/json"},
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
        timeout=120,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )


def request_json(client: httpx.Client, method: str, path: str, **kwargs: Any) -> Any:
    response = client.request(method, path, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"{method} {path} failed: {response.status_code} {response.text}")
    return response.json() if response.content else None


def preflight(client: httpx.Client, scope: str) -> None:
    datasets = request_json(client, "GET", "/datasets", params={"scope": scope})
    if datasets.get("items"):
        raise SystemExit(f"Scope `{scope}` already has datasets; aborting seed.")
    learning = request_json(client, "GET", "/learning/ab-results", params={"scope": scope, "limit": 5})
    if learning.get("items"):
        raise SystemExit(f"Scope `{scope}` already has A/B evidence; aborting seed.")


def upsert_dimension(client: httpx.Client, path: str, slug: str, payload: dict[str, Any]) -> dict[str, Any]:
    rows = request_json(client, "GET", path).get("items", [])
    existing = next((row for row in rows if row["slug"] == slug), None)
    if existing:
        return request_json(client, "PATCH", f"{path}/{existing['id']}", json=payload)
    return request_json(client, "POST", path, json=payload)


def upsert_team(client: httpx.Client, slug: str, payload: dict[str, Any]) -> dict[str, Any]:
    rows = request_json(client, "GET", "/teams").get("items", [])
    existing = next((row for row in rows if row["slug"] == slug), None)
    if existing:
        return request_json(client, "PATCH", f"/teams/{existing['id']}", json=payload)
    return request_json(client, "POST", "/teams", json=payload)


def upsert_metric_tree_graph(client: httpx.Client) -> None:
    rows = request_json(
        client,
        "GET",
        "/config/metric-tree-graphs",
        params={"template_name": METRIC_TREE_NAME, "version": METRIC_TREE_VERSION},
    ).get("items", [])
    if rows:
        validate = request_json(client, "POST", f"/config/metric-tree-graphs/{METRIC_TREE_NAME}/{METRIC_TREE_VERSION}/validate")
        if not validate.get("valid"):
            raise RuntimeError(f"Existing metric tree graph is invalid: {validate}")
        return

    payload = {
        "template_name": METRIC_TREE_NAME,
        "version": METRIC_TREE_VERSION,
        "is_default": True,
        "graph": {
            "nodes": [
                {"node_id": "mau", "label": "MAU", "is_targetable": True, "unit": "users"},
                {"node_id": "penetration", "label": "Penetration", "is_targetable": True, "unit": "ratio"},
                {"node_id": "conversion", "label": "Conversion", "is_targetable": True, "unit": "ratio"},
                {"node_id": "frequency", "label": "Frequency", "is_targetable": True, "unit": "orders_per_user_per_week"},
                {"node_id": "aoq", "label": "AOQ", "is_targetable": True, "unit": "items_per_order"},
                {"node_id": "aiv", "label": "AIV", "is_targetable": True, "unit": "rub_per_item"},
                {"node_id": "fm_pct", "label": "FM%", "is_targetable": True, "unit": "ratio"},
                {"node_id": "mau_effective", "label": "MAU effective", "formula": "mau * penetration"},
                {"node_id": "orders", "label": "Orders", "formula": "mau_effective * conversion * frequency"},
                {"node_id": "items", "label": "Items", "formula": "orders * aoq"},
                {"node_id": "aov", "label": "AOV", "formula": "aoq * aiv"},
                {"node_id": "rto", "label": "RTO", "formula": "orders * aov"},
                {"node_id": "fm", "label": "FM", "formula": "rto * fm_pct"},
            ],
            "edges": [
                {"from_node": "mau", "to_node": "mau_effective"},
                {"from_node": "penetration", "to_node": "mau_effective"},
                {"from_node": "mau_effective", "to_node": "orders"},
                {"from_node": "conversion", "to_node": "orders"},
                {"from_node": "frequency", "to_node": "orders"},
                {"from_node": "orders", "to_node": "items"},
                {"from_node": "aoq", "to_node": "items"},
                {"from_node": "aoq", "to_node": "aov"},
                {"from_node": "aiv", "to_node": "aov"},
                {"from_node": "orders", "to_node": "rto"},
                {"from_node": "aov", "to_node": "rto"},
                {"from_node": "rto", "to_node": "fm"},
                {"from_node": "fm_pct", "to_node": "fm"},
            ],
        },
    }
    request_json(client, "POST", "/config/metric-tree-graphs", json=payload)
    validate = request_json(client, "POST", f"/config/metric-tree-graphs/{METRIC_TREE_NAME}/{METRIC_TREE_VERSION}/validate")
    if not validate.get("valid"):
        raise RuntimeError(f"Seeded metric tree graph is invalid: {validate}")


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
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Upload failed for {dataset_name}: {response.status_code} {response.text}")
    return response.json()["dataset"]


def _segment_monthly_rows(rnd: random.Random) -> list[dict[str, Any]]:
    months = month_sequence(month_start(2024, 4), 12)
    segment_params = {
        "veteran_3plus_plu_6m": {"active_users": 420_000, "conversion": 0.18, "frequency": 2.15, "aoq": 3.4, "aiv": 182.0, "fm_pct": 0.24},
        "dormant_new_users_12m_no_orders": {"active_users": 180_000, "conversion": 0.05, "frequency": 1.05, "aoq": 2.1, "aiv": 141.0, "fm_pct": 0.19},
        "active_orange_card": {"active_users": 310_000, "conversion": 0.13, "frequency": 1.8, "aoq": 2.9, "aiv": 168.0, "fm_pct": 0.23},
        "loyalty_club_member": {"active_users": 260_000, "conversion": 0.11, "frequency": 1.65, "aoq": 3.1, "aiv": 176.0, "fm_pct": 0.25},
        "paket_subscriber": {"active_users": 220_000, "conversion": 0.1, "frequency": 1.95, "aoq": 2.8, "aiv": 171.0, "fm_pct": 0.22},
        "personal_promo_user_6m": {"active_users": 205_000, "conversion": 0.12, "frequency": 1.72, "aoq": 2.7, "aiv": 162.0, "fm_pct": 0.21},
    }
    rows: list[dict[str, Any]] = []
    for segment_id, _name in SEGMENTS:
        params = segment_params[segment_id]
        for idx, start in enumerate(months):
            seasonality = 0.94 + 0.1 * ((idx % 6) / 5)
            active_users = max(1, int(params["active_users"] * seasonality * (0.98 + rnd.random() * 0.04)))
            conversion = params["conversion"] * (0.98 + rnd.random() * 0.04)
            ordering_users = max(1, int(active_users * conversion))
            frequency = params["frequency"] * (0.97 + rnd.random() * 0.06)
            orders = max(1, int(ordering_users * frequency))
            aoq = params["aoq"] * (0.97 + rnd.random() * 0.06)
            items = max(orders, int(orders * aoq))
            aiv = params["aiv"] * (0.98 + rnd.random() * 0.04)
            rto = round(items * aiv, 2)
            fm_pct = min(0.45, max(0.08, params["fm_pct"] * (0.98 + rnd.random() * 0.04)))
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


def _funnel_rows(rnd: random.Random) -> list[dict[str, Any]]:
    months = month_sequence(month_start(2024, 4), 12)
    screen_defs = {
        "home": [("home_to_catalog", "Home → Catalog", 1, 0.34, 0.24)],
        "catalog_total_delivery": [("catalog_to_cart", "Catalog → Cart", 1, 0.16, 0.12)],
        "catalog_store": [("catalog_to_cart", "Catalog → Cart", 1, 0.15, 0.11)],
        "catalog_listing": [("catalog_to_cart", "Catalog → Cart", 1, 0.17, 0.12)],
        "search": [("catalog_to_cart", "Search → Cart", 1, 0.19, 0.13)],
        "cart": [("cart_to_checkout", "Cart → Checkout", 1, 0.58, 0.1)],
        "checkout": [("checkout_to_order", "Checkout → Order", 1, 0.72, 0.08)],
        "profile": [("profile_to_action", "Profile → Action", 1, 0.08, 0.05)],
        "loyalty_clubs": [("loyalty_to_action", "Club → Action", 1, 0.12, 0.07)],
        "notification_center": [("notification_to_action", "Notification → Action", 1, 0.09, 0.06)],
        "reviews": [("reviews_to_action", "Reviews → Action", 1, 0.06, 0.05)],
        "postorder": [("postorder_to_action", "Postorder → Action", 1, 0.11, 0.06)],
    }
    segment_active = {
        "veteran_3plus_plu_6m": 420_000,
        "dormant_new_users_12m_no_orders": 180_000,
        "active_orange_card": 310_000,
        "loyalty_club_member": 260_000,
        "paket_subscriber": 220_000,
        "personal_promo_user_6m": 205_000,
    }
    rows: list[dict[str, Any]] = []
    for segment_id, _name in SEGMENTS:
        base_users = segment_active[segment_id]
        for idx, start in enumerate(months):
            seasonality = 0.94 + 0.1 * ((idx % 6) / 5)
            month_users = base_users * seasonality
            for screen, defs in screen_defs.items():
                for step_id, step_name, step_order, exposure_share, rate in defs:
                    entered = max(1, int(month_users * exposure_share * (0.97 + rnd.random() * 0.06)))
                    advanced = max(0, min(entered, int(entered * rate * (0.96 + rnd.random() * 0.08))))
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


def _cannibalization_rows(rnd: random.Random) -> list[dict[str, Any]]:
    pairs = [
        ("catalog_total_delivery", "catalog_listing", 0.18),
        ("catalog_store", "catalog_listing", 0.14),
        ("search", "catalog_listing", 0.12),
        ("home", "catalog_listing", 0.08),
        ("cart", "checkout", 0.05),
    ]
    rows: list[dict[str, Any]] = []
    for segment_id, _name in SEGMENTS:
        for from_screen, to_screen, rate in pairs:
            rows.append(
                {
                    "from_screen": from_screen,
                    "to_screen": to_screen,
                    "segment_id": segment_id,
                    "cannibalization_rate": round(rate + rnd.uniform(-0.02, 0.02), 4),
                }
            )
    return rows


def build_seed_datasets() -> dict[str, tuple[str, str, str]]:
    rnd = random.Random(42)
    baseline_columns = ["segment_id", "date_start", "date_end", "active_users", "ordering_users", "orders", "items", "rto", "fm"]
    funnel_columns = ["segment_id", "screen", "step_id", "step_name", "step_order", "date_start", "date_end", "entered_users", "advanced_users"]
    cann_columns = ["from_screen", "to_screen", "segment_id", "cannibalization_rate"]
    return {
        "x5_test_baseline_metrics": ("bm_x5v2", "baseline_metrics", csv_text(baseline_columns, _segment_monthly_rows(rnd))),
        "x5_test_baseline_funnel_steps": ("bfs_x5v2", "baseline_funnel_steps", csv_text(funnel_columns, _funnel_rows(rnd))),
        "x5_test_cannibalization_matrix": ("can_x5v2", "cannibalization_matrix", csv_text(cann_columns, _cannibalization_rows(rnd))),
    }


def seed_ab_results(ctx: SeedContext) -> list[str]:
    rnd = random.Random(7)
    screens = ["home", "catalog_listing", "search", "cart", "checkout", "loyalty_clubs", "notification_center", "reviews", "postorder"]
    drivers = [
        "home_to_catalog",
        "catalog_to_cart",
        "cart_to_checkout",
        "checkout_to_order",
        "frequency_monthly",
        "aiv",
        "fm_pct",
        "aoq",
    ]
    created: list[str] = []
    now = datetime.now(UTC)
    for index in range(30):
        end_at = now - timedelta(days=rnd.randint(15, 720))
        start_at = end_at - timedelta(days=rnd.randint(7, 28))
        payload = {
            "experiment_id": f"x5-seed-exp-{index + 1:02d}",
            "scope": ctx.scope,
            "initiative_id": None,
            "screen": screens[index % len(screens)],
            "segment_id": SEGMENTS[index % len(SEGMENTS)][0] if index % 3 != 0 else None,
            "metric_driver": drivers[index % len(drivers)],
            "observed_uplift": round(rnd.uniform(-0.025, 0.18), 4),
            "ci_low": round(rnd.uniform(-0.05, 0.04), 4),
            "ci_high": round(rnd.uniform(0.05, 0.24), 4),
            "sample_size": rnd.randint(800, 40000),
            "significance_flag": bool(index % 4 != 0),
            "quality_score": round(rnd.uniform(0.55, 0.95), 4),
            "source": f"seed:{ctx.scope}",
            "start_at": as_iso(start_at),
            "end_at": as_iso(end_at),
        }
        row = request_json(ctx.client, "POST", "/learning/ab-results", json=payload)
        created.append(row["id"])
    return created


def build_input_versions(dataset_versions: dict[str, str]) -> dict[str, str]:
    return {
        "baseline_metrics": dataset_versions["x5_test_baseline_metrics"],
        "baseline_funnel_steps": dataset_versions["x5_test_baseline_funnel_steps"],
        "cannibalization_matrix": dataset_versions["x5_test_cannibalization_matrix"],
    }


def seed_initiatives_and_runs(ctx: SeedContext) -> tuple[list[str], str | None]:
    input_versions = build_input_versions(ctx.dataset_versions)
    initiatives = [
        {
            "external_id": "x5_home_catalog_funnel",
            "name": "X5 Demo • Ускорение перехода на каталог",
            "team_slug": "search",
            "screens": ["home", "catalog_listing"],
            "segment_id": "veteran_3plus_plu_6m",
            "metric_targets": [
                {"node": "home_to_catalog", "node_type": "funnel_step", "target_id": "home_to_catalog", "uplift_dist": {"type": "point", "value": 0.04}},
                {"node": "catalog_to_cart", "node_type": "funnel_step", "target_id": "catalog_to_cart", "uplift_dist": {"type": "point", "value": 0.025}},
            ],
        },
        {
            "external_id": "x5_search_uplift",
            "name": "X5 Demo • Подсказки поиска",
            "team_slug": "search",
            "screens": ["search"],
            "segment_id": "personal_promo_user_6m",
            "metric_targets": [{"node": "conversion", "metric_key": "conversion", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.05}}],
        },
        {
            "external_id": "x5_cart_friction_removal",
            "name": "X5 Demo • Устранение трения в корзине",
            "team_slug": "checkout",
            "screens": ["cart"],
            "segment_id": "dormant_new_users_12m_no_orders",
            "metric_targets": [{"node": "cart_to_checkout", "node_type": "funnel_step", "target_id": "cart_to_checkout", "uplift_dist": {"type": "point", "value": 0.06}}],
        },
        {
            "external_id": "x5_checkout_uplift",
            "name": "X5 Demo • Улучшение чекаута",
            "team_slug": "checkout",
            "screens": ["checkout"],
            "segment_id": "active_orange_card",
            "metric_targets": [{"node": "checkout_to_order", "node_type": "funnel_step", "target_id": "checkout_to_order", "uplift_dist": {"type": "point", "value": 0.045}}],
        },
        {
            "external_id": "x5_loyalty_aoq_uplift",
            "name": "X5 Demo • Рост AOQ через клубы",
            "team_slug": "loyalty",
            "screens": ["loyalty_clubs", "catalog_store"],
            "segment_id": "loyalty_club_member",
            "metric_targets": [{"node": "aoq", "metric_key": "aoq", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.06}}],
        },
        {
            "external_id": "x5_notification_frequency",
            "name": "X5 Demo • Уведомления и постордер частотность",
            "team_slug": "crm",
            "screens": ["notification_center", "postorder"],
            "segment_id": "paket_subscriber",
            "metric_targets": [{"node": "frequency_monthly", "metric_key": "frequency_monthly", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.07}}],
        },
    ]

    created_ids: list[str] = []
    evidence_types = ["ab_test_high", "ab_test_medium", "historical_observation", "expert_estimate"]
    for index, spec in enumerate(initiatives, start=1):
        learning_mode = "bayesian" if index % 3 == 0 else ("advisory" if index % 3 == 1 else "off")
        confidence_value = round(0.58 + 0.06 * (index % 4), 2)
        initiative = request_json(
            ctx.client,
            "POST",
            "/initiatives",
            json={
                "external_id": spec["external_id"],
                "name": spec["name"],
                "description": "Demo initiative for X5 test scope validation",
                "status": "active",
                "owner_team_id": ctx.team_ids[spec["team_slug"]],
                "tags": {"seed_scope": ctx.scope, "demo": True},
                "initial_version": {
                    "change_comment": "Initial X5 seed version",
                    "title_override": spec["name"],
                    "data_scope": ctx.scope,
                    "baseline_window": "quarter",
                    "screens": spec["screens"],
                    "segments": [
                        {
                            "id": spec["segment_id"],
                            "penetration": round(0.35 + 0.05 * (index % 3), 2),
                            "screen_penetration": {screen: 0.75 for screen in spec["screens"]},
                            "uplifts": {},
                        }
                    ],
                    "metric_targets": spec["metric_targets"],
                    "p_success": round(0.48 + 0.1 * (index % 4), 2),
                    "confidence": confidence_value,
                    "evidence_type": evidence_types[(index - 1) % len(evidence_types)],
                    "effort_cost": 180000 + index * 15000,
                    "strategic_weight": 1.1,
                    "learning_value": 1.0 + 0.05 * index,
                    "horizon_weeks": 26,
                    "horizons_weeks": [4, 13, 26, 52],
                    "decay": {"type": "exponential", "half_life_weeks": 26},
                    "discount_rate_annual": 0.1,
                    "cannibalization": {"mode": "matrix", "matrix_id": input_versions["cannibalization_matrix"], "conservative_shrink": 0.12},
                    "interactions": [],
                    "monte_carlo": {"enabled": True, "n": 4000, "seed": 100 + index},
                    "scenarios": {
                        "conservative": {"p_success": 0.4, "confidence": 0.55},
                        "upside": {"p_success": 0.88, "confidence": 0.9},
                    },
                    "sensitivity": {"enabled": True, "epsilon": 0.1, "top_n": 8, "target_metric": "net_margin"},
                    "learning": {"mode": learning_mode, "lookback_days": 730, "half_life_days": 180, "min_quality": 0.6, "min_sample_size": 500},
                    "input_versions": input_versions,
                    "metric_tree": {"template_name": METRIC_TREE_NAME, "version": METRIC_TREE_VERSION},
                    "scoring_policy": {"name": SCORING_POLICY_NAME, "version": SCORING_POLICY_VERSION},
                },
            },
        )
        created_ids.append(initiative["id"])
        ctx.initiative_ids.append(initiative["id"])
        run = request_json(
            ctx.client,
            "POST",
            "/score/run",
            json={"initiative_id": initiative["id"], "run_label": f"seed-run-{index}", "run_purpose": "baseline"},
        )
        ctx.run_ids.append(run["run_id"])

    failed_run_id: str | None = None
    failed_response = ctx.client.post(
        "/score/run",
        json={
            "initiative_name": "X5 Demo • Intentional failed run",
            "data_scope": ctx.scope,
            "segments": [{"id": SEGMENTS[0][0], "penetration": 0.4, "uplifts": {"aoq": 0.12}}],
            "screens": ["catalog_listing"],
            "metric_targets": [],
            "p_success": 0.7,
            "confidence": 0.8,
            "evidence_type": "expert_estimate",
            "effort_cost": 100000,
            "strategic_weight": 1.0,
            "learning_value": 1.0,
            "horizon_weeks": 26,
            "monte_carlo": {"enabled": True, "n": 2000, "seed": 999},
            "scenarios": {"base": {"p_success": 0.5}},
            "input_versions": input_versions,
            "metric_tree": {"template_name": METRIC_TREE_NAME, "version": METRIC_TREE_VERSION},
            "scoring_policy": {"name": SCORING_POLICY_NAME, "version": SCORING_POLICY_VERSION},
        },
    )
    if failed_response.status_code < 400:
        raise RuntimeError("Intentional failed run unexpectedly succeeded")
    failed_runs = request_json(ctx.client, "GET", "/score/runs", params={"run_status": "failed", "limit": 20}).get("items", [])
    for row in failed_runs:
        if row.get("initiative_name") == "X5 Demo • Intentional failed run":
            failed_run_id = row["id"]
            break
    return created_ids, failed_run_id


def main() -> None:
    token = login_supabase()
    with api_client(token) as client:
        preflight(client, SEED_SCOPE)

        ctx = SeedContext(client=client, scope=SEED_SCOPE, dataset_versions={}, team_ids={}, initiative_ids=[], run_ids=[])

        for slug, name in SCREENS:
            upsert_dimension(client, "/config/screens", slug, {"slug": slug, "name": name, "description": "Seeded for X5 test scope", "is_active": True})

        for slug, name in SEGMENTS:
            upsert_dimension(client, "/config/segments", slug, {"slug": slug, "name": name, "description": "Overlapping X5 demo slice", "is_active": True})

        metric_specs = [
            ("conversion_funnel", "Прирост конверсии сегмента", "input", "conversion_funnel", "ratio"),
            ("frequency_monthly", "Прирост частотности", "input", "frequency_monthly", "ratio"),
            ("aiv", "Прирост цены штуки", "input", "aiv", "rub"),
            ("aoq", "Прирост числа штук в чеке", "input", "aoq", "items"),
            ("fm_pct", "Прирост маржинальности", "input", "fm_pct", "ratio"),
            ("incremental_rto", "incr RTO", "output", "incremental_rto", "rub"),
            ("incremental_fm", "incr FM", "output", "incremental_fm", "rub"),
            ("incremental_aoq", "incr AOQ", "output", "incremental_aoq", "items"),
            ("incremental_aov", "incr AOV", "output", "incremental_aov", "rub"),
        ]
        for slug, name, kind, driver_key, unit in metric_specs:
            upsert_dimension(
                client,
                "/config/metrics",
                slug,
                {"slug": slug, "name": name, "kind": kind, "driver_key": driver_key, "unit": unit, "description": "Seeded for X5 test scope", "is_active": True},
            )

        upsert_metric_tree_graph(client)

        for slug, name in [("search", "Search"), ("checkout", "Checkout"), ("crm", "CRM"), ("loyalty", "Loyalty")]:
            team = upsert_team(client, slug, {"slug": slug, "name": name, "description": "Seeded for X5 test scope", "is_active": True})
            ctx.team_ids[slug] = team["id"]

        for dataset_name, (version, schema_type, content) in build_seed_datasets().items():
            uploaded = upload_dataset(client, dataset_name=dataset_name, version=version, schema_type=schema_type, content=content, scope=SEED_SCOPE)
            ctx.dataset_versions[dataset_name] = uploaded["version"]

        evidence_ids = seed_ab_results(ctx)
        initiative_ids, failed_run_id = seed_initiatives_and_runs(ctx)

        print(
            {
                "scope": SEED_SCOPE,
                "datasets": ctx.dataset_versions,
                "ab_results_created": len(evidence_ids),
                "initiatives_created": initiative_ids,
                "successful_run_ids": ctx.run_ids,
                "failed_run_id": failed_run_id,
            }
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
