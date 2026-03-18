from __future__ import annotations

from typing import Any


METHODOLOGY_VERSION = "v3"


def build_scoring_methodology() -> dict[str, Any]:
    return {
        "version": METHODOLOGY_VERSION,
        "canonical_metrics": {
            "primary": ["rto", "fm"],
            "deprecated_aliases": {
                "incremental_gmv": "incremental_rto",
                "incremental_margin": "incremental_fm",
                "expected_gmv": "expected_rto",
                "expected_margin": "expected_fm",
            },
        },
        "baseline_model": {
            "segment_economics": "Orders, RTO and FM are calibrated once per segment from monthly baseline facts.",
            "screen_layer": "Screens provide exposure and funnel transitions within a segment. They do not own standalone economics.",
            "baseline_window": {
                "supported": ["month", "quarter", "half_year", "year"],
                "default": "quarter",
                "anchor": "latest complete month available in the selected baseline dataset",
            },
        },
        "monthly_baseline_aggregation": {
            "baseline_metrics": {
                "grain": "segment_id + month",
                "derived_metrics": {
                    "mau": "avg(active_users)",
                    "conversion": "sum(ordering_users) / sum(active_users)",
                    "frequency_monthly": "sum(orders) / sum(ordering_users)",
                    "frequency": "frequency_monthly * 12 / 52",
                    "aoq": "sum(items) / sum(orders)",
                    "aiv": "sum(rto) / sum(items)",
                    "aov": "sum(rto) / sum(orders)",
                    "fm_pct": "sum(fm) / sum(rto)",
                },
            },
            "baseline_funnel_steps": {
                "grain": "segment_id + screen + step_id + month",
                "derived_metrics": {
                    "step_rate": "sum(advanced_users) / sum(entered_users)",
                    "screen_exposure": "entered_users of the first funnel step",
                },
            },
        },
        "causal_chain": [
            {
                "metric": "mau_effective",
                "formula": "mau * penetration",
                "description": "The reachable audience share of the segment for the initiative.",
            },
            {
                "metric": "orders",
                "formula": "mau_effective * conversion * frequency",
                "description": "Orders come from effective audience, conversion and repeat frequency.",
            },
            {
                "metric": "items",
                "formula": "orders * aoq",
                "description": "Items increase when orders increase or when basket size grows.",
            },
            {
                "metric": "aov",
                "formula": "aoq * aiv",
                "description": "Average order value is basket size multiplied by item value.",
            },
            {
                "metric": "rto",
                "formula": "orders * aov",
                "description": "RTO is the canonical revenue output in the model.",
            },
            {
                "metric": "fm",
                "formula": "rto * fm_pct",
                "description": "Front margin is revenue multiplied by margin rate.",
            },
        ],
        "screen_uplift_semantics": {
            "screen_uplift": "A screen uplift changes funnel transitions on that screen only.",
            "segment_conversion": "Updated screen step rates are aggregated back into one segment conversion.",
            "result": "After segment conversion changes, segment economics are recalculated into orders, items, RTO and FM.",
        },
        "per_screen_breakdown": {
            "meaning": "Per-screen breakdown is an attribution of segment delta, not a standalone screen P&L.",
            "method": [
                "Compute a counterfactual where only one screen’s step uplifts are active.",
                "Measure that screen’s contribution to segment conversion delta.",
                "Normalize screen weights inside the segment.",
                "Allocate segment incremental RTO and FM by those weights.",
                "If all screen deltas are zero, fall back to first-step exposure share.",
            ],
        },
        "driver_effects": [
            {"driver": "mau", "impacts": ["mau_effective", "orders", "items", "rto", "fm"]},
            {"driver": "penetration", "impacts": ["mau_effective", "orders", "items", "rto", "fm"]},
            {"driver": "conversion", "impacts": ["orders", "items", "rto", "fm"]},
            {"driver": "frequency", "impacts": ["orders", "items", "rto", "fm"]},
            {"driver": "aoq", "impacts": ["items", "aov", "rto", "fm"]},
            {"driver": "aiv", "impacts": ["aov", "rto", "fm"]},
            {"driver": "fm_pct", "impacts": ["fm"]},
        ],
        "physical_vs_expected": {
            "physical": "Physical impact is the modeled business change if the initiative works exactly as assumed.",
            "expected": "Expected impact scales the physical result by confidence, while Monte Carlo also models the chance that the effect does not materialize through p_success.",
            "formula": {
                "expected_fm": "incremental_fm * p_success * confidence",
                "expected_rto": "incremental_rto * p_success * confidence",
            },
        },
        "probability_and_confidence": {
            "p_success": "The probability that the initiative is delivered and the effect happens in production. In Monte Carlo this is modeled as a success/failure gate on each simulation.",
            "confidence": "How confident we are in the uplift estimate itself. Confidence does not change the physical Monte Carlo distribution; it scales expected outputs.",
            "confidence_resolution_order": [
                "explicit confidence in request/version",
                "evidence_type -> config evidence priors default confidence",
                "validation error if neither is available",
            ],
        },
        "learning": {
            "modes": {
                "off": "Historical A/B evidence does not change the assumptions.",
                "advisory": "Historical evidence is shown as guidance only.",
                "bayesian": "Historical evidence updates uplift assumptions through a posterior estimate.",
            },
            "matching": {
                "exact": "screen + metric_driver + segment_id",
                "fallback": "same screen + same metric_driver when evidence segment_id is null",
                "rule": "Evidence from one screen must not influence another screen.",
            },
            "bayesian_update": {
                "prior": "the run assumption for the uplift",
                "observation": "historical A/B uplift weighted by quality, recency and significance",
                "posterior": "the updated uplift used only in bayesian mode",
            },
        },
        "cannibalization": {
            "gross": "Gross impact is the local uplift before reallocation or shrink.",
            "net": "Net impact is the effect after outbound losses and inbound reallocations are applied.",
            "matrix_mode": {
                "outbound": "A screen can lose part of its uplift to other screens via cannibalization_rate.",
                "inbound": "The lost uplift is added back to destination screens as reallocated impact.",
                "conservative_shrink": "Optional shrink reduces local uplift before matrix reallocation.",
            },
        },
        "scenarios": {
            "base": "Base scenario is always defined by the top-level request fields.",
            "overrides": "Scenario overrides define conservative, upside or custom non-base variants.",
            "rule": "scenarios.base is forbidden. Use top-level fields for the base scenario.",
        },
        "horizons": {
            "supported": [4, 13, 26, 52],
            "custom": "Custom horizon_weeks values are supported within validation bounds.",
            "decay": {
                "no_decay": "Effect stays constant over time.",
                "exponential": "Effect decays according to half_life_weeks.",
                "linear": "Effect decays linearly until the configured floor.",
            },
            "discounting": "Discount rate is applied on top of horizon accumulation when configured.",
            "difference_from_baseline_window": "Baseline window calibrates historical starting values. Horizon controls future projection length.",
            "runtime_note": "Monthly history is converted into a weekly run rate before horizon accumulation because forecast horizons are expressed in weeks.",
        },
        "analytics_layers": {
            "sensitivity": "One-way local sensitivity ranks the assumptions that move net FM or priority score the most.",
            "explainability": "Explainability summarizes top segments, top screens, top drivers and cannibalization losses using deterministic results.",
        },
        "monte_carlo": {
            "physical_distribution": "Monte Carlo samples uplift uncertainty and applies p_success as a Bernoulli success/failure gate inside each simulation.",
            "same_seed": "Identical payload and seed must reproduce the same simulated distribution exactly.",
            "different_seed": "Different seeds should change the sampled distribution while keeping the central tendency close.",
            "confidence": "Confidence is applied after the physical simulation as a decision-layer adjustment and does not reshape the physical distribution.",
        },
        "examples": [
            {
                "case": "Рост конверсии на экране",
                "assumption": "Если uplift задан на funnel-step экрана, меняется segment conversion, а затем orders, RTO и FM всего сегмента.",
                "formula_path": "screen funnel -> segment conversion -> orders -> rto -> fm",
            },
            {
                "case": "Рост AOQ",
                "assumption": "Если aoq растёт, увеличиваются items и aov, а затем RTO и FM даже при неизменном числе заказов.",
                "formula_path": "aoq -> items and aov -> rto -> fm",
            },
            {
                "case": "Снижение p_success",
                "assumption": "Monte Carlo чаще выдаёт нулевой эффект, а deterministic expected FM и ROI снижаются пропорционально p_success.",
                "formula_path": "Bernoulli success/failure gate in Monte Carlo + expected_fm = incremental_fm * p_success * confidence",
            },
            {
                "case": "Каннибализация",
                "assumption": "Gross uplift одного экрана может частично перераспределиться на другой экран. Net impact после reallocation меньше локального gross.",
                "formula_path": "gross impact -> outbound loss + inbound reallocation -> net impact",
            },
        ],
    }


def render_scoring_methodology_text(doc: dict[str, Any]) -> str:
    lines = [
        "# Scoring Methodology",
        "",
        f"version: {doc['version']}",
        "",
        "## Canonical metrics",
        "- Primary metrics: RTO, FM",
        "- Deprecated aliases: GMV -> RTO, margin -> FM",
        "",
        "## Baseline model",
        f"- Segment economics: {doc['baseline_model']['segment_economics']}",
        f"- Screen layer: {doc['baseline_model']['screen_layer']}",
        f"- Default baseline window: {doc['baseline_model']['baseline_window']['default']}",
        f"- Window anchor: {doc['baseline_model']['baseline_window']['anchor']}",
        "",
        "## Monthly baseline aggregation",
        "- baseline_metrics is monthly segment-level data.",
        "- baseline_funnel_steps is monthly screen-level funnel data.",
    ]
    for key, formula in doc["monthly_baseline_aggregation"]["baseline_metrics"]["derived_metrics"].items():
        lines.append(f"- `{key} = {formula}`")
    lines.append("")
    lines.append("Note: monthly history is normalized into a weekly run rate before forecast horizons are applied.")

    lines.extend(["", "## Causal chain"])
    for item in doc["causal_chain"]:
        lines.append(f"- `{item['metric']} = {item['formula']}`")
        lines.append(f"  - {item['description']}")

    lines.extend(
        [
            "",
            "## Screen uplift semantics",
            f"- {doc['screen_uplift_semantics']['screen_uplift']}",
            f"- {doc['screen_uplift_semantics']['segment_conversion']}",
            f"- {doc['screen_uplift_semantics']['result']}",
            "",
            "## Per-screen breakdown",
            f"- {doc['per_screen_breakdown']['meaning']}",
        ]
    )
    for step in doc["per_screen_breakdown"]["method"]:
        lines.append(f"  - {step}")

    lines.extend(
        [
            "",
            "## Physical vs expected impact",
            f"- {doc['physical_vs_expected']['physical']}",
            f"- {doc['physical_vs_expected']['expected']}",
            f"- `expected_fm = {doc['physical_vs_expected']['formula']['expected_fm']}`",
            f"- `expected_rto = {doc['physical_vs_expected']['formula']['expected_rto']}`",
            "",
            "## Probability and confidence",
            f"- p_success: {doc['probability_and_confidence']['p_success']}",
            f"- confidence: {doc['probability_and_confidence']['confidence']}",
            "- Confidence resolution order:",
        ]
    )
    for step in doc["probability_and_confidence"]["confidence_resolution_order"]:
        lines.append(f"  - {step}")

    lines.extend(
        [
            "",
            "## Monte Carlo semantics",
            f"- {doc['monte_carlo']['physical_distribution']}",
            f"- {doc['monte_carlo']['same_seed']}",
            f"- {doc['monte_carlo']['different_seed']}",
            f"- {doc['monte_carlo']['confidence']}",
        ]
    )

    lines.extend(
        [
            "",
            "## Historical A/B learning",
            f"- Exact matching: {doc['learning']['matching']['exact']}",
            f"- Fallback: {doc['learning']['matching']['fallback']}",
            f"- Rule: {doc['learning']['matching']['rule']}",
            "- Modes:",
        ]
    )
    for key, value in doc["learning"]["modes"].items():
        lines.append(f"  - `{key}`: {value}")

    lines.extend(
        [
            "",
            "## Cannibalization",
            f"- Gross impact: {doc['cannibalization']['gross']}",
            f"- Net impact: {doc['cannibalization']['net']}",
            f"- Outbound: {doc['cannibalization']['matrix_mode']['outbound']}",
            f"- Inbound: {doc['cannibalization']['matrix_mode']['inbound']}",
            f"- Conservative shrink: {doc['cannibalization']['matrix_mode']['conservative_shrink']}",
            "",
            "## Scenarios",
            f"- Base: {doc['scenarios']['base']}",
            f"- Overrides: {doc['scenarios']['overrides']}",
            f"- Rule: {doc['scenarios']['rule']}",
            "",
            "## Horizons, decay, discounting",
            f"- Supported default horizons: {', '.join(str(v) for v in doc['horizons']['supported'])}",
            f"- Custom horizons: {doc['horizons']['custom']}",
            f"- No decay: {doc['horizons']['decay']['no_decay']}",
            f"- Exponential decay: {doc['horizons']['decay']['exponential']}",
            f"- Linear decay: {doc['horizons']['decay']['linear']}",
            f"- Discounting: {doc['horizons']['discounting']}",
            f"- Baseline window vs horizon: {doc['horizons']['difference_from_baseline_window']}",
            f"- Runtime note: {doc['horizons']['runtime_note']}",
            "",
            "## Sensitivity and explainability",
            f"- Sensitivity: {doc['analytics_layers']['sensitivity']}",
            f"- Explainability: {doc['analytics_layers']['explainability']}",
            "",
            "## Practical examples",
        ]
    )
    for item in doc["examples"]:
        lines.append(f"- {item['case']}: {item['assumption']}")
        lines.append(f"  - path: {item['formula_path']}")
    return "\n".join(lines)
