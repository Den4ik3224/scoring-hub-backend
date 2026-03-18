from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import numpy as np

from app.api.schemas.score import (
    ImpactBreakdown,
    LearningConfig,
    MetricTargetInput,
    ProbabilisticSummary,
    ScoreRunRequest,
    SegmentInput,
    SensitivityEntry,
    UpliftSpec,
)
from app.core.errors import ValidationError
from app.services.explainability import build_summary_text, classify_primary_driver
from app.services.funnel_engine import build_step_id_lookup, resolve_funnel_conversion
from app.services.horizon_engine import horizon_factor_sum, resolve_horizons
from app.services.learning_engine import (
    LearningApplication,
    apply_learning_to_payload,
    resolve_learning_config,
)
from app.services.monthly_baselines import screen_exposure_shares
from app.services.runtime_metric_tree import RuntimeMetricTree, build_runtime_metric_tree
from app.services.scenario_engine import materialize_scenarios
from app.services.scoring_policy import normalize_policy, prepare_metric_targets, prepare_segments
from app.services.sensitivity import build_candidates, perturb_payload
from app.services.simulation import compose_uplifts_multiplicative, sample_uplift, summarize_samples, uplift_mean
from app.services.version_resolver import ResolvedScoringInputs


@dataclass
class ScoringResult:
    deterministic: dict
    probabilistic: dict
    per_segment: dict[str, dict[str, float]]
    per_metric_node: dict[str, float]
    confidence: float
    gross_impact: dict
    net_incremental_impact: dict
    horizon_results: dict[str, dict]
    scenarios: dict[str, dict]
    scenario_comparison: dict[str, dict[str, float]]
    sensitivity: dict
    explainability: dict
    effective_input_metrics: list[str]
    derived_output_metrics: list[str]
    validation_warnings: list[str]
    learning_applied: bool
    learning_summary: dict | None
    learning_warnings: list[str]
    scoring_policy_version: str
    scoring_policy_source: str
    per_screen_breakdown: dict[str, dict[str, float]]


def _resolve_confidence(payload: ScoreRunRequest, evidence_priors: dict[str, dict] | None) -> float:
    if payload.confidence is not None:
        return payload.confidence

    if payload.evidence_type and evidence_priors and payload.evidence_type in evidence_priors:
        return float(evidence_priors[payload.evidence_type]["default_confidence"])

    raise ValidationError("Unable to resolve confidence: provide confidence or evidence_type with matching priors")


def _stable_scenario_seed(seed: int, scenario_name: str) -> int:
    digest = sha256(scenario_name.encode("utf-8")).hexdigest()
    offset = int(digest[:8], 16)
    return (seed + offset) % (2**32 - 1)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        if isinstance(value, float) and np.isnan(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-9:
        return 0.0
    return numerator / denominator


def _clip_probability(value: np.ndarray | float) -> np.ndarray | float:
    if isinstance(value, np.ndarray):
        return np.clip(value, 0.0, 1.0)
    return float(min(1.0, max(0.0, value)))


def _classify_bet_size(expected_value: float) -> str:
    abs_value = abs(expected_value)
    if abs_value < 10_000:
        return "small"
    if abs_value < 100_000:
        return "medium"
    return "large"


def _classify_uncertainty(cv: float) -> str:
    if cv < 0.2:
        return "low"
    if cv < 0.5:
        return "medium"
    return "high"


def _collect_target_specs(metric_targets: list[MetricTargetInput]) -> tuple[dict[str, list[UpliftSpec]], dict[str, list[UpliftSpec]]]:
    metric_targets_map: dict[str, list[UpliftSpec]] = {}
    step_targets_map: dict[str, list[UpliftSpec]] = {}
    for target in metric_targets:
        if target.node_type == "funnel_step":
            key = target.target_id or target.node
            step_targets_map.setdefault(key, []).append(target.uplift_dist)
        else:
            key = target.metric_key or target.node
            metric_targets_map.setdefault(key, []).append(target.uplift_dist)
    return metric_targets_map, step_targets_map


def _compose_uplifts(values: list[float]) -> float:
    if not values:
        return 0.0
    return compose_uplifts_multiplicative(values)


def _segment_uplift_value(segment: SegmentInput, key: str) -> float:
    if key not in segment.uplifts:
        return 0.0
    return uplift_mean(segment.uplifts[key])


def _combined_metric_uplift_mean(
    segment: SegmentInput,
    target_specs: dict[str, list[UpliftSpec]],
    metric_keys: list[str],
) -> float:
    parts: list[float] = []
    for key in metric_keys:
        if key in segment.uplifts:
            parts.append(uplift_mean(segment.uplifts[key]))
        for spec in target_specs.get(key, []):
            parts.append(uplift_mean(spec))
    return _compose_uplifts(parts)


def _combined_metric_uplift_samples(
    segment: SegmentInput,
    target_specs: dict[str, list[UpliftSpec]],
    metric_keys: list[str],
    n: int,
    rng: np.random.Generator,
) -> np.ndarray:
    samples: list[np.ndarray] = []
    for key in metric_keys:
        if key in segment.uplifts:
            samples.append(sample_uplift(segment.uplifts[key], n=n, rng=rng))
        for spec in target_specs.get(key, []):
            samples.append(sample_uplift(spec, n=n, rng=rng))

    if not samples:
        return np.zeros(shape=n, dtype=np.float64)

    product = np.ones(shape=n, dtype=np.float64)
    for arr in samples:
        product *= 1.0 + arr
    return product - 1.0


def _combined_step_uplift_mean(
    segment: SegmentInput,
    step_specs: dict[str, list[UpliftSpec]],
    step_id: str,
) -> float:
    values: list[float] = []
    if step_id in segment.uplifts:
        values.append(uplift_mean(segment.uplifts[step_id]))
    for spec in step_specs.get(step_id, []):
        values.append(uplift_mean(spec))
    return _compose_uplifts(values)


def _combined_step_uplift_samples(
    segment: SegmentInput,
    step_specs: dict[str, list[UpliftSpec]],
    step_id: str,
    n: int,
    rng: np.random.Generator,
) -> np.ndarray:
    samples: list[np.ndarray] = []
    if step_id in segment.uplifts:
        samples.append(sample_uplift(segment.uplifts[step_id], n=n, rng=rng))
    for spec in step_specs.get(step_id, []):
        samples.append(sample_uplift(spec, n=n, rng=rng))
    if not samples:
        return np.zeros(shape=n, dtype=np.float64)

    product = np.ones(shape=n, dtype=np.float64)
    for arr in samples:
        product *= 1.0 + arr
    return product - 1.0


def _build_cannibalization_maps(
    payload: ScoreRunRequest,
    resolved_inputs: ResolvedScoringInputs,
) -> tuple[dict[tuple[str, str], list[tuple[str, float]]], dict[tuple[str, str], float]]:
    flow_map: dict[tuple[str, str], list[tuple[str, float]]] = {}
    exposure_map: dict[tuple[str, str], float] = {}
    if payload.cannibalization.mode != "matrix" or resolved_inputs.cannibalization_table is None:
        return flow_map, exposure_map

    for row in resolved_inputs.cannibalization_table.to_pylist():
        segment_id = str(row["segment_id"])
        from_screen = str(row["from_screen"])
        to_screen = str(row["to_screen"])
        if from_screen not in payload.screens:
            continue
        rate = float(np.clip(_safe_float(row["cannibalization_rate"]), 0.0, 1.0))
        key = (segment_id, from_screen)
        flow_map.setdefault(key, []).append((to_screen, rate))
        exposure_map[key] = min(1.0, exposure_map.get(key, 0.0) + rate)
    return flow_map, exposure_map


def _validate_funnel_step_targets(payload: ScoreRunRequest, resolved_inputs: ResolvedScoringInputs) -> None:
    if not payload.metric_targets:
        return
    funnel_targets = [target for target in payload.metric_targets if target.node_type == "funnel_step"]
    if not funnel_targets:
        return

    lookup = build_step_id_lookup(resolved_inputs.funnel_index)
    if not lookup:
        raise ValidationError("Funnel-step target provided but `baseline_funnel_steps` dataset is not available")

    segment_ids = [segment.id for segment in payload.segments]
    for target in funnel_targets:
        step_id = target.target_id or target.node
        found = any((segment_id, screen, step_id) in lookup for segment_id in segment_ids for screen in payload.screens)
        if not found:
            raise ValidationError(
                f"Funnel step `{step_id}` not found for selected segments/screens in baseline_funnel_steps"
            )


def _init_metric_dict() -> dict[str, float]:
    return {"orders": 0.0, "items": 0.0, "gmv": 0.0, "margin": 0.0}


def _build_tree_inputs(
    *,
    mau: float | np.ndarray,
    penetration: float | np.ndarray,
    screen_penetration: float,
    conversion: float | np.ndarray,
    frequency_weekly: float | np.ndarray,
    aoq: float | np.ndarray,
    aiv: float | np.ndarray,
    fm_pct: float | np.ndarray,
) -> dict[str, Any]:
    return {
        "mau": mau,
        "penetration": penetration,
        "screen_penetration": screen_penetration,
        "conversion": conversion,
        "frequency": frequency_weekly,
        "frequency_weekly": frequency_weekly,
        # Runtime metric tree uses weekly-normalized frequency even when a formula references frequency_monthly.
        "frequency_monthly": frequency_weekly,
        "aoq": aoq,
        "aiv": aiv,
        "aov": aoq * aiv,
        "fm_pct": fm_pct,
    }


def _reallocate_metric_dicts(
    gross_by_key: dict[tuple[str, str], dict[str, float]],
    flow_map: dict[tuple[str, str], list[tuple[str, float]]],
    *,
    mode: str,
) -> tuple[dict[tuple[str, str], dict[str, float]], dict[str, float]]:
    if mode != "matrix":
        return deepcopy(gross_by_key), _init_metric_dict()

    net_by_key: dict[tuple[str, str], dict[str, float]] = {}
    reallocated = _init_metric_dict()
    for key, gross_metrics in gross_by_key.items():
        flows = flow_map.get(key, [])
        out_rate = min(1.0, max(0.0, sum(rate for _, rate in flows)))
        net_metrics: dict[str, float] = {}
        for metric, gross_value in gross_metrics.items():
            outbound = gross_value * out_rate
            net_metrics[metric] = gross_value - outbound
            reallocated[metric] += outbound
        net_by_key[key] = net_metrics

    for from_key, flows in flow_map.items():
        from_gross = gross_by_key.get(from_key)
        if not from_gross:
            continue
        from_segment, _from_screen = from_key
        for to_screen, rate in flows:
            to_key = (from_segment, to_screen)
            to_net = net_by_key.setdefault(to_key, _init_metric_dict())
            for metric, gross_value in from_gross.items():
                to_net[metric] += gross_value * rate

    return net_by_key, reallocated


def _reallocate_metric_arrays(
    gross_by_key: dict[tuple[str, str], dict[str, np.ndarray]],
    flow_map: dict[tuple[str, str], list[tuple[str, float]]],
    *,
    mode: str,
    n: int,
) -> dict[tuple[str, str], dict[str, np.ndarray]]:
    if mode != "matrix":
        return {
            key: {metric: values.copy() for metric, values in metrics.items()}
            for key, metrics in gross_by_key.items()
        }

    net_by_key: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    for key, gross_metrics in gross_by_key.items():
        flows = flow_map.get(key, [])
        out_rate = min(1.0, max(0.0, sum(rate for _, rate in flows)))
        net_by_key[key] = {
            metric: gross_value * (1.0 - out_rate)
            for metric, gross_value in gross_metrics.items()
        }

    for from_key, flows in flow_map.items():
        from_gross = gross_by_key.get(from_key)
        if not from_gross:
            continue
        from_segment, _from_screen = from_key
        for to_screen, rate in flows:
            to_key = (from_segment, to_screen)
            to_net = net_by_key.setdefault(
                to_key,
                {
                    "orders": np.zeros(shape=n, dtype=np.float64),
                    "items": np.zeros(shape=n, dtype=np.float64),
                    "gmv": np.zeros(shape=n, dtype=np.float64),
                    "margin": np.zeros(shape=n, dtype=np.float64),
                },
            )
            for metric, gross_value in from_gross.items():
                to_net[metric] += gross_value * rate

    return net_by_key


def _build_node_contributions_weekly(
    payload: ScoreRunRequest,
    metric_specs: dict[str, list[UpliftSpec]],
    step_specs: dict[str, list[UpliftSpec]],
    net_margin_weekly: float,
) -> dict[str, float]:
    weighted_nodes: list[tuple[str, float]] = []

    for idx, target in enumerate(payload.metric_targets or []):
        if target.node_type == "funnel_step":
            step_id = target.target_id or target.node
            weight = abs(uplift_mean(target.uplift_dist))
            weighted_nodes.append((f"metric_target:funnel_step:{step_id}:{idx}", weight))
        else:
            metric_key = target.metric_key or target.node
            weight = abs(uplift_mean(target.uplift_dist))
            weighted_nodes.append((f"metric_target:{metric_key}:{idx}", weight))

    for segment in payload.segments:
        for uplift_key, uplift_value in segment.uplifts.items():
            if uplift_key in step_specs:
                label = f"segment_uplift:funnel_step:{segment.id}:{uplift_key}"
            else:
                label = f"segment_uplift:{segment.id}:{uplift_key}"
            weighted_nodes.append((label, abs(uplift_mean(uplift_value))))

    total_weight = sum(weight for _name, weight in weighted_nodes) or 1.0
    return {
        name: net_margin_weekly * (weight / total_weight)
        for name, weight in weighted_nodes
    }


def _screen_shares_for_segment(
    payload: ScoreRunRequest,
    resolved_inputs: ResolvedScoringInputs,
    *,
    segment_id: str,
) -> dict[str, float]:
    return screen_exposure_shares(resolved_inputs.funnel_index, segment_id=segment_id, screens=payload.screens)


def _resolve_segment_conversion_state(
    *,
    payload: ScoreRunRequest,
    resolved_inputs: ResolvedScoringInputs,
    segment: SegmentInput,
    step_specs: dict[str, list[UpliftSpec]],
    conversion_uplift: float,
    exposure_map: dict[tuple[str, str], float],
) -> tuple[float, dict[str, float], dict[str, float]]:
    baseline = resolved_inputs.segment_baselines[segment.id]
    shares = _screen_shares_for_segment(payload, resolved_inputs, segment_id=segment.id)
    if not resolved_inputs.funnel_index:
        fallback = shares or {screen: 1.0 / len(payload.screens) for screen in payload.screens}
        return float(_clip_probability(baseline.conversion * (1.0 + conversion_uplift))), fallback, fallback

    base_weighted = 0.0
    updated_weighted = 0.0
    screen_deltas: dict[str, float] = {}
    any_funnel = False
    for screen in payload.screens:
        funnel_rows = resolved_inputs.funnel_index.get((segment.id, screen))
        if not funnel_rows:
            continue
        any_funnel = True
        share = shares.get(screen, 0.0)
        screen_pen = (segment.screen_penetration or {}).get(screen, 1.0)
        screen_shrink = 1.0
        if payload.cannibalization.mode == "matrix" and payload.cannibalization.conservative_shrink > 0:
            screen_shrink = 1.0 - payload.cannibalization.conservative_shrink * exposure_map.get((segment.id, screen), 0.0)
        step_uplifts = {
            row.step_id: _combined_step_uplift_mean(segment, step_specs, row.step_id) * screen_pen * screen_shrink
            for row in funnel_rows
        }
        state = resolve_funnel_conversion(
            funnel_rows=funnel_rows,
            step_uplifts=step_uplifts,
            conversion_uplift=0.0,
        )
        base_weighted += share * state.baseline_conversion
        updated_weighted += share * state.updated_conversion

        screen_only_weighted = 0.0
        for other_screen in payload.screens:
            other_rows = resolved_inputs.funnel_index.get((segment.id, other_screen))
            if not other_rows:
                continue
            other_share = shares.get(other_screen, 0.0)
            if other_screen == screen:
                other_state = state
            else:
                other_state = resolve_funnel_conversion(
                    funnel_rows=other_rows,
                    step_uplifts={},
                    conversion_uplift=0.0,
                )
            screen_only_weighted += other_share * other_state.updated_conversion
        screen_deltas[screen] = screen_only_weighted - base_weighted

    if not any_funnel or base_weighted <= 0:
        fallback = shares or {screen: 1.0 / len(payload.screens) for screen in payload.screens}
        return float(_clip_probability(baseline.conversion * (1.0 + conversion_uplift))), fallback, fallback

    total_delta = updated_weighted - base_weighted
    conversion_multiplier = updated_weighted / base_weighted if base_weighted > 0 else 1.0
    conversion_new = float(_clip_probability(baseline.conversion * conversion_multiplier * (1.0 + conversion_uplift)))

    if abs(total_delta) > 1e-12:
        weights = {screen: delta / total_delta for screen, delta in screen_deltas.items()}
    else:
        weights = shares or {screen: 1.0 / len(payload.screens) for screen in payload.screens}
    return conversion_new, weights, shares or weights


def _compute_weekly_state(
    payload: ScoreRunRequest,
    resolved_inputs: ResolvedScoringInputs,
    metric_specs: dict[str, list[UpliftSpec]],
    step_specs: dict[str, list[UpliftSpec]],
    runtime_tree: RuntimeMetricTree,
) -> dict[str, Any]:
    segment_map = {segment.id: segment for segment in payload.segments}
    flow_map, exposure_map = _build_cannibalization_maps(payload, resolved_inputs)
    gross_by_key: dict[tuple[str, str], dict[str, float]] = {}
    segment_stats: dict[str, dict[str, float]] = {}
    screen_gross: dict[str, dict[str, float]] = {}
    base_totals = {"orders": 0.0, "items": 0.0, "gmv": 0.0}

    for segment_id, segment in segment_map.items():
        baseline = resolved_inputs.segment_baselines.get(segment_id)
        if baseline is None:
            raise ValidationError(f"baseline_metrics has no data for segment `{segment_id}` in the selected baseline window")

        segment_exposure = 0.0
        if payload.screens:
            segment_exposure = sum(exposure_map.get((segment_id, screen), 0.0) for screen in payload.screens) / len(payload.screens)
        segment_shrink = 1.0
        if payload.cannibalization.mode == "matrix" and payload.cannibalization.conservative_shrink > 0:
            segment_shrink = 1.0 - payload.cannibalization.conservative_shrink * segment_exposure

        u_mau = _combined_metric_uplift_mean(segment, metric_specs, ["mau"]) * segment_shrink
        u_pen = _combined_metric_uplift_mean(segment, metric_specs, ["penetration"]) * segment_shrink
        u_conv = _combined_metric_uplift_mean(segment, metric_specs, ["conversion"]) * segment_shrink
        u_freq = _combined_metric_uplift_mean(segment, metric_specs, ["frequency_monthly", "frequency"]) * segment_shrink
        u_aoq = _combined_metric_uplift_mean(segment, metric_specs, ["aoq"]) * segment_shrink
        u_aiv = _combined_metric_uplift_mean(segment, metric_specs, ["aiv"]) * segment_shrink
        u_aov = _combined_metric_uplift_mean(segment, metric_specs, ["aov"]) * segment_shrink
        u_fm = _combined_metric_uplift_mean(segment, metric_specs, ["fm_pct"]) * segment_shrink

        penetration_new = float(_clip_probability(segment.penetration * (1.0 + u_pen)))
        conversion_new, screen_weights, exposure_shares = _resolve_segment_conversion_state(
            payload=payload,
            resolved_inputs=resolved_inputs,
            segment=segment,
            step_specs=step_specs,
            conversion_uplift=u_conv,
            exposure_map=exposure_map,
        )

        base_values = runtime_tree.evaluate(
            _build_tree_inputs(
                mau=baseline.mau,
                penetration=float(segment.penetration),
                screen_penetration=1.0,
                conversion=baseline.conversion,
                frequency_weekly=baseline.frequency_weekly,
                aoq=baseline.aoq,
                aiv=baseline.aiv,
                fm_pct=float(_clip_probability(baseline.fm_pct)),
            )
        )
        new_values = runtime_tree.evaluate(
            _build_tree_inputs(
                mau=max(0.0, baseline.mau * (1.0 + u_mau)),
                penetration=penetration_new,
                screen_penetration=1.0,
                conversion=conversion_new,
                frequency_weekly=max(0.0, baseline.frequency_weekly * (1.0 + u_freq)),
                aoq=max(0.0, baseline.aoq * (1.0 + u_aoq)),
                aiv=max(0.0, baseline.aiv * (1.0 + u_aiv)),
                fm_pct=float(_clip_probability(baseline.fm_pct * (1.0 + u_fm))),
            )
        )
        if abs(u_aov) > 1e-12:
            aov_override = max(0.0, _safe_float(base_values.get("aov")) * (1.0 + u_aov))
            new_values["aov"] = aov_override
            new_values["rto"] = _safe_float(new_values.get("orders")) * aov_override
            new_values["fm"] = new_values["rto"] * float(_clip_probability(baseline.fm_pct * (1.0 + u_fm)))

        orders_base = _safe_float(base_values.get("orders"))
        items_base = _safe_float(base_values.get("items"))
        gmv_base = _safe_float(base_values.get("rto"))
        margin_base = _safe_float(base_values.get("fm"))
        base_totals["orders"] += orders_base
        base_totals["items"] += items_base
        base_totals["gmv"] += gmv_base

        gross_delta = {
            "orders": _safe_float(new_values.get("orders")) - orders_base,
            "items": _safe_float(new_values.get("items")) - items_base,
            "gmv": _safe_float(new_values.get("rto")) - gmv_base,
            "margin": _safe_float(new_values.get("fm")) - margin_base,
        }

        for screen in payload.screens:
            weight = screen_weights.get(screen, exposure_shares.get(screen, 0.0))
            flow_key = (segment_id, screen)
            gross_by_key[flow_key] = {
                metric: value * weight
                for metric, value in gross_delta.items()
            }
            screen_entry = screen_gross.setdefault(screen, _init_metric_dict())
            for metric, value in gross_by_key[flow_key].items():
                screen_entry[metric] += value

        segment_stats[segment_id] = {
            "baseline_mau": baseline.mau,
            "effective_mau": _safe_float(base_values.get("mau_effective")),
            "penetration_applied": float(segment.penetration),
            "gross_orders_weekly": gross_delta["orders"],
            "gross_items_weekly": gross_delta["items"],
            "gross_gmv_weekly": gross_delta["gmv"],
            "gross_margin_weekly": gross_delta["margin"],
        }

    net_by_key, reallocated_weekly = _reallocate_metric_dicts(
        gross_by_key,
        flow_map,
        mode=payload.cannibalization.mode,
    )

    gross_weekly = _init_metric_dict()
    for values in gross_by_key.values():
        for metric, value in values.items():
            gross_weekly[metric] += value

    net_weekly = _init_metric_dict()
    for values in net_by_key.values():
        for metric, value in values.items():
            net_weekly[metric] += value

    per_segment_weekly: dict[str, dict[str, float]] = {}
    screen_net: dict[str, dict[str, float]] = {}
    for (segment_id, screen), metrics in net_by_key.items():
        seg = per_segment_weekly.setdefault(
            segment_id,
            {
                "net_orders_weekly": 0.0,
                "net_items_weekly": 0.0,
                "net_gmv_weekly": 0.0,
                "net_margin_weekly": 0.0,
            },
        )
        seg["net_orders_weekly"] += metrics["orders"]
        seg["net_items_weekly"] += metrics["items"]
        seg["net_gmv_weekly"] += metrics["gmv"]
        seg["net_margin_weekly"] += metrics["margin"]

        screen_entry = screen_net.setdefault(screen, _init_metric_dict())
        for metric, value in metrics.items():
            screen_entry[metric] += value

    for segment_id, seg in per_segment_weekly.items():
        base = segment_stats.get(segment_id, {})
        seg["baseline_mau"] = base.get("baseline_mau", 0.0)
        seg["effective_mau"] = base.get("effective_mau", 0.0)
        seg["penetration_applied"] = base.get("penetration_applied", 0.0)

    node_contributions_weekly = _build_node_contributions_weekly(
        payload,
        metric_specs,
        step_specs,
        net_weekly["margin"],
    )

    return {
        "gross_weekly": gross_weekly,
        "net_weekly": net_weekly,
        "base_totals": base_totals,
        "reallocated_weekly": reallocated_weekly,
        "per_segment_weekly": per_segment_weekly,
        "screen_gross_weekly": screen_gross,
        "screen_net_weekly": screen_net,
        "node_contributions_weekly": node_contributions_weekly,
    }


def _run_monte_carlo_weekly_margin(
    payload: ScoreRunRequest,
    resolved_inputs: ResolvedScoringInputs,
    metric_specs: dict[str, list[UpliftSpec]],
    step_specs: dict[str, list[UpliftSpec]],
    runtime_tree: RuntimeMetricTree,
    *,
    seed: int,
) -> np.ndarray:
    if not payload.monte_carlo.enabled:
        return np.zeros(shape=1, dtype=np.float64)

    n = payload.monte_carlo.n
    rng = np.random.default_rng(seed)
    segment_map = {segment.id: segment for segment in payload.segments}
    flow_map, exposure_map = _build_cannibalization_maps(payload, resolved_inputs)

    deterministic_weights: dict[str, dict[str, float]] = {}
    for segment_id, segment in segment_map.items():
        _conv_new, weights, exposure_shares = _resolve_segment_conversion_state(
            payload=payload,
            resolved_inputs=resolved_inputs,
            segment=segment,
            step_specs=step_specs,
            conversion_uplift=_combined_metric_uplift_mean(segment, metric_specs, ["conversion"]),
            exposure_map=exposure_map,
        )
        deterministic_weights[segment_id] = weights or exposure_shares

    gross_margin_by_key: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    for segment_id, segment in segment_map.items():
        baseline = resolved_inputs.segment_baselines.get(segment_id)
        if baseline is None:
            continue

        segment_exposure = 0.0
        if payload.screens:
            segment_exposure = sum(exposure_map.get((segment_id, screen), 0.0) for screen in payload.screens) / len(payload.screens)
        segment_shrink = 1.0
        if payload.cannibalization.mode == "matrix" and payload.cannibalization.conservative_shrink > 0:
            segment_shrink = 1.0 - payload.cannibalization.conservative_shrink * segment_exposure

        mau_u = _combined_metric_uplift_samples(segment, metric_specs, ["mau"], n=n, rng=rng) * segment_shrink
        pen_u = _combined_metric_uplift_samples(segment, metric_specs, ["penetration"], n=n, rng=rng) * segment_shrink
        conv_u = _combined_metric_uplift_samples(segment, metric_specs, ["conversion"], n=n, rng=rng) * segment_shrink
        freq_u = _combined_metric_uplift_samples(segment, metric_specs, ["frequency_monthly", "frequency"], n=n, rng=rng) * segment_shrink
        aoq_u = _combined_metric_uplift_samples(segment, metric_specs, ["aoq"], n=n, rng=rng) * segment_shrink
        aiv_u = _combined_metric_uplift_samples(segment, metric_specs, ["aiv"], n=n, rng=rng) * segment_shrink
        fm_u = _combined_metric_uplift_samples(segment, metric_specs, ["fm_pct"], n=n, rng=rng) * segment_shrink
        aov_u = _combined_metric_uplift_samples(segment, metric_specs, ["aov"], n=n, rng=rng) * segment_shrink

        shares = _screen_shares_for_segment(payload, resolved_inputs, segment_id=segment_id)
        weighted_base = 0.0
        weighted_new = np.zeros(shape=n, dtype=np.float64)
        any_funnel = False
        for screen in payload.screens:
            rows = resolved_inputs.funnel_index.get((segment_id, screen))
            if not rows:
                continue
            any_funnel = True
            share = shares.get(screen, 0.0)
            screen_pen = (segment.screen_penetration or {}).get(screen, 1.0)
            screen_shrink = 1.0
            if payload.cannibalization.mode == "matrix" and payload.cannibalization.conservative_shrink > 0:
                screen_shrink = 1.0 - payload.cannibalization.conservative_shrink * exposure_map.get((segment_id, screen), 0.0)
            step_base = 1.0
            step_new = np.ones(shape=n, dtype=np.float64)
            for row in rows:
                rate = float(np.clip(row.baseline_rate, 0.0, 1.0))
                step_u = _combined_step_uplift_samples(segment, step_specs, row.step_id, n=n, rng=rng) * screen_pen * screen_shrink
                step_base *= rate
                step_new *= np.clip(rate * (1.0 + step_u), 0.0, 1.0)
            weighted_base += share * step_base
            weighted_new += share * step_new
        if any_funnel and weighted_base > 0:
            conversion_new = np.clip(baseline.conversion * (weighted_new / weighted_base) * (1.0 + conv_u), 0.0, 1.0)
        else:
            conversion_new = np.clip(baseline.conversion * (1.0 + conv_u), 0.0, 1.0)

        base_values = runtime_tree.evaluate(
            _build_tree_inputs(
                mau=baseline.mau,
                penetration=segment.penetration,
                screen_penetration=1.0,
                conversion=baseline.conversion,
                frequency_weekly=baseline.frequency_weekly,
                aoq=baseline.aoq,
                aiv=baseline.aiv,
                fm_pct=baseline.fm_pct,
            )
        )
        fm_new = np.clip(baseline.fm_pct * (1.0 + fm_u), 0.0, 1.0)
        new_values = runtime_tree.evaluate(
            _build_tree_inputs(
                mau=np.maximum(0.0, baseline.mau * (1.0 + mau_u)),
                penetration=np.clip(segment.penetration * (1.0 + pen_u), 0.0, 1.0),
                screen_penetration=1.0,
                conversion=conversion_new,
                frequency_weekly=np.maximum(0.0, baseline.frequency_weekly * (1.0 + freq_u)),
                aoq=np.maximum(0.0, baseline.aoq * (1.0 + aoq_u)),
                aiv=np.maximum(0.0, baseline.aiv * (1.0 + aiv_u)),
                fm_pct=fm_new,
            )
        )
        if np.any(np.abs(aov_u) > 1e-12):
            base_aov = np.asarray(base_values.get("aov"))
            aov_new = np.maximum(0.0, base_aov * (1.0 + aov_u))
            orders_new = np.asarray(new_values.get("orders"))
            new_values["aov"] = aov_new
            new_values["rto"] = orders_new * aov_new
            new_values["fm"] = new_values["rto"] * fm_new

        gross_arrays = {
            "orders": np.asarray(new_values.get("orders")) - np.asarray(base_values.get("orders")),
            "items": np.asarray(new_values.get("items")) - np.asarray(base_values.get("items")),
            "gmv": np.asarray(new_values.get("rto")) - np.asarray(base_values.get("rto")),
            "margin": np.asarray(new_values.get("fm")) - np.asarray(base_values.get("fm")),
        }

        for screen in payload.screens:
            weight = deterministic_weights.get(segment_id, {}).get(screen, shares.get(screen, 0.0))
            gross_margin_by_key[(segment_id, screen)] = {
                metric: values * weight
                for metric, values in gross_arrays.items()
            }

    net_by_key = _reallocate_metric_arrays(
        gross_margin_by_key,
        flow_map,
        mode=payload.cannibalization.mode,
        n=n,
    )
    margin_samples = np.zeros(shape=n, dtype=np.float64)
    for metrics in net_by_key.values():
        margin_samples += metrics["margin"]
    if payload.p_success < 1.0:
        success_mask = rng.binomial(1, payload.p_success, size=n).astype(np.float64)
        margin_samples *= success_mask
    return margin_samples


def _compute_horizon_result(
    *,
    payload: ScoreRunRequest,
    confidence: float,
    weekly_state: dict[str, Any],
    margin_samples_weekly: np.ndarray,
    horizon_weeks: int,
) -> dict[str, Any]:
    factor = horizon_factor_sum(
        weeks=horizon_weeks,
        decay=payload.decay,
        discount_rate_annual=payload.discount_rate_annual,
    )

    gross = {
        "orders": weekly_state["gross_weekly"]["orders"] * factor,
        "items": weekly_state["gross_weekly"]["items"] * factor,
        "gmv": weekly_state["gross_weekly"]["gmv"] * factor,
        "margin": weekly_state["gross_weekly"]["margin"] * factor,
        "reallocated_orders": weekly_state["reallocated_weekly"]["orders"] * factor,
        "reallocated_items": weekly_state["reallocated_weekly"]["items"] * factor,
        "reallocated_gmv": weekly_state["reallocated_weekly"]["gmv"] * factor,
        "reallocated_margin": weekly_state["reallocated_weekly"]["margin"] * factor,
    }
    net = {
        "orders": weekly_state["net_weekly"]["orders"] * factor,
        "items": weekly_state["net_weekly"]["items"] * factor,
        "gmv": weekly_state["net_weekly"]["gmv"] * factor,
        "margin": weekly_state["net_weekly"]["margin"] * factor,
        "reallocated_orders": weekly_state["reallocated_weekly"]["orders"] * factor,
        "reallocated_items": weekly_state["reallocated_weekly"]["items"] * factor,
        "reallocated_gmv": weekly_state["reallocated_weekly"]["gmv"] * factor,
        "reallocated_margin": weekly_state["reallocated_weekly"]["margin"] * factor,
    }

    expected_multiplier = payload.p_success * confidence
    expected_fm = net["margin"] * expected_multiplier
    expected_rto = net["gmv"] * expected_multiplier
    expected_value = expected_fm
    roi = expected_fm / payload.effort_cost
    priority_score = (expected_value * payload.strategic_weight * payload.learning_value) / payload.effort_cost
    base_orders_total = weekly_state["base_totals"]["orders"] * factor
    base_items_total = weekly_state["base_totals"]["items"] * factor
    base_gmv_total = weekly_state["base_totals"]["gmv"] * factor
    new_orders_total = base_orders_total + net["orders"]
    new_items_total = base_items_total + net["items"]
    new_gmv_total = base_gmv_total + net["gmv"]
    incremental_aoq = _safe_ratio(new_items_total, new_orders_total) - _safe_ratio(base_items_total, base_orders_total)
    incremental_aov = _safe_ratio(new_gmv_total, new_orders_total) - _safe_ratio(base_gmv_total, base_orders_total)

    if payload.monte_carlo.enabled:
        probabilistic = summarize_samples(margin_samples_weekly * factor, bins=20)
    else:
        probabilistic = {
            "mean": 0.0,
            "median": 0.0,
            "p5": 0.0,
            "p95": 0.0,
            "prob_negative": 0.0,
            "stddev": 0.0,
            "cv": 0.0,
            "histogram": [],
        }

    deterministic = {
        "incremental_gmv": net["gmv"],
        "incremental_margin": net["margin"],
        "incremental_rto": net["gmv"],
        "incremental_fm": net["margin"],
        "incremental_orders": net["orders"],
        "incremental_items": net["items"],
        "incremental_aoq": incremental_aoq,
        "incremental_aov": incremental_aov,
        "expected_value": expected_value,
        "expected_margin": expected_fm,
        "expected_fm": expected_fm,
        "expected_gmv": expected_rto,
        "expected_rto": expected_rto,
        "roi": roi,
        "priority_score": priority_score,
        "bet_size": _classify_bet_size(expected_value),
        "uncertainty_tag": _classify_uncertainty(probabilistic.get("cv", 0.0)),
    }

    return {
        "deterministic": deterministic,
        "probabilistic": probabilistic,
        "gross_impact": gross,
        "net_incremental_impact": net,
        "discounted_summary": deterministic if payload.discount_rate_annual is not None else None,
    }


def _build_per_segment_for_horizon(
    payload: ScoreRunRequest,
    confidence: float,
    weekly_state: dict[str, Any],
    factor: float,
) -> dict[str, dict[str, float]]:
    expected_multiplier = payload.p_success * confidence
    per_segment: dict[str, dict[str, float]] = {}
    for segment_id, row in weekly_state["per_segment_weekly"].items():
        incremental_margin = row["net_margin_weekly"] * factor
        incremental_rto = row["net_gmv_weekly"] * factor
        per_segment[segment_id] = {
            "incremental_orders": row["net_orders_weekly"] * factor,
            "incremental_items": row["net_items_weekly"] * factor,
            "incremental_gmv": row["net_gmv_weekly"] * factor,
            "incremental_rto": incremental_rto,
            "incremental_margin": incremental_margin,
            "incremental_fm": incremental_margin,
            "expected_margin": incremental_margin * expected_multiplier,
            "expected_fm": incremental_margin * expected_multiplier,
            "baseline_mau": row["baseline_mau"],
            "effective_mau": row["effective_mau"],
            "penetration_applied": row["penetration_applied"],
        }
    return per_segment


def _format_impact_breakdown(raw: dict[str, float]) -> dict[str, float]:
    return ImpactBreakdown(
        orders=raw["orders"],
        items=raw["items"],
        gmv=raw["gmv"],
        margin=raw["margin"],
        rto=raw["gmv"],
        fm=raw["margin"],
        reallocated_orders=raw.get("reallocated_orders", 0.0),
        reallocated_items=raw.get("reallocated_items", 0.0),
        reallocated_gmv=raw.get("reallocated_gmv", 0.0),
        reallocated_margin=raw.get("reallocated_margin", 0.0),
        reallocated_rto=raw.get("reallocated_gmv", 0.0),
        reallocated_fm=raw.get("reallocated_margin", 0.0),
    ).model_dump(mode="json")


def _build_per_screen_breakdown(weekly_state: dict[str, Any], factor: float) -> dict[str, dict[str, float]]:
    screens = set(weekly_state.get("screen_gross_weekly", {}).keys()) | set(weekly_state.get("screen_net_weekly", {}).keys())
    result: dict[str, dict[str, float]] = {}
    for screen in sorted(screens):
        gross = weekly_state.get("screen_gross_weekly", {}).get(screen, {})
        net = weekly_state.get("screen_net_weekly", {}).get(screen, {})
        result[screen] = {
            "gross_delta_orders": _safe_float(gross.get("orders")) * factor,
            "gross_delta_items": _safe_float(gross.get("items")) * factor,
            "gross_delta_gmv": _safe_float(gross.get("gmv")) * factor,
            "gross_delta_margin": _safe_float(gross.get("margin")) * factor,
            "gross_delta_rto": _safe_float(gross.get("gmv")) * factor,
            "gross_delta_fm": _safe_float(gross.get("margin")) * factor,
            "net_delta_orders": _safe_float(net.get("orders")) * factor,
            "net_delta_items": _safe_float(net.get("items")) * factor,
            "net_delta_gmv": _safe_float(net.get("gmv")) * factor,
            "net_delta_margin": _safe_float(net.get("margin")) * factor,
            "net_delta_rto": _safe_float(net.get("gmv")) * factor,
            "net_delta_fm": _safe_float(net.get("margin")) * factor,
            "reallocated_orders": (_safe_float(gross.get("orders")) - _safe_float(net.get("orders"))) * factor,
            "reallocated_items": (_safe_float(gross.get("items")) - _safe_float(net.get("items"))) * factor,
            "reallocated_gmv": (_safe_float(gross.get("gmv")) - _safe_float(net.get("gmv"))) * factor,
            "reallocated_margin": (_safe_float(gross.get("margin")) - _safe_float(net.get("margin"))) * factor,
            "reallocated_rto": (_safe_float(gross.get("gmv")) - _safe_float(net.get("gmv"))) * factor,
            "reallocated_fm": (_safe_float(gross.get("margin")) - _safe_float(net.get("margin"))) * factor,
        }
    return result


def _prepare_payload_for_execution(
    payload: ScoreRunRequest,
    policy: dict[str, Any],
) -> tuple[ScoreRunRequest, list[str], list[str]]:
    prepared_payload = payload.model_copy(deep=True)
    prepared_segments = prepare_segments(prepared_payload.segments, policy)
    prepared_targets = prepare_metric_targets(prepared_payload.metric_targets, policy)
    prepared_payload.segments = prepared_segments.segments
    prepared_payload.metric_targets = prepared_targets.targets
    warnings = [*prepared_segments.warnings, *prepared_targets.warnings]
    effective_metrics = sorted(
        set(prepared_segments.effective_input_metrics).union(prepared_targets.effective_input_metrics)
    )
    return prepared_payload, warnings, effective_metrics


def _run_single_scenario(
    *,
    scenario_name: str,
    payload: ScoreRunRequest,
    resolved_inputs: ResolvedScoringInputs,
    mc_max_n: int,
    runtime_tree: RuntimeMetricTree,
) -> dict[str, Any]:
    if payload.monte_carlo.n > mc_max_n:
        raise ValidationError(f"monte_carlo.n exceeds max allowed value ({mc_max_n})")

    confidence = _resolve_confidence(payload, resolved_inputs.evidence_priors)
    _validate_funnel_step_targets(payload, resolved_inputs)

    metric_specs, step_specs = _collect_target_specs(payload.metric_targets)
    weekly_state = _compute_weekly_state(
        payload,
        resolved_inputs,
        metric_specs,
        step_specs,
        runtime_tree,
    )

    scenario_seed = _stable_scenario_seed(payload.monte_carlo.seed, scenario_name)
    margin_samples_weekly = _run_monte_carlo_weekly_margin(
        payload,
        resolved_inputs,
        metric_specs,
        step_specs,
        runtime_tree,
        seed=scenario_seed,
    )

    horizons = resolve_horizons(payload.horizon_weeks, payload.horizons_weeks)
    primary_horizon = payload.horizon_weeks if payload.horizon_weeks in horizons else horizons[0]

    horizon_results: dict[str, dict[str, Any]] = {}
    for horizon in horizons:
        horizon_results[str(horizon)] = _compute_horizon_result(
            payload=payload,
            confidence=confidence,
            weekly_state=weekly_state,
            margin_samples_weekly=margin_samples_weekly,
            horizon_weeks=horizon,
        )

    primary_result = horizon_results[str(primary_horizon)]
    factor = horizon_factor_sum(
        weeks=primary_horizon,
        decay=payload.decay,
        discount_rate_annual=payload.discount_rate_annual,
    )
    per_segment = _build_per_segment_for_horizon(payload, confidence, weekly_state, factor=factor)
    per_metric_node = {
        key: value * factor * payload.p_success * confidence
        for key, value in weekly_state["node_contributions_weekly"].items()
    }

    return {
        "confidence": confidence,
        "primary_horizon": primary_horizon,
        "deterministic": primary_result["deterministic"],
        "probabilistic": primary_result["probabilistic"],
        "gross_impact": primary_result["gross_impact"],
        "net_incremental_impact": primary_result["net_incremental_impact"],
        "horizon_results": horizon_results,
        "per_segment": per_segment,
        "per_metric_node": per_metric_node,
        "screen_net_weekly": weekly_state["screen_net_weekly"],
        "screen_breakdown": _build_per_screen_breakdown(weekly_state, factor),
    }


def _compute_sensitivity(
    payload: ScoreRunRequest,
    resolved_inputs: ResolvedScoringInputs,
    *,
    mc_max_n: int,
    runtime_tree: RuntimeMetricTree,
    policy: dict[str, Any],
) -> dict[str, Any]:
    if not payload.sensitivity.enabled:
        return {
            "top_sensitive_inputs": [],
            "elasticity_summary": {},
            "tornado": [],
        }

    base_payload, _warnings, _effective_metrics = _prepare_payload_for_execution(payload, policy)
    base_result = _run_single_scenario(
        scenario_name="base_sensitivity",
        payload=base_payload,
        resolved_inputs=resolved_inputs,
        mc_max_n=mc_max_n,
        runtime_tree=runtime_tree,
    )
    base_metric = (
        base_result["deterministic"]["incremental_margin"]
        if payload.sensitivity.target_metric == "net_margin"
        else base_result["deterministic"]["priority_score"]
    )
    epsilon = payload.sensitivity.epsilon
    candidates = build_candidates(payload)
    entries: list[SensitivityEntry] = []

    for candidate in candidates:
        perturbed_payload = perturb_payload(payload, candidate, epsilon)
        perturbed_payload, _warnings, _effective_metrics = _prepare_payload_for_execution(perturbed_payload, policy)
        perturbed_result = _run_single_scenario(
            scenario_name=f"sens_{candidate.name}",
            payload=perturbed_payload,
            resolved_inputs=resolved_inputs,
            mc_max_n=mc_max_n,
            runtime_tree=runtime_tree,
        )
        perturbed_metric = (
            perturbed_result["deterministic"]["incremental_margin"]
            if payload.sensitivity.target_metric == "net_margin"
            else perturbed_result["deterministic"]["priority_score"]
        )
        delta = perturbed_metric - base_metric
        if abs(base_metric) < 1e-9:
            elasticity = 0.0
        else:
            elasticity = (delta / base_metric) / epsilon
        entries.append(SensitivityEntry(input=candidate.name, elasticity=elasticity, delta_value=delta))

    entries.sort(key=lambda item: abs(item.elasticity), reverse=True)
    top_entries = entries[: payload.sensitivity.top_n]
    return {
        "top_sensitive_inputs": [item.model_dump(mode="json") for item in top_entries],
        "elasticity_summary": {item.input: item.elasticity for item in top_entries},
        "tornado": [item.model_dump(mode="json") for item in entries],
    }


def _build_scenario_comparison(scenarios: dict[str, dict[str, Any]]) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    if "conservative" in scenarios and "base" in scenarios:
        result["conservative_vs_base"] = {
            "delta_net_margin": scenarios["base"]["deterministic"]["incremental_margin"]
            - scenarios["conservative"]["deterministic"]["incremental_margin"],
            "delta_priority_score": scenarios["base"]["deterministic"]["priority_score"]
            - scenarios["conservative"]["deterministic"]["priority_score"],
        }
    if "base" in scenarios and "upside" in scenarios:
        result["base_vs_upside"] = {
            "delta_net_margin": scenarios["upside"]["deterministic"]["incremental_margin"]
            - scenarios["base"]["deterministic"]["incremental_margin"],
            "delta_priority_score": scenarios["upside"]["deterministic"]["priority_score"]
            - scenarios["base"]["deterministic"]["priority_score"],
        }
    return result


def run_scoring(payload: ScoreRunRequest, resolved_inputs: ResolvedScoringInputs, mc_max_n: int) -> ScoringResult:
    policy_source = resolved_inputs.scoring_policy_source
    policy_version = policy_source.split(":")[-1]
    policy_name = policy_source.split(":")[1] if ":" in policy_source else "builtin_ev_policy"
    policy = normalize_policy(
        resolved_inputs.scoring_policy_snapshot,
        source_name=policy_name,
        source_version=policy_version,
    )
    if resolved_inputs.learning_config:
        learning_config = LearningConfig.model_validate(resolved_inputs.learning_config)
    else:
        learning_config = resolve_learning_config(payload.learning, policy)

    scenarios_payload = materialize_scenarios(payload)
    scenario_outputs: dict[str, dict[str, Any]] = {}
    scenario_learning: dict[str, LearningApplication] = {}
    validation_warnings: list[str] = []
    learning_warnings: list[str] = []
    effective_input_metrics: list[str] = []
    derived_output_metrics: list[str] = sorted(policy.get("derived_metrics", []))
    runtime_tree = build_runtime_metric_tree(
        resolved_inputs.metric_tree_definition or policy.get("runtime_metric_tree")
    )

    for scenario_name, scenario_payload in scenarios_payload.items():
        scenario_payload = scenario_payload.model_copy(deep=True)
        scenario_payload, preparation_warnings, preparation_effective_metrics = _prepare_payload_for_execution(
            scenario_payload,
            policy,
        )
        validation_warnings.extend([f"[{scenario_name}] {warning}" for warning in preparation_warnings])
        effective_input_metrics = sorted(set(effective_input_metrics).union(preparation_effective_metrics))

        learning_out = apply_learning_to_payload(
            scenario_payload,
            learning_config=learning_config,
            evidence_rows=resolved_inputs.learning_evidence,
        )
        scenario_payload = learning_out.payload
        scenario_learning[scenario_name] = learning_out
        learning_warnings.extend([f"[{scenario_name}] {warning}" for warning in learning_out.learning_warnings])

        scenario_outputs[scenario_name] = _run_single_scenario(
            scenario_name=scenario_name,
            payload=scenario_payload,
            resolved_inputs=resolved_inputs,
            mc_max_n=mc_max_n,
            runtime_tree=runtime_tree,
        )

    base_key = "base" if "base" in scenario_outputs else next(iter(scenario_outputs.keys()))
    base_result = scenario_outputs[base_key]
    base_learning = scenario_learning.get(base_key)
    sensitivity_payload = base_learning.payload if base_learning else scenarios_payload[base_key]
    sensitivity_output = _compute_sensitivity(
        sensitivity_payload,
        resolved_inputs,
        mc_max_n=mc_max_n,
        runtime_tree=runtime_tree,
        policy=policy,
    )

    per_screen = base_result.get("screen_net_weekly", {})
    top_screens = sorted(
        [{"screen": screen, "net_margin_weekly": values.get("margin", 0.0)} for screen, values in per_screen.items()],
        key=lambda item: item["net_margin_weekly"],
        reverse=True,
    )[:5]
    top_segments = sorted(
        [
            {
                "segment_id": segment_id,
                "net_margin": values.get("incremental_margin", 0.0),
                "effective_mau": values.get("effective_mau", 0.0),
            }
            for segment_id, values in base_result["per_segment"].items()
        ],
        key=lambda item: item["net_margin"],
        reverse=True,
    )[:5]
    top_nodes = sorted(
        [{"node": node, "contribution": value} for node, value in base_result["per_metric_node"].items()],
        key=lambda item: abs(item["contribution"]),
        reverse=True,
    )[:5]

    largest_risk_driver = None
    if sensitivity_output["top_sensitive_inputs"]:
        largest_risk_driver = sensitivity_output["top_sensitive_inputs"][0]["input"]
    primary_driver = classify_primary_driver(base_result["per_metric_node"])
    gross_margin = base_result["gross_impact"]["margin"]
    net_margin = base_result["net_incremental_impact"]["margin"]
    cannibalization_loss = gross_margin - net_margin
    summary_text = build_summary_text(
        primary_driver=primary_driver,
        top_segment=top_segments[0]["segment_id"] if top_segments else None,
        top_screen=top_screens[0]["screen"] if top_screens else None,
        cannibalization_loss=cannibalization_loss,
    )

    explainability = {
        "top_segments": top_segments,
        "top_screens": top_screens,
        "top_nodes": top_nodes,
        "primary_driver": primary_driver,
        "largest_risk_driver": largest_risk_driver,
        "cannibalization_summary": (
            f"Gross margin delta={gross_margin:,.2f}; net margin delta={net_margin:,.2f}; "
            f"loss due to cannibalization={cannibalization_loss:,.2f}"
        ),
        "historical_evidence_summary": (
            f"{base_learning.learning_summary.get('evidence_count', 0)} evidences matched; "
            f"posterior shift {(base_learning.evidence_impact_ratio or 0.0) * 100:.1f}%."
            if base_learning and base_learning.learning_summary
            else "Historical A/B evidence not applied."
        ),
        "summary_text": summary_text,
    }

    scenario_comparison = _build_scenario_comparison(scenario_outputs)

    scenario_response = {
        scenario_name: {
            "deterministic": result["deterministic"],
            "probabilistic": result["probabilistic"],
            "gross_impact": _format_impact_breakdown(result["gross_impact"]),
            "net_incremental_impact": _format_impact_breakdown(result["net_incremental_impact"]),
            "horizon_results": {
                horizon: {
                    "deterministic": horizon_result["deterministic"],
                    "probabilistic": horizon_result["probabilistic"],
                    "gross_impact": _format_impact_breakdown(horizon_result["gross_impact"]),
                    "net_incremental_impact": _format_impact_breakdown(horizon_result["net_incremental_impact"]),
                    "discounted_summary": horizon_result["discounted_summary"],
                }
                for horizon, horizon_result in result["horizon_results"].items()
            },
        }
        for scenario_name, result in scenario_outputs.items()
    }

    return ScoringResult(
        deterministic=base_result["deterministic"],
        probabilistic=base_result["probabilistic"],
        per_segment=base_result["per_segment"],
        per_metric_node=base_result["per_metric_node"],
        confidence=base_result["confidence"],
        gross_impact=_format_impact_breakdown(base_result["gross_impact"]),
        net_incremental_impact=_format_impact_breakdown(base_result["net_incremental_impact"]),
        horizon_results={
            horizon: {
                "deterministic": horizon_result["deterministic"],
                "probabilistic": horizon_result["probabilistic"],
                "gross_impact": _format_impact_breakdown(horizon_result["gross_impact"]),
                "net_incremental_impact": _format_impact_breakdown(horizon_result["net_incremental_impact"]),
                "discounted_summary": horizon_result["discounted_summary"],
            }
            for horizon, horizon_result in base_result["horizon_results"].items()
        },
        scenarios=scenario_response,
        scenario_comparison=scenario_comparison,
        sensitivity=sensitivity_output,
        explainability=explainability,
        effective_input_metrics=effective_input_metrics,
        derived_output_metrics=derived_output_metrics,
        validation_warnings=validation_warnings,
        learning_applied=bool(base_learning.learning_applied if base_learning else False),
        learning_summary=base_learning.learning_summary if base_learning else None,
        learning_warnings=learning_warnings,
        scoring_policy_version=policy_version,
        scoring_policy_source=policy_source,
        per_screen_breakdown=base_result.get("screen_breakdown", {}),
    )
