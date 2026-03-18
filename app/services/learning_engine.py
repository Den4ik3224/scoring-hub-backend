from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import sqrt
from typing import Any

from app.api.schemas.score import DistributionSpec, LearningConfig, ScoreRunRequest
from app.services.simulation import uplift_mean


DEFAULT_LEARNING_CONFIG: dict[str, Any] = {
    "mode": "bayesian",
    "lookback_days": 730,
    "half_life_days": 180,
    "min_quality": 0.6,
    "min_sample_size": 500,
}


@dataclass
class LearningApplication:
    payload: ScoreRunRequest
    learning_applied: bool
    learning_summary: dict[str, Any] | None
    learning_warnings: list[str]
    evidence_ids: list[str]
    evidence_impact_ratio: float | None


def resolve_learning_config(
    payload_learning: LearningConfig | None,
    policy_snapshot: dict[str, Any] | None,
) -> LearningConfig:
    raw = dict(DEFAULT_LEARNING_CONFIG)
    if policy_snapshot:
        raw.update(policy_snapshot.get("learning_defaults", {}))
    if payload_learning:
        raw.update(payload_learning.model_dump(mode="json", exclude_none=True))
    return LearningConfig.model_validate(raw)


def scoring_metric_drivers(payload: ScoreRunRequest) -> list[str]:
    drivers: set[str] = set()
    for target in payload.metric_targets:
        if target.node_type == "funnel_step":
            step_id = target.target_id or target.node
            drivers.add(step_id)
            drivers.add(f"funnel_step:{step_id}")
        else:
            drivers.add(target.metric_key or target.node)
    for segment in payload.segments:
        for key in segment.uplifts.keys():
            drivers.add(key)
    return sorted(drivers)


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _prior_stats(spec: Any) -> tuple[float, float]:
    if isinstance(spec, (int, float)):
        value = float(spec)
        return value, max(0.05, abs(value) * 0.5)

    parsed = DistributionSpec.model_validate(spec)
    mean = uplift_mean(parsed)
    if parsed.type in {"normal", "lognormal"} and parsed.sd is not None:
        sd = max(1e-6, float(parsed.sd))
    elif parsed.type == "triangular":
        low = _safe_float(parsed.low, mean)
        mode = _safe_float(parsed.mode, mean)
        high = _safe_float(parsed.high, mean)
        variance = max(0.0, (low * low + mode * mode + high * high - low * mode - low * high - mode * high) / 18.0)
        sd = max(1e-6, sqrt(variance))
    else:
        sd = max(0.05, abs(mean) * 0.5)
    return mean, sd


def _evidence_weight(row: dict[str, Any], *, half_life_days: int, segment_id: str | None, as_of: datetime) -> float:
    quality = min(1.0, max(0.0, _safe_float(row.get("quality_score"), 0.0)))
    end_at = _as_utc(row.get("end_at"))
    age_days = max((as_of - end_at).total_seconds() / 86400.0, 0.0)
    recency = 0.5 ** (age_days / max(1.0, float(half_life_days)))
    similarity = 1.0 if segment_id and row.get("segment_id") == segment_id else 0.6
    significance_bonus = 1.0 if bool(row.get("significance_flag")) else 0.85
    return quality * recency * similarity * significance_bonus


def _evidence_stats(
    evidence_rows: list[dict[str, Any]],
    *,
    half_life_days: int,
    segment_id: str | None,
) -> tuple[float, float, float]:
    if not evidence_rows:
        return 0.0, 0.1, 0.0

    as_of = datetime.now(timezone.utc)
    weighted_sum = 0.0
    weighted_var_sum = 0.0
    weight_total = 0.0
    for row in evidence_rows:
        uplift = _safe_float(row.get("observed_uplift"), 0.0)
        ci_low = row.get("ci_low")
        ci_high = row.get("ci_high")
        if ci_low is not None and ci_high is not None:
            obs_sd = max(1e-6, (_safe_float(ci_high, uplift) - _safe_float(ci_low, uplift)) / 3.92)
        else:
            obs_sd = max(0.02, abs(uplift) * 0.5)
        weight = _evidence_weight(row, half_life_days=half_life_days, segment_id=segment_id, as_of=as_of)
        weighted_sum += uplift * weight
        weighted_var_sum += (obs_sd * obs_sd) * weight
        weight_total += weight

    if weight_total <= 1e-9:
        return 0.0, 0.1, 0.0
    return weighted_sum / weight_total, sqrt(max(1e-12, weighted_var_sum / weight_total)), weight_total


def _posterior(prior_mean: float, prior_sd: float, obs_mean: float, obs_sd: float, obs_weight: float) -> tuple[float, float]:
    prior_var = max(1e-12, prior_sd * prior_sd)
    obs_var = max(1e-12, (obs_sd * obs_sd) / max(obs_weight, 1e-9))
    prior_precision = 1.0 / prior_var
    obs_precision = 1.0 / obs_var
    posterior_var = 1.0 / (prior_precision + obs_precision)
    posterior_mean = posterior_var * (prior_mean * prior_precision + obs_mean * obs_precision)
    return posterior_mean, sqrt(max(1e-12, posterior_var))


def _find_evidence(
    evidence_rows: list[dict[str, Any]],
    *,
    metric_driver_keys: set[str],
    segment_id: str | None,
    screen: str,
) -> list[dict[str, Any]]:
    exact_segment: list[dict[str, Any]] = []
    fallback_screen_only: list[dict[str, Any]] = []
    for row in evidence_rows:
        driver = str(row.get("metric_driver") or "")
        row_screen = str(row.get("screen") or "")
        row_segment = row.get("segment_id")
        if driver not in metric_driver_keys or row_screen != screen:
            continue
        if segment_id is None:
            if row_segment is None:
                fallback_screen_only.append(row)
            continue
        if row_segment == segment_id:
            exact_segment.append(row)
        elif row_segment is None:
            fallback_screen_only.append(row)
    return exact_segment or fallback_screen_only


def apply_learning_to_payload(
    payload: ScoreRunRequest,
    *,
    learning_config: LearningConfig,
    evidence_rows: list[dict[str, Any]],
) -> LearningApplication:
    if learning_config.mode == "off":
        return LearningApplication(
            payload=payload,
            learning_applied=False,
            learning_summary=None,
            learning_warnings=[],
            evidence_ids=[],
            evidence_impact_ratio=None,
        )

    mutable_payload = payload.model_copy(deep=True)
    warnings: list[str] = []
    evidence_ids: set[str] = set()
    priors: list[tuple[float, float]] = []
    posteriors: list[tuple[float, float]] = []
    selected_screens = list(dict.fromkeys(payload.screens))
    single_screen = selected_screens[0] if len(selected_screens) == 1 else None
    single_segment_id = payload.segments[0].id if len(payload.segments) == 1 else None

    if single_screen is None:
        warnings.append(
            "Historical A/B learning is screen-specific. Automatic learning was skipped because the run targets multiple screens."
        )
    if single_screen is not None and len(payload.segments) > 1 and payload.metric_targets:
        warnings.append(
            "Metric-target learning uses exact segment matching only for single-segment runs. Multi-segment runs fall back to same-screen evidence without segment binding."
        )

    if single_screen is not None:
        for index, target in enumerate(mutable_payload.metric_targets):
            if target.node_type == "funnel_step":
                target_id = target.target_id or target.node
                keys = {target_id, f"funnel_step:{target_id}"}
                label = f"funnel_step:{target_id}"
            else:
                metric_key = target.metric_key or target.node
                keys = {metric_key}
                label = metric_key

            matched = _find_evidence(
                evidence_rows,
                metric_driver_keys=keys,
                segment_id=single_segment_id,
                screen=single_screen,
            )
            if not matched:
                continue

            prior_mean, prior_sd = _prior_stats(target.uplift_dist)
            obs_mean, obs_sd, obs_weight = _evidence_stats(
                matched,
                half_life_days=learning_config.half_life_days,
                segment_id=single_segment_id,
            )
            if obs_weight <= 1e-9:
                continue
            posterior_mean, posterior_sd = _posterior(prior_mean, prior_sd, obs_mean, obs_sd, obs_weight)
            priors.append((prior_mean, prior_sd))
            posteriors.append((posterior_mean, posterior_sd))
            for row in matched:
                evidence_ids.add(str(row.get("id")))

            if learning_config.mode == "bayesian":
                mutable_payload.metric_targets[index] = target.model_copy(
                    update={"uplift_dist": DistributionSpec(type="point", value=posterior_mean)}
                )
            else:
                warnings.append(
                    f"Advisory learning for `{label}` on screen `{single_screen}`: prior={prior_mean:.4f}, posterior={posterior_mean:.4f}"
                )

        for segment in mutable_payload.segments:
            for uplift_key, uplift_value in list(segment.uplifts.items()):
                matched = _find_evidence(
                    evidence_rows,
                    metric_driver_keys={uplift_key, f"funnel_step:{uplift_key}"},
                    segment_id=segment.id,
                    screen=single_screen,
                )
                if not matched:
                    continue
                prior_mean, prior_sd = _prior_stats(uplift_value)
                obs_mean, obs_sd, obs_weight = _evidence_stats(
                    matched,
                    half_life_days=learning_config.half_life_days,
                    segment_id=segment.id,
                )
                if obs_weight <= 1e-9:
                    continue
                posterior_mean, posterior_sd = _posterior(prior_mean, prior_sd, obs_mean, obs_sd, obs_weight)
                priors.append((prior_mean, prior_sd))
                posteriors.append((posterior_mean, posterior_sd))
                for row in matched:
                    evidence_ids.add(str(row.get("id")))
                if learning_config.mode == "bayesian":
                    segment.uplifts[uplift_key] = DistributionSpec(type="point", value=posterior_mean)
                else:
                    warnings.append(
                        f"Advisory learning for segment `{segment.id}` driver `{uplift_key}` on screen `{single_screen}`: "
                        f"prior={prior_mean:.4f}, posterior={posterior_mean:.4f}"
                    )

    if not priors or not posteriors:
        if learning_config.mode != "off":
            warnings.append("No matching historical A/B evidence found for selected screens/segments/metrics.")
        return LearningApplication(
            payload=mutable_payload,
            learning_applied=False,
            learning_summary={
                "prior_mean": 0.0,
                "prior_std": 0.0,
                "posterior_mean": 0.0,
                "posterior_std": 0.0,
                "evidence_count": 0,
                "evidence_ids": [],
            },
            learning_warnings=warnings,
            evidence_ids=[],
            evidence_impact_ratio=0.0,
        )

    prior_mean_avg = sum(item[0] for item in priors) / len(priors)
    prior_std_avg = sum(item[1] for item in priors) / len(priors)
    posterior_mean_avg = sum(item[0] for item in posteriors) / len(posteriors)
    posterior_std_avg = sum(item[1] for item in posteriors) / len(posteriors)
    evidence_impact_ratio = 0.0
    if abs(prior_mean_avg) > 1e-9:
        evidence_impact_ratio = abs(posterior_mean_avg - prior_mean_avg) / abs(prior_mean_avg)

    if learning_config.mode == "bayesian":
        warnings.append(
            f"Bayesian learning applied using {len(evidence_ids)} historical A/B evidences."
        )

    return LearningApplication(
        payload=mutable_payload,
        learning_applied=learning_config.mode == "bayesian",
        learning_summary={
            "prior_mean": prior_mean_avg,
            "prior_std": prior_std_avg,
            "posterior_mean": posterior_mean_avg,
            "posterior_std": posterior_std_avg,
            "evidence_count": len(evidence_ids),
            "evidence_ids": sorted(evidence_ids),
        },
        learning_warnings=warnings,
        evidence_ids=sorted(evidence_ids),
        evidence_impact_ratio=evidence_impact_ratio,
    )
