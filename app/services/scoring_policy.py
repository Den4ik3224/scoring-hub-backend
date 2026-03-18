from dataclasses import dataclass
from typing import Any

from app.api.schemas.score import DistributionSpec, MetricTargetInput, SegmentInput
from app.core.errors import ValidationError
from app.services.simulation import uplift_mean

DEFAULT_SCORING_POLICY: dict[str, Any] = {
    "name": "builtin_ev_policy",
    "version": "1",
    "primitive_metrics": [
        "mau",
        "penetration",
        "conversion",
        "frequency",
        "frequency_monthly",
        "aoq",
        "aiv",
        "fm_pct",
    ],
    "derived_metrics": [
        "orders",
        "items",
        "aov",
        "rto",
        "fm",
    ],
    "translator_enabled": True,
    "translations": {
        "aov": {
            "to": ["aoq", "aiv"],
            "weights": {"aoq": 0.5, "aiv": 0.5},
        }
    },
    "default_horizons": [4, 13, 26, 52],
}


@dataclass
class PreparedTargets:
    targets: list[MetricTargetInput]
    effective_input_metrics: list[str]
    derived_output_metrics: list[str]
    warnings: list[str]


@dataclass
class PreparedSegments:
    segments: list[SegmentInput]
    effective_input_metrics: list[str]
    warnings: list[str]


def normalize_policy(raw_policy: dict[str, Any] | None, *, source_name: str, source_version: str) -> dict[str, Any]:
    merged = dict(DEFAULT_SCORING_POLICY)
    if raw_policy:
        merged.update(raw_policy)
    merged["name"] = source_name
    merged["version"] = source_version
    merged.setdefault("translations", {})
    merged.setdefault("default_horizons", [4, 13, 26, 52])
    merged.setdefault("translator_enabled", True)
    raw_primitive = raw_policy.get("primitive_metrics", []) if raw_policy else []
    raw_derived = raw_policy.get("derived_metrics", []) if raw_policy else []
    merged["primitive_metrics"] = list(dict.fromkeys([*DEFAULT_SCORING_POLICY["primitive_metrics"], *raw_primitive]))
    merged["derived_metrics"] = list(dict.fromkeys([*DEFAULT_SCORING_POLICY["derived_metrics"], *raw_derived]))
    return merged


def _translate_aov_target(target: MetricTargetInput, weights: dict[str, float]) -> list[MetricTargetInput]:
    uplift = uplift_mean(target.uplift_dist)
    w_aoq = float(weights.get("aoq", 0.5))
    w_aiv = float(weights.get("aiv", 0.5))
    translated_aoq = (1.0 + uplift) ** w_aoq - 1.0
    translated_aiv = (1.0 + uplift) ** w_aiv - 1.0
    return [
        MetricTargetInput(
            node="aoq",
            metric_key="aoq",
            node_type="metric",
            uplift_dist=DistributionSpec(type="point", value=translated_aoq),
        ),
        MetricTargetInput(
            node="aiv",
            metric_key="aiv",
            node_type="metric",
            uplift_dist=DistributionSpec(type="point", value=translated_aiv),
        ),
    ]


def prepare_metric_targets(
    metric_targets: list[MetricTargetInput],
    policy: dict[str, Any],
) -> PreparedTargets:
    primitive = set(policy.get("primitive_metrics", []))
    derived = set(policy.get("derived_metrics", []))
    translator_enabled = bool(policy.get("translator_enabled", False))
    translations = policy.get("translations", {})

    warnings: list[str] = []
    effective_targets: list[MetricTargetInput] = []

    metric_keys = [
        (target.metric_key or target.node)
        for target in metric_targets
        if target.node_type == "metric"
    ]
    metric_key_set = set(metric_keys)
    has_aov_conflict = "aov" in metric_key_set and ("aoq" in metric_key_set or "aiv" in metric_key_set)

    if has_aov_conflict and not translator_enabled:
        raise ValidationError(
            "Target metric AOV is derived from AOQ × AIV. Do not provide simultaneous uplifts for AOV and AOQ/AIV in the same run."
        )

    for target in metric_targets:
        if target.node_type == "funnel_step":
            effective_targets.append(target)
            continue

        metric_key = target.metric_key or target.node
        if metric_key.startswith("aoq_component:"):
            raise ValidationError(
                "AOQ component targets are no longer supported. Use primitive `aoq` uplift for basket-size changes."
            )
        if metric_key in derived:
            if metric_key == "aov" and translator_enabled and "aov" in translations:
                weights = translations["aov"].get("weights", {"aoq": 0.5, "aiv": 0.5})
                effective_targets.extend(_translate_aov_target(target, weights=weights))
                warnings.append(
                    "Target metric AOV is derived from AOQ × AIV and was translated to primitive drivers."
                )
                continue
            raise ValidationError(
                f"Target metric `{metric_key}` is derived. Provide uplifts on primitive drivers instead."
            )

        if metric_key not in primitive:
            raise ValidationError(
                f"Unsupported target metric `{metric_key}` for current scoring policy."
            )
        effective_targets.append(target)

    if has_aov_conflict and translator_enabled:
        warnings.append(
            "AOV was provided with AOQ/AIV; translation applied under scoring policy and results may be conservative."
        )

    effective_metric_keys = sorted(
        {
            (target.metric_key or target.node)
            for target in effective_targets
            if target.node_type == "metric"
        }
    )
    return PreparedTargets(
        targets=effective_targets,
        effective_input_metrics=effective_metric_keys,
        derived_output_metrics=sorted(derived),
        warnings=warnings,
    )


def prepare_segments(
    segments: list[SegmentInput],
    policy: dict[str, Any],
) -> PreparedSegments:
    primitive = set(policy.get("primitive_metrics", []))
    derived = set(policy.get("derived_metrics", []))
    translator_enabled = bool(policy.get("translator_enabled", False))
    translations = policy.get("translations", {})

    warnings: list[str] = []
    effective_input_metrics: set[str] = set()
    prepared_segments: list[SegmentInput] = []

    for segment in segments:
        new_uplifts: dict[str, Any] = {}
        has_aov_conflict = "aov" in segment.uplifts and ("aoq" in segment.uplifts or "aiv" in segment.uplifts)
        if has_aov_conflict:
            raise ValidationError(
                f"Segment `{segment.id}` provides simultaneous AOV and AOQ/AIV uplifts. "
                "Use primitive AOQ/AIV drivers directly."
            )
        for key, uplift in segment.uplifts.items():
            if key.startswith("aoq_component:"):
                raise ValidationError(
                    "AOQ component targets are no longer supported. Use primitive `aoq` uplift for basket-size changes."
                )

            if key in derived:
                if key == "aov" and translator_enabled and "aov" in translations:
                    translated = _translate_aov_target(
                        MetricTargetInput(node="aov", metric_key="aov", node_type="metric", uplift_dist=uplift),
                        weights=translations["aov"].get("weights", {"aoq": 0.5, "aiv": 0.5}),
                    )
                    for translated_target in translated:
                        new_uplifts[translated_target.metric_key or translated_target.node] = translated_target.uplift_dist
                        effective_input_metrics.add(translated_target.metric_key or translated_target.node)
                    warnings.append(
                        f"Segment `{segment.id}` AOV uplift was translated to primitive AOQ/AIV drivers."
                    )
                    continue
                raise ValidationError(
                    f"Segment uplift `{key}` is derived. Provide uplifts on primitive drivers instead."
                )

            if key in primitive:
                new_uplifts[key] = uplift
                effective_input_metrics.add(key)
                continue

            # Unknown keys are allowed here because they can legitimately be funnel-step ids.
            new_uplifts[key] = uplift

        prepared_segments.append(segment.model_copy(update={"uplifts": new_uplifts}, deep=True))

    return PreparedSegments(
        segments=prepared_segments,
        effective_input_metrics=sorted(effective_input_metrics),
        warnings=warnings,
    )
