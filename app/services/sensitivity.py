from copy import deepcopy
from dataclasses import dataclass
from typing import Callable

from app.api.schemas.score import ScoreRunRequest


@dataclass
class SensitivityCandidate:
    name: str
    apply: Callable[[ScoreRunRequest, float], None]


def build_candidates(payload: ScoreRunRequest) -> list[SensitivityCandidate]:
    candidates: list[SensitivityCandidate] = []

    for idx, target in enumerate(payload.metric_targets):
        metric_name = target.target_id if target.node_type == "funnel_step" else (target.metric_key or target.node)

        def _apply_metric(p: ScoreRunRequest, epsilon: float, index: int = idx) -> None:
            base_target = p.metric_targets[index]
            if isinstance(base_target.uplift_dist, (int, float)):
                p.metric_targets[index].uplift_dist = float(base_target.uplift_dist) * (1.0 + epsilon)
            else:
                if base_target.uplift_dist.value is not None:
                    base_target.uplift_dist.value = base_target.uplift_dist.value * (1.0 + epsilon)
                elif base_target.uplift_dist.mean is not None:
                    base_target.uplift_dist.mean = base_target.uplift_dist.mean * (1.0 + epsilon)

        candidates.append(SensitivityCandidate(name=f"metric_target:{metric_name}:{idx}", apply=_apply_metric))

    for seg_idx, segment in enumerate(payload.segments):
        for uplift_key in segment.uplifts:
            def _apply_segment_uplift(
                p: ScoreRunRequest,
                epsilon: float,
                index: int = seg_idx,
                key: str = uplift_key,
            ) -> None:
                current = p.segments[index].uplifts[key]
                if isinstance(current, (int, float)):
                    p.segments[index].uplifts[key] = float(current) * (1.0 + epsilon)
                    return
                if current.value is not None:
                    current.value = current.value * (1.0 + epsilon)
                elif current.mean is not None:
                    current.mean = current.mean * (1.0 + epsilon)

            candidates.append(
                SensitivityCandidate(
                    name=f"segment_uplift:{segment.id}:{uplift_key}",
                    apply=_apply_segment_uplift,
                )
            )

        def _apply_segment_pen(p: ScoreRunRequest, epsilon: float, index: int = seg_idx) -> None:
            p.segments[index].penetration = min(1.0, p.segments[index].penetration * (1.0 + epsilon))

        candidates.append(SensitivityCandidate(name=f"segment_penetration:{segment.id}", apply=_apply_segment_pen))

        if segment.screen_penetration:
            for screen_key in segment.screen_penetration:
                def _apply_screen_pen(
                    p: ScoreRunRequest,
                    epsilon: float,
                    index: int = seg_idx,
                    screen: str = screen_key,
                ) -> None:
                    current = p.segments[index].screen_penetration or {}
                    current[screen] = min(1.0, current[screen] * (1.0 + epsilon))
                    p.segments[index].screen_penetration = current

                candidates.append(
                    SensitivityCandidate(
                        name=f"screen_penetration:{segment.id}:{screen_key}",
                        apply=_apply_screen_pen,
                    )
                )

    def _apply_p_success(p: ScoreRunRequest, epsilon: float) -> None:
        p.p_success = min(1.0, p.p_success * (1.0 + epsilon))

    candidates.append(SensitivityCandidate(name="p_success", apply=_apply_p_success))

    def _apply_confidence(p: ScoreRunRequest, epsilon: float) -> None:
        if p.confidence is not None:
            p.confidence = min(1.0, p.confidence * (1.0 + epsilon))

    candidates.append(SensitivityCandidate(name="confidence", apply=_apply_confidence))
    return candidates


def perturb_payload(payload: ScoreRunRequest, candidate: SensitivityCandidate, epsilon: float) -> ScoreRunRequest:
    perturbed = deepcopy(payload)
    candidate.apply(perturbed, epsilon)
    return perturbed
