from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np

from app.services.monthly_baselines import FunnelStepAggregate


@dataclass
class FunnelState:
    baseline_conversion: float
    updated_conversion: float
    used_funnel: bool


def build_step_id_lookup(funnel_index: dict[tuple[str, str], list[FunnelStepAggregate]] | None) -> set[tuple[str, str, str]]:
    if not funnel_index:
        return set()
    return {
        (segment_id, screen, row.step_id)
        for (segment_id, screen), rows in funnel_index.items()
        for row in rows
    }


def screen_conversion(rows: Iterable[FunnelStepAggregate]) -> float:
    value = 1.0
    has_rows = False
    for row in rows:
        has_rows = True
        value *= float(np.clip(row.baseline_rate, 0.0, 1.0))
    return float(np.clip(value if has_rows else 0.0, 0.0, 1.0))


def resolve_funnel_conversion(
    *,
    funnel_rows: list[FunnelStepAggregate] | None,
    step_uplifts: dict[str, float],
    conversion_uplift: float,
) -> FunnelState:
    if not funnel_rows:
        return FunnelState(baseline_conversion=0.0, updated_conversion=0.0, used_funnel=False)

    base_product = 1.0
    updated_product = 1.0
    for row in funnel_rows:
        rate = float(np.clip(row.baseline_rate, 0.0, 1.0))
        effective_uplift = float(step_uplifts.get(row.step_id, 0.0))
        base_product *= rate
        updated_product *= float(np.clip(rate * (1.0 + effective_uplift), 0.0, 1.0))

    updated_conversion = float(np.clip(updated_product * (1.0 + conversion_uplift), 0.0, 1.0))
    return FunnelState(
        baseline_conversion=float(np.clip(base_product, 0.0, 1.0)),
        updated_conversion=updated_conversion,
        used_funnel=True,
    )
