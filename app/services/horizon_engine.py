import numpy as np

from app.api.schemas.score import DecayConfig


def resolve_horizons(default_horizon: int, requested_horizons: list[int] | None) -> list[int]:
    if requested_horizons:
        unique = sorted(set(requested_horizons))
        return unique
    return [default_horizon]


def weekly_factors(
    weeks: int,
    decay: DecayConfig | None,
    discount_rate_annual: float | None,
) -> np.ndarray:
    factors = np.ones(shape=weeks, dtype=np.float64)
    for idx in range(weeks):
        t = idx + 1
        decay_factor = 1.0
        if decay:
            if decay.type == "exponential":
                assert decay.half_life_weeks is not None
                decay_factor = 0.5 ** ((t - 1) / decay.half_life_weeks)
            elif decay.type == "linear":
                decay_factor = max(decay.linear_floor, 1.0 - ((t - 1) / max(weeks, 1)))

        discount_factor = 1.0
        if discount_rate_annual is not None:
            discount_factor = 1.0 / ((1.0 + discount_rate_annual) ** (t / 52.0))

        factors[idx] = decay_factor * discount_factor
    return factors


def horizon_factor_sum(
    weeks: int,
    decay: DecayConfig | None,
    discount_rate_annual: float | None,
) -> float:
    return float(np.sum(weekly_factors(weeks=weeks, decay=decay, discount_rate_annual=discount_rate_annual)))
