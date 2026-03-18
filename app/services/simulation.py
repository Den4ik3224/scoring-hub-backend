from typing import Iterable

import numpy as np

from app.api.schemas.score import DistributionSpec, UpliftSpec


def uplift_mean(spec: UpliftSpec) -> float:
    if isinstance(spec, (int, float)):
        return float(spec)

    if spec.type == "point":
        if spec.value is not None:
            return float(spec.value)
        if spec.mean is not None:
            return float(spec.mean)
        raise ValueError("point distribution missing value")

    if spec.type == "normal":
        return float(spec.mean or 0.0)

    if spec.type == "lognormal":
        mu = float(spec.mean or 0.0)
        sigma = float(spec.sd or 0.0)
        return float(np.exp(mu + (sigma**2) / 2.0) - 1.0)

    if spec.type == "triangular":
        return float(((spec.low or 0.0) + (spec.mode or 0.0) + (spec.high or 0.0)) / 3.0)

    raise ValueError(f"Unsupported distribution type {spec.type}")


def sample_uplift(spec: UpliftSpec, n: int, rng: np.random.Generator) -> np.ndarray:
    if isinstance(spec, (int, float)):
        return np.full(shape=n, fill_value=float(spec), dtype=np.float64)

    dist: DistributionSpec = spec

    if dist.type == "point":
        value = dist.value if dist.value is not None else dist.mean
        if value is None:
            raise ValueError("point distribution missing value")
        return np.full(shape=n, fill_value=float(value), dtype=np.float64)

    if dist.type == "normal":
        return rng.normal(loc=float(dist.mean), scale=float(dist.sd), size=n)

    if dist.type == "lognormal":
        return rng.lognormal(mean=float(dist.mean), sigma=float(dist.sd), size=n) - 1.0

    if dist.type == "triangular":
        return rng.triangular(left=float(dist.low), mode=float(dist.mode), right=float(dist.high), size=n)

    raise ValueError(f"Unsupported distribution type {dist.type}")


def summarize_samples(samples: np.ndarray, bins: int = 20) -> dict:
    mean = float(np.mean(samples))
    median = float(np.median(samples))
    p5 = float(np.quantile(samples, 0.05))
    p95 = float(np.quantile(samples, 0.95))
    stddev = float(np.std(samples))
    prob_negative = float(np.mean(samples < 0.0))
    cv = float(stddev / mean) if mean != 0 else 0.0

    counts, edges = np.histogram(samples, bins=bins)
    histogram = [
        {
            "lower": float(edges[i]),
            "upper": float(edges[i + 1]),
            "count": int(counts[i]),
        }
        for i in range(len(counts))
    ]

    return {
        "mean": mean,
        "median": median,
        "p5": p5,
        "p95": p95,
        "prob_negative": prob_negative,
        "stddev": stddev,
        "cv": cv,
        "histogram": histogram,
    }


def compose_uplifts_multiplicative(uplifts: Iterable[float]) -> float:
    result = 1.0
    for uplift in uplifts:
        result *= 1.0 + uplift
    return result - 1.0
