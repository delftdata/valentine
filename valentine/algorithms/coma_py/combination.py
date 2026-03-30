from __future__ import annotations


def average(values: list[float]) -> float:
    """Arithmetic mean. Returns 0.0 for empty list."""
    if not values:
        return 0.0
    return sum(values) / len(values)


def maximum(values: list[float]) -> float:
    """Maximum value. Returns 0.0 for empty list."""
    if not values:
        return 0.0
    return max(values)


def weighted(values: list[float], weights: list[float]) -> float:
    """Weighted average. Returns 0.0 for empty list."""
    if not values or not weights:
        return 0.0
    total_weight = sum(weights)
    if total_weight == 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights, strict=True)) / total_weight


# Set combination functions operate the same way but are named distinctly
# for clarity in the matching pipeline (they combine across resolved element sets).
set_average = average
set_highest = maximum
