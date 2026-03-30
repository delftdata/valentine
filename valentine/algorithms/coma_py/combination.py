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


def set_average(sim_matrix: list[list[float]]) -> float:
    """
    COMA's computeSetSimilarity: max-matching Dice formula.

    For each row: find the max similarity to any column.
    For each column: find the max similarity to any row.
    Result = (sum_row_maxes + sum_col_maxes) / (m + n)

    This matches Java's SET_AVERAGE behavior for context element sets.
    """
    if not sim_matrix:
        return 0.0
    m = len(sim_matrix)
    if m == 0:
        return 0.0
    n = len(sim_matrix[0]) if sim_matrix[0] else 0
    if n == 0:
        return 0.0

    sum_row_max = sum(max(row) for row in sim_matrix)
    sum_col_max = sum(max(sim_matrix[i][j] for i in range(m)) for j in range(n))

    return (sum_row_max + sum_col_max) / (m + n)


def set_highest(sim_matrix: list[list[float]]) -> float:
    """Max over all values in the similarity matrix."""
    if not sim_matrix:
        return 0.0
    return max(max(row) for row in sim_matrix if row)
