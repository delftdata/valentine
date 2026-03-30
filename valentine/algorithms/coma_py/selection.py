from __future__ import annotations


def select_both_multiple(
    sim_matrix: dict[tuple, float],
    elements1: list,
    elements2: list,
    max_n: int = 0,
    delta: float = 0.01,
    threshold: float = 0.0,
) -> dict[tuple, float]:
    """
    Apply COMA's DIR_BOTH + SEL_MULTIPLE selection.

    Faithfully reproduces Java Selection.selectArrayMultiple + selectArrayDirection.

    For each source element, computes a minimum forward similarity as the most
    restrictive of: maxN cutoff, delta-from-best, and absolute threshold.
    Similarly for backward (per target element).
    DIR_BOTH keeps only pairs that pass both forward AND backward thresholds.

    Parameters:
        sim_matrix: {(elem1, elem2): score} for all pairs
        elements1: source elements
        elements2: target elements
        max_n: max matches per element (0 = unlimited)
        delta: fraction from best to keep (e.g., 0.01 = within 1% of best)
        threshold: absolute minimum similarity
    """
    # Compute forward thresholds (per source element)
    forward_min = {}
    for e1 in elements1:
        row_scores = sorted((sim_matrix.get((e1, e2), 0.0) for e2 in elements2), reverse=True)
        min_sim = 0.0

        if max_n > 0 and row_scores:
            idx = min(max_n, len(row_scores)) - 1
            min_sim = max(min_sim, row_scores[idx])

        if delta > 0 and row_scores:
            max_score = row_scores[0]
            min_sim = max(min_sim, max_score * (1.0 - delta))

        if threshold > 0:
            min_sim = max(min_sim, threshold)

        forward_min[e1] = min_sim

    # Compute backward thresholds (per target element)
    backward_min = {}
    for e2 in elements2:
        col_scores = sorted((sim_matrix.get((e1, e2), 0.0) for e1 in elements1), reverse=True)
        min_sim = 0.0

        if max_n > 0 and col_scores:
            idx = min(max_n, len(col_scores)) - 1
            min_sim = max(min_sim, col_scores[idx])

        if delta > 0 and col_scores:
            max_score = col_scores[0]
            min_sim = max(min_sim, max_score * (1.0 - delta))

        if threshold > 0:
            min_sim = max(min_sim, threshold)

        backward_min[e2] = min_sim

    # DIR_BOTH: keep only pairs that pass both forward AND backward thresholds
    return {
        (e1, e2): score
        for (e1, e2), score in sim_matrix.items()
        if score > 0 and score >= forward_min.get(e1, 0.0) and score >= backward_min.get(e2, 0.0)
    }
