from __future__ import annotations


def select_both_multiple(
    sim_matrix: dict[tuple, float],
    elements1: list,
    elements2: list,
    max_n: int = 0,
    threshold: float = 0.01,
    delta: float = 0.0,
) -> dict[tuple, float]:
    """
    Apply COMA's DIR_BOTH + SEL_MULTIPLE selection.

    DIR_BOTH: intersect forward-best and backward-best matches.
    SEL_MULTIPLE: combine maxN, threshold, and delta filters.

    Parameters:
        sim_matrix: {(elem1, elem2): score} for all pairs
        elements1: source elements
        elements2: target elements
        max_n: max matches per element (0 = unlimited)
        threshold: minimum similarity to keep
        delta: keep matches within delta of best (0.0 = keep all above threshold)
    """
    # Forward selection: for each source element, find acceptable target matches
    forward = _select_direction(sim_matrix, elements1, elements2, max_n, threshold, delta)

    # Backward selection: for each target element, find acceptable source matches
    backward_raw = _select_direction(
        {(e2, e1): v for (e1, e2), v in sim_matrix.items()},
        elements2,
        elements1,
        max_n,
        threshold,
        delta,
    )
    # Flip backward keys back to (e1, e2) order
    backward = {(e1, e2) for (e2, e1) in backward_raw}

    # DIR_BOTH: intersection
    selected = forward & backward

    return {pair: sim_matrix[pair] for pair in selected if sim_matrix.get(pair, 0) > 0}


def _select_direction(
    sim_matrix: dict[tuple, float],
    primaries: list,
    secondaries: list,
    max_n: int,
    threshold: float,
    delta: float,
) -> set[tuple]:
    """Select matches in one direction (primary -> secondary)."""
    selected = set()

    for p in primaries:
        # Get all scores for this primary element
        scored = []
        for s in secondaries:
            score = sim_matrix.get((p, s), 0.0)
            if score >= threshold:
                scored.append((s, score))

        if not scored:
            continue

        # Sort by score descending
        scored.sort(key=lambda x: x[1], reverse=True)

        best_score = scored[0][1]

        for s, score in scored:
            # Delta filter: keep if within delta of best
            if delta > 0 and (best_score - score) > delta * best_score:
                break  # Since sorted, all remaining will also fail

            selected.add((p, s))

            # MaxN filter
            if max_n > 0 and sum(1 for pair in selected if pair[0] is p) >= max_n:
                break

    return selected
