from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..algorithms.match import ColumnPair
    from ..algorithms.matcher_results import MatcherResults
    from .base_metric import OneToOneMethod


def _apply_one_to_one(matches: MatcherResults, method: OneToOneMethod) -> MatcherResults:
    """Dispatch ``matches`` through the requested 1:1 selection algorithm.

    ``method`` is one of ``"greedy"``, ``"hungarian"``, or ``"mutual_top"``;
    invalid values raise ``ValueError``. Mutual-top defaults to ``n=1``
    (mutual nearest neighbour).
    """
    if method == "hungarian":
        return matches.one_to_one_hungarian()
    if method == "greedy":
        return matches.one_to_one_greedy()
    if method == "mutual_top":
        return matches.one_to_one_mutual_top()
    raise ValueError(
        f"Unknown one_to_one_method: {method!r}; "
        "expected 'greedy', 'hungarian', or 'mutual_top'"
    )


def _normalize_ground_truth(
    ground_truth: list[tuple[str, str]] | list[ColumnPair],
) -> tuple[list[tuple], bool]:
    """Normalize ground truth into a comparable list of tuples.

    Two accepted formats:

    - **Column-name pairs** — ``[("source_col", "target_col"), ...]``.
      Table names are ignored when comparing against matcher results.
    - **ColumnPair** — full 4-field entries with table names. Comparisons
      are then table-aware, which matters when matching more than two
      tables or when source and target share column names.

    Returns
    -------
    tuple[list[tuple], bool]
        The normalized ground truth and a ``table_aware`` flag indicating
        whether comparisons should include table names.
    """
    if not ground_truth:
        return [], False
    first = ground_truth[0]
    if len(first) == 4:
        # Full ColumnPair format — keep table names for exact comparison
        return [(e[0], e[1], e[2], e[3]) for e in ground_truth], True
    # Simple (source_col, target_col) format — column names only
    return [(e[0], e[1]) for e in ground_truth], False


def _matches_as_tuples(matches: MatcherResults, table_aware: bool) -> list[tuple]:
    if table_aware:
        return [(m.source_table, m.source_column, m.target_table, m.target_column) for m in matches]
    return [(m.source_column, m.target_column) for m in matches]


def get_tp_fn(
    matches: MatcherResults,
    ground_truth: list[tuple[str, str]] | list[ColumnPair],
    n: int | None = None,
):
    """Count true positives and false negatives.

    Parameters
    ----------
    matches : MatcherResults
        Match results from a matcher.
    ground_truth : list
        Expected column matches as ``(source_col, target_col)`` pairs
        or full :class:`ColumnPair` instances.
    n : int, optional
        If provided, only consider the first ``n`` matches.

    Returns
    -------
    tuple[int, int]
        (true_positives, false_negatives)
    """
    gt_pairs, table_aware = _normalize_ground_truth(ground_truth)
    all_matches = _matches_as_tuples(matches, table_aware)

    if n is not None:
        all_matches = all_matches[:n]

    match_set = set(all_matches)
    tp = 0
    fn = 0
    for expected_match in gt_pairs:
        if expected_match in match_set:
            tp += 1
        else:
            fn += 1

    return tp, fn


def get_fp(
    matches: MatcherResults,
    ground_truth: list[tuple[str, str]] | list[ColumnPair],
    n: int | None = None,
):
    """Count false positives.

    Parameters
    ----------
    matches : MatcherResults
        Match results from a matcher.
    ground_truth : list
        Expected column matches as ``(source_col, target_col)`` pairs
        or full :class:`ColumnPair` instances.
    n : int, optional
        If provided, only consider the first ``n`` matches.

    Returns
    -------
    int
        Number of false positives.
    """
    gt_pairs, table_aware = _normalize_ground_truth(ground_truth)
    all_matches = _matches_as_tuples(matches, table_aware)

    if n is not None:
        all_matches = all_matches[:n]

    gt_set = set(gt_pairs)
    fp = 0
    for possible_match in all_matches:
        if possible_match not in gt_set:
            fp += 1

    return fp
