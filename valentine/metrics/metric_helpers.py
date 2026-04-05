from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..algorithms.match import ColumnPair
    from ..algorithms.matcher_results import MatcherResults


def _normalize_ground_truth(
    ground_truth: list[tuple[str, str]] | list[ColumnPair],
) -> tuple[list[tuple[str, str]], bool]:
    """Normalize ground truth to a list of (source_col, target_col) pairs.

    Returns the normalized list and a flag indicating whether full
    ColumnPair matching should be used (when all entries have 4 fields).
    """
    if not ground_truth:
        return [], False
    first = ground_truth[0]
    if len(first) == 4:
        # Full ColumnPair format — compare exactly
        return [(e[1], e[3]) for e in ground_truth], False
    # Simple (source_col, target_col) format
    return [tuple(e) for e in ground_truth], False


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
    gt_pairs, _ = _normalize_ground_truth(ground_truth)
    all_matches = [(m.source_column, m.target_column) for m in matches]

    if n is not None:
        all_matches = all_matches[:n]

    tp = 0
    fn = 0
    for expected_match in gt_pairs:
        if expected_match in all_matches:
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
    gt_pairs, _ = _normalize_ground_truth(ground_truth)
    all_matches = [(m.source_column, m.target_column) for m in matches]

    if n is not None:
        all_matches = all_matches[:n]

    fp = 0
    for possible_match in all_matches:
        if possible_match not in gt_pairs:
            fp += 1

    return fp
