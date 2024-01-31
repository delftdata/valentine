from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..algorithms.matcher_results import MatcherResults
from typing import Tuple, List


def get_tp_fn(matches: MatcherResults,
              ground_truth: List[Tuple[str, str]],
              n: int | None = None):
    """Counts the amount of true positives and the amount of false
    negatives among the matches in the given MatcherResults.

    Parameters
    ----------
    matches : MatcherResults
        A MatcherResults object that is obtained from a matcher.
    ground_truth : list
        A list with tuples that correspond to the ground truth matches.
        e.g. [("col1_tab_A", "col1_tab_B"), ...etc...]
    n : int, optional
        The percentage of matches to consider.
        e.g. (90) for 90% of the matches

    Returns
    -------
    (int, int)
        Amount of true positives and amount of false negatives.
    """
    tp = 0
    fn = 0

    matches_dict = matches.get_copy()
    all_matches = [(m[0][1], m[1][1]) for m in matches_dict.keys()]

    if n is not None:
        all_matches = all_matches[:n]

    for expected_match in ground_truth:
        if expected_match in all_matches:
            tp += 1
        else:
            fn += 1

    return tp, fn


def get_fp(matches: MatcherResults,
           ground_truth: List[Tuple[str, str]],
           n: int | None = None):
    """Counts the amount of false positives among the matches in the
    given MatcherResults.

    Parameters
    ----------
    matches : MatcherResults
        A MatcherResults object that is obtained from a matcher.
    ground_truth : list
        A list with tuples that correspond to the ground truth matches.
        e.g. [("col1_tab_A", "col1_tab_B"), ...etc...]
    n : int, optional
        The percentage of matches to consider.
        e.g. (90) for 90% of the matches

    Returns
    -------
    int
        Amount of false positives.
    """
    fp = 0
    matches_dict = matches.get_copy()
    all_matches = [(m[0][1], m[1][1]) for m in matches_dict.keys()]

    if n is not None:
        all_matches = all_matches[:n]

    for possible_match in all_matches:
        if possible_match not in ground_truth:
            fp += 1

    return fp
