import math
from typing import Dict, Tuple, List


def one_to_one_matches(matches: dict):
    """
    A filter that takes a dict of column matches and returns a dict of 1 to 1 matches. The filter works in the following
    way: At first it gets the median similarity of the set of the values and removes all matches
    that have a similarity lower than that. Then from what remained it matches columns for me highest similarity
    to the lowest till the columns have at most one match.
    Parameters
    ----------
    matches : dict
        The ranked list of matches
    Returns
    -------
    dict
        The ranked list of matches after the 1 to 1 filter
    """
    set_match_values = set(matches.values())

    if len(set_match_values) < 2:
        return matches

    matched = dict()

    for key in matches.keys():
        matched[key[0]] = False
        matched[key[1]] = False

    median = list(set_match_values)[math.ceil(len(set_match_values)/2)]

    matches1to1 = dict()

    for key in matches.keys():
        if (not matched[key[0]]) and (not matched[key[1]]):
            similarity = matches.get(key)
            if similarity >= median:
                matches1to1[key] = similarity
                matched[key[0]] = True
                matched[key[1]] = True
            else:
                break
    return matches1to1


def get_tp_fn(matches: Dict[Tuple[Tuple[str, str], Tuple[str, str]], float],
              golden_standard: List[Tuple[str, str]],
              n: int = None):
    """
    Calculate the true positive  and false negative numbers of the given matches

    Parameters
    ----------
    matches : dict
        Ranked list of matches from the match with higher similarity to lower
    golden_standard : list
        A list that contains the golden standard
    n : int, optional
        The percentage number that we want to consider from the ranked list (matches)
        e.g. (90) for 90% of the matches

    Returns
    -------
    (int, int)
        True positive and false negative counts
    """
    tp = 0
    fn = 0

    all_matches = [(m[0][1], m[1][1]) for m in matches.keys()]

    if n is not None:
        all_matches = all_matches[:n]

    for expected_match in golden_standard:
        if expected_match in all_matches:
            tp = tp + 1
        else:
            fn = fn + 1
    return tp, fn


def get_fp(matches: Dict[Tuple[Tuple[str, str], Tuple[str, str]], float],
           golden_standard: List[Tuple[str, str]],
           n: int = None):
    """
    Calculate the false positive number of the given matches

    Parameters
    ----------
    matches : dict
        Ranked list of matches from the match with higher similarity to lower
    golden_standard : list
        A list that contains the golden standard
    n : int, optional
        The percentage number that we want to consider from the ranked list (matches)
        e.g. (90) for 90% of the matches

    Returns
    -------
    int
        False positive
    """
    fp = 0

    all_matches = [(m[0][1], m[1][1]) for m in matches.keys()]

    if n is not None:
        all_matches = all_matches[:n]

    for possible_match in all_matches:
        if possible_match not in golden_standard:
            fp = fp + 1
    return fp


def recall(matches: Dict[Tuple[Tuple[str, str], Tuple[str, str]], float],
           golden_standard: List[Tuple[str, str]],
           one_to_one=True):
    """
    Function that calculates the recall of the matches against the golden standard. If one_to_one is set to true, it
    also performs an 1-1 match filer. Meaning that each column will match only with another one.

    Parameters
    ----------
    matches : dict
        Ranked list of matches from the match with higher similarity to lower
    golden_standard : list
        A list that contains the golden standard
    one_to_one : bool, optional
        If to perform the 1-1 match filter

    Returns
    -------
    float
        The recall
    """
    if one_to_one:
        matches = one_to_one_matches(matches)
    tp, fn = get_tp_fn(matches, golden_standard)
    if tp + fn == 0:
        return 0
    return tp / (tp + fn)


def precision(matches: Dict[Tuple[Tuple[str, str], Tuple[str, str]], float],
              golden_standard: List[Tuple[str, str]],
              one_to_one=True):
    """
    Function that calculates the precision of the matches against the golden standard. If one_to_one is set to true, it
    also performs an 1-1 match filer. Meaning that each column will match only with another one.

    Parameters
    ----------
    matches : dict
        Ranked list of matches from the match with higher similarity to lower
    golden_standard : list
        A list that contains the golden standard
    one_to_one : bool, optional
        If to perform the 1-1 match filter

    Returns
    -------
    float
        The precision
    """
    if one_to_one:
        matches = one_to_one_matches(matches)
    tp, fn = get_tp_fn(matches, golden_standard)
    fp = get_fp(matches, golden_standard)
    if tp + fp == 0:
        return 0
    return tp / (tp + fp)


def f1_score(matches: Dict[Tuple[Tuple[str, str], Tuple[str, str]], float],
             golden_standard: List[Tuple[str, str]],
             one_to_one=True):
    """
    Function that calculates the F1 score of the matches against the golden standard. If one_to_one is set to true, it
    also performs an 1-1 match filer. Meaning that each column will match only with another one.

    Parameters
    ----------
    matches : dict
        Ranked list of matches from the match with higher similarity to lower
    golden_standard : list
        A list that contains the golden standard
    one_to_one : bool, optional
        If to perform the 1-1 match filter

    Returns
    -------
    float
        The f1_score
    """
    pr = precision(matches, golden_standard, one_to_one)
    re = recall(matches, golden_standard, one_to_one)
    if pr + re == 0:
        return 0
    return 2 * ((pr * re) / (pr + re))


def precision_at_n_percent(matches: Dict[Tuple[Tuple[str, str], Tuple[str, str]], float],
                           golden_standard: List[Tuple[str, str]],
                           n: int):
    """
    Function that calculates the precision at n %
    e.g. if n is 10 then only the first 10% of the matches will be considered for the precision calculation

    Parameters
    ----------
    matches : dict
        Ranked list of matches from the match with higher similarity to lower
    golden_standard : list
        A list that contains the golden standard
    n : int
        The integer percentage number

    Returns
    -------
    float
        The precision at n %
    """
    number_to_keep = int(math.ceil((n / 100) * len(matches.keys())))
    tp, fn = get_tp_fn(matches, golden_standard, number_to_keep)
    fp = get_fp(matches, golden_standard, number_to_keep)
    if tp + fp == 0:
        return 0
    return tp / (tp + fp)


def recall_at_sizeof_ground_truth(matches: Dict[Tuple[Tuple[str, str], Tuple[str, str]], float],
                                  golden_standard: List[Tuple[str, str]],):
    """
    Function that calculates the recall at the size of the ground truth.
    e.g. if the size of ground truth size is 10 then only the first 10 matches will be considered for
    the recall calculation

    Parameters
    ----------
    matches : dict
        Ranked list of matches from the match with higher similarity to lower
    golden_standard : list
        A list that contains the golden standard

    Returns
    -------
    float
        The recall at the size of ground truth
    """
    tp, fn = get_tp_fn(matches, golden_standard, len(golden_standard))
    if tp + fn == 0:
        return 0
    return tp / (tp + fn)
