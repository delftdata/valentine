import math

from data_loader.golden_standard_loader import GoldenStandardLoader
from utils.utils import one_to_one_matches


def get_tp_fn(matches: dict, golden_standard: GoldenStandardLoader, n: int = None):
    tp = 0
    fn = 0

    all_matches = list(map(lambda m: frozenset(m), list(matches.keys())))

    if n is not None:
        all_matches = all_matches[:n]

    for expected_match in golden_standard.expected_matches:
        if expected_match in all_matches:
            tp = tp + 1
        else:
            fn = fn + 1
    return tp, fn


def get_fp(matches: dict, golden_standard: GoldenStandardLoader, n: int = None):
    fp = 0

    all_matches = list(map(lambda m: frozenset(m), list(matches.keys())))

    if n is not None:
        all_matches = all_matches[:n]

    for possible_match in all_matches:
        if possible_match not in golden_standard.expected_matches:
            fp = fp + 1
    return fp


def recall(matches: dict, golden_standard: GoldenStandardLoader, one_to_one=False):
    if one_to_one:
        matches = one_to_one_matches(matches)
    tp, fn = get_tp_fn(matches, golden_standard)
    if tp + fn == 0:
        return 0
    return tp / (tp + fn)


def precision(matches: dict, golden_standard: GoldenStandardLoader, one_to_one=False):
    if one_to_one:
        matches = one_to_one_matches(matches)
    tp, fn = get_tp_fn(matches, golden_standard)
    fp = get_fp(matches, golden_standard)
    if tp + fp == 0:
        return 0
    return tp / (tp + fp)


def f1_score(matches: dict, golden_standard: GoldenStandardLoader, one_to_one=False):
    pr = precision(matches, golden_standard, one_to_one)
    re = recall(matches, golden_standard, one_to_one)
    if pr + re == 0:
        return 0
    return 2 * ((pr * re) / (pr + re))


def precision_at_n_percent(matches: dict, golden_standard: GoldenStandardLoader, n: int):
    number_to_keep = int(math.ceil((n / 100) * len(matches.keys())))
    tp, fn = get_tp_fn(matches, golden_standard, number_to_keep)
    fp = get_fp(matches, golden_standard, number_to_keep)
    if tp + fp == 0:
        return 0
    return tp / (tp + fp)


def recall_at_sizeof_ground_truth(matches: dict, golden_standard: GoldenStandardLoader):
    tp, fn = get_tp_fn(matches, golden_standard, golden_standard.size)
    if tp + fn == 0:
        return 0
    return tp / (tp + fn)
