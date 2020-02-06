from data_loader.golden_standard_loader import GoldenStandardLoader


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


def recall(matches: dict, golden_standard: GoldenStandardLoader):
    tp, fn = get_tp_fn(matches, golden_standard)
    return tp / (tp + fn)


def precision(matches: dict, golden_standard: GoldenStandardLoader):
    tp, fn = get_tp_fn(matches, golden_standard)
    fp = get_fp(matches, golden_standard)
    return tp / (tp + fp)


def precision_at_n_percent(matches: dict, golden_standard: GoldenStandardLoader, n: int):
    number_to_keep = int((n / 100) * len(matches.keys()))
    tp, fn = get_tp_fn(matches, golden_standard, number_to_keep)
    fp = get_fp(matches, golden_standard, number_to_keep)
    return tp / (tp + fp)


def recall_at_sizeof_ground_truth(matches: dict, golden_standard: GoldenStandardLoader):
    tp, fn = get_tp_fn(matches, golden_standard, golden_standard.size)
    return tp / (tp + fn)
