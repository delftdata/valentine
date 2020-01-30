from data_loader.golden_standard_loader import GoldenStandardLoader


def get_tp_fn(matches: dict, golden_standard: GoldenStandardLoader):
    tp = 0
    fn = 0
    all_matches = list(map(lambda m: frozenset(m.split("|")), list(matches.keys())))
    for expected_match in golden_standard.expected_matches:
        if expected_match in all_matches:
            tp = tp + 1
        else:
            fn = fn + 1
    return tp, fn


def get_fp(matches: dict, golden_standard: GoldenStandardLoader):
    fp = 0
    all_matches = list(map(lambda m: frozenset(m.split("|")), list(matches.keys())))
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


# def top_10_accuracy(matches: dict, golden_standard: GoldenStandardLoader):
#     if not is_sorted(matches):
#         matches = dict(sorted(matches.items(), key=lambda x: -x[1]))  # assuming that matches = {match: similarity}
#     top_10_matches = list(map(lambda m: frozenset(m.split("|")), list(matches.keys())[0:10]))
#     correct = 0
#     for expected_match in golden_standard.expected_matches:
#         if expected_match in top_10_matches:
#             correct = correct + 1
#     return correct / golden_standard.size


# def accuracy(matches: dict, golden_standard: GoldenStandardLoader):
#     all_matches = list(map(lambda m: frozenset(m.split("|")), list(matches.keys())))
#     correct = 0
#     for expected_match in golden_standard.expected_matches:
#         if expected_match in all_matches:
#             correct = correct + 1
#     return correct / golden_standard.size
