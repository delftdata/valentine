from itertools import product
from multiprocessing import Pool, get_context
import Levenshtein as Lv

from algorithms.base_matcher import BaseMatcher
from data_loader.instance_loader import InstanceLoader


class JaccardLevenMatcher(BaseMatcher):
    """
    Class containing the methods for implementing a simple baseline matcher that uses Jaccard Similarity between
    columns to assess their correspondence score, enhanced by Levenshtein Distance.
    """

    def __init__(self, threshold_leven: float = 0.75, process_num: int = 1):
        self.threshold_leven = threshold_leven
        self.process_num = process_num

    def get_matches(self, source: InstanceLoader, target: InstanceLoader, dataset_name: str):

        matches = dict()
        src_table = source.table.name
        trg_table = target.table.name

        with get_context("spawn").Pool(self.process_num) as process_pool:
            for (col_src_name, col_src_obj), (col_trg_name, col_trg_obj) in \
                    product(source.table.columns.items(), target.table.columns.items()):

                sim = self.jaccard_leven(col_src_obj.data, col_trg_obj.data, self.threshold_leven, process_pool)

                matches[((src_table, col_src_name), (trg_table, col_trg_name))] = sim

        matches = dict(filter(lambda elem: elem[1] > 0, matches.items()))

        sorted_matches = {k: v for k, v in sorted(matches.items(), key=lambda item: item[1], reverse=True)}

        return sorted_matches

    @staticmethod
    def jaccard_leven(list1, list2, threshold, process_pool: Pool):

        if len(set(list1)) < len(set(list2)):
            set1 = set(list1)
            set2 = set(list2)
        else:
            set1 = set(list2)
            set2 = set(list1)

        combinations = list(get_set_combinations(set1, set2, threshold))

        intersection_cnt_list = list(process_pool.map(process_lv, combinations))

        intersection_cnt = sum(intersection_cnt_list)

        union_cnt = len(set1) + len(set2) - intersection_cnt

        return float(intersection_cnt) / union_cnt


def get_set_combinations(set1: set, set2: set, threshold: float):
    for s1 in set1:
        yield str(s1), set2, threshold


def process_lv(tup):
    s1, set2, threshold = tup
    for s2 in set2:
        if Lv.ratio(s1, str(s2)) >= threshold:
            return 1
    return 0
