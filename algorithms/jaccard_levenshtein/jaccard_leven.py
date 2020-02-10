from algorithms.base_matcher import BaseMatcher
from data_loader.instance_loader import InstanceLoader
import Levenshtein as Lv


class JaccardLevenMatcher(BaseMatcher):
    """
    Class containing the methods for implementing a simple baseline matcher that uses Jaccard Similarity between
    columns to assess their correspondence score, enhanced by Levenshtein Distance.
    """

    def __init__(self, threshold_leven: float = 0.75):
        self.threshold_leven = threshold_leven

    def get_matches(self, source: InstanceLoader, target: InstanceLoader, dataset_name: str):

        matches = dict()
        src_table = source.table.name
        trg_table = target.table.name

        for col_src in source.table.columns:
            for col_trg in target.table.columns:

                data_src = source.table.columns.get(col_src).data
                data_trg = target.table.columns.get(col_trg).data

                sim = self.jaccard_leven(data_src, data_trg, self.threshold_leven)

                matches[((src_table, col_src), (trg_table, col_trg))] = sim

        sorted_matches = {k: v for k, v in sorted(matches.items(), key=lambda item: item[1], reverse=True)}

        return sorted_matches

    @staticmethod
    def jaccard_leven(list1, list2, threshold):

        if len(set(list1)) < len(set(list2)):
            set1 = set(list1)
            set2 = set(list2)
        else:
            set1 = set(list2)
            set2 = set(list1)

        intersection_cnt = 0

        for s1 in set1:
            for s2 in set2:
                if Lv.ratio(str(s1), str(s2)) >= threshold:
                    intersection_cnt += 1
                    break

        union_cnt = len(set1.union(set2))
        return float(intersection_cnt) / union_cnt
