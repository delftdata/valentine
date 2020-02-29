from itertools import product
from multiprocessing import Pool, get_context
import Levenshtein as Lv

from algorithms.base_matcher import BaseMatcher
from data_loader.instance_loader import InstanceLoader


class JaccardLevenMatcher(BaseMatcher):
    """
    Class containing the methods for implementing a simple baseline matcher that uses Jaccard Similarity between
    columns to assess their correspondence score, enhanced by Levenshtein Distance.

    Methods
    -------
    jaccard_leven(list1, list2, threshold, process_pool)

    """

    def __init__(self, threshold_leven: float = 0.75, process_num: int = 1):
        """
        Parameters
        ----------
        threshold_leven : float, optional
            The Levenshtein ratio between the two column entries (lower ration, the entries are more different)
        process_num : int, optional
            Te number of processes to spawn
        """
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

        matches = dict(filter(lambda elem: elem[1] > 0.0, matches.items()))  # Remove the pairs with zero similarity

        sorted_matches = {k: v for k, v in sorted(matches.items(), key=lambda item: item[1], reverse=True)}

        return sorted_matches

    @staticmethod
    def jaccard_leven(list1: list, list2: list, threshold: float, process_pool: Pool):
        """
        Function that takes two columns and returns their Jaccard similarity based on the Levenshtein ratio between the
        column entries (lower ration, the entries are more different)

        Parameters
        ----------
        list1 : list
            The first column's data
        list2 : list
            The second column's data
        threshold : float
            The Levenshtein ratio
        process_pool : Pool
            The process pool that will check multiple column combinations in parallel

        Returns
        -------
        float
            The Jaccard Levenshtein similarity between the two columns
        """

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

        if union_cnt == 0:
            return 0

        return float(intersection_cnt) / union_cnt


def get_set_combinations(set1: set, set2: set, threshold: float):
    """
    Function that creates combination between elements of set1 and set2

    Parameters
    ----------
    set1 : set
        The first set that its elements will be taken
    set2 : set
        The second set
    threshold : float
        The Levenshtein ratio

    Returns
    -------
    generator
        A generator that yields one element from the first set, the second set and the Levenshtein ratio
    """
    for s1 in set1:
        yield str(s1), set2, threshold


def process_lv(tup: tuple):
    """
    Function that check if there exist entry from the second set that has a greater Levenshtein ratio with the
    element from the first set than the given threshold

    Parameters
    ----------
    tup : tuple
        A tuple containing one element from the first set, the second set and the threshold of the Levenshtein ratio

    Returns
    -------
    int
        1 if there is such an element 0 if not
    """
    s1, set2, threshold = tup
    for s2 in set2:
        if Lv.ratio(s1, str(s2)) >= threshold:
            return 1
    return 0
