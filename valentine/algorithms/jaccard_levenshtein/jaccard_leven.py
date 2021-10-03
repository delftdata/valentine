from itertools import product
from multiprocessing import get_context
from typing import Dict, Tuple

import Levenshtein as Lv

from ..base_matcher import BaseMatcher
from ..match import Match
from ...data_sources.base_table import BaseTable


class JaccardLevenMatcher(BaseMatcher):
    """
    Class containing the methods for implementing a simple baseline matcher that uses Jaccard Similarity between
    columns to assess their correspondence score, enhanced by Levenshtein Distance.

    Methods
    -------
    jaccard_leven(list1, list2, threshold, process_pool)

    """

    def __init__(self,
                 threshold_leven: float = 0.8,
                 process_num: int = 1):
        """
        Parameters
        ----------
        threshold_leven : float, optional
            The Levenshtein ratio between the two column entries (lower ratio, the entries are more different)
        process_num : int, optional
            Te number of processes to spawn
        """
        self.__threshold_leven = float(threshold_leven)
        self.__process_num = int(process_num)

    def get_matches(self,
                    source_input: BaseTable,
                    target_input: BaseTable) -> Dict[Tuple[Tuple[str, str], Tuple[str, str]], float]:
        source_id = source_input.unique_identifier
        target_id = target_input.unique_identifier
        matches = {}
        if self.__process_num == 1:
            for combination in self.__get_column_combinations(source_input,
                                                              target_input,
                                                              self.__threshold_leven,
                                                              target_id,
                                                              source_id):
                matches.update(self.__process_jaccard_leven(combination))
        else:
            with get_context("spawn").Pool(self.__process_num) as process_pool:
                matches = dict(process_pool.map(self.__process_jaccard_leven,
                                                self.__get_column_combinations(source_input,
                                                                               target_input,
                                                                               self.__threshold_leven,
                                                                               target_id,
                                                                               source_id)))
        matches = {k: v for k, v in matches.items() if v > 0.0}  # Remove the pairs with zero similarity
        return matches

    def __process_jaccard_leven(self, tup: tuple):

        source_data, target_data, threshold, target_id, target_table_name, target_table_unique_identifier, \
            target_column_name, target_column_unique_identifier, source_table_name, source_table_unique_identifier, \
            source_id, source_column_name, source_column_unique_identifier = tup

        if len(set(source_data)) < len(set(target_data)):
            set1 = set(source_data)
            set2 = set(target_data)
        else:
            set1 = set(target_data)
            set2 = set(source_data)

        combinations = self.__get_set_combinations(set1, set2, threshold)

        intersection_cnt = 0
        for cmb in combinations:
            intersection_cnt = intersection_cnt + self.__process_lv(cmb)

        union_cnt = len(set1) + len(set2) - intersection_cnt

        if union_cnt == 0:
            sim = 0.0
        else:
            sim = float(intersection_cnt) / union_cnt

        return Match(target_table_name, target_column_name,
                     source_table_name, source_column_name,
                     sim).to_dict

    @staticmethod
    def __get_column_combinations(source_table: BaseTable,
                                  target_table: BaseTable,
                                  threshold,
                                  target_id,
                                  source_id):
        for source_column, target_column in product(source_table.get_columns(), target_table.get_columns()):
            yield source_column.data, target_column.data, threshold, target_id, \
                  target_table.name, target_table.unique_identifier, \
                  target_column.name, target_column.unique_identifier, \
                  source_table.name, source_table.unique_identifier, source_id, \
                  source_column.name, source_column.unique_identifier

    @staticmethod
    def __get_set_combinations(set1: set,
                               set2: set,
                               threshold: float):
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

    @staticmethod
    def __process_lv(tup: tuple):
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
