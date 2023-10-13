from itertools import product
from multiprocessing import get_context
from typing import Dict, Tuple

from jellyfish import levenshtein_distance, damerau_levenshtein_distance, \
                      jaro_similarity, jaro_winkler_similarity, hamming_distance

from ..jaccard_distance import StringDistanceFunction

from ..base_matcher import BaseMatcher
from ..match import Match
from ...data_sources.base_table import BaseTable
from ...utils.utils import normalize_distance


class JaccardDistanceMatcher(BaseMatcher):
    """
    Class containing the methods for implementing a simple baseline matcher that uses Jaccard Similarity between
    columns to assess their correspondence score, enhanced by a string distance measure.

    Methods
    -------
    jaccard_distance(list1, list2, threshold, process_pool)

    """

    def __init__(self,
                 threshold_dist: float = 0.8,
                 distance_fun: StringDistanceFunction = StringDistanceFunction.Levenshtein,
                 process_num: int = 1):
        """
        Parameters
        ----------
        threshold_dist : float, optional
            The acceptance threshold for two strings to be considered as equal
        process_num : int, optional
            Te number of processes to spawn
        """
        self.__threshold_dist = float(threshold_dist)
        self.__process_num = int(process_num)
        self.__distance_function = distance_fun

    def get_matches(self,
                    source_input: BaseTable,
                    target_input: BaseTable) -> Dict[Tuple[Tuple[str, str], Tuple[str, str]], float]:
        source_id = source_input.unique_identifier
        target_id = target_input.unique_identifier
        matches = {}
        if self.__process_num == 1:
            for combination in self.__get_column_combinations(source_input,
                                                              target_input,
                                                              self.__threshold_dist,
                                                              target_id,
                                                              source_id,
                                                              self.__distance_function):
                matches.update(self.process_jaccard_distance(combination))
        else:
            with get_context("spawn").Pool(self.__process_num) as process_pool:
                matches = {}
                list_of_matches = process_pool.map(self.process_jaccard_distance,
                                                   self.__get_column_combinations(source_input,
                                                                                  target_input,
                                                                                  self.__threshold_dist,
                                                                                  target_id,
                                                                                  source_id,
                                                                                  self.__distance_function))
                for match in list_of_matches:
                    matches.update(match)
        # Remove the pairs with zero similarity
        matches = {k: v for k, v in matches.items() if v > 0.0}
        return matches

    def process_jaccard_distance(self, tup: tuple):

        source_data, target_data, threshold, _, target_table_name, _, \
            target_column_name, _, source_table_name, _, \
            _, source_column_name, _, distance_function = tup

        if len(set(source_data)) < len(set(target_data)):
            set1 = set(source_data)
            set2 = set(target_data)
        else:
            set1 = set(target_data)
            set2 = set(source_data)

        if distance_function == StringDistanceFunction.Exact:
            threshold = 1.0
        combinations = self.__get_set_combinations(set1, set2, threshold)

        intersection_cnt = 0
        for cmb in combinations:
            if distance_function in [StringDistanceFunction.Levenshtein, StringDistanceFunction.Exact]:
                intersection_cnt = intersection_cnt + self.__process_distance(cmb + (levenshtein_distance,
                                                                                     True)
                                                                              )
            elif distance_function == StringDistanceFunction.DamerauLevenshtein:
                intersection_cnt = intersection_cnt + self.__process_distance(cmb + (damerau_levenshtein_distance,
                                                                                     True)
                                                                              )
            elif distance_function == StringDistanceFunction.Hamming:
                intersection_cnt = intersection_cnt + self.__process_distance(cmb + (hamming_distance,
                                                                                     True)
                                                                              )
            elif distance_function == StringDistanceFunction.Jaro:
                intersection_cnt = intersection_cnt + self.__process_distance(cmb + (jaro_similarity,
                                                                                     False)
                                                                              )
            elif distance_function == StringDistanceFunction.JaroWinkler:
                intersection_cnt = intersection_cnt + self.__process_distance(cmb + (jaro_winkler_similarity,
                                                                                     False)
                                                                              )

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
                                  source_id,
                                  distance_function: StringDistanceFunction):
        for source_column, target_column in product(source_table.get_columns(), target_table.get_columns()):
            yield source_column.data, target_column.data, threshold, target_id, \
                  target_table.name, target_table.unique_identifier, \
                  target_column.name, target_column.unique_identifier, \
                  source_table.name, source_table.unique_identifier, source_id, \
                  source_column.name, source_column.unique_identifier, distance_function

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
    def __process_distance(tup: tuple):
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
        s1, set2, threshold, distance_function, normalize = tup

        for s2 in set2:
            str_s2 = str(s2)
            dist = distance_function(s1, str_s2)
            if normalize:
                if normalize_distance(dist, s1, str_s2) >= threshold:
                    return 1
            else:
                if dist >= threshold:
                    return 1
        return 0
