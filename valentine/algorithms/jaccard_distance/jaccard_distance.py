from itertools import product

import numpy as np
from rapidfuzz import process
from rapidfuzz.distance import (
    DamerauLevenshtein,
    Hamming,
    Jaro,
    JaroWinkler,
    Levenshtein,
)

from ...data_sources.base_table import BaseTable
from ..base_matcher import BaseMatcher
from ..jaccard_distance import StringDistanceFunction
from ..match import Match

# Map our public StringDistanceFunction enum to the rapidfuzz scorer that
# returns a normalized similarity in [0, 1]. rapidfuzz.process.cdist runs
# the comparison in a C++ inner loop with optional thread-level parallelism.
_SCORER_MAP = {
    StringDistanceFunction.Levenshtein: Levenshtein.normalized_similarity,
    StringDistanceFunction.DamerauLevenshtein: DamerauLevenshtein.normalized_similarity,
    StringDistanceFunction.Hamming: Hamming.normalized_similarity,
    StringDistanceFunction.Jaro: Jaro.normalized_similarity,
    StringDistanceFunction.JaroWinkler: JaroWinkler.normalized_similarity,
}


class JaccardDistanceMatcher(BaseMatcher):
    """Baseline instance-based matcher using Jaccard similarity.

    Columns are compared by Jaccard similarity of their value sets, with
    element equality decided by a configurable string distance function.
    This is a simple but effective baseline for instance-based matching.

    Parameters
    ----------
    threshold_dist : float, optional
        Acceptance threshold above which two string values are considered
        equal under the chosen ``distance_fun``, in ``[0, 1]``
        (default: ``0.8``). Ignored when ``distance_fun`` is
        :attr:`StringDistanceFunction.Exact`.
    distance_fun : StringDistanceFunction, optional
        String similarity function. One of
        :attr:`StringDistanceFunction.Levenshtein` (default),
        :attr:`StringDistanceFunction.DamerauLevenshtein`,
        :attr:`StringDistanceFunction.Hamming`,
        :attr:`StringDistanceFunction.Jaro`,
        :attr:`StringDistanceFunction.JaroWinkler`, or
        :attr:`StringDistanceFunction.Exact`.
    process_num : int, optional
        Number of worker threads passed to ``rapidfuzz.process.cdist``
        (must be ``>= 1``, default: ``1``). Earlier versions used a
        ``multiprocessing.Pool``; with rapidfuzz the inner kernel is
        already C++ and parallelises via OpenMP threads, so the pool is
        no longer needed.
    """

    def __init__(
        self,
        threshold_dist: float = 0.8,
        distance_fun: StringDistanceFunction = StringDistanceFunction.Levenshtein,
        process_num: int = 1,
    ):
        self.__threshold_dist = float(threshold_dist)
        self.__process_num = int(process_num)
        self.__distance_function = distance_fun
        if not 0.0 <= self.__threshold_dist <= 1.0:
            raise ValueError(
                f"threshold_dist must be between 0.0 and 1.0, got {self.__threshold_dist}"
            )
        if self.__process_num < 1:
            raise ValueError(f"process_num must be >= 1, got {self.__process_num}")

    def get_matches(self, source_input: BaseTable, target_input: BaseTable) -> dict:
        matches: dict = {}
        for combination in self.__get_column_combinations(
            source_input,
            target_input,
            self.__threshold_dist,
            self.__distance_function,
        ):
            matches.update(self.process_jaccard_distance(combination))
        # Remove the pairs with zero similarity
        return {k: v for k, v in matches.items() if v > 0.0}

    def process_jaccard_distance(self, tup: tuple):
        (
            source_data,
            target_data,
            threshold,
            target_table_name,
            target_column_name,
            source_table_name,
            source_column_name,
            distance_function,
        ) = tup

        set1 = {str(x) for x in source_data}
        set2 = {str(x) for x in target_data}
        # Iterate over the smaller set as queries: cdist scales with
        # rows x cols, and the row dimension dominates Python-side overhead.
        if len(set1) > len(set2):
            set1, set2 = set2, set1

        if distance_function == StringDistanceFunction.Exact:
            intersection_cnt = len(set1 & set2)
        elif not set1 or not set2:
            intersection_cnt = 0
        else:
            scorer = _SCORER_MAP[distance_function]
            queries = list(set1)
            choices = list(set2)
            scores = process.cdist(
                queries,
                choices,
                scorer=scorer,
                score_cutoff=threshold,
                workers=self.__process_num,
            )
            # Each query string in set1 contributes 1 to the intersection
            # if at least one choice in set2 scores >= threshold. Scores
            # below score_cutoff are returned as 0 by rapidfuzz, so the
            # comparison is exact even when threshold == 0.
            intersection_cnt = int(np.count_nonzero((scores >= threshold).any(axis=1)))

        union_cnt = len(set1) + len(set2) - intersection_cnt
        sim = 0.0 if union_cnt == 0 else float(intersection_cnt) / union_cnt

        return Match(
            target_table_name,
            target_column_name,
            source_table_name,
            source_column_name,
            sim,
        ).to_dict

    @staticmethod
    def __get_column_combinations(
        source_table: BaseTable,
        target_table: BaseTable,
        threshold,
        distance_function: StringDistanceFunction,
    ):
        for source_column, target_column in product(
            source_table.get_instances_columns(), target_table.get_instances_columns()
        ):
            yield (
                source_column.data,
                target_column.data,
                threshold,
                target_table.name,
                target_column.name,
                source_table.name,
                source_column.name,
                distance_function,
            )
