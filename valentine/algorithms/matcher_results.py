from __future__ import annotations

import math
from typing import Any

from ..metrics import METRICS_CORE
from ..metrics.base_metric import Metric


class MatcherResults(dict):
    """This is a dictionary with additional valentine-specific functionality.
    This class is the result of a matcher's `get_matches` method.

    Certain transformations such as "one_to_one" get cached, since they do not
    differ from call to call and are required by many metrics.

    The assumption is that the results are sorted from high similarity to low
    similarity. This is also enforced upon creation through sorting, as
    dictionaries preserve their insertion order as of Python 3.6.

    Aside from transformations, one can also obtain metric scores based on the
    results, which can be imported from the metrics module. The metrics come in
    handy predefined sets as well, e.g. METRICS_CORE, which is the default.
    """

    def __init__(
        self: MatcherResults,
        res: dict[tuple[tuple[str, str], tuple[str, str]], float],
        *args,
        **kwargs,
    ):
        self._cached_one_to_one = None
        sorted_res = {k: res[k] for k in sorted(res, key=res.get, reverse=True)}
        dict.__init__(self, sorted_res, *args, **kwargs)

    def one_to_one(self: MatcherResults, threshold: float | None = None) -> MatcherResults:
        """Filter to one-to-one column matches.

        Starting from the highest-scoring pair, greedily assigns each source
        and target column at most one match. Pairs below ``threshold`` are
        discarded. When ``threshold`` is ``None`` (the default), the median
        similarity score is used.

        Parameters
        ----------
        threshold : float | None
            Minimum similarity to keep. If None, uses the median score.

        Returns
        -------
        MatcherResults
            MatcherResults with one-to-one matches.
        """
        if threshold is None and self._cached_one_to_one is not None:
            return MatcherResults(self._cached_one_to_one.copy())

        matches_dict = self.get_copy()

        set_match_values = set(matches_dict.values())

        if len(set_match_values) < 2:
            if threshold is None:
                self._cached_one_to_one = matches_dict
            return MatcherResults(matches_dict)

        matched = {}

        for key in matches_dict:
            matched[key[0]] = False
            matched[key[1]] = False

        if threshold is None:
            min_sim = sorted(set_match_values, reverse=True)[math.ceil(len(set_match_values) / 2)]
        else:
            min_sim = threshold

        matches1to1_dict = {}

        for key in matches_dict:
            if (not matched[key[0]]) and (not matched[key[1]]):
                similarity = matches_dict.get(key)
                if similarity is not None and similarity >= min_sim:
                    matches1to1_dict[key] = similarity
                    matched[key[0]] = True
                    matched[key[1]] = True
                else:
                    break

        if threshold is None:
            self._cached_one_to_one = matches1to1_dict
        return MatcherResults(matches1to1_dict)

    def filter(self: MatcherResults, min_score: float) -> MatcherResults:
        """Filter matches by minimum similarity score.

        Parameters
        ----------
        min_score : float
            Minimum similarity score to keep.

        Returns
        -------
        MatcherResults
            MatcherResults containing only matches with score >= min_score.
        """
        return MatcherResults({k: v for k, v in self.items() if v >= min_score})

    def take_top_percent(self: MatcherResults, percent: int) -> MatcherResults:
        """Summary
        Takes the top 'percent' of matches and returns a new MatcherResults
        containing only these matches.

        Parameters
        ----------
        percent : int
            Percentage of matches to keep.

        Returns
        -------
        MatcherResults
            Matcher results containing only the
            top 'percent' of matches.
        """
        matches = self.get_copy()
        number_to_keep = math.ceil((percent / 100) * len(matches.keys()))
        matches = dict(sorted(matches.items(), key=lambda x: x[1], reverse=True)[:number_to_keep])
        return MatcherResults(matches)

    def take_top_n(self: MatcherResults, n: int) -> MatcherResults:
        """Summary
        Takes the top 'n' matches and returns a new MatcherResults
        containing only these matches.

        Parameters
        ----------
        n : int
            Number of matches to keep.

        Returns
        -------
        MatcherResults
            Matcher results containing only the
            top 'n' matches.
        """
        matches = self.get_copy()
        matches = dict(sorted(matches.items(), key=lambda x: x[1], reverse=True)[:n])
        return MatcherResults(matches)

    def get_metrics(
        self: MatcherResults,
        ground_truth: list[tuple[str, str]],
        metrics: set[Metric] = METRICS_CORE,
    ) -> dict[str, Any]:
        """Summary
        Given ground truth column matches and a set of metric instances, this
        method will calculate scores for these metrics. Metrics can be imported
        from the 'metrics' module, which also contains predefined sets of
        metrics.

        Parameters
        ----------
        ground_truth : List[Tuple[str, str]]
            The ground truth column matches as a list of column name tuples.
        metrics : Set[Metric], optional
            The set of metric instances.

        Returns
        -------
        Dict[str, Any]
            A dictionary with metric scores.
        """
        res = {}
        for metric in metrics:
            res.update(metric.apply(self, ground_truth))
        return res

    def get_copy(self: MatcherResults) -> MatcherResults:
        """Summary
        Returns a copy of this instance.

        Returns
        -------
        MatcherResults
            A copy of this MatcherResults instance.
        """
        return MatcherResults(self.copy())
