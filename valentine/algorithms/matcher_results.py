from __future__ import annotations

import math
from collections.abc import Iterator, Mapping
from typing import Any

from ..metrics import METRICS_CORE
from ..metrics.base_metric import Metric
from .match import ColumnPair


class MatcherResults(Mapping):
    """Immutable mapping of :class:`ColumnPair` to similarity scores.

    Returned by :func:`valentine_match` and individual matcher methods.
    Results are sorted from highest to lowest similarity and cannot be
    mutated after creation (preventing accidental invalidation of cached
    derived views like :meth:`one_to_one`).

    Aside from standard mapping operations (``len``, iteration, indexing),
    provides convenience methods for filtering, subsetting, and computing
    evaluation metrics.

    Parameters
    ----------
    matches : dict[ColumnPair, float]
        Raw match scores.
    details : dict[ColumnPair, dict[str, float]] | None
        Optional per-pair breakdown of sub-matcher scores (e.g. from Coma).
    """

    def __init__(
        self,
        matches: dict[ColumnPair, float],
        details: dict[ColumnPair, dict[str, float]] | None = None,
    ):
        sorted_matches = dict(sorted(matches.items(), key=lambda x: x[1], reverse=True))
        self._data: dict[ColumnPair, float] = sorted_matches
        self._details: dict[ColumnPair, dict[str, float]] = details or {}
        self._cached_one_to_one: MatcherResults | None = None

    # -- Mapping protocol --------------------------------------------------

    def __getitem__(self, key: ColumnPair) -> float:
        return self._data[key]

    def __iter__(self) -> Iterator[ColumnPair]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        items = ", ".join(f"{k}: {v:.4f}" for k, v in list(self._data.items())[:5])
        suffix = ", ..." if len(self._data) > 5 else ""
        return f"MatcherResults({{{items}{suffix}}})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, MatcherResults):
            return self._data == other._data
        if isinstance(other, dict):
            return self._data == other
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]  # mutable-ish semantics; unhashable

    # -- Details -----------------------------------------------------------

    @property
    def details(self) -> dict[ColumnPair, dict[str, float]]:
        """Per-pair sub-matcher score breakdowns.

        When the matcher provides component scores (e.g. Coma's name,
        path, leaves, parents, instances matchers), this maps each
        :class:`ColumnPair` to ``{matcher_name: score}``.

        Returns an empty dict when the matcher does not provide details.
        """
        return self._details

    def get_details(self, key: ColumnPair) -> dict[str, float] | None:
        """Get the sub-matcher breakdown for a specific column pair.

        Returns ``None`` if no details are available for the given pair.
        """
        return self._details.get(key)

    # -- Transformations ---------------------------------------------------

    def one_to_one(self, threshold: float | None = None) -> MatcherResults:
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
            A new instance with one-to-one matches only.
        """
        if threshold is None and self._cached_one_to_one is not None:
            return self._cached_one_to_one

        set_match_values = set(self._data.values())

        if len(set_match_values) < 2:
            result = MatcherResults(dict(self._data), details=dict(self._details))
            if threshold is None:
                self._cached_one_to_one = result
            return result

        matched: dict[tuple[str, str], bool] = {}
        for key in self._data:
            matched[key.source] = False
            matched[key.target] = False

        if threshold is None:
            min_sim = sorted(set_match_values, reverse=True)[math.ceil(len(set_match_values) / 2)]
        else:
            min_sim = threshold

        matches1to1: dict[ColumnPair, float] = {}
        for key, similarity in self._data.items():
            if not matched[key.source] and not matched[key.target]:
                if similarity >= min_sim:
                    matches1to1[key] = similarity
                    matched[key.source] = True
                    matched[key.target] = True
                else:
                    break

        filtered_details = {k: v for k, v in self._details.items() if k in matches1to1}
        result = MatcherResults(matches1to1, details=filtered_details)
        if threshold is None:
            self._cached_one_to_one = result
        return result

    def filter(self, min_score: float) -> MatcherResults:
        """Filter matches by minimum similarity score.

        Parameters
        ----------
        min_score : float
            Minimum similarity score to keep.

        Returns
        -------
        MatcherResults
            A new instance containing only matches with score >= min_score.
        """
        filtered = {k: v for k, v in self._data.items() if v >= min_score}
        filtered_details = {k: v for k, v in self._details.items() if k in filtered}
        return MatcherResults(filtered, details=filtered_details)

    def take_top_percent(self, percent: int) -> MatcherResults:
        """Keep the top ``percent``% of matches by score.

        Parameters
        ----------
        percent : int
            Percentage of matches to keep (0-100).

        Returns
        -------
        MatcherResults
            A new instance with only the top matches.
        """
        number_to_keep = math.ceil((percent / 100) * len(self._data))
        top_items = dict(list(self._data.items())[:number_to_keep])
        filtered_details = {k: v for k, v in self._details.items() if k in top_items}
        return MatcherResults(top_items, details=filtered_details)

    def take_top_n(self, n: int) -> MatcherResults:
        """Keep the top ``n`` matches by score.

        Parameters
        ----------
        n : int
            Number of matches to keep.

        Returns
        -------
        MatcherResults
            A new instance with only the top ``n`` matches.
        """
        top_items = dict(list(self._data.items())[:n])
        filtered_details = {k: v for k, v in self._details.items() if k in top_items}
        return MatcherResults(top_items, details=filtered_details)

    # -- Metrics -----------------------------------------------------------

    def get_metrics(
        self,
        ground_truth: list[tuple[str, str]] | list[ColumnPair],
        metrics: set[Metric] = METRICS_CORE,
    ) -> dict[str, Any]:
        """Compute evaluation metrics against a ground truth.

        Parameters
        ----------
        ground_truth : list[tuple[str, str]] | list[ColumnPair]
            Expected column matches. Can be simple column-name pairs like
            ``[("col_a", "col_b")]`` or full :class:`ColumnPair` instances.
            When column-name pairs are used, table names are ignored during
            comparison.
        metrics : set[Metric], optional
            Set of metric instances to compute (default: ``METRICS_CORE``).

        Returns
        -------
        dict[str, Any]
            Metric name to score mapping.
        """
        res: dict[str, Any] = {}
        for metric in metrics:
            res.update(metric.apply(self, ground_truth))
        return res

    # -- Copies ------------------------------------------------------------

    def get_copy(self) -> MatcherResults:
        """Return a shallow copy of this instance."""
        return MatcherResults(dict(self._data), details=dict(self._details))
