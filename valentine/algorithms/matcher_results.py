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
        # Cached default 1:1 selection (Hungarian, since it is the default
        # filter used by Precision / Recall / F1Score). Greedy and mutual
        # variants are niche and not cached.
        self._cached_hungarian: MatcherResults | None = None

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

    def one_to_one_hungarian(self, threshold: float | None = None) -> MatcherResults:  # noqa: PLR0912
        """Globally optimal 1:1 column matching via Hungarian assignment.

        This is the **default** 1:1 selector — it is what
        :class:`Precision` / :class:`Recall` / :class:`F1Score` call when
        their ``one_to_one`` flag is set. Each source and target appears
        in at most one returned pair, with the assignment chosen to
        maximise **total** similarity over all valid one-to-one
        assignments. Cost is O(n³) on column counts via
        ``scipy.optimize.linear_sum_assignment`` — negligible for
        typical schema sizes — and almost always strictly better than
        the greedy variant.

        Parameters
        ----------
        threshold : float | None
            Minimum similarity to keep. If ``None``, uses the median
            similarity score.

        Returns
        -------
        MatcherResults
            A new instance with the Hungarian-optimal one-to-one
            assignment, post-thresholding.
        """
        if threshold is None and self._cached_hungarian is not None:
            return self._cached_hungarian
        if not self._data:
            result = MatcherResults({})
            if threshold is None:
                self._cached_hungarian = result
            return result

        # Stable index of unique sources and targets.
        sources: list[tuple[str, str]] = []
        source_idx: dict[tuple[str, str], int] = {}
        targets: list[tuple[str, str]] = []
        target_idx: dict[tuple[str, str], int] = {}
        for cp in self._data:
            if cp.source not in source_idx:
                source_idx[cp.source] = len(sources)
                sources.append(cp.source)
            if cp.target not in target_idx:
                target_idx[cp.target] = len(targets)
                targets.append(cp.target)

        m, n = len(sources), len(targets)
        sim = [[0.0] * n for _ in range(m)]
        pair_lookup: dict[tuple, ColumnPair] = {}
        for cp, score in self._data.items():
            sim[source_idx[cp.source]][target_idx[cp.target]] = score
            pair_lookup[(cp.source, cp.target)] = cp

        # Hungarian minimises cost; we want max similarity.
        from scipy.optimize import linear_sum_assignment  # noqa: PLC0415

        cost = [[-s for s in row] for row in sim]
        row_ind, col_ind = linear_sum_assignment(cost)

        set_match_values = set(self._data.values())
        if len(set_match_values) < 2:
            result = MatcherResults(dict(self._data), details=dict(self._details))
            if threshold is None:
                self._cached_hungarian = result
            return result
        if threshold is None:
            min_sim = sorted(set_match_values, reverse=True)[math.ceil(len(set_match_values) / 2)]
        else:
            min_sim = threshold

        selected: dict[ColumnPair, float] = {}
        for r, c in zip(row_ind, col_ind, strict=False):
            cp = pair_lookup.get((sources[r], targets[c]))
            if cp is None:
                continue  # no actual pair at this (s, t)
            score = self._data[cp]
            if score >= min_sim:
                selected[cp] = score

        filtered_details = {k: v for k, v in self._details.items() if k in selected}
        result = MatcherResults(selected, details=filtered_details)
        if threshold is None:
            self._cached_hungarian = result
        return result

    def one_to_one_greedy(self, threshold: float | None = None) -> MatcherResults:
        """Greedy 1:1 column matching, kept for backwards compatibility.

        Starting from the highest-scoring pair, greedily assigns each
        source and target column at most one match. Pairs below
        ``threshold`` are discarded. When ``threshold`` is ``None`` (the
        default), the median similarity score is used.

        Greedy can lock in a locally-best pair that blocks a better
        globally-optimal assignment, so :meth:`one_to_one_hungarian` is
        the recommended default; this method is exposed for
        compatibility and for test pinning.

        Parameters
        ----------
        threshold : float | None
            Minimum similarity to keep. If ``None``, uses the median score.

        Returns
        -------
        MatcherResults
            A new instance with the greedy 1:1 assignment.
        """
        set_match_values = set(self._data.values())

        if len(set_match_values) < 2:
            return MatcherResults(dict(self._data), details=dict(self._details))

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
        return MatcherResults(matches1to1, details=filtered_details)

    def one_to_one_mutual_top(self, n: int = 1) -> MatcherResults:
        """Keep pairs where each side ranks the other in its top *n*.

        Pair ``(s, t)`` survives iff ``t`` is among ``s``'s ``n`` highest-
        scoring targets AND ``s`` is among ``t``'s ``n`` highest-scoring
        sources. With ``n=1`` this is the classic mutual nearest-
        neighbour filter — high-precision, drops one-sided affinities.
        Strictly stricter than :meth:`one_to_one_hungarian`: only
        mutually-confirmed pairs survive, even at the cost of recall.

        Parameters
        ----------
        n : int
            Top-n cutoff per side (default 1 = mutual nearest neighbour).

        Returns
        -------
        MatcherResults
            A new instance with only the mutually-confirmed pairs.
        """
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")
        if not self._data:
            return MatcherResults({})

        by_source: dict[tuple[str, str], list[tuple[float, tuple[str, str]]]] = {}
        by_target: dict[tuple[str, str], list[tuple[float, tuple[str, str]]]] = {}
        for cp, score in self._data.items():
            by_source.setdefault(cp.source, []).append((score, cp.target))
            by_target.setdefault(cp.target, []).append((score, cp.source))

        src_top: dict[tuple[str, str], set] = {}
        for s, lst in by_source.items():
            lst.sort(reverse=True)
            src_top[s] = {t for _, t in lst[:n]}
        tgt_top: dict[tuple[str, str], set] = {}
        for t, lst in by_target.items():
            lst.sort(reverse=True)
            tgt_top[t] = {s for _, s in lst[:n]}

        selected: dict[ColumnPair, float] = {
            cp: score
            for cp, score in self._data.items()
            if cp.target in src_top.get(cp.source, set())
            and cp.source in tgt_top.get(cp.target, set())
        }

        filtered_details = {k: v for k, v in self._details.items() if k in selected}
        return MatcherResults(selected, details=filtered_details)

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

    def take_top_n_per_source(self, n: int) -> MatcherResults:
        """Keep the top ``n`` matches per source column.

        Parameters
        ----------
        n : int
            Number of matches to keep per source column.

        Returns
        -------
        MatcherResults
            A new instance with only the top ``n`` matches per source column.
        """
        counts: dict[tuple[str, str], int] = {}
        filtered: dict[ColumnPair, float] = {}
        for key, score in self._data.items():
            source = key.source
            if counts.get(source, 0) < n:
                filtered[key] = score
                counts[source] = counts.get(source, 0) + 1
        filtered_details = {k: v for k, v in self._details.items() if k in filtered}
        return MatcherResults(filtered, details=filtered_details)

    # -- Metrics -----------------------------------------------------------

    def get_metrics(
        self,
        ground_truth: list[tuple[str, str]] | list[ColumnPair],
        metrics: set[Metric] = METRICS_CORE,
        one_to_one_method: str = "hungarian",
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
        one_to_one_method : {"greedy", "hungarian", "mutual_top"}
            Selection algorithm passed to each metric's ``apply`` method
            for use when the metric's ``one_to_one`` flag is ``True``
            (default: ``"hungarian"``).

        Returns
        -------
        dict[str, Any]
            Metric name to score mapping.
        """
        res: dict[str, Any] = {}
        for metric in metrics:
            res.update(metric.apply(self, ground_truth, one_to_one_method=one_to_one_method))
        return res

    # -- Copies ------------------------------------------------------------

    def get_copy(self) -> MatcherResults:
        """Return a shallow copy of this instance."""
        return MatcherResults(dict(self._data), details=dict(self._details))
