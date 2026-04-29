"""Common metric implementations for Valentine.

Custom metrics can be created by subclassing :class:`Metric`. We use ``@dataclass``
with ``frozen=True`` so instances are hashable and comparable without boilerplate.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .base_metric import Metric
from .metric_helpers import _matches_as_tuples, _normalize_ground_truth, get_fp, get_tp_fn

# Public exports
__all__ = [
    "F1Score",
    "MeanReciprocalRank",
    "Precision",
    "PrecisionTopNPercent",
    "Recall",
    "RecallAtSizeofGroundTruth",
]

# Ground truth can be either (source_col, target_col) name pairs or
# full ColumnPair instances (table-aware comparison).
GroundTruth = Sequence[tuple[str, str]] | Sequence[tuple]


def _safe_div(numerator: float, denominator: float) -> float:
    """Return numerator/denominator, guarding against division by zero."""
    return numerator / denominator if denominator else 0.0


@dataclass(eq=True, frozen=True)
class Precision(Metric):
    """Precision = TP / (TP + FP).

    Attributes
    ----------
    one_to_one : bool
        Whether to apply the one-to-one filter to the MatcherResults first.
    """

    one_to_one: bool = True

    def apply(self, matches: Any, ground_truth: GroundTruth) -> dict[str, float]:
        if self.one_to_one:
            matches = matches.one_to_one()

        tp, _ = get_tp_fn(matches, ground_truth)
        fp = get_fp(matches, ground_truth)
        precision = _safe_div(tp, tp + fp)
        return self.return_format(precision)


@dataclass(eq=True, frozen=True)
class Recall(Metric):
    """Recall = TP / (TP + FN).

    Attributes
    ----------
    one_to_one : bool
        Whether to apply the one-to-one filter to the MatcherResults first.
    """

    one_to_one: bool = True

    def apply(self, matches: Any, ground_truth: GroundTruth) -> dict[str, float]:
        if self.one_to_one:
            matches = matches.one_to_one()

        tp, fn = get_tp_fn(matches, ground_truth)
        recall = _safe_div(tp, tp + fn)
        return self.return_format(recall)


@dataclass(eq=True, frozen=True)
class F1Score(Metric):
    """F1 score = 2 * (Precision * Recall) / (Precision + Recall).

    Attributes
    ----------
    one_to_one : bool
        Whether to apply the one-to-one filter to the MatcherResults first.
    """

    one_to_one: bool = True

    def apply(self, matches: Any, ground_truth: GroundTruth) -> dict[str, float]:
        if self.one_to_one:
            matches = matches.one_to_one()

        tp, fn = get_tp_fn(matches, ground_truth)
        fp = get_fp(matches, ground_truth)

        precision = _safe_div(tp, tp + fp)
        recall = _safe_div(tp, tp + fn)
        f1 = _safe_div(2.0 * (precision * recall), (precision + recall))
        return self.return_format(f1)


@dataclass(eq=True, frozen=True)
class PrecisionTopNPercent(Metric):
    """Precision restricted to the top-N% of predicted matches by score.

    Attributes
    ----------
    one_to_one : bool
        Whether to apply the one-to-one filter to the MatcherResults first.
    n : int
        Percentage of matches to consider (0-100).
    """

    one_to_one: bool = True
    n: int = 10

    def name(self) -> str:
        # Replace the 'N' in the base name with the chosen percent, e.g. "PrecisionTop70Percent".
        return super().name().replace("N", str(self.n))

    def apply(self, matches: Any, ground_truth: GroundTruth) -> dict[str, float]:
        if self.one_to_one:
            matches = matches.one_to_one()

        # Clamp N to a sensible range without mutating the dataclass.
        n_clamped = min(100, max(0, int(self.n)))
        n_matches = matches.take_top_percent(n_clamped)

        tp, _ = get_tp_fn(n_matches, ground_truth)
        fp = get_fp(n_matches, ground_truth)
        precision_top_n_percent = _safe_div(tp, tp + fp)
        return self.return_format(precision_top_n_percent)


@dataclass(eq=True, frozen=True)
class RecallAtSizeofGroundTruth(Metric):
    """Recall when considering the top-|GT| predictions.

    This simulates selecting as many predictions as there are gold pairs and
    computing recall at that cutoff: TP / (TP + FN) where the candidate set is
    the top-``len(ground_truth)`` matches by score.

    Attributes
    ----------
    one_to_one : bool
        Whether to apply the one-to-one filter to the MatcherResults first.
    """

    one_to_one: bool = False

    def apply(self, matches: Any, ground_truth: GroundTruth) -> dict[str, float]:
        if self.one_to_one:
            matches = matches.one_to_one()
        n_matches = matches.take_top_n(len(ground_truth))
        tp, fn = get_tp_fn(n_matches, ground_truth)
        recall = _safe_div(tp, tp + fn)
        return self.return_format(recall)


@dataclass(eq=True, frozen=True)
class MeanReciprocalRank(Metric):
    """Mean Reciprocal Rank (MRR).

    For each ground truth pair, finds its rank (1-indexed position) in
    the matcher results sorted by descending score. The reciprocal rank
    is ``1/rank`` if the pair is found, or ``0`` if absent. MRR is the
    mean of reciprocal ranks across all ground truth pairs.

    Attributes
    ----------
    one_to_one : bool
        Whether to apply the one-to-one filter to the MatcherResults first.
    """

    one_to_one: bool = False

    def apply(self, matches: Any, ground_truth: GroundTruth) -> dict[str, float]:
        if self.one_to_one:
            matches = matches.one_to_one()

        gt_pairs, table_aware = _normalize_ground_truth(ground_truth)
        ranked = _matches_as_tuples(matches, table_aware)

        if not gt_pairs:
            return self.return_format(0.0)

        # Group gold targets by source key (table-aware or column-only).
        gold_by_source: dict[tuple, set[tuple]] = {}
        for pair in gt_pairs:
            source_key = pair[:2] if table_aware else (pair[0],)
            target_key = pair[2:] if table_aware else (pair[1],)
            gold_by_source.setdefault(source_key, set()).add(target_key)

        if not gold_by_source:
            return self.return_format(0.0)

        # Walk ranked predictions; record the rank (1-based, per-source) of
        # the first correct target for each source.
        per_source_rank: dict[tuple, int] = {}
        per_source_seen: dict[tuple, int] = {}
        for pred in ranked:
            source_key = pred[:2] if table_aware else (pred[0],)
            target_key = pred[2:] if table_aware else (pred[1],)
            if source_key not in gold_by_source or source_key in per_source_rank:
                continue
            per_source_seen[source_key] = per_source_seen.get(source_key, 0) + 1
            if target_key in gold_by_source[source_key]:
                per_source_rank[source_key] = per_source_seen[source_key]

        reciprocal_sum = sum(1.0 / r for r in per_source_rank.values())
        mrr = reciprocal_sum / len(gold_by_source)
        return self.return_format(mrr)
