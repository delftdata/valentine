"""Common metric implementations for Valentine.

Custom metrics can be created by subclassing :class:`Metric`. We use ``@dataclass``
with ``frozen=True`` so instances are hashable and comparable without boilerplate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence, Tuple

from .base_metric import Metric
from .metric_helpers import get_fp, get_tp_fn

# Public exports
__all__ = [
    "Precision",
    "Recall",
    "F1Score",
    "PrecisionTopNPercent",
    "RecallAtSizeofGroundTruth",
]

# Ground-truth is expressed as pairs of (left_col_name, right_col_name)
GroundTruth = Sequence[Tuple[str, str]]


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
        Percentage of matches to consider (0–100).
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
    """

    def apply(self, matches: Any, ground_truth: GroundTruth) -> dict[str, float]:
        n_matches = matches.take_top_n(len(ground_truth))
        tp, fn = get_tp_fn(n_matches, ground_truth)
        recall = _safe_div(tp, tp + fn)
        return self.return_format(recall)
