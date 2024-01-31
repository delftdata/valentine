"""Here one can find some common metric implementations. Custom metrics can be
made by subclassing the `Metric` ABC. Marking them with the dataclass decorator
allows for proper hashing/equals without the boilerplate.
"""
from .base_metric import Metric
from .metric_helpers import *
from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Precision(Metric):
    """Metric for calculating precision.

    Attributes
    ----------
    one_to_one : bool
    Whether to apply the one-to-one filter to the MatcherResults first.
    """
    one_to_one: bool = True

    def apply(self, matches, ground_truth):
        if self.one_to_one:
            matches = matches.one_to_one()

        tp, _ = get_tp_fn(matches, ground_truth)
        fp = get_fp(matches, ground_truth)
        precision = 0
        if tp + fp > 0:
            precision = tp / (tp + fp)

        return self.return_format(precision)


@dataclass(eq=True, frozen=True)
class Recall(Metric):
    """Metric for calculating recall.

    Attributes
    ----------
    one_to_one : bool
    Whether to apply the one-to-one filter to the MatcherResults first.
    """
    one_to_one: bool = True

    def apply(self, matches, ground_truth):
        if self.one_to_one:
            matches = matches.one_to_one()

        tp, fn = get_tp_fn(matches, ground_truth)
        recall = 0
        if tp + fn > 0:
            recall = tp / (tp + fn)

        return self.return_format(recall)


@dataclass(eq=True, frozen=True)
class F1Score(Metric):
    """Metric for calculating f1 score.

    Attributes
    ----------
    one_to_one : bool
    Whether to apply the one-to-one filter to the MatcherResults first.
    """
    one_to_one: bool = True

    def apply(self, matches, ground_truth):
        if self.one_to_one:
            matches = matches.one_to_one()

        tp, fn = get_tp_fn(matches, ground_truth)
        fp = get_fp(matches, ground_truth)
        f1 = 0
        if tp > 0:
            pr = tp / (tp + fp)
            re = tp / (tp + fn)
            f1 = 2 * ((pr * re) / (pr + re))

        return self.return_format(f1)


@dataclass(eq=True, frozen=True)
class PrecisionTopNPercent(Metric):
    """Metric for calculating precision of the top N percent of matches.

    Attributes
    ----------
    one_to_one : bool
    Whether to apply the one-to-one filter to the MatcherResults first.
    n : int
    The percent of matches to consider.
    """
    one_to_one: bool = True
    n: int = 10

    def name(self):
        return super().name().replace('N', str(self.n))

    def apply(self, matches, ground_truth):
        if self.one_to_one:
            matches = matches.one_to_one()

        n_matches = matches.take_top_percent(self.n)

        tp, _ = get_tp_fn(n_matches, ground_truth)
        fp = get_fp(n_matches, ground_truth)
        precision_top_n_percent = 0
        if tp + fp > 0:
            precision_top_n_percent = tp / (tp + fp)

        return self.return_format(precision_top_n_percent)


@dataclass(eq=True, frozen=True)
class RecallAtSizeofGroundTruth(Metric):
    """Metric for calculating recall at the size of the ground truth.
    """

    def apply(self, matches, ground_truth):
        n_matches = matches.take_top_n(len(ground_truth))

        tp, fn = get_tp_fn(n_matches, ground_truth)
        recall = 0
        if tp + fn > 0:
            recall = tp / (tp + fn)

        return self.return_format(recall)
