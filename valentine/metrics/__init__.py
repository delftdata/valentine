from .base_metric import Metric
from .metrics import (
    F1Score,
    Precision,
    PrecisionTopNPercent,
    Recall,
    RecallAtSizeofGroundTruth,
)

__all__ = [
    "METRICS_ALL",
    "METRICS_CORE",
    "METRICS_PRECISION_INCREASING_N",
    "METRICS_PRECISION_RECALL",
    "F1Score",
    "Metric",
    "Precision",
    "PrecisionTopNPercent",
    "Recall",
    "RecallAtSizeofGroundTruth",
]

# Predefined metric sets.
#
# ``METRICS_ALL`` is an explicit listing rather than a dynamic scan of
# :class:`Metric` subclasses so that:
#   1. metrics requiring constructor arguments aren't silently dropped, and
#   2. user-defined metrics don't accidentally bleed into the predefined set.
METRICS_ALL = {
    Precision(),
    Precision(one_to_one=False),
    Recall(),
    Recall(one_to_one=False),
    F1Score(),
    F1Score(one_to_one=False),
    PrecisionTopNPercent(),
    RecallAtSizeofGroundTruth(),
}
METRICS_CORE = {
    Precision(),
    Recall(),
    F1Score(),
    PrecisionTopNPercent(),
    RecallAtSizeofGroundTruth(),
}
METRICS_PRECISION_RECALL = {Precision(), Recall()}
METRICS_PRECISION_INCREASING_N = {PrecisionTopNPercent(n=x + 10) for x in range(0, 100, 10)}
