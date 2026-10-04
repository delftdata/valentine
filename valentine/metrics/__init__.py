from .base_metric import Metric
from .metrics import (
    F1Score,
    MeanReciprocalRank,
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
    "MeanReciprocalRank",
    "Metric",
    "Precision",
    "PrecisionTopNPercent",
    "Recall",
    "RecallAtSizeofGroundTruth",
]

# Predefined metric sets.
#
# ``METRICS_ALL`` is an explicit listing rather than a dynamic scan of
# `Metric` subclasses so that:
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
    MeanReciprocalRank(),
}
"""Both ``one_to_one=True`` and ``one_to_one=False`` variants of
`Precision`, `Recall`, `F1Score`, plus
`PrecisionTopNPercent`, `RecallAtSizeofGroundTruth`, and
`MeanReciprocalRank`.
"""
METRICS_CORE = {
    Precision(),
    Recall(),
    F1Score(),
    PrecisionTopNPercent(),
    RecallAtSizeofGroundTruth(),
    MeanReciprocalRank(),
}
"""``Precision``, ``Recall``, ``F1Score``, ``PrecisionTopNPercent``,
``RecallAtSizeofGroundTruth``, ``MeanReciprocalRank`` (defaults).
"""
METRICS_PRECISION_RECALL = {Precision(), Recall()}
"""``{Precision(), Recall()}``."""
METRICS_PRECISION_INCREASING_N = {PrecisionTopNPercent(n=x + 10) for x in range(0, 100, 10)}
"""``PrecisionTopNPercent`` for ``n`` in ``{10, 20, 30, ..., 100}``."""
