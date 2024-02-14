from valentine.metrics.base_metric import Metric
from .metrics import *

# Some predefined sets of metrics
METRICS_ALL = {metric() for metric in Metric.__subclasses__()}  # Note: will also catch newly defined metrics
METRICS_CORE = {Precision(), Recall(), F1Score(), PrecisionTopNPercent(), RecallAtSizeofGroundTruth()}
METRICS_PRECISION_RECALL = {Precision(), Recall()}
METRICS_PRECISION_INCREASING_N = {PrecisionTopNPercent(n=x + 10) for x in range(0, 100, 10)}
