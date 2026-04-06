---
icon: lucide/gauge
---

# Evaluation metrics

Given a ground truth — a list of expected column matches — Valentine
computes Precision, Recall, F1 and related metrics in one call:

```python
metrics = matches.get_metrics(ground_truth)
```

This page is the **how-to guide** for using metrics. For the full list
of built-in metric classes, their parameters, and the predefined metric
sets, see the [API reference](api.md#metrics-valentinemetrics).

## Ground-truth formats

`ground_truth` can be expressed in two formats, both accepted by
[`MatcherResults.get_metrics`](api.md#get_metrics).

**Column-name pairs** (table names ignored):

```python
ground_truth = [
    ("emp_id", "employee_number"),
    ("fname",  "first_name"),
    ("lname",  "last_name"),
]
```

**Full [`ColumnPair`](api.md#columnpair) instances** (table-aware
comparison):

```python
from valentine.algorithms import ColumnPair

ground_truth = [
    ColumnPair("hr", "emp_id", "payroll", "employee_number"),
    ColumnPair("hr", "fname",  "payroll", "first_name"),
]
```

Use [`ColumnPair`](api.md#columnpair) ground truth when you're matching
more than two tables, or when source and target tables share column
names — without table info the metric code can't tell which match is
which.

## Built-in metrics

Valentine ships five metrics, all in `valentine.metrics`:

```python
from valentine.metrics import (
    Precision,
    Recall,
    F1Score,
    PrecisionTopNPercent,
    RecallAtSizeofGroundTruth,
)
```

| Metric                                                   | What it measures                                                   |
|----------------------------------------------------------|---------------------------------------------------------------------|
| [`Precision`](api.md#precision)                           | TP / (TP + FP).                                                    |
| [`Recall`](api.md#recall)                                 | TP / (TP + FN).                                                    |
| [`F1Score`](api.md#f1score)                               | Harmonic mean of precision and recall.                             |
| [`PrecisionTopNPercent`](api.md#precisiontopnpercent)     | Precision restricted to the top `n%` of matches by score.          |
| [`RecallAtSizeofGroundTruth`](api.md#recallatsizeofgroundtruth) | Recall when selecting the top `len(ground_truth)` matches.  |

`Precision`, `Recall`, `F1Score` and `PrecisionTopNPercent` all accept a
`one_to_one: bool` flag that applies
[`MatcherResults.one_to_one()`](api.md#one_to_one) before counting.
`PrecisionTopNPercent` additionally takes `n: int` for the cutoff, and
`RecallAtSizeofGroundTruth` defaults to `one_to_one=False`. See the
[API reference](api.md#built-in-metrics) for full defaults.

## Default metric set

If you call [`get_metrics`](api.md#get_metrics) without specifying
metrics, Valentine uses `METRICS_CORE`:

```python
metrics = matches.get_metrics(ground_truth)
# {
#   "Precision": ...,
#   "Recall": ...,
#   "F1Score": ...,
#   "PrecisionTop10Percent": ...,
#   "RecallAtSizeofGroundTruth": ...,
# }
```

Valentine also ships [`METRICS_ALL`](api.md#predefined-metric-sets),
[`METRICS_PRECISION_RECALL`](api.md#predefined-metric-sets), and
[`METRICS_PRECISION_INCREASING_N`](api.md#predefined-metric-sets) for
common experiment shapes.

```python
from valentine.metrics import METRICS_PRECISION_INCREASING_N

metrics = matches.get_metrics(
    ground_truth,
    metrics=METRICS_PRECISION_INCREASING_N,
)
```

## Custom metric selection

Pass any `set` of metric instances to pick exactly what you want:

```python
from valentine.metrics import F1Score, PrecisionTopNPercent

metrics = matches.get_metrics(
    ground_truth,
    metrics={F1Score(one_to_one=False), PrecisionTopNPercent(n=70)},
)
```

Each metric is computed independently, and the returned dict is keyed
by the metric's `name()` — which for
[`PrecisionTopNPercent`](api.md#precisiontopnpercent) substitutes the
`n` value, so you get `PrecisionTop70Percent` in the output.

## Defining your own metric

Subclass [`Metric`](api.md#metric) and implement
[`apply`](api.md#apply):

```python
from dataclasses import dataclass
from valentine.metrics import Metric


@dataclass(eq=True, frozen=True)
class SupportAtK(Metric):
    k: int = 5

    def apply(self, matches, ground_truth):
        top_k = matches.take_top_n(self.k)
        return self.return_format(len(top_k) / self.k)


metrics = matches.get_metrics(ground_truth, metrics={SupportAtK(k=10)})
```

The dataclass **must** be `frozen=True` so metric instances are
hashable and comparable — [`get_metrics`](api.md#get_metrics) takes a
`set` of metrics.
