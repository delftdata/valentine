---
icon: lucide/gauge
---

# Evaluation metrics

Given a ground truth — a list of expected column matches — Valentine
computes Precision, Recall, F1 and related metrics in one call:

```python
metrics = matches.get_metrics(ground_truth)
```

The ground truth can be expressed in two formats.

## Ground-truth formats

**Column-name pairs** (table names ignored):

```python
ground_truth = [
    ("emp_id", "employee_number"),
    ("fname",  "first_name"),
    ("lname",  "last_name"),
]
```

**Full `ColumnPair` instances** (table-aware comparison):

```python
from valentine.algorithms import ColumnPair

ground_truth = [
    ColumnPair("hr", "emp_id", "payroll", "employee_number"),
    ColumnPair("hr", "fname",  "payroll", "first_name"),
]
```

Use `ColumnPair` ground truth when you're matching more than two tables, or
when source and target tables share column names — without table info the
metric code can't tell which match is which.

## Built-in metrics

Valentine ships five metrics, all living in `valentine.metrics`:

```python
from valentine.metrics import (
    Precision,
    Recall,
    F1Score,
    PrecisionTopNPercent,
    RecallAtSizeofGroundTruth,
)
```

| Metric                     | Description                                                                                          |
|----------------------------|------------------------------------------------------------------------------------------------------|
| `Precision`                | TP / (TP + FP)                                                                                       |
| `Recall`                   | TP / (TP + FN)                                                                                       |
| `F1Score`                  | Harmonic mean of precision and recall                                                                 |
| `PrecisionTopNPercent`     | Precision restricted to the top-N% of matches by score                                               |
| `RecallAtSizeofGroundTruth`| Recall when considering the top-`len(ground_truth)` matches                                          |

`Precision`, `Recall`, `F1Score`, and `PrecisionTopNPercent` have a
`one_to_one: bool` flag (default `True`). When enabled, the metric is
computed after reducing the results with `matches.one_to_one()`.
`PrecisionTopNPercent` additionally takes `n: int` (default `10`) for the
percentage cutoff.

## Default metric set

If you call `get_metrics` with no explicit metric set, Valentine uses
`METRICS_CORE`:

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

## Predefined metric sets

Valentine also ships a few preconfigured sets for common experiments:

| Set                              | Contents                                                                  |
|----------------------------------|---------------------------------------------------------------------------|
| `METRICS_CORE`                   | Default set used by `get_metrics`.                                        |
| `METRICS_ALL`                    | Every built-in metric, including both `one_to_one=True` and `=False` variants. |
| `METRICS_PRECISION_RECALL`       | Just `Precision` and `Recall`.                                            |
| `METRICS_PRECISION_INCREASING_N` | `PrecisionTopNPercent` at `n = 10, 20, ..., 100`.                          |

```python
from valentine.metrics import METRICS_PRECISION_INCREASING_N

metrics = matches.get_metrics(ground_truth, metrics=METRICS_PRECISION_INCREASING_N)
```

## Custom metric selection

Pass any set of metric instances to pick exactly what you want:

```python
from valentine.metrics import F1Score, PrecisionTopNPercent

metrics = matches.get_metrics(
    ground_truth,
    metrics={F1Score(one_to_one=False), PrecisionTopNPercent(n=70)},
)
```

Each metric is computed independently, and the returned dict is keyed by
the metric's `name()` — which for `PrecisionTopNPercent` substitutes the
`n` value, so you get `PrecisionTop70Percent` in the output.

## Defining your own metric

Subclass `Metric` and implement `apply`:

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

The dataclass **must** be `frozen=True` so metric instances are hashable
and comparable — `get_metrics` takes a `set` of metrics.
