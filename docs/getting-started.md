---
icon: lucide/rocket
---

# Getting started

## Installation

Valentine is published on [PyPI][pypi] and installs with a single `pip`
command. It requires **Python 3.10 or newer** (and is tested up to 3.14).

  [pypi]: https://pypi.org/project/valentine/

=== "pip"

    ```shell
    pip install valentine
    ```

=== "uv"

    ```shell
    uv add valentine
    ```

=== "poetry"

    ```shell
    poetry add valentine
    ```

For local development, clone the repo and install in editable mode:

```shell
git clone https://github.com/delftdata/valentine
cd valentine
pip install -e ".[dev]"
```

## Your first match

The single entry point for matching is
[`valentine_match`](api.md#valentine_match). It takes an iterable of
DataFrames and a matcher instance, and returns a
[`MatcherResults`](api.md#matcherresults) mapping — see the
[Matcher results](results.md) guide for everything you can do with it.

```python
import pandas as pd
from valentine import valentine_match
from valentine.algorithms import Coma

df1 = pd.read_csv("source_candidates.csv")
df2 = pd.read_csv("target_candidates.csv")

matcher = Coma(use_instances=True)
matches = valentine_match([df1, df2], matcher)

for pair, score in matches.items():
    print(f"{pair.source_column} <-> {pair.target_column}: {score:.3f}")
```

!!! note "Table names"

    Each [`ColumnPair`](api.md#columnpair) key in the results carries
    both a `source_table` and a `target_table`. By default these default
    to `"aaa"`, `"bbb"`, `"ccc"`, … — low-similarity names that won't
    bias schema-based matchers. Pass `df_names=["sales", "orders", ...]`
    to set your own.

## Matching many DataFrames

Pass any iterable of DataFrames — list, tuple, generator — and Valentine
computes all unique pairs:

```python
matches = valentine_match(
    [sales_df, orders_df, products_df],
    Coma(),
    df_names=["sales", "orders", "products"],
)
```

Each matcher decides for itself how to handle the batch. Algorithms
that benefit from a holistic view of all tables ([`Coma`](api.md#coma)'s
TF-IDF corpus, [`SimilarityFlooding`](api.md#similarityflooding)'s IDF
weights, [`DistributionBased`](api.md#distributionbased)'s global ranks)
override [`get_matches_batch`](api.md#get_matches_batch) so their
statistics reflect the *entire* input rather than just the current
pair.

## Picking a matcher

Valentine ships with five matching algorithms covering both schema- and
instance-based matching:

| Matcher                                                      | Type                 | Good at                                      |
|--------------------------------------------------------------|----------------------|----------------------------------------------|
| [`Coma`](api.md#coma)                                         | Schema + Instance    | General-purpose, interpretable, well-tuned   |
| [`Cupid`](api.md#cupid)                                       | Schema only          | Tree/linguistic similarity                   |
| [`DistributionBased`](api.md#distributionbased)               | Instance only        | Numeric & categorical value distributions    |
| [`JaccardDistanceMatcher`](api.md#jaccarddistancematcher)     | Instance only        | Exact/fuzzy Jaccard on value sets            |
| [`SimilarityFlooding`](api.md#similarityflooding)             | Schema only          | Graph-based fixpoint propagation             |

See [Matchers](matchers.md) for the conceptual guide, or jump straight
to the [API reference](api.md#matchers-valentinealgorithms) for
parameter defaults.

## Evaluating a match

If you have a ground truth — a list of expected column pairs — Valentine
computes Precision, Recall, F1 and other metrics in one call:

```python
ground_truth = [
    ("emp_id", "employee_number"),
    ("fname",  "first_name"),
    ("lname",  "last_name"),
    ("dept",   "department"),
]

metrics = matches.get_metrics(ground_truth)
print(metrics)
```

Full details are in [Evaluation metrics](metrics.md), with the method
signature documented under
[`MatcherResults.get_metrics`](api.md#get_metrics).
