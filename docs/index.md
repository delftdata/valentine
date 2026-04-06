---
icon: lucide/heart
---

# Valentine 💘

*(Schema-) Matching DataFrames Made Easy.*

Valentine is a Python package for capturing potential relationships among
columns of different tabular datasets, given as pandas DataFrames. It
implements several schema- and instance-based matching algorithms behind a
single, uniform API, and ships with evaluation metrics so you can measure
match quality against a ground truth.

Valentine is based on the paper
[*Valentine: Evaluating Matching Techniques for Dataset Discovery*][paper].

  [paper]: https://ieeexplore.ieee.org/abstract/document/9458921

## Why Valentine?

!!! tip "One API, many algorithms"

    Pick any matcher — Coma, Cupid, DistributionBased, JaccardDistanceMatcher,
    SimilarityFlooding — and run it against any number of DataFrames with a
    single `valentine_match(...)` call.

- **Multi-table matching.** Pass any iterable of DataFrames and Valentine
  computes all `N * (N - 1) / 2` unique pairs.
- **Rich results.** [`MatcherResults`][results] is an immutable mapping of
  [`ColumnPair`][results] to similarity scores, with convenience methods for
  filtering, one-to-one reduction, and top-N selection.
- **Built-in evaluation.** Compute Precision, Recall, F1, and other metrics
  against a ground truth with a single call.
- **Match explanations.** Coma exposes per-sub-matcher score breakdowns so
  you can see *why* two columns were matched.

  [results]: results.md

## Installation

```shell
pip install valentine
```

Requires Python **>=3.10, <3.15**.

## A 30-second taste

```python
import pandas as pd
from valentine import valentine_match
from valentine.algorithms import Coma

df1 = pd.read_csv("source_candidates.csv")
df2 = pd.read_csv("target_candidates.csv")

matches = valentine_match([df1, df2], Coma(use_instances=True))

for pair, score in matches.items():
    print(f"{pair.source_column} <-> {pair.target_column}: {score:.3f}")
```

Ready for more? Head over to [Getting started](getting-started.md).

## Research

Valentine started as a research project at [Delft Data][delftdata] and is
based on the ICDE 2021 paper. See the [Research](research.md) page for the
papers behind the package, the algorithms it implements, and citation info.

  [delftdata]: https://delftdata.github.io/
