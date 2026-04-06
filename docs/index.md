---
icon: lucide/heart
hide:
  - navigation
  - toc
---

<div class="valentine-hero" markdown>

# Valentine 💘

<p class="valentine-tagline"><em>(Schema-) Matching DataFrames Made Easy.</em></p>

<p class="valentine-badges">
<a href="https://pypi.org/project/valentine/"><img src="https://img.shields.io/pypi/v/valentine.svg" alt="PyPI version"></a>
<a href="https://pypi.org/project/valentine/"><img src="https://img.shields.io/pypi/pyversions/valentine.svg" alt="Python versions"></a>
<a href="https://pypi.org/project/valentine/"><img src="https://img.shields.io/pypi/dm/valentine.svg" alt="PyPI downloads"></a>
<a href="https://github.com/delftdata/valentine/actions/workflows/build.yml"><img src="https://github.com/delftdata/valentine/actions/workflows/build.yml/badge.svg" alt="Build"></a>
<a href="https://codecov.io/gh/delftdata/valentine"><img src="https://codecov.io/gh/delftdata/valentine/branch/master/graph/badge.svg?token=4QR0X315CL" alt="codecov"></a>
<a href="https://github.com/delftdata/valentine/blob/master/LICENSE"><img src="https://img.shields.io/github/license/delftdata/valentine.svg" alt="License"></a>
</p>

[Get started :material-rocket-launch:](getting-started.md){ .md-button .md-button--primary }
[API reference :material-book-open:](api.md){ .md-button }
[View on GitHub :fontawesome-brands-github:](https://github.com/delftdata/valentine){ .md-button }

</div>

Valentine is a Python package for capturing potential relationships among
columns of different tabular datasets, given as pandas DataFrames. It
implements several schema- and instance-based matching algorithms behind a
single, uniform API, and ships with evaluation metrics so you can measure
match quality against a ground truth.

<div class="valentine-chips" markdown>
[Coma](api.md#coma)
[Cupid](api.md#cupid)
[DistributionBased](api.md#distributionbased)
[JaccardDistanceMatcher](api.md#jaccarddistancematcher)
[SimilarityFlooding](api.md#similarityflooding)
</div>

## Why Valentine?

<div class="grid cards" markdown>

-   :material-layers-triple:{ .lg .middle } __One API, many algorithms__

    ---

    Pick any matcher — [`Coma`](api.md#coma), [`Cupid`](api.md#cupid),
    [`DistributionBased`](api.md#distributionbased),
    [`JaccardDistanceMatcher`](api.md#jaccarddistancematcher),
    [`SimilarityFlooding`](api.md#similarityflooding) — and run it with a
    single [`valentine_match(...)`](api.md#valentine_match) call.

-   :material-table-multiple:{ .lg .middle } __Multi-table matching__

    ---

    Pass any iterable of DataFrames and Valentine computes all
    `N * (N - 1) / 2` unique pairs in one go. Lists, tuples, generators
    — they all work.

-   :material-filter-variant:{ .lg .middle } __Rich, immutable results__

    ---

    [`MatcherResults`](api.md#matcherresults) is an immutable mapping of
    [`ColumnPair`](api.md#columnpair) to similarity scores, with
    convenience methods for filtering, one-to-one reduction, and top-N
    selection.

-   :material-chart-bar:{ .lg .middle } __Built-in evaluation__

    ---

    Compute Precision, Recall, F1, and more against a ground truth with
    a single call to [`get_metrics`](api.md#get_metrics). Plug in your
    own [`Metric`](api.md#metric) subclasses too.

-   :material-magnify-scan:{ .lg .middle } __Match explanations__

    ---

    [`Coma`](api.md#coma) exposes per-sub-matcher score breakdowns via
    [`get_details`](api.md#get_details) so you can see *why* two columns
    were matched.

-   :material-language-python:{ .lg .middle } __Pure Python, no JVM__

    ---

    Every matcher — including Coma — is implemented in pure Python.
    No Java, no subprocess, no temp files. Just `pip install valentine`.

</div>

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

Ready for more? Head over to [Getting started](getting-started.md),
or jump straight to the [API reference](api.md).

## Research

Valentine started as a research project at [Delft Data][delftdata] and is
based on the ICDE 2021 paper. See the [Research](research.md) page for the
papers behind the package, the algorithms it implements, and citation info.

  [delftdata]: https://www.wis.ewi.tudelft.nl/data-management
