---
icon: lucide/heart
hide:
  - toc
---

<div class="valentine-hero" markdown>

# <img src="assets/favicon.png" alt="" class="valentine-hero-logo"> Valentine

<p class="valentine-tagline"><em>(Schema-) Matching DataFrames Made Easy.</em></p>

[Get started :material-rocket-launch:](getting-started.md){ .md-button .md-button--primary }
[API reference :material-book-open:](api.md){ .md-button }
[View on GitHub :fontawesome-brands-github:](https://github.com/delftdata/valentine){ .md-button }

<div class="grid cards valentine-matcher-cards" markdown>

- __[Coma](api.md#coma)__

    ---

    Schema + instances. General-purpose first choice — strong defaults,
    informative sub-scores.

- __[Cupid](api.md#cupid)__

    ---

    Schema only. Nested schemas where column names and structure matter
    more than data.

- __[DistributionBased](api.md#distributionbased)__

    ---

    Instances only. Matching by value distributions when names are
    unreliable.

- __[JaccardDistanceMatcher](api.md#jaccarddistancematcher)__

    ---

    Instances only. Simple, explainable baseline — useful for sanity
    checks.

- __[SimilarityFlooding](api.md#similarityflooding)__

    ---

    Schema only. Structure-heavy schemas where graph neighbourhoods
    carry signal.

</div>

<p class="valentine-badges">
<a href="https://pypi.org/project/valentine/"><img src="https://img.shields.io/pypi/v/valentine.svg" alt="PyPI version"></a>
<a href="https://pypi.org/project/valentine/"><img src="https://img.shields.io/pypi/pyversions/valentine.svg" alt="Python versions"></a>
<a href="https://pypi.org/project/valentine/"><img src="https://static.pepy.tech/badge/valentine/month" alt="PyPI downloads"></a>
<a href="https://github.com/delftdata/valentine/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/delftdata/valentine/build.yml?label=build" alt="Build"></a>
<a href="https://codecov.io/gh/delftdata/valentine"><img src="https://img.shields.io/codecov/c/github/delftdata/valentine?label=coverage" alt="codecov"></a>
<a href="https://github.com/delftdata/valentine/blob/master/LICENSE"><img src="https://img.shields.io/github/license/delftdata/valentine.svg" alt="License"></a>
</p>

</div>

Valentine is a Python package for capturing potential relationships among
columns of different tabular datasets, given as pandas or Polars DataFrames.
It implements several schema- and instance-based matching algorithms behind a
single, uniform API, and ships with evaluation metrics so you can measure
match quality against a ground truth. Pandas and Polars frames can be freely
mixed in the same call.

## Installation

```shell
pip install valentine             # pandas only
pip install valentine[polars]     # pandas + Polars support
pip install valentine[embeddings] # pandas + embeddings support
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

Valentine started as a research project at [TU Delft][delftdata] and is
based on the ICDE 2021 paper. See the [Research](research.md) page for the
papers behind the package, the algorithms it implements, and citation info.

  [delftdata]: https://dis.ewi.tudelft.nl
