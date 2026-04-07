---
icon: lucide/heart
hide:
  - navigation
  - toc
---

<div class="valentine-hero" markdown>

# <img src="assets/favicon.png" alt="" class="valentine-hero-logo"> Valentine

<p class="valentine-tagline"><em>(Schema-) Matching DataFrames Made Easy.</em></p>

[Get started :material-rocket-launch:](getting-started.md){ .md-button .md-button--primary }
[API reference :material-book-open:](api.md){ .md-button }
[View on GitHub :fontawesome-brands-github:](https://github.com/delftdata/valentine){ .md-button }

<div class="valentine-chips" markdown>
[Coma](api.md#coma)
[Cupid](api.md#cupid)
[DistributionBased](api.md#distributionbased)
[JaccardDistanceMatcher](api.md#jaccarddistancematcher)
[SimilarityFlooding](api.md#similarityflooding)
</div>

<p class="valentine-badges">
<a href="https://pypi.org/project/valentine/"><img src="https://img.shields.io/pypi/v/valentine.svg" alt="PyPI version"></a>
<a href="https://pypi.org/project/valentine/"><img src="https://img.shields.io/pypi/pyversions/valentine.svg" alt="Python versions"></a>
<a href="https://pypi.org/project/valentine/"><img src="https://img.shields.io/pypi/dm/valentine.svg" alt="PyPI downloads"></a>
<a href="https://github.com/delftdata/valentine/actions/workflows/build.yml"><img src="https://github.com/delftdata/valentine/actions/workflows/build.yml/badge.svg" alt="Build"></a>
<a href="https://codecov.io/gh/delftdata/valentine"><img src="https://codecov.io/gh/delftdata/valentine/branch/master/graph/badge.svg?token=4QR0X315CL" alt="codecov"></a>
<a href="https://github.com/delftdata/valentine/blob/master/LICENSE"><img src="https://img.shields.io/github/license/delftdata/valentine.svg" alt="License"></a>
</p>

</div>

Valentine is a Python package for capturing potential relationships among
columns of different tabular datasets, given as pandas DataFrames. It
implements several schema- and instance-based matching algorithms behind a
single, uniform API, and ships with evaluation metrics so you can measure
match quality against a ground truth.

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
