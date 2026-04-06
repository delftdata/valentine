---
icon: lucide/cpu
---

# Matchers

Every matcher in Valentine subclasses [`BaseMatcher`](#base-matcher) and is
compatible with the top-level [`valentine_match`](getting-started.md#your-first-match)
API. You can also call matcher methods directly when you already have
[`BaseTable`][base-table] objects.

  [base-table]: https://github.com/delftdata/valentine/blob/master/valentine/data_sources/base_table.py

All five algorithms live in `valentine.algorithms`:

```python
from valentine.algorithms import (
    Coma,
    Cupid,
    DistributionBased,
    JaccardDistanceMatcher,
    SimilarityFlooding,
)
```

## `Coma`

A pure-Python implementation of the COMA 3.0 schema matching algorithm.
Coma composes multiple sub-matchers — each targeting a different aspect of
schema or data similarity — and combines their scores.

**Schema matchers** (enabled by `use_schema=True`, the default):

- **Name** — trigram (Dice) similarity on column names
- **Path** — trigram similarity on dot-separated schema paths
- **Leaves** — name similarity across all leaf-level columns
- **Parents** — structural similarity via parent-level leaf comparison

**Instance matcher** (enabled by `use_instances=True`):

- **TF-IDF cosine similarity** — each cell value is treated as a document, a
  global IDF is computed across all columns of both tables, and per-column
  similarity is aggregated with a max-matching Dice formula.

After computing all-pairs similarity scores, a selection step filters
results using bidirectional best-match logic (DIR_BOTH) controlled by
`max_n`, `delta`, and `threshold`.

```python
from valentine.algorithms import Coma

matcher = Coma(use_instances=True)
```

| Parameter       | Type    | Default | Description                                                                    |
|-----------------|---------|---------|--------------------------------------------------------------------------------|
| `max_n`         | `int`   | `0`     | Maximum number of matches to keep per column (`0` means unlimited).            |
| `use_instances` | `bool`  | `False` | Enable TF-IDF instance-based matching.                                         |
| `use_schema`    | `bool`  | `True`  | Enable schema-based matching.                                                  |
| `delta`         | `float` | `0.15`  | Fraction from the best score within which matches are kept.                    |
| `threshold`     | `float` | `0.0`   | Absolute minimum similarity score to keep a match.                             |

!!! tip "Match explanations"

    Coma is the only matcher that fills in per-sub-matcher score breakdowns.
    After running Coma, call `matches.get_details(pair)` to see how each
    individual sub-matcher contributed to the final score. See
    [Match details](results.md#match-details-coma).

## `Cupid`

Python implementation of [*Generic Schema Matching with Cupid*][cupid]
(Madhavan et al., VLDB 2001). Cupid combines linguistic similarity of
column names with structural similarity derived from schema tree shape.

  [cupid]: https://www.vldb.org/conf/2001/P049.pdf

```python
from valentine.algorithms import Cupid

matcher = Cupid(w_struct=0.2, leaf_w_struct=0.2, th_accept=0.7)
```

| Parameter       | Type    | Default | Description                                                    |
|-----------------|---------|---------|----------------------------------------------------------------|
| `leaf_w_struct` | `float` | `0.2`   | Weight of structural similarity at leaf level.                 |
| `w_struct`      | `float` | `0.2`   | Weight of structural similarity at inner-node level.           |
| `th_accept`     | `float` | `0.7`   | Acceptance similarity threshold for the final mapping.         |
| `th_high`       | `float` | `0.6`   | High-confidence threshold during structural propagation.      |
| `th_low`        | `float` | `0.35`  | Low-confidence threshold during structural propagation.       |
| `c_inc`         | `float` | `1.2`   | Positive reinforcement coefficient for matching children.     |
| `c_dec`         | `float` | `0.9`   | Negative reinforcement coefficient for non-matching children. |
| `th_ns`         | `float` | `0.7`   | Name-similarity threshold.                                     |
| `process_num`   | `int`   | `1`     | Number of worker processes.                                    |

## `DistributionBased`

Python implementation of [*Automatic Discovery of Attributes in Relational
Databases*][zhang] (Zhang et al., SIGMOD 2011). Columns are compared by
quantile histograms of their value distributions; Earth Mover's Distance
drives the ranking of matches within each cluster.

  [zhang]: https://dl.acm.org/doi/10.1145/1989323.1989336

```python
from valentine.algorithms import DistributionBased

matcher = DistributionBased(threshold1=0.15, threshold2=0.15)
```

| Parameter           | Type    | Default | Description                                                            |
|---------------------|---------|---------|------------------------------------------------------------------------|
| `threshold1`        | `float` | `0.15`  | Distance threshold for phase 1 (distribution clustering).              |
| `threshold2`        | `float` | `0.15`  | Distance threshold for phase 2 (attribute clustering).                 |
| `quantiles`         | `int`   | `256`   | Number of quantiles used for histogram summaries.                      |
| `process_num`       | `int`   | `1`     | Number of worker processes.                                            |
| `use_bloom_filters` | `bool`  | `False` | Use Bloom filters for approximate set intersection in phase 2.        |

## `JaccardDistanceMatcher`

A baseline instance-based matcher. Columns are compared by Jaccard
similarity of their value sets, with element equality decided by a
configurable string distance function.

```python
from valentine.algorithms import JaccardDistanceMatcher
from valentine.algorithms.jaccard_distance import StringDistanceFunction

matcher = JaccardDistanceMatcher(
    threshold_dist=0.8,
    distance_fun=StringDistanceFunction.Levenshtein,
)
```

| Parameter        | Type                     | Default        | Description                                                         |
|------------------|--------------------------|----------------|---------------------------------------------------------------------|
| `threshold_dist` | `float`                  | `0.8`          | Threshold above which two strings are considered equal.             |
| `distance_fun`   | `StringDistanceFunction` | `Levenshtein`  | String similarity function (see below).                             |
| `process_num`    | `int`                    | `1`            | Number of worker processes.                                         |

`StringDistanceFunction` is an enum exposing:

- `Levenshtein` — [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance)
- `DamerauLevenshtein` — [Damerau–Levenshtein distance](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance)
- `Hamming` — [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance)
- `Jaro` — [Jaro distance](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
- `JaroWinkler` — [Jaro–Winkler distance](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
- `Exact` — exact string equality (`==`)

## `SimilarityFlooding`

Python implementation of [*Similarity Flooding: A Versatile Graph Matching
Algorithm and its Application to Schema Matching*][sf]
(Melnik, Garcia-Molina, Rahm — ICDE 2002). Each schema is represented as a
labelled graph; an initial element-level similarity is iteratively
propagated across the graph until a fixpoint is reached.

  [sf]: https://ieeexplore.ieee.org/document/994702

```python
from valentine.algorithms import (
    Formula,
    Policy,
    SimilarityFlooding,
    StringMatcher,
)

matcher = SimilarityFlooding(
    coeff_policy=Policy.INVERSE_AVERAGE,
    formula=Formula.FORMULA_C,
    string_matcher=StringMatcher.PREFIX_SUFFIX,
)
```

| Parameter        | Type            | Default             | Description                                                                           |
|------------------|-----------------|---------------------|---------------------------------------------------------------------------------------|
| `coeff_policy`   | `Policy`        | `INVERSE_AVERAGE`   | Coefficient policy for the propagation graph.                                         |
| `formula`        | `Formula`       | `FORMULA_C`         | Fixpoint iteration formula.                                                            |
| `string_matcher` | `StringMatcher` | `PREFIX_SUFFIX`     | String similarity function used for the initial element-level mapping.                 |
| `tfidf_corpus`   | `list[BaseTable] \| None` | `None`    | Extra tables included when computing IDF weights for `PREFIX_SUFFIX_TFIDF`.            |

- `Policy` — `INVERSE_AVERAGE` (default), `INVERSE_PRODUCT`
- `Formula` — `BASIC`, `FORMULA_A`, `FORMULA_B`, `FORMULA_C` (default)
- `StringMatcher` — `PREFIX_SUFFIX` (default), `PREFIX_SUFFIX_TFIDF`, `LEVENSHTEIN`

## Base matcher

Every matcher extends `BaseMatcher`:

```python
class BaseMatcher(ABC):
    def get_matches(self, source, target) -> dict[ColumnPair, float]: ...
    def get_matches_batch(self, tables) -> dict[ColumnPair, float]: ...

    @property
    def match_details(self) -> dict[ColumnPair, dict[str, float]]: ...
```

- `get_matches(source, target)` — match a pair of tables. Must be
  implemented by every matcher.
- `get_matches_batch(tables)` — match all unique table pairs. The default
  implementation loops over pairs, but matchers that benefit from a
  holistic view (e.g. Coma's TF-IDF corpus) override this to compute
  cross-table statistics once.
- `match_details` — optional per-pair sub-matcher score breakdowns. Only
  Coma currently populates this; other matchers return an empty dict.

Invalid parameters raise `ValueError` at construction time — e.g. thresholds
outside `[0, 1]`, negative counts, or using `Coma(use_schema=False,
use_instances=False)`.
