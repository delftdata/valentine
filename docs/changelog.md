---
icon: lucide/history
---

# Changelog & migration

This page tracks user-visible changes to Valentine and explains how to
port code between releases. The format is based on
[Keep a Changelog][kac] and the project follows [Semantic Versioning][semver].
For the full commit history, see [GitHub releases][releases].

  [kac]: https://keepachangelog.com/en/1.1.0/
  [semver]: https://semver.org/spec/v2.0.0.html
  [releases]: https://github.com/delftdata/valentine/releases

!!! info "Maintainers: how to update this page"

    When preparing a release, move the contents of the
    **Unreleased** section below into a new versioned heading
    (`## vX.Y.Z — YYYY-MM-DD`) and reset the Unreleased sub-sections
    to empty. Keep sub-section order consistent:
    *Added · Changed · Deprecated · Removed · Fixed · Security*.

## Unreleased

### Added

- _Nothing yet._

### Changed

- _Nothing yet._

### Deprecated

- _Nothing yet._

### Removed

- _Nothing yet._

### Fixed

- _Nothing yet._

### Security

- _Nothing yet._

## v1.0.0 — 2026-05-06

v1.0.0 is a significant redesign of Valentine's public API together
with a performance and accuracy overhaul of every matcher. If you are
coming from 0.5.x or earlier, the changes below will affect your code.

**Headline:** ~27× wall-clock speedup on the full NYU Open Data
benchmark (1048s → 39s), pure-Python Coma (no JVM), Polars support,
embedding-based Jaccard, and Hungarian as the new default 1:1
selector.

### Added

- [`ColumnPair`](api.md#columnpair) `NamedTuple` with explicit
  `source_table`, `source_column`, `target_table`, `target_column`
  fields — replacing the previous nested-tuple match keys.
- Sub-matcher score breakdowns exposed via
  [`MatcherResults.details`](api.md#details) and
  [`get_details(pair)`](api.md#get_details). Currently populated by
  [`Coma`](api.md#coma).
- Ground-truth input accepts table-aware
  [`ColumnPair`](api.md#columnpair) instances in addition to
  column-name pairs — see
  [Evaluation metrics](metrics.md#ground-truth-formats).
- Top-level `instance_sample_size` parameter on
  [`valentine_match`](api.md#valentine_match) (default `1000`) for
  controlling instance sampling without constructing a custom
  [`DataframeTable`](api.md#dataframetable).
- Predefined metric sets: `METRICS_ALL`, `METRICS_PRECISION_RECALL`,
  and `METRICS_PRECISION_INCREASING_N` alongside the existing
  `METRICS_CORE` — see
  [Predefined metric sets](api.md#predefined-metric-sets).
- `MeanReciprocalRank` (MRR) metric, also added to `METRICS_ALL` and
  `METRICS_CORE`. Per-source ranking: for each source column, finds
  the rank of the first correct target in the column's ranked
  predictions.
- **Polars support.** New `PolarsTable` / `PolarsColumn` adapters in
  `valentine/data_sources/polars/`. `valentine_match` auto-detects
  pandas and Polars frames and supports mixing them in a single call.
  Install with `pip install valentine[polars]`.
- **Embedding-based string distance** for `JaccardDistanceMatcher` via
  `StringDistanceFunction.Embedding`, using sentence-transformers
  cosine similarity. Knobs: `embedding_model`, `embedding_device`
  (auto-picks `cuda` → `mps` → `cpu`), `embedding_batch_size`. One
  global encode pass per match call. Install with
  `pip install valentine[embeddings]`. *(Closes #65.)*
- **Tversky-based set-similarity reduction** in
  `JaccardDistanceMatcher` (`tversky_alpha`, `tversky_beta`). Defaults
  reproduce Jaccard exactly; `α=β=0.5` gives Sørensen-Dice, `α=1, β=0`
  gives containment.
- **Three named one-to-one selectors** on
  [`MatcherResults`](api.md#matcherresults):
  [`one_to_one_hungarian()`](api.md#one_to_one_hungarian) (new
  default — globally optimal via `scipy.optimize.linear_sum_assignment`),
  [`one_to_one_greedy()`](api.md#one_to_one_greedy) (previous
  behaviour), and
  [`one_to_one_mutual_top(n)`](api.md#one_to_one_mutual_top) (mutual
  nearest-neighbour filter).
- **Pluggable 1:1 algorithm** in the metrics API. New
  `one_to_one_method` keyword on `Metric.apply()` and
  `MatcherResults.get_metrics()` accepts `"hungarian" | "greedy" |
  "mutual_top"`. Defaults to `"hungarian"`.
- Configurable `instance_weight` constructor parameter on
  [`Coma`](api.md#coma) (default `1.0`).
- Generic abbreviation matching in Coma name similarity — handles
  prefix and ordered-subsequence forms (`dept`→`department`,
  `fname`→`firstname`, `st`→`street`).
- Full [documentation site](https://delftdata.github.io/valentine/)
  with matcher guide, API reference, and migration notes.

### Changed

- **Unified top-level match API.** A single
  [`valentine_match`](api.md#valentine_match) now accepts any iterable
  of DataFrames (list, tuple, generator), replacing the previous
  `valentine_match` / `valentine_match_batch` pair.
- **Immutable [`MatcherResults`](api.md#matcherresults).** The result
  object is now a `Mapping`, not a `dict` subclass. Derived views
  (e.g. [`one_to_one_hungarian()`](api.md#one_to_one_hungarian)) are cached and cannot be
  silently invalidated.
- [`Coma`](api.md#coma) is now a pure-Python implementation of
  COMA 3.0 — no JVM dependency. Constructor signature updated to
  `max_n`, `use_instances`, `use_schema`, `delta`, `threshold`.
- `METRICS_ALL` is now an explicit set rather than a dynamic scan of
  `Metric.__subclasses__()`, so user-defined metrics no longer bleed
  into the predefined set.
- Parameter validation happens at matcher construction time: invalid
  thresholds, negative counts, or mutually-exclusive flags raise
  `ValueError` immediately rather than failing mid-match.
- **~27× faster across the suite.** Coma uses TF-IDF cosine on cached
  float32 sparse CSR matrices with pair-level memoisation; Cupid
  caches WordNet synsets and lemma walks; DistributionBased replaces
  the per-row `bucket_binary_search` with `np.searchsorted` +
  `np.bincount` over precomputed bound arrays;
  `JaccardDistanceMatcher` uses `rapidfuzz.process.cdist` with
  `score_cutoff` short-circuit. NYU full-suite numbers in the
  [v1.0.0 PR description](https://github.com/delftdata/valentine/pull/96).
- `BaseTable.get_data_type` treats pandas `"str"` / `"string"` dtypes
  as text (previously misclassified as unknown).
- Cupid datatype compatibility is now binary (same family = 1.0,
  different = 0.0); a generic family-based classifier handles
  arbitrary SQL type strings (`varchar(255)`, `bigint`, …).
- Coma TF-IDF stopwords switched from a 33-word Lucene frozenset to
  NLTK's 179-word English stopwords for stronger noise filtering.
- The default 1:1 selector for the metrics API is now Hungarian.
  Existing callers that relied on greedy selection should pass
  `one_to_one_method="greedy"`.

### Deprecated

- `NotAValentineMatcher` is kept as an alias for
  [`InvalidMatcherError`](api.md#invalidmatchererror) but will be
  removed in a future release. Update `except` clauses to use the new
  name.

### Removed

- `valentine_match_batch` — use
  [`valentine_match`](api.md#valentine_match) with an iterable
  instead.
- The Java-backed COMA wrapper and its JVM dependency.
- Mutable `dict` semantics on match results (`__setitem__`, `update`,
  `pop`, …).
- `MatcherResults.one_to_one()` — use one of the three explicitly
  named selectors:
  [`one_to_one_hungarian()`](api.md#one_to_one_hungarian) (new
  default), [`one_to_one_greedy()`](api.md#one_to_one_greedy)
  (previous behaviour), or
  [`one_to_one_mutual_top(n)`](api.md#one_to_one_mutual_top).
- Redundant Coma matchers in flat tabular schemas: `LEAVES_CM`,
  `PARENTS_CM`, `PATH_CM`, `SIBLINGS_CM`, `DATATYPE_MATCHER`, and the
  predefined `INSTANCES_CM`. These produced constant or
  duplicate-of-`NAME_CM` scores on tabular inputs, diluting the
  signal.

### Fixed

- DistributionBased: `quantile_emd` now returns `inf` instead of
  dividing by zero when histogram values sum to zero.
- Coma: TF-IDF cache stores list reference alongside its `id()` key
  to detect `id()` reuse after garbage collection, preventing stale
  cache hits.
- SimilarityFlooding: `NodeID` prefix collision fixed (columns named
  `"NodeID*"` no longer collide with internal graph nodes); tokeniser
  now handles `snake_case`, `SCREAMING_SNAKE`, hyphens, and embedded
  digits.
- Data source utilities: `get_encoding` handles `chardet` returning
  `None`; `get_delimiter` catches `csv.Sniffer` failures on malformed
  input.
- NLTK data downloads are now resilient: retried, atomic, and silent
  when data is already present.

### Migrating from 0.5.x

#### 1. `valentine_match_batch` is gone

Before (0.5.x):

```python
from valentine import valentine_match, valentine_match_batch

matches = valentine_match(df1, df2, matcher)              # two DataFrames
matches = valentine_match_batch([df1, df2, df3], matcher) # many DataFrames
```

After (1.0):

```python
from valentine import valentine_match

matches = valentine_match([df1, df2], matcher)            # any iterable
matches = valentine_match([df1, df2, df3], matcher)
```

[`valentine_match`](api.md#valentine_match) now accepts any iterable of
DataFrames; pairs, lists, tuples, and generators all work the same way.

#### 2. Match keys are `ColumnPair` instances, not nested tuples

Before:

```python
for ((t1, c1), (t2, c2)), score in matches.items():
    print(f"{c1} <-> {c2}: {score}")
```

After:

```python
for pair, score in matches.items():
    print(f"{pair.source_column} <-> {pair.target_column}: {score}")
```

[`ColumnPair`](api.md#columnpair) is a `NamedTuple`, so positional
indexing still works if you really need it, and destructuring into four
names is a simple migration path:

```python
for (src_table, src_col, tgt_table, tgt_col), score in matches.items():
    ...
```

#### 3. `MatcherResults` is immutable

Before:

```python
matches[("t1", "c1"), ("t2", "c2")] = 1.0   # allowed
del matches[some_key]                        # allowed
```

After — these raise `TypeError` / `AttributeError`. Use the
transformation methods instead:

```python
matches = matches.filter(min_score=0.7)
matches = matches.take_top_n(10)
matches = matches.take_top_percent(25)
```

Each returns a **new** [`MatcherResults`](api.md#matcherresults)
instance.

#### 4. Ground truth accepts `ColumnPair` instances

Before — only `(col, col)` pairs were allowed:

```python
ground_truth = [("emp_id", "employee_number"), ...]
```

After — both work, and table-aware comparison is now possible for
multi-table matching:

```python
from valentine.algorithms import ColumnPair

ground_truth = [
    ColumnPair("hr", "emp_id", "payroll", "employee_number"),
    ...
]
```

See [Evaluation metrics → Ground-truth formats](metrics.md#ground-truth-formats).

#### 5. `NotAValentineMatcher` is deprecated

The exception raised for bad matcher arguments is now
[`InvalidMatcherError`](api.md#invalidmatchererror). The old name is
kept as an alias for backward compatibility but will be removed in a
future release — update your `except` clauses.

```python
# Before
from valentine import NotAValentineMatcher

# After
from valentine import InvalidMatcherError
```

#### 6. The Java COMA wrapper has been removed

If you were relying on the previous Java-backed `Coma` implementation,
you no longer need a JVM — [`Coma`](api.md#coma) is now pure Python and
ships with the package. The constructor signature has changed slightly;
see the [API reference](api.md#coma) for the new parameters
(`max_n`, `use_instances`, `use_schema`, `delta`, `threshold`).

#### 7. `one_to_one()` is gone — pick a selector

`MatcherResults.one_to_one()` has been replaced by three explicitly
named selectors:

```python
# Before
filtered = matches.one_to_one()

# After — globally optimal (new default), recommended:
filtered = matches.one_to_one_hungarian()

# After — preserve previous greedy behaviour:
filtered = matches.one_to_one_greedy()

# After — mutual nearest neighbour:
filtered = matches.one_to_one_mutual_top(n=1)
```

The metrics API also takes the algorithm as a per-call argument:

```python
matches.get_metrics(gt, metrics={F1Score()},
                    one_to_one_method="hungarian")  # default
```

Custom [`Metric`](api.md#metric) subclasses that override `apply` need
to accept the new `one_to_one_method` keyword (or `**kwargs`).
