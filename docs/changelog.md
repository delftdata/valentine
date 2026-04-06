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

## v1.0.0 — API redesign

v1.0.0 is a significant redesign of Valentine's public API. If you are
coming from 0.5.x or earlier, the changes below will affect your code.

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
- Full [documentation site](https://delftdata.github.io/valentine/)
  with matcher guide, API reference, and migration notes.

### Changed

- **Unified top-level match API.** A single
  [`valentine_match`](api.md#valentine_match) now accepts any iterable
  of DataFrames (list, tuple, generator), replacing the previous
  `valentine_match` / `valentine_match_batch` pair.
- **Immutable [`MatcherResults`](api.md#matcherresults).** The result
  object is now a `Mapping`, not a `dict` subclass. Derived views
  (e.g. [`one_to_one()`](api.md#one_to_one)) are cached and cannot be
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
