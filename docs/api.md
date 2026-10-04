---
icon: lucide/book-marked
---

# API reference

This page documents every public-facing class, function, and enum exported
by the `valentine` package. For task-oriented guides see
[Getting started](getting-started.md), [Matchers](matchers.md),
[Matcher results](results.md), and [Evaluation metrics](metrics.md).

!!! abstract "Jump to section"

    [Core](#valentine_match) ·
    [`ColumnPair`](#columnpair) ·
    [`MatcherResults`](#matcherresults) ·
    [`InvalidMatcherError`](#invalidmatchererror) ·
    [Matchers](#matchers-valentinealgorithms) ·
    [Metrics](#metrics-valentinemetrics) ·
    [Data sources](#data-sources-valentinedata_sources)

The top-level package exports:

```python
from valentine import (
    valentine_match,      # main entry point
    ColumnPair,           # NamedTuple key for matches
    MatcherResults,       # immutable Mapping returned by valentine_match
    InvalidMatcherError,  # raised for invalid matcher arguments
)
```

---

## `valentine_match`

::: valentine.valentine_match
    options:
      show_root_heading: false
      show_root_toc_entry: false

---

## `ColumnPair`

::: valentine.ColumnPair
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: [source_table, source_column, target_table, target_column, source, target]

---

## `MatcherResults`

::: valentine.algorithms.matcher_results.MatcherResults
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### Mapping protocol

| Operation         | Behaviour                                                                 |
|-------------------|---------------------------------------------------------------------------|
| `len(results)`    | Number of matches.                                                        |
| `iter(results)`   | Iterate `ColumnPair` keys in descending score order.                      |
| `results[pair]`   | Look up the similarity score for a given `ColumnPair`.                    |
| `pair in results` | Check membership.                                                         |
| `results.items()` | Yield `(ColumnPair, float)` pairs in descending score order.              |
| `results == other`| Equality with another `MatcherResults` or a plain `dict[ColumnPair, float]`. |

`MatcherResults` is **not hashable** (`__hash__` is `None`).

### Details

#### `details`

::: valentine.algorithms.matcher_results.MatcherResults.details
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `get_details`

::: valentine.algorithms.matcher_results.MatcherResults.get_details
    options:
      show_root_heading: false
      show_root_toc_entry: false

### Transformations

All transformations return a **new** `MatcherResults` instance; the
original is left untouched. Sub-matcher details are carried over to the
filtered subset.

#### `one_to_one_hungarian`

::: valentine.algorithms.matcher_results.MatcherResults.one_to_one_hungarian
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `one_to_one_greedy`

::: valentine.algorithms.matcher_results.MatcherResults.one_to_one_greedy
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `one_to_one_mutual_top`

::: valentine.algorithms.matcher_results.MatcherResults.one_to_one_mutual_top
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `filter`

::: valentine.algorithms.matcher_results.MatcherResults.filter
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `take_top_n`

::: valentine.algorithms.matcher_results.MatcherResults.take_top_n
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `take_top_percent`

::: valentine.algorithms.matcher_results.MatcherResults.take_top_percent
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `take_top_n_per_source`

::: valentine.algorithms.matcher_results.MatcherResults.take_top_n_per_source
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `get_copy`

::: valentine.algorithms.matcher_results.MatcherResults.get_copy
    options:
      show_root_heading: false
      show_root_toc_entry: false

### Metrics

#### `get_metrics`

::: valentine.algorithms.matcher_results.MatcherResults.get_metrics
    options:
      show_root_heading: false
      show_root_toc_entry: false

---

## `InvalidMatcherError`

::: valentine.InvalidMatcherError
    options:
      show_root_heading: false
      show_root_toc_entry: false

!!! warning "Deprecated alias"

    `NotAValentineMatcher` is kept as an alias for backward compatibility
    with pre-1.0 code and will be removed in a future release. New code
    should catch `InvalidMatcherError` directly.

## `Match` (internal)

`valentine.algorithms.match.Match` is an internal dataclass used by
matchers to build up result entries before they are merged into a
`dict[ColumnPair, float]`. It is intentionally **not** re-exported from
the top-level package and should not be used in user code —
[`ColumnPair`](#columnpair) is the stable, public key type.

---

## Matchers (`valentine.algorithms`)

Every matcher extends the abstract [`BaseMatcher`](#basematcher) class.
The module exports:

```python
from valentine.algorithms import (
    BaseMatcher,
    Coma,
    Cupid,
    DistributionBased,
    JaccardDistanceMatcher,
    SimilarityFlooding,
    # Enums used by the matchers:
    Formula, Policy, StringMatcher,
    # Groupings:
    schema_only_algorithms,
    instance_only_algorithms,
    schema_instance_algorithms,
    all_matchers,
    # Key types:
    ColumnPair,
)
```

The groupings are plain lists of class names:

| Constant                     | Contents                                |
|------------------------------|------------------------------------------|
| `schema_only_algorithms`     | `["SimilarityFlooding", "Cupid"]`       |
| `instance_only_algorithms`   | `["DistributionBased", "JaccardDistanceMatcher"]` |
| `schema_instance_algorithms` | `["Coma"]`                              |
| `all_matchers`               | Union of the three lists above.         |

### `BaseMatcher`

Abstract base. Subclasses must implement [`get_matches`](#get_matches);
[`get_matches_batch`](#get_matches_batch) has a default fall-back that
calls [`get_matches`](#get_matches) on each unique pair.

::: valentine.algorithms.BaseMatcher
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

#### `get_matches`

::: valentine.algorithms.BaseMatcher.get_matches
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `get_matches_batch`

::: valentine.algorithms.BaseMatcher.get_matches_batch
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `match_details`

::: valentine.algorithms.BaseMatcher.match_details
    options:
      show_root_heading: false
      show_root_toc_entry: false

### `Coma`

::: valentine.algorithms.Coma
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### `Cupid`

::: valentine.algorithms.Cupid
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### `DistributionBased`

::: valentine.algorithms.DistributionBased
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### `JaccardDistanceMatcher`

::: valentine.algorithms.JaccardDistanceMatcher
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

#### `StringDistanceFunction`

::: valentine.algorithms.jaccard_distance.StringDistanceFunction
    options:
      show_root_heading: false
      show_root_toc_entry: false

```python
from valentine.algorithms.jaccard_distance import StringDistanceFunction
from valentine.algorithms import JaccardDistanceMatcher

m = JaccardDistanceMatcher(
    threshold_dist=0.9,
    distance_fun=StringDistanceFunction.JaroWinkler,
)
```

### `SimilarityFlooding`

::: valentine.algorithms.SimilarityFlooding
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

#### `Policy`

::: valentine.algorithms.Policy
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `Formula`

::: valentine.algorithms.Formula
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `StringMatcher`

::: valentine.algorithms.StringMatcher
    options:
      show_root_heading: false
      show_root_toc_entry: false

---

## Metrics (`valentine.metrics`)

```python
from valentine.metrics import (
    Metric,                       # abstract base class
    Precision,
    Recall,
    F1Score,
    PrecisionTopNPercent,
    RecallAtSizeofGroundTruth,
    MeanReciprocalRank,
    METRICS_CORE,
    METRICS_ALL,
    METRICS_PRECISION_RECALL,
    METRICS_PRECISION_INCREASING_N,
)
```

### `Metric`

Abstract base class (`@dataclass(frozen=True)`). Subclass to implement
custom metrics:

```python
@dataclass(eq=True, frozen=True)
class MyMetric(Metric):
    threshold: float = 0.5

    def apply(self, matches, ground_truth):
        # ... compute score ...
        return self.return_format(score)
```

#### `apply`

::: valentine.metrics.Metric.apply
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `name`

::: valentine.metrics.Metric.name
    options:
      show_root_heading: false
      show_root_toc_entry: false

#### `return_format`

::: valentine.metrics.Metric.return_format
    options:
      show_root_heading: false
      show_root_toc_entry: false

### Built-in metrics

All built-in metrics are `@dataclass(frozen=True)` and hashable, so they
can live in the predefined metric sets.

#### `Precision`

::: valentine.metrics.Precision
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

#### `Recall`

::: valentine.metrics.Recall
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

#### `F1Score`

::: valentine.metrics.F1Score
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

#### `PrecisionTopNPercent`

::: valentine.metrics.PrecisionTopNPercent
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

#### `RecallAtSizeofGroundTruth`

::: valentine.metrics.RecallAtSizeofGroundTruth
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

#### `MeanReciprocalRank`

::: valentine.metrics.MeanReciprocalRank
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### Predefined metric sets

| Set                              | Contents                                                                                                                                         |
|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `METRICS_CORE`                   | `Precision`, `Recall`, `F1Score`, `PrecisionTopNPercent`, `RecallAtSizeofGroundTruth`, `MeanReciprocalRank` (defaults).                          |
| `METRICS_ALL`                    | Both `one_to_one=True` and `one_to_one=False` variants of `Precision`, `Recall`, `F1Score`, plus `PrecisionTopNPercent`, `RecallAtSizeofGroundTruth`, and `MeanReciprocalRank`. |
| `METRICS_PRECISION_RECALL`       | `{Precision(), Recall()}`.                                                                                                                       |
| `METRICS_PRECISION_INCREASING_N` | `PrecisionTopNPercent` for `n ∈ {10, 20, 30, …, 100}`.                                                                                          |

---

## Data sources (`valentine.data_sources`)

Valentine wraps each DataFrame in a [`DataframeTable`](#dataframetable)
(pandas) or [`PolarsTable`](#polarstable) (Polars) before handing it to
a matcher. Most users never touch this layer —
[`valentine_match`](#valentine_match) auto-detects the frame type and
builds the tables for you — but the classes are public so that custom
matchers and custom data sources can be written against the abstractions.

```python
from valentine.data_sources import (
    BaseTable,
    BaseColumn,
    DataframeTable,
    DataframeColumn,
)

# With the polars extra installed:
from valentine.data_sources import PolarsTable, PolarsColumn
```

### `BaseTable`

::: valentine.data_sources.BaseTable
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

**Abstract members** (must be provided by subclasses):

::: valentine.data_sources.BaseTable.name
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseTable.unique_identifier
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseTable.get_columns
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseTable.get_df
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseTable.is_empty
    options: {show_root_heading: false, show_root_toc_entry: false}

**Concrete members** (provided by `BaseTable`, override if needed):

::: valentine.data_sources.BaseTable.get_instances_df
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseTable.get_instances_columns
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseTable.get_guid_column_lookup
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseTable.get_data_type
    options: {show_root_heading: false, show_root_toc_entry: false}

### `BaseColumn`

::: valentine.data_sources.BaseColumn
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

**Abstract members**:

::: valentine.data_sources.BaseColumn.name
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseColumn.unique_identifier
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseColumn.data_type
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseColumn.data
    options: {show_root_heading: false, show_root_toc_entry: false}

**Concrete members**:

::: valentine.data_sources.BaseColumn.size
    options: {show_root_heading: false, show_root_toc_entry: false}
::: valentine.data_sources.BaseColumn.is_empty
    options: {show_root_heading: false, show_root_toc_entry: false}

### `DataframeTable`

::: valentine.data_sources.DataframeTable
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### `DataframeColumn`

::: valentine.data_sources.DataframeColumn
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### `PolarsTable`

::: valentine.data_sources.PolarsTable
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### `PolarsColumn`

::: valentine.data_sources.PolarsColumn
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

### Writing a custom data source

If your data doesn't live in a pandas DataFrame, implement
[`BaseTable`](#basetable) and [`BaseColumn`](#basecolumn) directly. A
minimal custom source just needs a name, a unique identifier, and the
ability to enumerate its columns:

```python
import uuid
import pandas as pd

from valentine import valentine_match
from valentine.algorithms import Coma
from valentine.data_sources import BaseColumn, BaseTable


class DictColumn(BaseColumn):
    def __init__(self, name: str, data: list, data_type: str = "varchar"):
        self._name = name
        self._data = data
        self._data_type = data_type
        self._guid = str(uuid.uuid4())

    @property
    def name(self) -> str:
        return self._name

    @property
    def unique_identifier(self) -> str:
        return self._guid

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def data(self) -> list:
        return self._data


class DictTable(BaseTable):
    def __init__(self, name: str, columns: dict[str, list]):
        self._name = name
        self._guid = str(uuid.uuid4())
        self._columns = [DictColumn(k, v) for k, v in columns.items()]

    @property
    def name(self) -> str:
        return self._name

    @property
    def unique_identifier(self) -> str:
        return self._guid

    def get_columns(self) -> list[BaseColumn]:
        return self._columns

    def get_df(self) -> pd.DataFrame:
        return pd.DataFrame({c.name: c.data for c in self._columns})

    @property
    def is_empty(self) -> bool:
        return all(len(c.data) == 0 for c in self._columns)


# Matchers call get_matches / get_matches_batch directly on BaseTable
# instances, so custom sources bypass valentine_match:
source = DictTable("hr", {"emp_id": [1, 2, 3], "fname": ["a", "b", "c"]})
target = DictTable("payroll", {"employee_number": [1, 2, 3], "first_name": ["a", "b", "c"]})

raw = Coma().get_matches_batch([source, target])
```

If you want to reuse Valentine's instance-sampling logic, override
[`get_instances_df`](#basetable) to return a capped DataFrame. Custom
sources are accepted by every built-in matcher — only
[`valentine_match`](#valentine_match) itself is DataFrame-specific.
