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

```python
valentine_match(
    dfs: Iterable[pd.DataFrame],
    matcher: BaseMatcher,
    df_names: list[str] | None = None,
    instance_sample_size: int | None = 1000,
) -> MatcherResults
```

Match columns across every unique pair of DataFrames.

**Parameters**

| Name                   | Type                         | Default | Description                                                                                                                                                                                                     |
|------------------------|------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs`                  | `Iterable[pd.DataFrame]`     | —       | Two or more DataFrames to match against each other. Any iterable works (list, tuple, generator).                                                                                                               |
| `matcher`              | `BaseMatcher`                | —       | Matcher instance (e.g. `Coma()`, `Cupid()`).                                                                                                                                                                     |
| `df_names`             | `list[str] \| None`          | `None`  | Optional names for each DataFrame. When `None`, defaults to `"aaa"`, `"bbb"`, `"ccc"`, … (chosen for minimum string similarity so defaults don't influence schema-based matchers). Limited to 26 unnamed tables. |
| `instance_sample_size` | `int \| None`                | `1000`  | Cap on the number of non-empty rows sampled per column for instance-based matchers (Coma with `use_instances=True`, `DistributionBased`, `JaccardDistanceMatcher`). Pass `None` to use every row. Pass `0` to skip instance data entirely — schema-only matchers are unaffected, but instance-based matchers will see empty columns. |

**Returns**

A [`MatcherResults`](#matcherresults) instance — an immutable mapping of
[`ColumnPair`](#columnpair) to similarity scores, sorted high to low.

**Raises**

- `ValueError` — fewer than 2 DataFrames, mismatched `df_names` length, or
  more than 26 DataFrames without explicit names.
- `InvalidMatcherError` — `matcher` is not a `BaseMatcher` instance.

**Example**

```python
import pandas as pd
from valentine import valentine_match
from valentine.algorithms import Coma

df1 = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
df2 = pd.DataFrame({"user_id": [1, 2], "full_name": ["a", "b"]})

matches = valentine_match(
    [df1, df2],
    matcher=Coma(use_instances=True),
    df_names=["users", "accounts"],
    instance_sample_size=500,
)
```

---

## `ColumnPair`

```python
class ColumnPair(NamedTuple):
    source_table: str
    source_column: str
    target_table: str
    target_column: str
```

Immutable, hashable key identifying a matched pair of columns. Used
everywhere a match result or ground truth entry is required.

**Attributes**

| Attribute       | Type   | Description                             |
|-----------------|--------|-----------------------------------------|
| `source_table`  | `str`  | Name of the source table.               |
| `source_column` | `str`  | Name of the source column.              |
| `target_table`  | `str`  | Name of the target table.               |
| `target_column` | `str`  | Name of the target column.              |

**Computed properties**

| Property | Type               | Description                     |
|----------|--------------------|---------------------------------|
| `source` | `tuple[str, str]`  | `(source_table, source_column)` |
| `target` | `tuple[str, str]`  | `(target_table, target_column)` |

Because `ColumnPair` is a `NamedTuple`, it also supports positional
indexing, iteration, and unpacking:

```python
pair = ColumnPair("orders", "price", "sales", "amount")
st, sc, tt, tc = pair
pair[0]            # "orders"
pair.source        # ("orders", "price")
```

---

## `MatcherResults`

```python
class MatcherResults(Mapping[ColumnPair, float]):
    def __init__(
        self,
        matches: dict[ColumnPair, float],
        details: dict[ColumnPair, dict[str, float]] | None = None,
    ): ...
```

Immutable `Mapping` returned by [`valentine_match`](#valentine_match).
Entries are sorted from highest to lowest similarity score on
construction. Because the mapping is immutable, derived views (such as
the cached result of [`one_to_one`](#one_to_one)) cannot be silently
invalidated.

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

```python
@property
def details -> dict[ColumnPair, dict[str, float]]
```

Per-pair sub-matcher score breakdowns. Returns an empty `dict` when the
matcher does not provide details. Currently populated by
[`Coma`](#coma), which exposes scores for its `name`, `path`, `leaves`,
`parents`, and `instances` sub-matchers.

#### `get_details`

```python
def get_details(key: ColumnPair) -> dict[str, float] | None
```

Return the sub-matcher breakdown for a single pair, or `None` if no
details are available.

### Transformations

All transformations return a **new** `MatcherResults` instance; the
original is left untouched. Sub-matcher details are carried over to the
filtered subset.

#### `one_to_one`

```python
def one_to_one(threshold: float | None = None) -> MatcherResults
```

Greedy bipartite filter: starting from the highest-scoring pair, assign
each source and each target column **at most one** partner. Pairs below
`threshold` are discarded.

- `threshold=None` (default) uses the median of unique similarity scores
  as the cutoff, and the result is cached.
- Passing an explicit `threshold` bypasses the cache.
- When the input has fewer than two distinct score values, all entries
  are returned unchanged.

#### `filter`

```python
def filter(min_score: float) -> MatcherResults
```

Return only matches whose similarity is `>= min_score`.

#### `take_top_n`

```python
def take_top_n(n: int) -> MatcherResults
```

Return the top `n` matches by score.

#### `take_top_percent`

```python
def take_top_percent(percent: int) -> MatcherResults
```

Return the top `percent`% (0–100) of matches, rounded up.

#### `get_copy`

```python
def get_copy() -> MatcherResults
```

Return a shallow copy of the instance.

### Metrics

#### `get_metrics`

```python
def get_metrics(
    ground_truth: list[tuple[str, str]] | list[ColumnPair],
    metrics: set[Metric] = METRICS_CORE,
) -> dict[str, Any]
```

Compute evaluation metrics against a ground truth. The ground truth can
be either:

- **Column-name pairs** — `[("src_col", "tgt_col"), …]`. Table names are
  ignored during comparison, which is convenient when you only care
  about column-level alignment.
- **`ColumnPair` instances** — full table-aware comparison. Use this
  when the same column name appears in multiple tables.

Both formats may also be passed as plain 2- or 4-tuples; they are
normalized internally. Returns a flat `dict` keyed by metric name
(e.g. `{"Precision": 0.9, "Recall": 0.8, "F1Score": 0.85, …}`).

---

## `InvalidMatcherError`

```python
class InvalidMatcherError(Exception): ...
```

Raised by [`valentine_match`](#valentine_match) when the `matcher`
argument is not a [`BaseMatcher`](#basematcher) subclass instance.

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
|------------------------------|-----------------------------------------|
| `schema_only_algorithms`     | `["SimilarityFlooding", "Cupid"]`       |
| `instance_only_algorithms`   | `["DistributionBased", "JaccardDistanceMatcher"]` |
| `schema_instance_algorithms` | `["Coma"]`                              |
| `all_matchers`               | Union of the three lists above.         |

### `BaseMatcher`

Abstract base. Subclasses must implement [`get_matches`](#get_matches);
[`get_matches_batch`](#get_matches_batch) has a default fall-back that
calls [`get_matches`](#get_matches) on each unique pair.

#### `get_matches`

```python
@abstractmethod
def get_matches(
    source_input: BaseTable,
    target_input: BaseTable,
) -> dict[ColumnPair, float]
```

Match columns between a single pair of tables. Returns a raw dict, not a
`MatcherResults`.

#### `get_matches_batch`

```python
def get_matches_batch(tables: list[BaseTable]) -> dict[ColumnPair, float]
```

Match columns across every unique pair of tables. Override this method
in subclasses that benefit from a holistic view (e.g. global TF-IDF
corpus, global distribution ranks). Both `Coma`, `DistributionBased`,
and `SimilarityFlooding` override it.

#### `match_details`

```python
@property
def match_details -> dict[ColumnPair, dict[str, float]]
```

Per-pair score breakdowns from the most recent match call. Empty by
default; populated by matchers that combine multiple sub-scorers. The
contents are propagated into `MatcherResults.details` by
`valentine_match`.

### `Coma`

```python
Coma(
    max_n: int = 0,
    use_instances: bool = False,
    use_schema: bool = True,
    delta: float = 0.15,
    threshold: float = 0.0,
)
```

Pure-Python COMA 3.0 implementation. Combines schema-based matchers
(name, path, leaves, parents) with an optional TF-IDF instance matcher
and selects results using bidirectional best-match logic.

| Parameter       | Type    | Default | Description                                                                                                                     |
|-----------------|---------|---------|---------------------------------------------------------------------------------------------------------------------------------|
| `max_n`         | `int`   | `0`     | Maximum number of matches to keep per column. `0` means unlimited. Must be `>= 0`.                                              |
| `use_instances` | `bool`  | `False` | Enable TF-IDF instance-based matching.                                                                                          |
| `use_schema`    | `bool`  | `True`  | Enable schema-based matching. At least one of `use_schema` and `use_instances` must be `True`.                                  |
| `delta`         | `float` | `0.15`  | Fraction from the best per-column score within which matches are kept (e.g. `0.15` keeps all within 15% of the column's best). Must be in `[0, 1]`. |
| `threshold`     | `float` | `0.0`   | Absolute minimum similarity to keep a match. Must be in `[0, 1]`.                                                               |

Populates `MatcherResults.details` with `{name, path, leaves, parents, instances}` sub-scores.

### `Cupid`

```python
Cupid(
    leaf_w_struct: float = 0.2,
    w_struct: float = 0.2,
    th_accept: float = 0.7,
    th_high: float = 0.6,
    th_low: float = 0.35,
    c_inc: float = 1.2,
    c_dec: float = 0.9,
    th_ns: float = 0.7,
    process_num: int = 1,
)
```

Python implementation of Cupid (Madhavan, Bernstein & Rahm, VLDB 2001):
combines linguistic similarity of column names with structural
similarity derived from schema tree shape.

| Parameter       | Type    | Default | Description                                                                                                          |
|-----------------|---------|---------|----------------------------------------------------------------------------------------------------------------------|
| `leaf_w_struct` | `float` | `0.2`   | Weight of structural similarity at leaf level. Must be in `[0, 1]`.                                                  |
| `w_struct`      | `float` | `0.2`   | Weight of structural similarity at inner-node level. Must be in `[0, 1]`.                                            |
| `th_accept`     | `float` | `0.7`   | Acceptance similarity threshold for the final mapping. Must be in `[0, 1]`.                                          |
| `th_high`       | `float` | `0.6`   | High-confidence threshold used during structural propagation. Must be in `[0, 1]`.                                   |
| `th_low`        | `float` | `0.35`  | Low-confidence threshold used during structural propagation. Must be in `[0, 1]`.                                    |
| `c_inc`         | `float` | `1.2`   | Positive reinforcement coefficient for matching children. Must be `> 0`.                                             |
| `c_dec`         | `float` | `0.9`   | Negative reinforcement coefficient for non-matching children. Must be `> 0`.                                         |
| `th_ns`         | `float` | `0.7`   | Name-similarity threshold. Must be in `[0, 1]`.                                                                      |
| `process_num`   | `int`   | `1`     | Number of worker processes. Must be `>= 1`.                                                                          |

### `DistributionBased`

```python
DistributionBased(
    threshold1: float = 0.15,
    threshold2: float = 0.15,
    quantiles: int = 256,
    process_num: int = 1,
    use_bloom_filters: bool = False,
)
```

Instance-based matcher from *Automatic Discovery of Attributes in
Relational Databases* (Zhang et al., SIGMOD 2011). Compares quantile
histograms with Earth Mover's Distance.

| Parameter           | Type    | Default | Description                                                                                                           |
|---------------------|---------|---------|-----------------------------------------------------------------------------------------------------------------------|
| `threshold1`        | `float` | `0.15`  | Distance threshold for phase 1 distribution clustering. Must be in `[0, 1]`.                                          |
| `threshold2`        | `float` | `0.15`  | Distance threshold for phase 2 attribute clustering. Must be in `[0, 1]`.                                             |
| `quantiles`         | `int`   | `256`   | Number of quantiles for histogram summaries. Must be `>= 1`.                                                          |
| `process_num`       | `int`   | `1`     | Number of worker processes. Must be `>= 1`.                                                                           |
| `use_bloom_filters` | `bool`  | `False` | Use Bloom filters for approximate set intersection in phase 2. Trades a small false-positive rate for cheaper cost.   |

Overrides `get_matches_batch` to compute global distribution ranks
across **all** tables.

### `JaccardDistanceMatcher`

```python
JaccardDistanceMatcher(
    threshold_dist: float = 0.8,
    distance_fun: StringDistanceFunction = StringDistanceFunction.Levenshtein,
    process_num: int = 1,
)
```

Baseline instance-based matcher using Jaccard similarity of column
value sets, with configurable string-distance-based element equality.

| Parameter        | Type                     | Default                          | Description                                                                                                                      |
|------------------|--------------------------|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `threshold_dist` | `float`                  | `0.8`                            | Threshold above which two strings are considered equal under `distance_fun`. Ignored when `distance_fun` is `Exact`. `[0, 1]`.   |
| `distance_fun`   | `StringDistanceFunction` | `StringDistanceFunction.Levenshtein` | String similarity function. See [`StringDistanceFunction`](#stringdistancefunction).                                          |
| `process_num`    | `int`                    | `1`                              | Number of worker processes. Must be `>= 1`.                                                                                      |

#### `StringDistanceFunction`

Enum of supported element-equality functions for
`JaccardDistanceMatcher`:

| Value                                  | Description                                       |
|----------------------------------------|---------------------------------------------------|
| `StringDistanceFunction.Levenshtein`   | Normalized Levenshtein ratio (default).           |
| `StringDistanceFunction.DamerauLevenshtein` | Normalized Damerau–Levenshtein ratio.        |
| `StringDistanceFunction.Hamming`       | Normalized Hamming distance (strings of equal length). |
| `StringDistanceFunction.Jaro`          | Jaro similarity.                                  |
| `StringDistanceFunction.JaroWinkler`   | Jaro–Winkler similarity.                          |
| `StringDistanceFunction.Exact`         | Exact string equality (forces threshold to 1.0).  |

```python
from valentine.algorithms.jaccard_distance import StringDistanceFunction
from valentine.algorithms import JaccardDistanceMatcher

m = JaccardDistanceMatcher(
    threshold_dist=0.9,
    distance_fun=StringDistanceFunction.JaroWinkler,
)
```

### `SimilarityFlooding`

```python
SimilarityFlooding(
    coeff_policy: Policy = Policy.INVERSE_AVERAGE,
    formula: Formula = Formula.FORMULA_C,
    string_matcher: StringMatcher = StringMatcher.PREFIX_SUFFIX,
    tfidf_corpus: list[BaseTable] | None = None,
)
```

Python implementation of Similarity Flooding (Melnik, Garcia-Molina &
Rahm, ICDE 2002). Treats each schema as a labelled graph and iteratively
propagates an initial element-level similarity to a fixpoint.

| Parameter        | Type                      | Default                      | Description                                                                                               |
|------------------|---------------------------|------------------------------|-----------------------------------------------------------------------------------------------------------|
| `coeff_policy`   | `Policy`                  | `Policy.INVERSE_AVERAGE`     | Coefficient policy for the propagation graph.                                                             |
| `formula`        | `Formula`                 | `Formula.FORMULA_C`          | Fixpoint iteration formula.                                                                               |
| `string_matcher` | `StringMatcher`           | `StringMatcher.PREFIX_SUFFIX`| String similarity function for the initial element-level mapping.                                        |
| `tfidf_corpus`   | `list[BaseTable] \| None` | `None`                       | Additional tables to include when computing IDF weights for the `PREFIX_SUFFIX_TFIDF` matcher. Ignored otherwise. |

Overrides `get_matches_batch` to compute a global IDF across all tables
when `string_matcher=PREFIX_SUFFIX_TFIDF`.

#### `Policy`

| Value                     | Description                                     |
|---------------------------|-------------------------------------------------|
| `Policy.INVERSE_AVERAGE`  | Inverse of the average in-degree (default).     |
| `Policy.INVERSE_PRODUCT`  | Inverse of the product of in-degrees.           |

#### `Formula`

| Value              | Description                                      |
|--------------------|--------------------------------------------------|
| `Formula.BASIC`    | Basic fixpoint formula.                          |
| `Formula.FORMULA_A`| Variant A from the Similarity Flooding paper.    |
| `Formula.FORMULA_B`| Variant B from the Similarity Flooding paper.    |
| `Formula.FORMULA_C`| Variant C (default in Valentine).                |

#### `StringMatcher`

| Value                               | Description                                                       |
|-------------------------------------|-------------------------------------------------------------------|
| `StringMatcher.PREFIX_SUFFIX`       | Prefix/suffix trigram matcher (default).                          |
| `StringMatcher.PREFIX_SUFFIX_TFIDF` | Prefix/suffix matcher weighted by IDF computed from the corpus.   |
| `StringMatcher.LEVENSHTEIN`         | Normalized Levenshtein similarity on node labels.                 |

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

```python
@abstractmethod
def apply(
    matches: MatcherResults,
    ground_truth: list[tuple[str, str]] | list[ColumnPair],
) -> dict[str, Any]
```

Compute the metric value. `ground_truth` accepts either column-name
pairs (table-agnostic) or full `ColumnPair` tuples (table-aware).

#### `name`

```python
def name() -> str
```

Default: the class name. Override to parameterize the reported name
(e.g. `PrecisionTopNPercent` substitutes the current `n` into its name).

#### `return_format`

```python
@final
def return_format(value: Any) -> dict[str, Any]
```

Final helper that formats a metric value as `{self.name(): value}`.

### Built-in metrics

All built-in metrics are `@dataclass(frozen=True)` and hashable, so they
can live in the predefined metric sets.

#### `Precision`

```python
Precision(one_to_one: bool = True)
```

`TP / (TP + FP)`. When `one_to_one=True` (default), applies
`MatcherResults.one_to_one()` before counting.

#### `Recall`

```python
Recall(one_to_one: bool = True)
```

`TP / (TP + FN)`. Honors `one_to_one` the same way as `Precision`.

#### `F1Score`

```python
F1Score(one_to_one: bool = True)
```

Harmonic mean of precision and recall. Honors `one_to_one`.

#### `PrecisionTopNPercent`

```python
PrecisionTopNPercent(one_to_one: bool = True, n: int = 10)
```

Precision restricted to the top `n%` of predictions by score. `n` is
clamped to `[0, 100]`. The reported metric name reflects the chosen
percentage (e.g. `PrecisionTop10Percent`).

#### `RecallAtSizeofGroundTruth`

```python
RecallAtSizeofGroundTruth(one_to_one: bool = False)
```

Recall at the top `len(ground_truth)` predictions — i.e. what fraction
of gold pairs you recover if you select as many predictions as there
are gold matches. One-to-one filtering is **off** by default here.

### Predefined metric sets

| Set                              | Contents                                                                                  |
|----------------------------------|-------------------------------------------------------------------------------------------|
| `METRICS_CORE`                   | `Precision`, `Recall`, `F1Score`, `PrecisionTopNPercent`, `RecallAtSizeofGroundTruth` (defaults). |
| `METRICS_ALL`                    | Both `one_to_one=True` and `one_to_one=False` variants of `Precision`, `Recall`, `F1Score`, plus `PrecisionTopNPercent` and `RecallAtSizeofGroundTruth`. |
| `METRICS_PRECISION_RECALL`       | `{Precision(), Recall()}`.                                                                |
| `METRICS_PRECISION_INCREASING_N` | `PrecisionTopNPercent` for `n ∈ {10, 20, 30, …, 100}`.                                    |

---

## Data sources (`valentine.data_sources`)

Valentine wraps each DataFrame in a [`DataframeTable`](#dataframetable)
before handing it to a matcher. Most users never touch this layer —
[`valentine_match`](#valentine_match) builds the tables for you — but
the classes are public so that custom matchers and custom data sources
can be written against the abstractions.

```python
from valentine.data_sources import (
    BaseTable,
    BaseColumn,
    DataframeTable,
    DataframeColumn,
)
```

### `BaseTable`

Abstract base for a table-like data source. Implement this to plug a
non-DataFrame backend (e.g. SQL cursor, Parquet file, Arrow table) into
Valentine's matchers.

**Abstract members** (must be provided by subclasses):

| Member                         | Kind                  | Description                                                   |
|--------------------------------|-----------------------|---------------------------------------------------------------|
| `name`                         | `property -> str`     | Table name. Becomes `source_table`/`target_table` in emitted `ColumnPair`s. |
| `unique_identifier`            | `property -> object`  | Stable identifier used internally to key per-table state.     |
| `get_columns()`                | `method -> list[BaseColumn]` | All columns in the table.                              |
| `get_df()`                     | `method -> pd.DataFrame`     | Full DataFrame view of the table.                      |
| `is_empty`                     | `property -> bool`    | Whether the table has zero rows.                              |

**Concrete members** (provided by `BaseTable`, override if needed):

| Member                       | Kind                           | Description                                                                                  |
|------------------------------|--------------------------------|----------------------------------------------------------------------------------------------|
| `get_instances_df()`         | `method -> pd.DataFrame`       | DataFrame used for instance-based sampling. Defaults to `get_df()`.                          |
| `get_instances_columns()`    | `method -> list[BaseColumn]`   | Columns built from the instance-sampled DataFrame. Defaults to `get_columns()`.              |
| `get_guid_column_lookup()`   | `method -> dict[str, object]`  | `{column_name: column.unique_identifier}` lookup.                                            |
| `get_data_type(data, d_type)`| `staticmethod -> str`          | Normalize a pandas dtype into one of `"varchar"`, `"int"`, `"float"`, or `"date"`.           |

### `BaseColumn`

Abstract base for a single column. A `BaseColumn` knows its name, its
values, and its detected data type.

**Abstract members**:

| Member              | Kind                  | Description                                              |
|---------------------|-----------------------|----------------------------------------------------------|
| `name`              | `property -> str`     | Column name.                                             |
| `unique_identifier` | `property -> object`  | Stable identifier used internally.                       |
| `data_type`         | `property -> str`     | Detected type: one of `"varchar"`, `"int"`, `"float"`, `"date"`. |
| `data`              | `property -> list`    | The column's values.                                     |

**Concrete members**:

| Member     | Kind                | Description                                          |
|------------|---------------------|------------------------------------------------------|
| `size`     | `property -> int`   | Number of elements in `data`.                        |
| `is_empty` | `property -> bool`  | `True` when `size == 0`.                             |

### `DataframeTable`

```python
DataframeTable(
    df: pd.DataFrame,
    name: str,
    instance_sample_size: int | None = 1000,
)
```

[`BaseTable`](#basetable) adapter for a pandas DataFrame — the concrete
implementation used by [`valentine_match`](#valentine_match).

| Parameter              | Type            | Default | Description                                                                                     |
|------------------------|-----------------|---------|-------------------------------------------------------------------------------------------------|
| `df`                   | `pd.DataFrame`  | —       | The DataFrame to wrap.                                                                          |
| `name`                 | `str`           | —       | Name of the table. Used as `source_table` / `target_table` in emitted [`ColumnPair`](#columnpair)s. |
| `instance_sample_size` | `int \| None`   | `1000`  | Cap on the number of non-empty rows sampled per column. Pass `None` to use the full DataFrame; pass `0` to expose no instance data at all. Must be `>= 0` or `None`; other values raise `ValueError`. |

Automatic data-type detection classifies each column as `"varchar"`,
`"int"`, `"float"`, or `"date"` based on the DataFrame's dtype and
content.

### `DataframeColumn`

[`BaseColumn`](#basecolumn) adapter for a single pandas `Series`.
Constructed internally by [`DataframeTable`](#dataframetable); exposes
the column name, detected data type, unique identifier, and sampled
instance values via the standard [`BaseColumn`](#basecolumn) interface.

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
