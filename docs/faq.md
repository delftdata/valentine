---
icon: lucide/help-circle
---

# FAQ

Common questions and gotchas. If yours isn't here, open an issue on
[GitHub](https://github.com/delftdata/valentine/issues).

## Which matcher should I use?

Start with [`Coma`](api.md#coma). It is the strongest default, handles
both schema and instance signals, and is the only matcher that ships
per-sub-matcher [score breakdowns](results.md#match-details-coma) so
you can tell *why* two columns matched. Move to a different matcher
only when you have a clear reason — see the
[decision diagram](matchers.md#which-matcher-should-i-pick) on the
matchers page.

## How do I match more than two DataFrames?

Pass any iterable to [`valentine_match`](api.md#valentine_match) and
Valentine computes all `N * (N - 1) / 2` unique pairs:

```python
matches = valentine_match(
    [sales_df, orders_df, products_df],
    Coma(),
    df_names=["sales", "orders", "products"],
)
```

Each [`ColumnPair`](api.md#columnpair) in the result carries both the
source and target table names, so you can group, filter, or pretty-print
results by table pair.

## Why is the matcher slow / using a lot of memory?

The two biggest dials are **how many columns** each table has and **how
much instance data** the matcher sees per column. For instance-based
matchers (`Coma(use_instances=True)`, `DistributionBased`,
`JaccardDistanceMatcher`), Valentine samples up to
`instance_sample_size` rows per column (default `1000`). If your tables
are large:

- Lower `instance_sample_size` to `200`–`500` for a quick first pass.
- Set `instance_sample_size=None` to use the full DataFrame *only* when
  you need a final, high-quality match.
- Set `instance_sample_size=0` to disable instance data entirely and
  fall back to schema-only matching.

See [`valentine_match`](api.md#valentine_match) for the full signature,
and the [Matchers](matchers.md) page for per-matcher performance notes.

## What's the difference between `instance_sample_size=None` and `0`?

- **`None`** — feed the *entire* column to the matcher. Most accurate,
  most expensive. Use this for final runs on small/medium tables.
- **`0`** — feed *no* instance data. Schema-only matching. Use this when
  the data is sensitive, unavailable, or irrelevant.
- **Positive integer `n`** — sample at most `n` rows per column. The
  default `1000` is a good speed/accuracy trade-off for most workloads.

## My column names are non-ASCII / contain Unicode. Will it work?

Yes. All matchers operate on Python strings and handle Unicode
identifiers correctly. Trigram and edit-distance comparisons run on
code points, not bytes.

## How do I get only the top N matches?

[`MatcherResults`](api.md#matcherresults) is sorted high-to-low and
provides three reduction helpers:

```python
matches.take_top_n(10)              # absolute top 10
matches.take_top_percent(5)         # top 5%
matches.one_to_one()                # bidirectional best matches
```

All three return a new `MatcherResults` — the original is immutable.
See [Matcher results](results.md) for the full pipeline.

## How do I evaluate match quality?

If you have a ground truth — a list of expected `(source_col,
target_col)` pairs — call
[`get_metrics`](api.md#get_metrics):

```python
ground_truth = [("emp_id", "employee_number"), ("fname", "first_name")]
print(matches.get_metrics(ground_truth))
```

By default this computes Precision, Recall, and F1. Pass a
`metrics={...}` set with custom thresholds or your own
[`Metric`](api.md#metric) subclasses for more detail. See
[Evaluation metrics](metrics.md).

## How do I plug in my own data source (not a DataFrame)?

Subclass [`BaseTable`](api.md#basetable) and
[`BaseColumn`](api.md#basecolumn). The
[API reference includes a runnable `DictTable` example](api.md#writing-a-custom-data-source)
that wraps a plain Python `dict[str, list]`. The same example is
exercised by the test suite, so it is guaranteed to stay in sync with
the API.

## How do I plug in my own matcher?

Subclass [`BaseMatcher`](api.md#basematcher) and implement
[`get_matches`](api.md#get_matches). If your matcher benefits from a
holistic view across all input tables, override
[`get_matches_batch`](api.md#get_matches_batch). Populate
[`match_details`](api.md#match_details) from inside your matcher if
you want users to access per-sub-score breakdowns via
[`get_details`](api.md#get_details). Raise `ValueError` from
`__init__` for invalid parameters — the built-in matchers all do.

## I get the same column matched to itself. How do I exclude self-matches?

Self-pairs (same table, same column) never appear in
[`MatcherResults`](api.md#matcherresults). If you see *cross-table*
matches with identical column names that you want to exclude, filter
the result mapping yourself:

```python
filtered = {
    pair: score
    for pair, score in matches.items()
    if pair.source_column != pair.target_column
}
```

## Does Valentine require Java?

**No.** Every matcher in v1.x is pure Python — including the COMA
implementation. There is no JVM, no subprocess, no temp file shuffling.
The Java version of COMA was removed in
[v1.0.0](changelog.md#v100-api-redesign).

## What changed in v1.0.0?

The matching API was unified, `valentine_match_batch` was removed,
results became immutable, and metrics were overhauled. The full story
and a step-by-step migration guide are in the
[changelog](changelog.md#migrating-from-05x).
