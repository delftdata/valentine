---
icon: lucide/list-tree
---

# Matcher results

`valentine_match(...)` returns a `MatcherResults` object — an **immutable
mapping** of `ColumnPair` keys to similarity scores, sorted from highest
score to lowest. It behaves like a `dict` for lookup and iteration, but
cannot be mutated (preventing accidental invalidation of cached derived
views such as `one_to_one()`).

## `ColumnPair`

Each key in a `MatcherResults` is a `ColumnPair` namedtuple with four
named fields:

```python
from valentine.algorithms import ColumnPair

pair = ColumnPair(
    source_table="sales",
    source_column="customer_id",
    target_table="orders",
    target_column="cust_id",
)

pair.source_table   # "sales"
pair.source_column  # "customer_id"
pair.target_table   # "orders"
pair.target_column  # "cust_id"

pair.source         # ("sales", "customer_id")
pair.target         # ("orders", "cust_id")
```

`ColumnPair` is a `NamedTuple`, so it still unpacks like a plain tuple and
is hashable, immutable, and cheap to store.

## Iterating results

```python
for pair, score in matches.items():
    print(f"{pair.source_column} <-> {pair.target_column}: {score:.3f}")
```

Standard mapping operations all work:

```python
len(matches)
pair in matches
matches[pair]
list(matches)
```

But mutation methods do not — `MatcherResults` is immutable by design:

```python
matches.update({...})  # AttributeError
matches.pop(pair)      # AttributeError
```

## Convenience methods

```python
# Keep only the top 5 matches by score
top5 = matches.take_top_n(5)

# Keep the top 25% of matches by score
top_quarter = matches.take_top_percent(25)

# Filter by an absolute score threshold
strong = matches.filter(min_score=0.7)

# Reduce to one-to-one matches (greedy, highest-first). Threshold defaults
# to the median score of the current results.
one_to_one = matches.one_to_one()

# Override the threshold to be stricter
strict = matches.one_to_one(threshold=0.8)
```

Every transformation returns a **new** `MatcherResults` instance, so you
can chain them:

```python
best_strict_pairs = matches.filter(min_score=0.5).one_to_one(threshold=0.7)
```

!!! tip "Details propagation"

    When a matcher provides per-pair sub-matcher breakdowns, those details
    are filtered alongside the data when you call `filter`, `one_to_one`,
    `take_top_n`, or `take_top_percent` — the derived `MatcherResults`
    keeps only the details for its surviving pairs.

## Match details (Coma)

When you use `Coma`, each `ColumnPair` comes with a breakdown showing how
each sub-matcher contributed to the final similarity score:

```python
for pair, score in matches.items():
    details = matches.get_details(pair)
    if details:
        print(f"{pair.source_column} <-> {pair.target_column}: {score:.3f}")
        for sub_matcher, sub_score in details.items():
            print(f"  {sub_matcher}: {sub_score:.3f}")
```

Typical output looks like:

```
customer_id <-> cust_id: 0.832
  NameCM: 0.72
  PathCM: 0.65
  LeavesCM: 0.58
  ParentsCM: 0.43
  InstancesCM: 0.91
```

`get_details(pair)` returns `None` for matchers that do not populate
details (i.e. everything except Coma). The full mapping is also available
via `matches.details`.

## Computing metrics

If you have a ground truth, compute evaluation metrics directly on the
results object:

```python
ground_truth = [
    ("emp_id", "employee_number"),
    ("fname",  "first_name"),
]

metrics = matches.get_metrics(ground_truth)
```

See [Evaluation metrics](metrics.md) for the full list of supported
metrics and custom-metric options.

## Copying

Need an independent copy (e.g. to hand off to downstream code):

```python
copy = matches.get_copy()
```
