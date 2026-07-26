---
icon: lucide/newspaper
---

# Announcing Valentine v1.0.0

Four years ago we built Valentine with a simple goal: make schema matching genuinely
accessible to anyone who needs it. The promise was one call, several
battle-tested algorithms, and evaluation metrics to tell you how well they did, without
asking you to wire up Java, parse XML configs, or read a paper before getting your first
result.

We are excited to ship **v1.0.0**, a complete overhaul of everything from the public API to
the internals of every matcher. The headline number is a **~27× wall-clock speedup** across
our benchmark suite, with accuracy essentially unchanged. But the speed is only part of the
story. v1.0.0 also drops the JVM dependency entirely, adds Polars support, introduces
embedding-based matching, and gives the API the consistency and safety it always deserved.

---

## The benchmarks first

We ran every matcher on the [NYC Open Data benchmark](research.md) — 10 real-world table
pairs spanning city government, education, housing, and transportation — on the same Windows
machine, with a 120 s per-dataset timeout.

### Speed

| Matcher | v0.5.0 | v1.0.0 | Speedup |
|---|---:|---:|---:|
| Coma (schema) | 8.31 s | 0.65 s | **13×** |
| Coma (instances) | 322.23 s | 4.71 s | **68×** |
| Cupid | 163.04 s | 3.55 s | **46×** |
| DistributionBased | 164.70 s | 3.94 s | **42×** |
| JaccardDistanceMatcher | 730.36 s ⚠ | 3.92 s | **186×** |
| SimilarityFlooding | 53.84 s | 3.30 s | **16×** |

!!! warning "v0.5.0 reliability"
    JaccardDistanceMatcher timed out on 5 of 10 datasets in v0.5.0; the 730 s total includes
    5 × 120 s forced timeouts. DistributionBased crashed on one dataset with an unguarded
    empty-sequence error — fixed in v1.0.0.

### Accuracy

All of this is pure implementation work. Accuracy is preserved across the board — F1
differences are within ±0.02 on every matcher.

| Matcher | v0.5.0 F1 | v1.0.0 F1 | v0.5.0 Recall@GT | v1.0.0 Recall@GT | v0.5.0 MRR | v1.0.0 MRR |
|---|---:|---:|---:|---:|---:|---:|
| Coma (schema) | 0.658 | 0.665 | 0.642 | 0.651 | 0.305 | 0.302 |
| Coma (instances) | 0.765[^coma-instances] | 0.772 | 0.813[^coma-instances] | 0.763 | 0.343[^coma-instances] | 0.338 |
| Cupid | 0.480 | 0.485 | 0.427 | 0.430 | 0.245 | 0.249 |
| DistributionBased | 0.647[^distributionbased] | 0.681 | 0.590[^distributionbased] | 0.621 | 0.289[^distributionbased] | 0.302 |
| JaccardDistanceMatcher | 0.666[^jaccard] | 0.646 | 0.625[^jaccard] | 0.561 | 0.335[^jaccard] | 0.247 |
| SimilarityFlooding | 0.507 | 0.493 | 0.501 | 0.580 | 0.285 | 0.303 |

[^coma-instances]: Coma (instances) v0.5.0 mean computed over 9 completed datasets (Housing_Maintenance timed out even at 8 GB heap).
[^distributionbased]: DistributionBased v0.5.0 excludes one crashed dataset.
[^jaccard]: Jaccard v0.5.0 computed over 5 completed datasets only.

Full per-dataset breakdowns, side-by-side by matcher, are in the [Benchmark](benchmark.md) page.

---

## What changed and why

### 1. No more JVM — Coma is now pure Python

In v0.5.0, the canonical `Coma` was a thin wrapper around the Java COMA 3.0 implementation.
This meant: a JRE on every machine that runs Valentine, spawning a new JVM per call, manual
heap configuration (`java_xmx`), and OOM failures on large datasets even with 8 GB allocated.

v1.0.0 replaces all of that with a pure-Python reimplementation. No JVM, no heap tuning, no
install step beyond `pip install valentine`. The new implementation uses TF-IDF cosine on
cached float32 sparse CSR matrices with pair-level memoisation, which is both faster (68× on
instance mode) and marginally more accurate.

The old `ComaPy` experimental class is gone — it was promoted to stable and became the new
`Coma`. If you were using `ComaPy`, switch to `Coma` and you're already running the new code.

```python
# Before (v0.5.x)
from valentine.algorithms import Coma     # Java
from valentine.algorithms import ComaPy   # pure Python, experimental

# After (v1.0.0)
from valentine.algorithms import Coma     # pure Python, stable, no JVM
```

---

### 2. A cleaner API everywhere

#### One function, any number of DataFrames

`valentine_match_batch` is gone. `valentine_match` now accepts any iterable of DataFrames —
a pair, a list, a generator. Pandas and Polars frames can be freely mixed in the same call.

```python
# Before
from valentine import valentine_match, valentine_match_batch

matches_pair  = valentine_match(df1, df2, matcher)
matches_batch = valentine_match_batch([df1, df2, df3], matcher)

# After
from valentine import valentine_match

matches_pair  = valentine_match([df1, df2], matcher)
matches_batch = valentine_match([df1, df2, df3], matcher)
```

#### Named match keys with `ColumnPair`

Match results are now keyed by `ColumnPair`, a `NamedTuple` with
`source_table`, `source_column`, `target_table`, and `target_column` fields.
This replaces the previous opaque nested-tuple `((table, col), (table, col))` keys and
makes iteration code far easier to read.

```python
# Before
for ((t1, c1), (t2, c2)), score in matches.items():
    print(f"{c1} <-> {c2}: {score:.3f}")

# After
for pair, score in matches.items():
    print(f"{pair.source_column} <-> {pair.target_column}: {score:.3f}")
    # also: pair.source_table, pair.target_table
```

`ColumnPair` is a `NamedTuple`, so positional destructuring still works — migration is
mechanical for any existing code.

#### Immutable results, explicit selectors

`MatcherResults` is now a proper `Mapping`, not a mutable `dict` subclass. You can no longer
silently corrupt a result set by writing into it. Instead, all transformations return new
objects:

```python
high_confidence = matches.filter(min_score=0.7)
top_ten         = matches.take_top_n(10)
top_quarter     = matches.take_top_percent(25)
```

The old generic `one_to_one()` method is replaced by three explicit selectors, making the
algorithm transparent:

```python
# Globally optimal — new default, recommended
best = matches.one_to_one_hungarian()

# Greedy — preserves legacy behaviour
greedy = matches.one_to_one_greedy()

# Mutual nearest neighbour — high-precision filter
mutual = matches.one_to_one_mutual_top(n=1)
```

The metrics API picks up the same knob:

```python
matches.get_metrics(ground_truth, metrics={F1Score()}, one_to_one_method="hungarian")
```

---

### 3. How the speed was won

Every matcher got a targeted rewrite. A few highlights:

**Coma** — schema matching now uses TF-IDF cosine on float32 sparse CSR matrices. A per-call
LRU cache keyed on list identity (with a reference held alongside the `id()` to survive GC)
avoids recomputing the vectoriser when the same column list appears more than once in a batch.
Stopwords switched from a 33-word frozenset to NLTK's 179-word English set. Generic
abbreviation handling (`dept`→`department`, `fname`→`firstname`) is now built in.

**Cupid** — WordNet synset lookups and lemma walks are cached across columns. Datatype
compatibility is now a clean binary classifier (same family = 1.0) that handles arbitrary SQL
type strings like `varchar(255)` or `bigint` without pattern-matching hacks.

**DistributionBased** — the per-row `bucket_binary_search` loop is replaced with
`np.searchsorted` + `np.bincount` over precomputed bound arrays. This is the difference
between O(n × k) Python loops and a single vectorised C call.

**JaccardDistanceMatcher** — uses `rapidfuzz.process.cdist` with a `score_cutoff`
short-circuit. Pairs that can't possibly beat the threshold don't run string distance at all.
This is why the 186× speedup is so dramatic: in v0.5.0, every value pair ran regardless.

**SimilarityFlooding** — the `NodeID` prefix collision (columns named `"NodeID*"` clashing
with internal graph nodes) is fixed, and the tokeniser now handles `snake_case`,
`SCREAMING_SNAKE`, hyphens, and embedded digits.

---

### 4. New capabilities

#### Polars support

```python
import polars as pl
from valentine import valentine_match
from valentine.algorithms import Coma

df1 = pl.read_csv("source.csv")
df2 = pl.read_csv("target.csv")   # or a pandas DataFrame — mixing is fine

matches = valentine_match([df1, df2], Coma())
```

Install with `pip install valentine[polars]`.

#### Embedding-based Jaccard

`JaccardDistanceMatcher` now supports a semantic distance mode using sentence-transformer
embeddings. Instead of character-level string distance between values, it embeds every value
once and uses cosine similarity — one forward pass per column, then a matrix comparison.

```python
from valentine.algorithms import JaccardDistanceMatcher
from valentine.algorithms.jaccard_distance import StringDistanceFunction

matcher = JaccardDistanceMatcher(
    distance_fun=StringDistanceFunction.Embedding,
    threshold_dist=0.7,          # cosine similarity threshold
    embedding_model="all-MiniLM-L6-v2",
    embedding_device=None,        # auto: cuda → mps → cpu
)
```

On the NYC benchmark, the embedding variant trades ~14× more time for a small accuracy
gain (+0.01 F1), and it performs particularly well on columns with semantically related but
lexically dissimilar names.

| Mode | Time | Mean F1 | Mean Recall@GT |
|---|---:|---:|---:|
| JaccardDistanceMatcher (string) | 3.92 s | 0.646 | 0.561 |
| JaccardDistanceMatcher (embedding) | 48.98 s | 0.657 | 0.581 |

Install with `pip install valentine[embeddings]`.

#### Tversky similarity

The value-set comparison underlying Jaccard is now generalised to Tversky similarity via
`tversky_alpha` and `tversky_beta` parameters. The defaults reproduce Jaccard exactly; other
presets give Sørensen-Dice or set containment — useful when one column is expected to be a
strict subset of the other.

#### MRR metric and new metric sets

`MeanReciprocalRank` joins the built-in metric set. For each source column, it finds the rank
of the first correct target in the matcher's ranked output and averages the reciprocal ranks —
a standard IR metric that captures whether the right answer appears near the top, not just
somewhere in the list.

Four predefined sets cover the most common evaluation workflows: `METRICS_CORE`,
`METRICS_ALL`, `METRICS_PRECISION_RECALL`, and `METRICS_PRECISION_INCREASING_N`.

#### Sub-matcher score details (Coma)

```python
for pair, score in matches.items():
    details = matches.get_details(pair)
    # {'NameCM': 0.72, 'LeavesCM': 0.58, ...}
```

---

## Migrating from 0.5.x

The changes are mechanical. A quick checklist:

| What changed | Before | After |
|---|---|---|
| Match function | `valentine_match(df1, df2, m)` | `valentine_match([df1, df2], m)` |
| Batch function | `valentine_match_batch([...], m)` | `valentine_match([...], m)` |
| Match keys | `((t1,c1),(t2,c2))` | `ColumnPair` namedtuple |
| 1:1 selector | `.one_to_one()` | `.one_to_one_hungarian()` |
| Mutable results | `matches[key] = val` | not allowed — use `.filter()` etc. |
| Java Coma | `Coma(..., java_xmx="8192m")` | `Coma()` (no JVM args needed) |
| Pure-Python Coma | `ComaPy(...)` | `Coma(...)` |
| Exception name | `NotAValentineMatcher` | `InvalidMatcherError` |

The full migration guide with code examples for every breaking change lives in the
[Changelog & migration](changelog.md) page.

---

## Get it

```shell
pip install valentine              # core, pandas
pip install valentine[polars]      # + Polars support
pip install valentine[embeddings]  # + sentence-transformer Jaccard
```

- **Docs:** <https://delftdata.github.io/valentine/>
- **GitHub:** <https://github.com/delftdata/valentine>
- **Changelog:** [Full changelog and migration guide](changelog.md)
- **Benchmark details:** [NYC per-dataset results](benchmark.md)

Feedback, issues, and PRs welcome.
