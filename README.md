<h1 align="center">Valentine 💘</h1>
<p align="center"><em>(Schema-) Matching DataFrames Made Easy</em></p>

<p align="center">
  <a href="https://github.com/delftdata/valentine/actions/workflows/build.yml">
    <img src="https://github.com/delftdata/valentine/actions/workflows/build.yml/badge.svg" alt="Build">
  </a>
  <a href="https://codecov.io/gh/delftdata/valentine">
    <img src="https://codecov.io/gh/delftdata/valentine/branch/master/graph/badge.svg?token=4QR0X315CL" alt="codecov">
  </a>
  <a href="https://app.codacy.com/gh/delftdata/valentine/dashboard">
    <img src="https://app.codacy.com/project/badge/Grade/85cfebfc9c6a43359c5b2e56a5fdf3a3" alt="Codacy Badge">
  </a>
  <a href="https://github.com/astral-sh/ruff">
    <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json" alt="Ruff">
  </a>
  <a href="https://pypi.org/project/valentine/">
    <img src="https://img.shields.io/pypi/v/valentine.svg" alt="PyPI version">
  </a>
  <a href="https://pypi.org/project/valentine/">
    <img src="https://img.shields.io/pypi/pyversions/valentine.svg" alt="Python versions">
  </a>
  <a href="https://pypi.org/project/valentine/">
    <img src="https://img.shields.io/pypi/dm/valentine.svg" alt="PyPI downloads">
  </a>
  <a href="https://github.com/delftdata/valentine/blob/master/LICENSE">
    <img src="https://img.shields.io/github/license/delftdata/valentine.svg" alt="License">
  </a>
  <a href="https://delftdata.github.io/valentine/">
    <img src="https://img.shields.io/badge/docs-GitHub%20Pages-blue.svg" alt="Docs">
  </a>
</p>

---

A Python package for capturing potential relationships among columns of different tabular datasets, given as pandas DataFrames.  
Valentine is based on the paper [**Valentine: Evaluating Matching Techniques for Dataset Discovery**](https://ieeexplore.ieee.org/abstract/document/9458921).

You can find more information about the research supporting Valentine [here](https://delftdata.github.io/valentine/).


## Experimental suite version

The original experimental suite version of Valentine, as first published for the needs of the research paper, can be still found [here](https://github.com/delftdata/valentine/tree/v1.1).

## Installation instructions
### Requirements

*   *Python* >=3.10,<3.15

To install Valentine simply run:

```shell
pip install valentine
```


## Usage
Valentine can be used to find matches among columns of a given pair of pandas DataFrames. 

### Matching methods
In order to do so, the user can choose one of the following matching methods:

1.   `Coma(int: max_n, bool: use_instances, bool: use_schema, float: delta, float: threshold)` is a pure Python implementation of the [COMA 3.0](https://sourceforge.net/projects/coma-ce/) schema matching algorithm.
     *    **Parameters**:
           *    **max_n**(*int*) - Maximum number of matches to keep per column, 0 means unlimited (default: 0).
           *    **use_instances**(*bool*) - Whether to use TF-IDF instance-based matching on data values (default: False).
           *    **use_schema**(*bool*) - Whether to use schema-based matching on column names, paths, and structure (default: True).
           *    **delta**(*float*) - Fraction from the best score within which matches are kept (default: 0.15).
           *    **threshold**(*float*) - Absolute minimum similarity score to keep a match (default: 0.0).

2.   `Cupid(float: w_struct, float: leaf_w_struct, float: th_accept)` is the python implementation of the paper [Generic Schema Matching with Cupid](https://www.vldb.org/conf/2001/P049.pdf)
     *    **Parameters**:
          *    **w_struct**(*float*) - Structural similarity threshold, default is 0.2.
          *    **leaf_w_struct**(*float*) - Structural similarity threshold, leaf level, default is 0.2.
          *    **th_accept**(*float*) - Accept similarity threshold, default is 0.7.

3.   `DistributionBased(float: threshold1, float: threshold2)` is the python implementation of the paper [Automatic Discovery of Attributes in Relational Databases](https://dl.acm.org/doi/10.1145/1989323.1989336)
     *    **Parameters**:
          *    **threshold1**(*float*) - The threshold for phase 1 of the method, default is 0.15.
          *    **threshold2**(*float*) - The threshold for phase 2 of the method, default is 0.15.

4.   `JaccardDistanceMatcher(float: threshold_dist)` is a baseline method that uses Jaccard Similarity between columns to assess their correspondence score, optionally enhanced by a string similarity measure of choice.
     *    **Parameters**:
          *    **threshold_dist**(*float*) - Acceptance threshold for assessing two strings as equal, default is 0.8.

          *    **distance_fun**(*StringDistanceFunction*) - String similarity function used to assess whether two strings are equal. The enumeration class type `StringDistanceFunction` can be imported from `valentine.algorithms.jaccard_distance`. Functions currently supported are:
   		       * `StringDistanceFunction.Levenshtein`: [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance)
               * `StringDistanceFunction.DamerauLevenshtein`: [Damerau-Levenshtein distance](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance)
               * `StringDistanceFunction.Hamming`: [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance)
               * `StringDistanceFunction.Jaro`: [Jaro distance](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
               * `StringDistanceFunction.JaroWinkler`: [Jaro-Winkler distance](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
              * `StringDistanceFunction.Exact`: String equality `==`

5.   `SimilarityFlooding(str: coeff_policy, str: formula)` is the python implementation of the paper [Similarity Flooding: A Versatile Graph Matching Algorithmand its Application to Schema Matching](https://ieeexplore.ieee.org/document/994702)
     * **Parameters**: 
        *    **coeff_policy**(*str*) - Policy for deciding the weight coefficients of the propagation graph. Choice of "inverse\_product" or "inverse\_average" (default).
        *    **formula**(*str*) - Formula on which iterative fixpoint computation is based. Choice of "basic", "formula\_a", "formula\_b" and "formula\_c" (default).

### Matching DataFrames

Pass two or more DataFrames as a list (or any iterable) along with a matcher. Valentine will match columns across all unique pairs:

```python
# Match a pair of DataFrames
matches = valentine_match([df1, df2], matcher)

# Match multiple DataFrames (computes all N×(N-1)/2 pairs)
matches = valentine_match([df1, df2, df3], matcher, df_names=["sales", "orders", "products"])
```

Optionally provide `df_names` to label each DataFrame (defaults to "aaa", "bbb", etc. — designed to have zero similarity so they don't influence schema-based matchers). Function `valentine_match` returns a `MatcherResults` object, which is a dictionary with additional convenience methods, such as `one_to_one`, `take_top_percent`, `filter`, `get_metrics` and more. It stores as keys column pairs from the DataFrames and as values the corresponding similarity scores.


### MatcherResults instance
The `MatcherResults` instance has convenience methods for filtering and subsetting. It is a dictionary sorted from high similarity to low similarity.
```python
top_n_matches = matches.take_top_n(5)

top_n_percent_matches = matches.take_top_percent(25)

one_to_one_matches = matches.one_to_one()

# Filter by minimum score
high_confidence = matches.filter(min_score=0.7)

# One-to-one with custom threshold (default uses median)
one_to_one_strict = matches.one_to_one(threshold=0.5)
```


### Measuring effectiveness
The MatcherResults instance that is returned by `valentine_match` also has a `get_metrics` method that the user can use

```python 
metrics = matches.get_metrics(ground_truth)
``` 

in order to get all effectiveness metrics, such as Precision, Recall, F1-score and others as described in the original Valentine paper. In order to do so, the user needs to also input the ground truth of matches based on which the metrics will be calculated. The ground truth can be given as a list of tuples representing column matches that should hold (see example below).

By default, all the core metrics will be used for this with default parameters, but the user can also customize which metrics to run with what parameters, and implement own custom metrics by extending from the `Metric` base class. Some sets of metrics are available as well.

```python
from valentine.metrics import F1Score, PrecisionTopNPercent, METRICS_PRECISION_INCREASING_N
metrics_custom = matches.get_metrics(ground_truth, metrics={F1Score(one_to_one=False), PrecisionTopNPercent(n=70)})
metrics_prefefined_set = matches.get_metrics(ground_truth, metrics=METRICS_PRECISION_INCREASING_N)

```


### Example
The following block of code shows: 1) how to run a matcher from Valentine on two DataFrames storing information about job candidates, and then 2) how to assess its effectiveness based on a given ground truth (a more extensive example is shown in [`valentine_example.py`](https://github.com/delftdata/valentine/blob/master/examples/valentine_example.py)):

```python
import pandas as pd
from valentine import valentine_match
from valentine.algorithms import Coma

# Load data using pandas
df1 = pd.read_csv("source_candidates.csv")
df2 = pd.read_csv("target_candidates.csv")

# Instantiate matcher and run
matcher = Coma(use_instances=True)
matches = valentine_match([df1, df2], matcher)

print(matches)

# If ground truth available valentine could calculate the metrics
ground_truth = [
    ("emp_id", "employee_number"),
    ("fname", "first_name"),
    ("lname", "last_name"),
    ("dept", "department"),
    ("annual_salary", "compensation"),
    ("hire_date", "start_date"),
    ("office_loc", "work_location"),
]

metrics = matches.get_metrics(ground_truth)

print(metrics)
```

## Cite Valentine
```
Original Valentine paper:
@inproceedings{koutras2021valentine,
  title={Valentine: Evaluating Matching Techniques for Dataset Discovery},
  author={Koutras, Christos and Siachamis, George and Ionescu, Andra and Psarakis, Kyriakos and Brons, Jerry and Fragkoulis, Marios and Lofi, Christoph and Bonifati, Angela and Katsifodimos, Asterios},
  booktitle={2021 IEEE 37th International Conference on Data Engineering (ICDE)},
  pages={468--479},
  year={2021},
  organization={IEEE}
}
Demo Paper:
@article{koutras2021demo,
  title={Valentine in Action: Matching Tabular Data at Scale},
  author={Koutras, Christos and Psarakis, Kyriakos and Siachamis, George and Ionescu, Andra and Fragkoulis, Marios and Bonifati, Angela and Katsifodimos, Asterios},
  journal={VLDB},
  volume={14},
  number={12},
  pages={2871--2874},
  year={2021},
  publisher={VLDB Endowment}
}
```
