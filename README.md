# Valentine: (Schema-) Matching DataFrames Made Easy

[![build](https://github.com/delftdata/valentine/actions/workflows/build.yml/badge.svg)](https://github.com/delftdata/valentine/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/delftdata/valentine/branch/master/graph/badge.svg?token=4QR0X315CL)](https://codecov.io/gh/delftdata/valentine)
[![PyPI version](https://badge.fury.io/py/valentine.svg)](https://badge.fury.io/py/valentine)
![PyPI - Downloads](https://img.shields.io/pypi/dm/valentine)
[![Python 3.8+](https://img.shields.io/badge/python-3.8|3.9|3.10|3.11|3.12-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/85cfebfc9c6a43359c5b2e56a5fdf3a3)](https://app.codacy.com/gh/delftdata/valentine/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

A python package for capturing potential relationships among columns of different tabular datasets, which are given in the form of pandas DataFrames. Valentine is based on [Valentine: Evaluating Matching Techniques for Dataset Discovery](https://ieeexplore.ieee.org/abstract/document/9458921)

You can find more information about the research supporting Valentine [here](https://delftdata.github.io/valentine/).

## Experimental suite version

The original experimental suite version of Valentine, as first published for the needs of the research paper, can be still found [here](https://github.com/delftdata/valentine/tree/v1.1).

## Installation instructions
### Requirements

*   *Python* >=3.8,<3.13
*   *Java*: For the Coma matcher it is required to have java (jre) installed

To install Valentine simply run:

```shell
pip install valentine
```


## Usage
Valentine can be used to find matches among columns of a given pair of pandas DataFrames. 

### Matching methods
In order to do so, the user can choose one of the following 5 matching methods:

1.   `Coma(int: max_n, bool: use_instances, str: java_xmx)` is a python wrapper around [COMA 3.0 Comunity edition](https://sourceforge.net/projects/coma-ce/)
     *    **Parameters**: 
           *    **max_n**(*int*) - Accept similarity threshold, (default: 0).
           *    **use_instances**(*bool*) - Wheather Coma will make use of the data instances or just the schema information, (default: False).
           *    **java_xmx**(*str*) - The amount of RAM that Coma is allowed to use, (default: "1024m") .

2.   `Cupid(float: w_struct, float: leaf_w_struct, float: th_accept)` is the python implementation of the paper [Generic Schema Matching with Cupid](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.79.4079&rep=rep1&type=pdf)
     *    **Parameters**:
          *    **w_struct**(*float*) - Structural similarity threshold, default is 0.2.
          *    **leaf_w_struct**(*float*) - Structural similarity threshold, leaf level, default is 0.2.
          *    **th_accept**(*float*) - Accept similarity threshold, default is 0.7.

3.   `DistributionBased(float: threshold1, float: threshold2)` is the python implementation of the paper [Automatic Discovery of Attributes in Relational Databases](https://dl-acm-org.tudelft.idm.oclc.org/doi/pdf/10.1145/1989323.1989336)
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

5.   `SimilarityFlooding(str: coeff_policy, str: formula)` is the python implementation of the paper [Similarity Flooding: A Versatile Graph Matching Algorithmand its Application to Schema Matching](http://p8090-ilpubs.stanford.edu.tudelft.idm.oclc.org/730/1/2002-1.pdf)
     * **Parameters**: 
        *    **coeff_policy**(*str*) - Policy for deciding the weight coefficients of the propagation graph. Choice of "inverse\_product" or "inverse\_average" (default).
        *    **formula**(*str*) - Formula on which iterative fixpoint computation is based. Choice of "basic", "formula\_a", "formula\_b" and "formula\_c" (default).

### Matching DataFrame Pair

After selecting one of the 5 matching methods, the user can initiate the pairwise matching process in the following way:

```python
matches = valentine_match(df1, df2, matcher, df1_name, df2_name)
```

where df1 and df2 are the two pandas DataFrames for which we want to find matches and matcher is one of Coma, Cupid, DistributionBased, JaccardLevenMatcher or SimilarityFlooding. The user can also input a name for each DataFrame (defaults are "table\_1" and "table\_2"). Function ```valentine_match``` returns a MatcherResults object, which is a dictionary with additional convenience methods, such as `one_to_one`, `take_top_percent`, `get_metrics` and more. It stores as keys column pairs from the two DataFrames and as values the corresponding similarity scores.

### Matching DataFrame Batch

After selecting one of the 5 matching methods, the user can initiate the batch matching process in the following way:

```python
matches = valentine_match_batch(df_iter_1, df_iter_2, matcher, df_iter_1_names, df_iter_2_names)
```

where df_iter_1 and df_iter_2 are the two iterable structures containing pandas DataFrames for which we want to find matches and matcher is one of Coma, Cupid, DistributionBased, JaccardLevenMatcher or SimilarityFlooding. The user can also input an iterable with names for each DataFrame. Function ```valentine_match_batch``` returns a MatcherResults object, which is a dictionary with additional convenience methods, such as `one_to_one`, `take_top_percent`, `get_metrics` and more. It stores as keys column pairs from the two DataFrames and as values the corresponding similarity scores.


### MatcherResults instance
The `MatcherResults` instance has some convenience methods that the user can use to either obtain a subset of the data or to transform the data. This instance is a dictionary and is sorted upon instantiation, from high similarity to low similarity.
```python
top_n_matches = matches.take_top_n(5)

top_n_percent_matches = matches.take_top_percent(25)

one_to_one_matches = matches.one_to_one()
```


### Measuring effectiveness
The MatcherResults instance that is returned by `valentine_match` or `valentine_match_batch` also has a `get_metrics` method that the user can use 

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
The following block of code shows: 1) how to run a matcher from Valentine on two DataFrames storing information about authors and their publications, and then 2) how to assess its effectiveness based on a given ground truth (a more extensive example is shown in [`valentine_example.py`](https://github.com/delftdata/valentine/blob/master/examples/valentine_example.py)):

```python
import os
import pandas as pd
from valentine import valentine_match
from valentine.algorithms import Coma

# Load data using pandas
d1_path = os.path.join('data', 'authors1.csv')
d2_path = os.path.join('data', 'authors2.csv')
df1 = pd.read_csv(d1_path)
df2 = pd.read_csv(d2_path)

# Instantiate matcher and run
matcher = Coma(use_instances=True)
matches = valentine_match(df1, df2, matcher)

print(matches)

# If ground truth available valentine could calculate the metrics
ground_truth = [('Cited by', 'Cited by'),
                ('Authors', 'Authors'),
                ('EID', 'EID')]

metrics = matches.get_metrics(ground_truth)
    
print(metrics)
```

The output of the above code block is:

```
{
     (('table_1', 'Cited by'), ('table_2', 'Cited by')): 0.86994505, 
     (('table_1', 'Authors'), ('table_2', 'Authors')): 0.8679843, 
     (('table_1', 'EID'), ('table_2', 'EID')): 0.8571245
}
{
     'Recall': 1.0, 
     'F1Score': 1.0, 
     'RecallAtSizeofGroundTruth': 1.0, 
     'Precision': 1.0, 
     'PrecisionTop10Percent': 1.0
}
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
