# Valentine: Matching DataFrames Easily

A python package for capturing potential relationships among columns of different tabular datasets, which are given in the form of pandas DataFrames. Valentine is based on [Valentine: Evaluating Matching Techniques for Dataset Discovery](https://ieeexplore.ieee.org/abstract/document/9458921)


## Installation instructions
To install Valentine simply run

```
pip install valentine
```

## Installation requirements

* Python 3.7 or later



## Usage
Valentine can be used to find matches among columns of a given pair of pandas DataFrames. 

### Matching methods
In order to do so, the user can choose one of the following 5 matching methods:

 1. `Coma(int: max_n str: strategy)` is a python wrapper around [COMA 3.0 Comunity edition](https://sourceforge.net/projects/coma-ce/)
 	* **Parameters**: 
 		* **max_n**(*int*) - Accept similarity threshold, default is 0.
 		* **strategy**(*str*) - Choice of "COMA\_OPT" (schema based matching - default) or "COMA\_OPT\_INST" (schema and instance based matching)
 2.  `Cupid(float: w_struct, float: leaf_w_struct, float: th_accept)` is the python implementation of the paper [Generic Schema Matching with Cupid](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.79.4079&rep=rep1&type=pdf)
 	* **Parameters**: 
 		* **w_struct**(*float*) - Structural similarity threshold, default is 0.2.
 		* **leaf_w_struct**(*float*) - Structural similarity threshold, leaf level, default is 0.2.
 		* **th_accept**(*float*) - Accept similarity threshold, default is 0.7.

 3.  `DistributionBased(float: threshold1, float: threshold2)` is the python implementation of the paper [Automatic Discovery of Attributes in Relational Databases](https://dl-acm-org.tudelft.idm.oclc.org/doi/pdf/10.1145/1989323.1989336)
 	* **Parameters**: 
 		* **threshold1**(*float*) - The threshold for phase 1 of the method, default is 0.15.
 		* **threshold2**(*float*) - The threshold for phase 2 of the method, default is 0.15.
 4.  `JaccardLevenMatcher(float: threshold_leven)` is a baseline method that uses Jaccard Similarity between columns to assess their correspondence score, enhanced by Levenshtein Distance
 	* **Parameters**: 
 		* **threshold_leven**(*float*) - Levenshtein ratio threshold for deciding whether two instances are same or not, default is 0.8.
 		

 5. `SimilarityFlooding(str: coeff_policy, str: formula)` is the python implementation of the paper [Similarity Flooding: A Versatile Graph Matching Algorithmand its Application to Schema Matching](http://p8090-ilpubs.stanford.edu.tudelft.idm.oclc.org/730/1/2002-1.pdf)
	* **Parameters**: 
 		* **coeff_policy**(*str*) - Policy for deciding the weight coefficients of the propagation graph. Choice of "inverse\_product" or "inverse\_average" (default).
 		* **formula**(*str*) - Formula on which iterative fixpoint computation is based. Choice of "basic", "formula\_a", "formula\_b" and "formula\_c" (default).

### Matching DataFrames

After selecting one of the 5 matching methods, the user can initiate the matching process in the following way:

```python
matches = valentine_match(df1, df2, matcher, df1_name, df2_name)
```

where df1 and df2 are the two pandas DataFrames for which we want to find matches and matcher is one of Coma, Cupid, DistributionBased, JaccardLevenMatcher or SimilarityFlooding. The user can also input a name for each DataFrame (defaults are "table\_1" and "table\_2"). Function ```valentine_match``` returns a dictionary storing as keys column pairs from the two DataFrames and as keys the corresponding similarity scores.

### Measuring effectiveness

Based on the matches retrieved by calling `valentine_match` the user can use 

```python 
metrics = valentine_metrics.all_metrics(matches, ground_truth)
``` 

in order to get all effectiveness metrics, such as Precision, Recall, F1-score and others as described in the original Valentine paper. In order to do so, the user needs to also input the ground truth of matches based on which the metrics will be calculated. The ground truth can be given as a list of tuples representing column matches that should hold.


### Example
The following block of code shows: 1) how to run a matcher from Valentine on two DataFrames storing information about authors and their publications, and then 2) how to assess its effectiveness based on a given ground truth (as found in [`valentine_example.py`](https://github.com/delftdata/valentine/blob/package/examples/valentine_example.py)):

```python
# Load data using pandas
d1_path = os.path.join('data', 'authors1.csv')
d2_path = os.path.join('data', 'authors2.csv')
df1 = pd.read_csv(d1_path)
df2 = pd.read_csv(d2_path)

# Instantiate matcher and run
matcher = Coma(strategy="COMA_OPT")
matches = valentine_match(df1, df2, matcher)

print(matches)

# If ground truth available valentine could calculate the metrics
ground_truth = [('Cited by', 'Cited by'),
                ('Authors', 'Authors'),
                ('EID', 'EID')]

metrics = valentine_metrics.all_metrics(matches, ground_truth)
    
print(metrics)
```

The output of the above code block is:

```
{(('table_1', 'Cited by'), ('table_2', 'Cited by')): 0.8374313, 
(('table_1', 'Authors'), ('table_2', 'Authors')): 0.83498037, 
(('table_1', 'EID'), ('table_2', 'EID')): 0.8214057}
{'precision': 1.0, 'recall': 1.0, 'f1_score': 1.0, 
'precision_at_10_percent': 1.0, 
'precision_at_30_percent': 1.0,
'precision_at_50_percent': 1.0, 
'precision_at_70_percent': 1.0, 
'precision_at_90_percent': 1.0, 
'recall_at_sizeof_ground_truth': 1.0}

```
## Experimental suite version

The original experimental suite version of Valentine, as first published for the needs of the research paper, can be still found [here](https://github.com/delftdata/valentine/tree/v1.1).

## Project page

The project page containing information about the research supporting Valentine can be accessed [here](https://delftdata.github.io/valentine/).

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
  author={Koutras, Christos and Psarakis, Kyriakos and Ionescu, Andra and Fragkoulis, Marios and Bonifati, Angela and Katsifodimos, Asterios},
  journal={VLDB},
  volume={14},
  number={12},
  pages={2871--2874},
  year={2021},
  publisher={VLDB Endowment}
}
```
