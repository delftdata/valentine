# The Valentine Experiment Suite for Schema Matching

This is the main repository that contains the framework used in the paper **The Valentine Experiment Suite for Schema Matching**. The data generator and the framework's output and visualizations are on the following repositories:

Data generator: [valentine-generator](https://github.com/delftdata/valentine-generator)

Paper results and visualizations: [valentine-paper-results](https://github.com/delftdata/valentine-paper-results)

The datasets used for experiments in Valentine can be found in the [datasets-archive](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku).

## Project structure

* [`algorithms`](https://github.com/delftdata/valentine-suite/tree/master/algorithms) Module containing all the implemented algorithms in the suite.
   * [`coma`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/coma) Python wrapper around [COMA 3.0 Comunity edition](https://sourceforge.net/projects/coma-ce/)
   * [`cupid`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/cupid) Contains the python implementation of the paper [Generic Schema Matching with Cupid](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.79.4079&rep=rep1&type=pdf)
   * [`distribution_based`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/distribution_based) Contains the python implementation of the paper [Automatic Discovery of Attributes in Relational Databases](https://dl-acm-org.tudelft.idm.oclc.org/doi/pdf/10.1145/1989323.1989336)
   * [`jaccard_levenshtein`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/jaccard_levenshtein) Contains a baseline that uses Jaccard Similarity between columns to assess their correspondence score, enhanced by Levenshtein Distance
   * [`sem_prop`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/sem_prop) Contains the code of [Seeping Semantics](http://da.qcri.org/ntang/pubs/icde2018semantic.pdf) provided in [Aurum](https://github.com/mitdbg/aurum-datadiscovery)
   * [`similarity_flooding`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/similarity_flooding) Contains the python implementation of the paper [Similarity Flooding: A Versatile Graph Matching Algorithmand its Application to Schema Matching](http://p8090-ilpubs.stanford.edu.tudelft.idm.oclc.org/730/1/2002-1.pdf)
   
* [`data_loader`](https://github.com/delftdata/valentine-suite/tree/master/data_loader) Module used to load the relational data coming from the [valentine-generator](https://github.com/delftdata/valentine-generator)
* [`metrics`](https://github.com/delftdata/valentine-suite/tree/master/metrics) Module containing the metrics that the framework supports (e.g. Precision, Recall, ...) 
* [`utils`](https://github.com/delftdata/valentine-suite/tree/master/utils) Module containing some utility functions used throughout the framework
