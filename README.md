# Valentine: Evaluating Matching Techniques for Dataset Discovery

Project page: https://delftdata.github.io/valentine/

This is the main repository that contains the framework used in the paper **Valentine: Evaluating Matching Techniques for Dataset Discovery**. The data generator and the framework's output and visualizations are on the following repositories:

Data generator: [valentine-generator](https://github.com/delftdata/valentine-generator)

Paper results and visualizations: [valentine-paper-results](https://github.com/delftdata/valentine-paper-results)

The datasets used for experiments in Valentine can be found in the [datasets-archive](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku).

## Installation instructions
The following instructions have been tested on a newly created Ubuntu 18.04 LTS VM. If you prefer to run the entire suite on docker, skip this and the [Run experiments](#run-experiments) sections and go directly to the [Run with docker](#run-with-docker) section.

1. Clone the repo to your machine using git `git clone https://github.com/delftdata/valentine-suite`
2. To install all the dependencies required by the suite, run the [`install-dependencies.sh`](https://github.com/delftdata/valentine-suite/blob/master/install-dependencies.sh) script. 

> NOTE: This script installs programs and hence requires `sudo` rights in some parts

After these two steps, the framework should not require anything more regarding dependencies.

## Run experiments
1. Download the data from the [datasets-archive](https://surfdrive.surf.nl/files/index.php/s/QU5oxyNMuVguEku) and put them into a folder called data on the project root level. 

2. Set the grid-search configuration that you want to run for all the algorithms in the file [algorithm_configurations.json](https://github.com/delftdata/valentine-suite/blob/master/algorithm_configurations.json)

3. Activate the conda environment created in the installation phase with the following command `conda activate valentine-suite` and run the [generate_configuration_files.py](https://github.com/delftdata/valentine-suite/blob/master/generate_configuration_files.py) script with the command `python generate_configuration_files.py`. This will create all the configuration files that specify a schema matching job (Run a specific method with specific parameters on a specific dataset). 

> NOTE: if your system does not find conda you might need to run `source ~/.bashrc`

4. To run the schema matching jobs in parallel run the script [run_experiments.sh](https://github.com/delftdata/valentine-suite/blob/master/run_experiments.sh) with the command `./run_experiments.sh {method_name} {number_of_parallel_jobs}` e.g. to run 40 Cupid jobs concurrently run `./run_experiments.sh Cupid 40` (This would require a 40 CPU VM to run smoothly). The output will be written in the output folder at the project root level.

## Run with docker
The entire suite is also available as a docker image with name [`kpsarakis/valentine-suite:1.0`](https://hub.docker.com/layers/kpsarakis/valentine-suite/1.0/images/sha256-a3e4a359bb45d05687475af6d19104a85c494b2ac91384373d6d1a769639e62e?context=repo). The steps to run with docker are the following: 

1. Run the following command ` sudo docker run --privileged=true -it -v /var/run/docker.sock:/var/run/docker.sock kpsarakis/valentine-suite:1.0` this will download the image and start a shell on the image containing the valentine suite.

2. Activate the conda environment by running `conda activate valentine-suite`

3. Go into the folder of the suite using `cd /home/valentine-benchmark`

4. Now you are able to run the suite with the data used in the paper **Valentine: Evaluating Matching Techniques for Dataset Discovery** by running `./run_experiments.sh {method_name} {number_of_parallel_jobs}` e.g. to run 40 Cupid jobs concurrently run `./run_experiments.sh Cupid 40` (This would require a 40 CPU VM to run smoothly). The output will be written in the output folder in the project root level, i.e. `\home\valentine-benchmark\output`.


## Integrate new methods
Since Valentine is an experiment suit, it is designed to be extended with more schema matching methods. To extend Valentine with such methods, please visit the following wiki [guide](https://github.com/delftdata/valentine-suite/wiki/Integrate-new-methods) on how to do so. 

## Project structure

* [`algorithms`](https://github.com/delftdata/valentine-suite/tree/master/algorithms) Module containing all the implemented algorithms in the suite.
   * [`coma`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/coma) Python wrapper around [COMA 3.0 Comunity edition](https://sourceforge.net/projects/coma-ce/)
   * [`cupid`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/cupid) Contains the python implementation of the paper [Generic Schema Matching with Cupid](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.79.4079&rep=rep1&type=pdf)
   * [`distribution_based`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/distribution_based) Contains the python implementation of the paper [Automatic Discovery of Attributes in Relational Databases](https://dl-acm-org.tudelft.idm.oclc.org/doi/pdf/10.1145/1989323.1989336)
    * [`embdi`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/embdi) Contains the code of [EmbDI](https://dl.acm.org/doi/10.1145/3318464.3389742) provided by the authors in their [GitLab repository](https://gitlab.eurecom.fr/cappuzzo/embdi)
   * [`jaccard_levenshtein`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/jaccard_levenshtein) Contains a baseline that uses Jaccard Similarity between columns to assess their correspondence score, enhanced by Levenshtein Distance
   * [`sem_prop`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/sem_prop) Contains the code of [Seeping Semantics](http://da.qcri.org/ntang/pubs/icde2018semantic.pdf) provided in [Aurum](https://github.com/mitdbg/aurum-datadiscovery)
   * [`similarity_flooding`](https://github.com/delftdata/valentine-suite/tree/master/algorithms/similarity_flooding) Contains the python implementation of the paper [Similarity Flooding: A Versatile Graph Matching Algorithmand its Application to Schema Matching](http://p8090-ilpubs.stanford.edu.tudelft.idm.oclc.org/730/1/2002-1.pdf)
   
* [`data_loader`](https://github.com/delftdata/valentine-suite/tree/master/data_loader) Module used to load the relational data coming from the [valentine-generator](https://github.com/delftdata/valentine-generator)
* [`metrics`](https://github.com/delftdata/valentine-suite/tree/master/metrics) Module containing the metrics that the framework supports (e.g. Precision, Recall, ...) 
* [`utils`](https://github.com/delftdata/valentine-suite/tree/master/utils) Module containing some utility functions used throughout the framework
