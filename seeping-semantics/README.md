# Seeping Semantics [1] - Tutorial

[1] [Fernandez, Raul Castro, et al. "Seeping semantics: Linking datasets using word embeddings for data discovery." 2018 IEEE 34th International Conference on Data Engineering (ICDE). IEEE, 2018.](http://da.qcri.org/ntang/pubs/icde2018semantic.pdf)

The following tutorial will walk you through the required steps to set up aurum and work with it.

The framework depends on external ontologies, word-embeddings and elastic search. 
Aurum can easily read data in csv, postgres, mysql, oracle10g, oracle11g formats and it converts it into pkl format. 

### Step 0
Clone/Fork https://github.com/AndraIonescu/aurum-datadiscovery in this directory. 

This version is a fork from the original repository, that contains small updates to make it compatible with python3.6 and files to easily run the pipeline.

### Step 1 

``
./pipeline.sh 
``

This will download ElasticSearch and will build the profiler (the module responsible with loading the data). 

**Requirements**: jre and jdk 8, unzip 
**Note**
>Elastic Search can't be run as root. 

### Step 2
Create an yml config file to access the database. The example file is access-db-movies.yml. Please use it as a template to create other config files.

Next, run the following:

``
./load_db.sh <absolute_path_to_yml_file>
``

>It is important to give the absolut path, because ES (which is in /tmp/) will look for the file. Be patient, loading the models takes a while.

### Step 3
Create a python environment using your preferred library. 
Install pip and the requirements from requirements-aurum.txt file. 

``
pip install -r requirements-aurum.txt
``

### Step 4
Create a model from the previously loaded db, by running create_pkl_model.sh. 
This script creates the model in .pkl format and other metadata files used in the framework. 

``
./create_pkl_model.sh
``

### Step 5
Download the ontology: https://www.ebi.ac.uk/efo/ 

Convert the ontology into pkl format by running create_pkl_ontology.sh <input_file.owl> <output_path.pkl>

``
./create_pkl_ontology.sh path_to_the_downloaded_onto /cache_onto/efo
``

> **Note**: The output file will be generated into aurum-datadiscovery folder. 
If the path contains folders, make sure they exist already in aurum-datadiscovery.
To convert the ontology from .owl to .pkl format takes a while. 
Moreover, the framework doens't give any update on the progress, so just be patient (e.g. efo.owl ontology took 25 mins to parse).

## Run

### Before running
1. Download the [glove.6B.zip](https://nlp.stanford.edu/projects/glove/)
2. Create a folder _glove_ inside the _aurum_ repository and place the file **glove.6b.100d.txt**

The _aurum_ repository contains a python script *run_semprop* that
has configured the entire pipeline of SemProp as described in the paper [1]. 

Running example:

``
python run_semprop.py models/chembl22/ efo cache_onto/efo.pkl glove/glove.6B.100d.txt raw/ MATCHINGS_GROUND_TRUTH_CHEMBL 
``