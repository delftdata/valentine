The following tutorial will walk you through the required steps to set up aurum and work with it.

The framework depends on external ontologies, word-embeddings and elastic search. 
Aurum can easily read data in csv, postgres, mysql, oracle10g, oracle11g formats and it converts it into pkl format. 

### Step 0
Clone/Fork https://github.com/AndraIonescu/aurum-datadiscovery in this directory. 

This version is a fork from the original repository, that contains small updates to make it compatible with python3.6 
It also contains special files that contains the running pipeline.
### Step 1 
Run pipeline.sh 
This will download ElasticSearch and will build the profiler (the module responsible with loading the data). 

**Requirements**: jre and jdk 8, unzip 

**Note**: ES can't be run as root. 

### Step 2
Create an yml config file to access the database. The example file is access-db-movies.yml. Please use it as a template to create other config files.

Next, run load_db.sh <absolute_path_to_yml_file>

### Step 3
Create a python environment using your preferred library. 
Install pip and the requirements from requirements-aurum.txt file. 

### Step 4
Create a model from the previously loaded db, by running create_pkl_model.sh. 
This script creates the model in .pkl format and other metadata files used in the framework. 

### Step 5
Convert the ontology into pkl format by running create_pkl_ontology.sh <input_file.owl> <output_path.pkl>

**Note**: the output file will be generated into aurum-datadiscovery folder. 
If the path contains folders, make sure they exist already in aurum-datadiscovery.
To convert the ontology from .owl to .pkl format takes a while. 
Moreover, the framework doens't give any update on the progress, so just be patient (e.g. efo.owl ontology took 25 mins to parse).

## Run
The _aurum_ repository contains a python script *semprop_pipeline* that
has configured the entire pipeline of SemProp as described in the 
[paper](http://raulcastrofernandez.com/papers/icde18-seeping.pdf). 

Check the function _init_test()_ and update with the corresponding paths.
Function _test()_ performs the test, that can also be found in 
_test_semprop_pipeline.py_. 