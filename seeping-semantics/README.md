### Step 0
Clone/Fork https://github.com/AndraIonescu/aurum-datadiscovery in this directory. 

### Step 1 
Run pipeline.sh 
This will download ElasticSearch and will build the profiler. 

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
This script create the model in .pkl format and other metadata files used in the framework. 

### Step 5
Move "create_pkl_ontology.py" to the aurum-datadiscovery repo. 
Convert the ontology into pkl format by running create_pkl_ontology.sh <input_file.owl> <output_path.pkl>

**Note**: the output file will be generated into aurum-datadiscovery folder. If the path contains folders, make sure the folder exists already in aurum-datadiscovery. To convert the ontology from .owl to .pkl format takes a while. Moreover, the framework doens't give any update on the progress, so just be patient. 
