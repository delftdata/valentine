### Step 0
Clone/Fork https://github.com/AndraIonescu/aurum-datadiscovery

### Step 1 
Run pipeline.sh 
This will download ElasticSearch and will build the profiler. 

### Step 2
Create an yml config file to access the database. The example file is access-db-movies.yml. Please use it as a template to create other config files.

Next, run load_db.sh <absolute_path_to_yml_file>

### Step 3
Create a python environment using your preferred library. 
Install pip and the requirements from requirements-aurum.txt file. 

### Step 4
Create a model from the previously loaded db, by running create_pkl_model.sh

### Step 5
Move "create_pkl_ontology.py" to the aurum-datadiscovery repo. 
Convert the ontology into pkl format using create_pkl_ontology.sh script.