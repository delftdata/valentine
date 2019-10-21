### Step 1 
Run pipeline.sh 
This will download ElasticSearch and will build the profiler. 

### Step 2
Create an yml config file to access the database. The example file is access-db-movies.yml. Please use it as a template to create other config files.

Next, run load_db.sh

### Step 3 
Create a model from the previously loaded db, by running create_pkl_model.sh

### Step 4 
Convert the ontology into pkl format using create_pkl_ontology.sh script.