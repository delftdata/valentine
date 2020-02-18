# Overlap parameters
join_overlap_th = 0.4

# Schema similarity parameters
max_distance_schema_similarity = 10

# Serde parameters
serdepath = "./data"
signcollectionfile = "sigcolfile.pickle"
graphfile = "graphfile.pickle"
graphcachedfile = "graphcachedfile.pickle"
datasetcolsfile = "datasetcols.pickle"
simrankfile = "simrankfile.pickle"
jgraphfile = "jgraphfile.pickle"

# DB connection
db_host = 'localhost'
db_port = '9200'

###########
## minhash
###########
k = 512


###########
## DoD
###########
separator = '|'
join_chunksize = 1000
memory_limit_join_processing = 0.6  # 60% of total memory
