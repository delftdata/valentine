# from cupid import pipeline 

# tree, sims = pipeline.example()
from experiments import cupid_cupid_data

cupid_cupid_data.run_experiments()
cupid_cupid_data.compute_statistics()