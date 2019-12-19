import pandas as pd
import os
from multiprocessing import get_context, Pool
import sys
import json

from clustering_scale.correlation_clustering_scale import CorrelationClustering


def load_dataset(path: str, threshold: float, quantiles: int, pool: Pool):
    """
    Loads the TPCH dataset to the correlation clustering algorithm mentioned in
    "Automatic Discovery of Attributes in Relational Databases" [1]

    Parameters
    ---------
    path : str
        The root folder of the TPCH dataset
    threshold : float
         The global threshold described in [1]
    quantiles: int
        The number of quantiles of the histograms
    pool: multiprocessing.Pool
            the process pool that will be used in the pre-processing of the table's columns

    Returns
    -------
    CorrelationClustering
        the correlation clustering object with the data loaded
    """
    if not os.path.exists('cache'):
        os.makedirs('cache')
    cc = CorrelationClustering(quantiles, threshold)
    for root, dirs, files in os.walk(os.path.join(path)):
        for file in files:
            cc.add_data(pd.read_csv(root + "/" + file, index_col=False).fillna(0), str(file.split(".")[0]), pool)
    return cc


def get_results(path: str, threshold: float, quantiles: int, process_pool: Pool, chuck_size: int = None):
    """
    Runs the Schema Matching pipeline described in
    "Automatic Discovery of Attributes in Relational Databases" [1]

    Parameters
    ---------
    path : str
        The root folder of the TPCH dataset
    threshold : float
         The global threshold described in [1]
    quantiles: int
        The number of quantiles that the histograms are split on
    process_pool: multiprocessing.Pool
            the process pool that will be used in the pre-processing of the table's columns
    chuck_size: int, optional
            the number of chunks of each job process (default let the framework decide)
    """
    correlation_clustering = load_dataset(path, threshold, quantiles, process_pool)
    print("DATA LOADED")

    matches = correlation_clustering.find_matches(process_pool, chuck_size)

    write_clusters_to_json(matches)


def write_clusters_to_json(matches: list):
    """
    Writes the clusters with their attributes and their connections in a json file

    Parameters
    ---------
    matches : list(list(str))
        a list with the clusters, their attributes and their connections
    """
    i = 1
    d = {}
    for match in matches:
        d["Cluster " + str(i)] = list(match)
        i = i + 1

    with open('clustering_matches.json', 'w') as fp:
        json.dump(d, fp)


if __name__ == "__main__":
    """
    argv[1] -> the dataset root path
    argv[2] -> the global threshold
    argv[3] -> the number of quantiles 
    argv[4] -> the number of processes to spawn
    argv[5] -> the number of chunks of each job process
    
    e.g. python3 run_tpch_benchmark.py data/TPCH/ 0.5 100 4 10
    """
    with get_context("spawn").Pool(int(sys.argv[4])) as p:  # Create a pool of processes
        get_results(sys.argv[1], float(sys.argv[2]), int(sys.argv[3]), p, int(sys.argv[5]))
