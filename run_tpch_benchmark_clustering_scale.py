import subprocess
import time

import pandas as pd
import os
import pickle
from multiprocessing import get_context, Pool

from clustering_scale.correlation_clustering_scale import CorrelationClustering


def convert_data_type(string: str):
    try:
        f = float(string)
        if f.is_integer():
            return int(f)
        return f
    except ValueError:
        return string


def generate_global_ranks(path):
    all_data = []
    for root, dirs, files in os.walk(os.path.join(path)):
        for file in files:
            table = pd.read_csv(root + "/" + file, index_col=False).fillna(0)
            for (_, column_data) in table.iteritems():
                all_data.extend(column_data)
    ranks = unix_sort_ranks(set(all_data))

    with open('cache/global_ranks/ranks.pkl', 'wb') as output:
        pickle.dump(ranks, output, pickle.HIGHEST_PROTOCOL)


def unix_sort_ranks(corpus):

    with open("./cache/sorts/unsorted_file.txt", 'w') as out:
        for var in corpus:
            print(str(var), file=out)

    subprocess.Popen(['sort -n cache/sorts/unsorted_file.txt > cache/sorts/sorted_file.txt'],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    time.sleep(20)

    rank = 1
    ranks = []

    with open('./cache/sorts/sorted_file.txt', 'r') as f:
        txt = f.read()
        for var in txt.splitlines():
            ranks.append((convert_data_type(var.replace('\n', '')), rank))
            rank = rank + 1
    return dict(ranks)


def load_dataset(path: str, threshold1: float, threshold2, quantiles: int, pool: Pool, clear_cache: bool = False):
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
    clear_cache: bool
            if true it clears the global ranks
    Returns
    -------
    CorrelationClustering
        the correlation clustering object with the data loaded
    """
    if clear_cache:
        generate_global_ranks(path)

    cc = CorrelationClustering(quantiles, threshold1, threshold2)
    for root, dirs, files in os.walk(os.path.join(path)):
        for file in files:
            cc.add_data(pd.read_csv(root + "/" + file, index_col=False).fillna(0), str(file.split(".")[0]), pool)
    return cc


def create_cache_dirs():
    if not os.path.exists('cache'):
        os.makedirs('cache')
    if not os.path.exists('cache/global_ranks'):
        os.makedirs('cache/global_ranks')
    if not os.path.exists('cache/sorts'):
        os.makedirs('cache/sorts')



def get_results(path: str, threshold1: float, threshold2: float, quantiles: int, process_pool: Pool,
                chunk_size: int = None, clear_cache: bool = False):
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
    chunk_size: int, optional
            the number of chunks of each job process (default let the framework decide)
    clear_cache: bool, optional
            clear the previous global ranks
    """
    create_cache_dirs()

    correlation_clustering = load_dataset(path, threshold1, threshold2, quantiles, process_pool,
                                          clear_cache=clear_cache)
    print("DATA LOADED")

    correlation_clustering.find_matches(process_pool, chunk_size)


if __name__ == "__main__":
    with get_context("spawn").Pool(4) as p:  # Create a pool of processes
        get_results("data/Customer_Example/", threshold1=0.2, threshold2=0.1, quantiles=50, process_pool=p,
                    chunk_size=1, clear_cache=True)
