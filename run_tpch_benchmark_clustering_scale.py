import pandas as pd
import os
import pickle
from multiprocessing import get_context, Pool
import scipy.stats as ss

from clustering_scale.correlation_clustering_scale import CorrelationClustering


def generate_global_ranks(path):
    all_data = []
    for root, dirs, files in os.walk(os.path.join(path)):
        for file in files:
            table = pd.read_csv(root + "/" + file, index_col=False).fillna(0)
            for (_, column_data) in table.iteritems():
                all_data.extend(column_data)
    ranks = get_rank_dict(set(all_data))
    with open('cache/global_ranks/ranks.pkl', 'wb') as output:
        pickle.dump(ranks, output, pickle.HIGHEST_PROTOCOL)


def get_rank_dict(all_data: set):
    return dict(
        zip(all_data,
            ss.rankdata(
                list(
                    map(lambda x: x[1],
                        sorted(map(lambda x_y: (float(x_y[0]), x_y[1]) if type(x_y[0]) is int else (x_y[0], x_y[1]),
                                   list(zip(all_data, range(len(all_data))))),
                               key=lambda item: ([str, float].index(type(item[0])), item)))), method='dense')))


def load_dataset(path: str, threshold: float, quantiles: int, pool: Pool, clear_cache: bool = False):
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

    cc = CorrelationClustering(quantiles, threshold)
    for root, dirs, files in os.walk(os.path.join(path)):
        for file in files:
            cc.add_data(pd.read_csv(root + "/" + file, index_col=False).fillna(0), str(file.split(".")[0]), pool)
    return cc


def create_cache_dirs():
    if not os.path.exists('cache'):
        os.makedirs('cache')
    if not os.path.exists('cache/global_ranks'):
        os.makedirs('cache/global_ranks')


def get_results(path: str, threshold: float, quantiles: int, process_pool: Pool, chunk_size: int = None,
                clear_cache: bool = False):
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

    correlation_clustering = load_dataset(path, threshold, quantiles, process_pool, clear_cache=clear_cache)
    print("DATA LOADED")

    correlation_clustering.find_matches(process_pool, chunk_size)


if __name__ == "__main__":
    number_of_processes = 4
    with get_context("spawn").Pool(number_of_processes) as p:  # Create a pool of processes
        get_results("data/clustering/meeting_example", threshold=0.1, quantiles=256, process_pool=p, chunk_size=1,
                    clear_cache=True)
