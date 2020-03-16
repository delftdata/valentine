import pickle
import os
import re
import shutil
import subprocess
from functools import lru_cache

from algorithms.distribution_based.column_model import CorrelationClusteringColumn
from algorithms.distribution_based.emd_utils import quantile_emd, intersection_emd
from algorithms.distribution_based.quantile_histogram import QuantileHistogram
from utils.utils import convert_data_type, create_folder


def compute_cutoff_threshold(C: list, threshold: float):
    """
    Algorithm 1 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]
    This algorithm computes the threshold of a column that determines if any other column is to be considered
    its neighbour.

    Parameters
    ---------
    C : list
        A list containing dicts of EMD/ColumnName pairs
    threshold : float
        The conservative global EMD cutoff threshold described in [1]

    Returns
    -------
    float
        The cutoff threshold of the input column
    """
    C.append({'e': threshold, 'c': 0})
    C = sorted(C, key=lambda k: k['e'])
    cutoff = 0.0
    gap = 0.0
    i = 0
    while i < len(C) - 1 and C[i + 1]['e'] <= threshold:
        if gap < (C[i + 1]['e'] - C[i]['e']):
            gap = C[i + 1]['e'] - C[i]['e']
            cutoff = C[i]['e']
        i += 1
    return cutoff


def column_combinations(columns: list, dataset_name: str, quantiles: int, intersection: bool = False):
    """
    All the unique combinations between all the columns

    Parameters
    ---------
    columns : list
        A list that contains all the column names
    dataset_name : str
        Other name of the dataset
    quantiles : int
        The number of quantiles that the histograms are split on
    intersection : bool, optional
        If true do the intersection EMD else the normal EMD

    Returns
    -------
    tuple
        A tuple with ((column_name1, column_name1), quantiles, intersection)
    """
    c = len(columns)
    c_i = 0
    while c_i < c:
        name_i = columns[c_i]
        table_i = name_i[0]
        c_j = c_i + 1
        while c_j < c:
            name_j = columns[c_j]
            table_j = name_j[0]
            if table_i != table_j:
                yield (name_i, name_j), dataset_name, quantiles, intersection
            c_j = c_j + 1
        c_i = c_i + 1


def process_emd(tup: tuple):
    """
    Function defining a single quantile_emd process between two columns.

    Parameters
    ---------
    tup : tuple
        A tuple with ((column_name1, column_name1), quantiles, intersection)

    Returns
    -------
    tuple
        a dictionary entry {k: joint key of the column combination, v: quantile_emd calculation}
    """
    name_i, name_j, k, dataset_name, quantile, intersection = unwrap_process_input_tuple(tup)

    c1 = read_from_cache(name_i, dataset_name)
    c2 = read_from_cache(name_j, dataset_name)

    if intersection:
        return k, intersection_emd(c1, c2, quantile)
    else:
        return k, quantile_emd(c1, c2, quantile)


@lru_cache
def read_from_cache(file_name: str, dataset_name: str):
    """
    Function that reads from a pickle file lru cache a column after pre-processing

    Parameters
    ----------
    file_name: str
        The file name that contains the
    dataset_name : str
        The name of the dataset
    Returns
    -------
    CorrelationClusteringColumn
        The preprocessed column
    """

    file_path = 'cache/' + dataset_name + '_' + re.sub('\\W+', '_', str(file_name)) + '.pkl'
    if os.path.getsize(file_path) > 0:
        with open(file_path, 'rb') as pkl_file:
            data = pickle.load(pkl_file)
    return data


def unwrap_process_input_tuple(tup: tuple):
    """
    Helper function that unwraps a tuple to its components and creates a unique key for the column combination

    Parameters
    ---------
    tup : tuple
        the tuple to unwrap
    """
    names, dataset_name, quantile, intersection = tup
    name_i, name_j = names
    k = (name_i, name_j)
    return name_i, name_j, k, dataset_name, quantile, intersection


def insert_to_dict(dc: dict, k: str, v: dict):
    """
    Helper function that instantiates a list to a dictionary key if it is not present and then appends an
    EMD/ColumnName pair to it

    Parameters
    ---------
    dc : dict
        the dictionary
    k : str
        the key
    v : dict
         EMD/ColumnName pair
    """
    if k not in dc:
        dc[k] = list()
    dc[k].append(v)


def transform_dict(dc: dict):
    """
    Helper function that transforms a dict with composite column combination keys to a dict with column keys and
    values EMD/ColumnName pairs in a sorted list (ascending based on the EMD value)

    Parameters
    ---------
    dc : dict
        the dictionary
    """
    tmp_dict = dict()
    for k, v in dc.items():
        k1, k2 = k
        v1 = {'e': v, 'c': k2}
        v2 = {'e': v, 'c': k1}
        insert_to_dict(tmp_dict, k1, v1)
        insert_to_dict(tmp_dict, k2, v2)
    return tmp_dict


def process_columns(tup: tuple):
    """
    Process a pandas dataframe column to a column_model_scale.Column

    Parameters
    ---------
    tup : tuple
        tuple containing the information of the column to be processed
    """
    column_name, data, source_name, dataset_name, quantiles = tup
    column = CorrelationClusteringColumn(column_name, data, source_name, dataset_name, quantiles)
    if column.size > 0:
        column.quantile_histogram = QuantileHistogram(column.long_name, column.ranks, column.size, quantiles)
    pickle_path = 'cache/' + dataset_name + '_' + re.sub('\\W+', '_', str(column.long_name)) + '.pkl'
    if not os.path.isfile(pickle_path):
        with open(pickle_path, 'wb') as output:
            pickle.dump(column, output, pickle.HIGHEST_PROTOCOL)


def parallel_cutoff_threshold(tup: tuple):
    """
    Process the cutoff threshold in parallel for each column

    Parameters
    ---------
    tup : tuple
        tuple containing the information of the column to be processed
    """
    A, column, threshold = tup
    name_i = column.long_name
    theta = compute_cutoff_threshold(A[name_i], threshold)
    Nc = [(name_i, i['c']) for i in A[name_i] if i['e'] <= theta]
    return Nc


def ingestion_column_generator(columns: list, dataset_name: str, quantiles: int):
    """
    Generator of incoming pandas dataframe columns
    """
    for column in columns:
        yield column.name, column.data, column.table_name, dataset_name, quantiles


def cuttoff_column_generator(A: dict, columns: list, dataset_name:str, threshold: float):
    """
    Generator of columns for the cutoff threshold computation
    """
    for column_name in columns:
        file_path = 'cache/' + dataset_name + '_' + re.sub('\\W+', '_', str(column_name)) + '.pkl'
        if os.path.getsize(file_path) > 0:
            with open(file_path, 'rb') as pkl_file:
                column = pickle.load(pkl_file)
        yield A, column, threshold


def generate_global_ranks(data: list, file_name: str):
    """
    Function that creates a pickle file with the global ranks of all the values inside the database.

    Parameters
    ----------
    data : list
        All the values from every column
    file_name
        The name of the file to sore these "global" ranks
    """
    if not os.path.isfile('cache/global_ranks/' + file_name + '.pkl'):
        print("Generating ranks for ", file_name)
        ranks = unix_sort_ranks(set(data), file_name)
        with open('cache/global_ranks/' + file_name + '.pkl', 'wb') as output:
            pickle.dump(ranks, output, pickle.HIGHEST_PROTOCOL)


def unix_sort_ranks(corpus: set, file_name: str):
    """
    Function that takes a corpus sorts it with the unix sort -n command and generates the global ranks
    for each value in the corpus.

    Parameters
    ----------
    corpus: set
        The corpus (all the unique values from every column)
    file_name : str
        The name of the file to sore these "global" ranks

    Returns
    -------
    dict
        The ranks in the form of k: value, v: the rank of the value
    """
    create_folder("./cache/sorts/" + file_name)
    with open("./cache/sorts/" + file_name + "/unsorted_file.txt", 'w') as out:
        for var in corpus:
            print(str(var), file=out)

    sort_env = os.environ.copy()
    sort_env['LC_ALL'] = 'C'

    with open('cache/sorts/' + file_name + '/sorted_file.txt', 'w') as f:
        subprocess.call(['sort', '-n', 'cache/sorts/' + file_name + '/unsorted_file.txt'], stdout=f, env=sort_env)

    rank = 1
    ranks = []

    with open('./cache/sorts/' + file_name + '/sorted_file.txt', 'r') as f:
        txt = f.read()
        for var in txt.splitlines():
            ranks.append((convert_data_type(var.replace('\n', '')), rank))
            rank = rank + 1

    shutil.rmtree('./cache/sorts/' + file_name)
    os.mkdir('./cache/sorts/' + file_name)

    return dict(ranks)


def calc_chunksize(n_workers: int, len_iterable: int, factor: int = 4):
    """
    Calculate chunksize argument for Pool-methods.

    Resembles source-code within `multiprocessing.pool.Pool._map_async`.
    """
    chunksize, extra = divmod(len_iterable, n_workers * factor)
    if extra:
        chunksize += 1
    return chunksize


def create_cache_dirs():
    """ Create the directories needed for the correlation clustering algorithm"""
    if not os.path.exists('cache'):
        os.makedirs('cache')
    if not os.path.exists('cache/global_ranks'):
        os.makedirs('cache/global_ranks')
    if not os.path.exists('cache/sorts'):
        os.makedirs('cache/sorts')
