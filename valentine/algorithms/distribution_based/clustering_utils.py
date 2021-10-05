import pickle
import os
import subprocess
from functools import lru_cache
from typing import List, Tuple

from .column_model import CorrelationClusteringColumn
from .emd_utils import intersection_emd, quantile_emd
from .quantile_histogram import QuantileHistogram
from ...data_sources.base_column import BaseColumn
from ...utils.utils import convert_data_type


def compute_cutoff_threshold(matrix_c: list, threshold: float):
    """
    Algorithm 1 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]
    This algorithm computes the threshold of a column that determines if any other column is to be considered
    its neighbour.

    Parameters
    ---------
    matrix_c : list
        A list containing dicts of EMD/ColumnName pairs
    threshold : float
        The conservative global EMD cutoff threshold described in [1]

    Returns
    -------
    float
        The cutoff threshold of the input column
    """
    matrix_c.append({'e': threshold, 'c': 0})
    matrix_c = sorted(matrix_c, key=lambda k: k['e'])
    cutoff = 0.0
    gap = 0.0
    i = 0
    while i < len(matrix_c) - 1 and matrix_c[i + 1]['e'] <= threshold:
        if gap < (matrix_c[i + 1]['e'] - matrix_c[i]['e']):
            gap = matrix_c[i + 1]['e'] - matrix_c[i]['e']
            cutoff = matrix_c[i]['e']
        i += 1
    return cutoff


def column_combinations(columns: List[Tuple],
                        quantiles: int,
                        tmp_folder_path: str,
                        intersection: bool = False):
    """
    All the unique combinations between all the columns

    Parameters
    ---------
    columns : list
        A list that contains all the column names
    quantiles : int
        The number of quantiles that the histograms are split on
    tmp_folder_path: str
        The path of the temporary folder that will serve as a cache for the run
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
        _, table_guid_i, _, column_guid_i = columns[c_i]
        c_j = c_i + 1
        while c_j < c:
            _, table_guid_j, _, column_guid_j = columns[c_j]
            if table_guid_i != table_guid_j:
                yield (columns[c_i], columns[c_j]), quantiles, intersection, tmp_folder_path
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
    name_i, name_j, k, quantile, intersection, tmp_folder_path = unwrap_process_input_tuple(tup)
    tn_i, _, cn_i, _ = name_i
    tn_j, _, cn_j, _ = name_j
    c1 = read_from_cache(f'{make_filename_safe(tn_i)}_{make_filename_safe(cn_i)}', tmp_folder_path)
    c2 = read_from_cache(f'{make_filename_safe(tn_j)}_{make_filename_safe(cn_j)}', tmp_folder_path)
    if intersection:
        return k, intersection_emd(c1, c2, tmp_folder_path, quantile)
    else:
        return k, quantile_emd(c1, c2, quantile)


@lru_cache(maxsize=32)
def read_from_cache(file_name: str, tmp_folder_path):
    """
    Function that reads from a pickle file lru cache a column after pre-processing

    Parameters
    ----------
    file_name: str
        The file name that contains the
    tmp_folder_path: str
        The path of the temporary folder that will serve as a cache for the run

    Returns
    -------
    CorrelationClusteringColumn
        The preprocessed column
    """
    return get_column_from_store(file_name, tmp_folder_path)


def unwrap_process_input_tuple(tup: tuple):
    """
    Helper function that unwraps a tuple to its components and creates a unique key for the column combination

    Parameters
    ---------
    tup : tuple
        the tuple to unwrap
    """
    names, quantile, intersection, tmp_folder_path = tup
    name_i, name_j = names
    k = (name_i, name_j)
    return name_i, name_j, k, quantile, intersection, tmp_folder_path


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
    column_name, column_uid, data, source_name, source_guid, quantiles, tmp_folder_path = tup
    column = CorrelationClusteringColumn(column_name, column_uid, data, source_name, source_guid, quantiles, tmp_folder_path)
    if column.size > 0:
        column.quantile_histogram = QuantileHistogram(column.long_name, column.ranks, column.size, quantiles)
    with open(os.path.join(tmp_folder_path,
                           f'{make_filename_safe(column.table_name)}_{make_filename_safe(column.name)}.pkl'),
              'wb') as output:
        pickle.dump(column, output, pickle.HIGHEST_PROTOCOL)
    del column


def parallel_cutoff_threshold(tup: tuple):
    """
    Process the cutoff threshold in parallel for each column

    Parameters
    ---------
    tup : tuple
        tuple containing the information of the column to be processed
    """
    matrix_a, column, threshold = tup
    name_i = column.long_name
    theta = compute_cutoff_threshold(matrix_a[name_i], threshold)
    n_c = [(name_i, i['c']) for i in matrix_a[name_i] if i['e'] <= theta]
    return n_c


def ingestion_column_generator(columns: List[BaseColumn],
                               table_name: str,
                               table_guid: object,
                               quantiles: int,
                               tmp_folder_path: str):
    """
    Generator of incoming pandas dataframe columns
    """
    for column in columns:
        yield column.name, column.unique_identifier, column.data, table_name, table_guid, quantiles, tmp_folder_path


def cuttoff_column_generator(matrix_a: dict,
                             columns: List[Tuple[str, str, str, str]],
                             threshold: float,
                             tmp_folder_path: str):
    """
    Generator of columns for the cutoff threshold computation
    """
    for column_name in columns:
        tn_i, _, cn_i, _ = column_name
        f_name = f'{make_filename_safe(tn_i)}_{make_filename_safe(cn_i)}'
        column = get_column_from_store(f_name, tmp_folder_path)
        yield matrix_a, column, threshold


def generate_global_ranks(data: list, tmp_folder_path: str):
    """
    Function that creates a pickle file with the global ranks of all the values inside the database.

    Parameters
    ----------
    data : list
        All the values from every column
    tmp_folder_path: str
        The path of the temporary folder that will serve as a cache for the run
    """
    ranks = unix_sort_ranks(set(data), tmp_folder_path)
    with open(os.path.join(tmp_folder_path, 'ranks.pkl'), 'wb') as output:
        pickle.dump(ranks, output, pickle.HIGHEST_PROTOCOL)


def unix_sort_ranks(corpus: set,
                    tmp_folder_path: str):
    """
    Function that takes a corpus sorts it with the unix sort -n command and generates the global ranks
    for each value in the corpus.

    Parameters
    ----------
    corpus: set
        The corpus (all the unique values from every column)
    tmp_folder_path: str
        The path of the temporary folder that will serve as a cache for the run

    Returns
    -------
    dict
        The ranks in the form of k: value, v: the rank of the value
    """
    unsorted_file_path = os.path.join(tmp_folder_path, 'unsorted_file.txt')
    sorted_file_path = os.path.join(tmp_folder_path, 'sorted_file.txt')

    with open(unsorted_file_path, 'w') as out:
        for var in corpus:
            print(str(var), file=out)

    with open(sorted_file_path, 'w') as f:
        if os.name == 'nt':
            subprocess.call(['sort',
                             unsorted_file_path],
                            stdout=f)
        else:
            sort_env = os.environ.copy()
            sort_env['LC_ALL'] = 'C'
            subprocess.call(['sort', '-n',
                             unsorted_file_path],
                            stdout=f, env=sort_env)

    rank = 1
    ranks = []

    with open(sorted_file_path, 'r') as f:
        txt = f.read()
        for var in txt.splitlines():
            ranks.append((convert_data_type(var.replace('\n', '')), rank))
            rank = rank + 1

    return dict(ranks)


def get_column_from_store(file_name: str, tmp_folder_path: str):
    file_path = os.path.join(tmp_folder_path, f'{file_name}.pkl')
    if os.path.getsize(file_path) > 0:
        with open(file_path, 'rb') as pkl_file:
            data = pickle.load(pkl_file)
    return data


def make_filename_safe(file_name: str):
    return "".join([c for c in file_name if c.isalpha() or c.isdigit() or c == ' ']).rstrip()
