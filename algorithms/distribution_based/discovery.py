from ast import literal_eval

import numpy as np
import networkx as nx
import pulp as plp
from tqdm import tqdm
from multiprocessing import Pool

from algorithms.distribution_based.clustering_utils import transform_dict, process_emd, column_combinations, \
    parallel_cutoff_threshold, cuttoff_column_generator, compute_cutoff_threshold, calc_chunksize


def compute_distribution_clusters(columns: list, dataset_name: str, threshold: float, pool: Pool,
                                  chunk_size: int = None, quantiles: int = 256):
    """
    Algorithm 2 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]. This
    algorithm captures which columns contain data with similar distributions based on the EMD distance metric.

    Parameters
    ---------
    columns : list(str)
        The columns of the database
    dataset_name : str
        Other name of the dataset
    threshold : float
        The conservative global EMD cutoff threshold described in [1]
    pool: multiprocessing.Pool
        The process pool that will be used in the pre-processing of the table's columns
    chunk_size : int, optional
        The number of chunks of each job process (default let the framework decide)
    quantiles : int, optional
        The number of quantiles that the histograms are split on (default is 256)

    Returns
    -------
    list(list(str))
        A list that contains the distribution clusters that contain the column names in the cluster
    """
    combinations = list(column_combinations(columns, dataset_name, quantiles, intersection=False))

    total = len(combinations)

    if chunk_size is None:
        chunk_size = int(calc_chunksize(pool._processes, total))

    A: dict = transform_dict(dict(tqdm(pool.imap_unordered(process_emd, combinations, chunksize=chunk_size),
                                       total=total)))

    edges_per_column = list(pool.map(parallel_cutoff_threshold, list(cuttoff_column_generator(A, columns, dataset_name,
                                                                                              threshold))))

    graph = create_graph(columns, edges_per_column)

    connected_components = list(nx.connected_components(graph))

    return connected_components


def compute_attributes(DC: list, dataset_name: str, threshold: float, pool: Pool, chunk_size: int = None,
                       quantiles: int = 256):
    """
    Algorithm 3 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al.[1]
    This algorithm creates the attribute graph of the distribution clusters computed in algorithm 2.

    Parameters
    ---------
    DC : list(str)
        The distribution clusters computed in algorithm 2
    dataset_name : str
        Other name of the dataset
    threshold : float
        The conservative global EMD cutoff threshold described in [1]
    pool: multiprocessing.Pool
        The process pool that will be used in the pre-processing of the table's columns
    chunk_size : int, optional
        The number of chunks of each job process (default let the framework decide)
    quantiles : int, optional
        The number of quantiles that the histograms are split on (default is 256)

    Returns
    -------
    dict
        A dictionary that contains the attribute graph of the distribution clusters
    """

    combinations = list(column_combinations(DC, dataset_name, quantiles, intersection=True))

    total = len(combinations)

    if chunk_size is None:
        chunk_size = int(calc_chunksize(pool._processes, total))

    I = transform_dict(dict(tqdm(pool.imap_unordered(process_emd, combinations, chunksize=chunk_size), total=total)))

    GA = dict()
    E = np.zeros((len(DC), len(DC)))

    for i in range(len(DC)):
        name_i = DC[i]

        cutoff_i = compute_cutoff_threshold(I[name_i], threshold)

        Nc = [i['c'] for i in I[name_i] if i['e'] <= cutoff_i]

        for Cj in Nc:
            E[i][DC.index(Cj)] = 1
        GA[DC[i]] = dict()

    M = E + np.dot(E, E)
    for i in range(len(DC)):
        for j in range(len(DC)):
            if M[i][j] == 0:
                GA[DC[i]][DC[j]] = -1
            else:
                GA[DC[i]][DC[j]] = 1

    return GA


def correlation_clustering_pulp(vertexes: list, edges: dict):
    """
    The LP solver used to perform the correlation clustering

    Parameters
    ----------
    vertexes : list
        The vertices of the graph
    edges : dict
        The edges of the graph

    Returns
    -------
    dict
        The clusters
    """
    opt_model = plp.LpProblem(name="MIP_Model", sense=plp.LpMinimize)

    set_u = vertexes
    set_v = vertexes

    x_vars = {(i, j): plp.LpVariable(cat=plp.LpInteger, lowBound=0, upBound=1, name="({0},{1})"
                                     .format(str(i).replace(" ", "__WHITESPACE__").replace("-", "__DASH__"),
                                             str(j).replace(" ", "__WHITESPACE__").replace("-", "__DASH__")))
              for i in set_u for j in set_v}

    sum1 = plp.lpSum(x_vars[i, j] for i in set_u for j in set_v if edges[i][j] == 1)
    sum2 = plp.lpSum(1 - x_vars[i, j] for i in set_u for j in set_v if edges[i][j] == -1)

    opt_model.setObjective(sum1 + sum2)

    opt_model.solve()

    result = dict()

    for v in opt_model.variables():
        result[literal_eval(v.name.replace("__WHITESPACE__", " ").replace("__DASH__", "-"))] = v.varValue

    return result


def process_correlation_clustering_result(results: list, columns: list):
    """
    Function that takes the output of the correlation clustering and returns the connected components

    Parameters
    ----------
    results : list(dict)
        The clusters
    columns : list
        The columns of the database

    Returns
    -------
    list
        The connected components "matches"
    """
    clusters = []
    for cluster in results:
        clusters.extend([k for (k, v) in cluster.items() if v == 0])
    edges_per_column = []
    for match in clusters:
        m1, m2 = match
        edges_per_column.append([(m1, m2)])
    graph = create_graph(columns, edges_per_column)
    connected_components = list(nx.connected_components(graph))
    return connected_components


def create_graph(nodes: list, edges_per_column: list):
    """
    Simple function that creates a graph give the vertices and their corresponding edges

    Parameters
    ----------
    nodes : list
        The nodes of the graph (columns in our case)
    edges_per_column : list
        The edges of the graph (which columns connect with which)

    Returns
    -------
    nx.Graph
        The graph
    """
    graph = nx.Graph()
    for node in nodes:
        graph.add_node(node)
    for edges in edges_per_column:
        graph.add_edges_from(edges)
    return graph
