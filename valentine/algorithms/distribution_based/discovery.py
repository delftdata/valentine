from ast import literal_eval
from typing import List, Tuple

import numpy as np
import networkx as nx
import pulp as plp
from multiprocessing import Pool

from pulp import PULP_CBC_CMD

from .clustering_utils import column_combinations, transform_dict, process_emd, parallel_cutoff_threshold, \
    cuttoff_column_generator, compute_cutoff_threshold


def compute_distribution_clusters(columns: List[Tuple[str, str, str, str]], threshold: float, uuid: str,
                                  quantiles: int = 256):
    """
    Algorithm 2 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]. This
    algorithm captures which columns contain data with similar distributions based on the EMD distance metric.

    Parameters
    ---------
    columns : list(str)
        The column names of the database
    threshold : float
        The conservative global EMD cutoff threshold described in [1]
    quantiles : int, optional
        The number of quantiles that the histograms are split on (default is 256)
    uuid:
        The unique identifier of the run

    Returns
    -------
    list(list(str))
        A list that contains the distribution clusters that contain the column names in the cluster
    """
    combinations = column_combinations(columns, quantiles, uuid, intersection=False)

    matrix_a: dict = transform_dict({k: v for k, v in [process_emd(cmb) for cmb in combinations]})

    ctf_clm_gnr = cuttoff_column_generator(matrix_a, columns, threshold, uuid)

    edges_per_column: list = [parallel_cutoff_threshold(tup) for tup in ctf_clm_gnr]

    graph = create_graph(columns, edges_per_column)

    connected_components = list(nx.connected_components(graph))

    return connected_components


def compute_distribution_clusters_parallel(columns: list, threshold: float, pool: Pool, uuid: str,
                                           quantiles: int = 256):
    """
    Algorithm 2 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]. This
    algorithm captures which columns contain data with similar distributions based on the EMD distance metric.

    Parameters
    ---------
    columns : list(str)
        The column names of the database
    threshold : float
        The conservative global EMD cutoff threshold described in [1]
    pool: multiprocessing.Pool
        The process pool that will be used in the pre-processing of the table's columns
    quantiles : int, optional
        The number of quantiles that the histograms are split on (default is 256)
    uuid:
        The unique identifier of the run

    Returns
    -------
    list(list(str))
        A list that contains the distribution clusters that contain the column names in the cluster
    """
    combinations = column_combinations(columns, quantiles, uuid, intersection=False)
    matrix_a: dict = transform_dict(dict(pool.map(process_emd, combinations, chunksize=1)))

    edges_per_column = list(pool.map(parallel_cutoff_threshold, cuttoff_column_generator(matrix_a, columns, threshold,
                                                                                         uuid), chunksize=1))

    graph = create_graph(columns, edges_per_column)

    connected_components = list(nx.connected_components(graph))

    return connected_components


def compute_attributes(distribution_clusters: list, threshold: float, uuid: str,
                       quantiles: int = 256):
    """
    Algorithm 3 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al.[1]
    This algorithm creates the attribute graph of the distribution clusters computed in algorithm 2.

    Parameters
    ---------
    distribution_clusters : list(str)
        The distribution clusters computed in algorithm 2
    threshold : float
        The conservative global EMD cutoff threshold described in [1]
    quantiles : int, optional
        The number of quantiles that the histograms are split on (default is 256)
    uuid:
        The unique identifier of the run

    Returns
    -------
    dict
        A dictionary that contains the attribute graph of the distribution clusters
    """

    combinations = column_combinations(distribution_clusters, quantiles, uuid, intersection=True)

    matrix_i: dict = transform_dict({k: v for k, v in [process_emd(cmb) for cmb in combinations]})

    return get_attribute_graph(distribution_clusters, matrix_i, threshold)


def compute_attributes_parallel(distribution_clusters: list, threshold: float, pool: Pool, uuid: str,
                                quantiles: int = 256):
    """
    Algorithm 3 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al.[1]
    This algorithm creates the attribute graph of the distribution clusters computed in algorithm 2.

    Parameters
    ---------
    distribution_clusters : list(str)
        The distribution clusters computed in algorithm 2
    threshold : float
        The conservative global EMD cutoff threshold described in [1]
    pool: multiprocessing.Pool
        The process pool that will be used in the pre-processing of the table's columns
    quantiles : int, optional
        The number of quantiles that the histograms are split on (default is 256)
    uuid:
        The unique identifier of the run

    Returns
    -------
    dict
        A dictionary that contains the attribute graph of the distribution clusters
    """

    combinations = column_combinations(distribution_clusters, quantiles, uuid, intersection=True)

    matrix_i = transform_dict(dict(pool.map(process_emd, combinations, chunksize=1)))

    return get_attribute_graph(distribution_clusters, matrix_i, threshold)


def get_attribute_graph(distribution_clusters: list, matrix_i: dict, threshold: float):
    g_a = dict()
    matrix_e = np.zeros((len(distribution_clusters), len(distribution_clusters)))

    for i, cluster in enumerate(distribution_clusters):
        name_i = cluster

        cutoff_i = compute_cutoff_threshold(matrix_i[name_i], threshold)

        n_c = [i['c'] for i in matrix_i[name_i] if i['e'] <= cutoff_i]

        for c_j in n_c:
            matrix_e[i][distribution_clusters.index(c_j)] = 1
        g_a[cluster] = dict()

    matrix_m = matrix_e + np.dot(matrix_e, matrix_e)
    for i, cluster_i in enumerate(distribution_clusters):
        for j, cluster_j in enumerate(distribution_clusters):
            if matrix_m[i][j] == 0:
                g_a[cluster_i][cluster_j] = -1
            else:
                g_a[cluster_i][cluster_j] = 1
    return g_a


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
                                     .format(str(i)
                                             .replace(" ", "__WHITESPACE__")
                                             .replace("-", "__DASH__")
                                             .replace(">", "__GREATER__")
                                             .replace("/", "__BACKSLASH__"),
                                             str(j)
                                             .replace(" ", "__WHITESPACE__")
                                             .replace("-", "__DASH__")
                                             .replace(">", "__GREATER__")
                                             .replace("/", "__BACKSLASH__")))
              for i in set_u for j in set_v}

    sum1 = plp.lpSum(x_vars[i, j] for i in set_u for j in set_v if edges[i][j] == 1)
    sum2 = plp.lpSum(1 - x_vars[i, j] for i in set_u for j in set_v if edges[i][j] == -1)

    opt_model.setObjective(sum1 + sum2)

    opt_model.solve(PULP_CBC_CMD(msg=False))

    result = dict()

    for v in opt_model.variables():
        result[literal_eval(v.name
                            .replace("__WHITESPACE__", " ")
                            .replace("__DASH__", "-")
                            .replace("__GREATER__", ">")
                            .replace("__BACKSLASH__", "/"))] = v.varValue

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
