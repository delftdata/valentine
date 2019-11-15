import numpy as np
from multiprocessing.pool import Pool
from sortedcontainers import SortedList
import psutil
import networkx as nx

from clustering.column_model import Column
from clustering_scale.scale_utils import transform_dict, process_intersection_emd, process_emd, column_combinations


num_of_cpus = psutil.cpu_count(logical=True)  # The amount of cpus available in the VM (including logical)


def compute_cutoff_threshold(C: SortedList, threshold):
    """
    Algorithm 1 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]
    This algorithm computes the threshold of a column that determines if any other column is to be considered 
    its neighbour.
    :param C: A sorted list containing dicts of EMD/ColumnName pairs (sorted in increasing order based on the EMD value)
    :param threshold: The conservative global EMD cutoff threshold described in [1]
    :return: Returns the cutoff threshold of the input column
    """
    C.add({'e': threshold, 'c': 0})
    cutoff = 0
    gap = 0.0
    i = 0
    while i < len(C)-1 and C[i + 1]['e'] <= threshold:
        if gap < (C[i + 1]['e'] - C[i]['e']):
            gap = C[i + 1]['e'] - C[i]['e']
            cutoff = C[i]['e']
        i += 1
    C.remove({'e': threshold, 'c': 0})
    return cutoff


def compute_distribution_clusters(columns, threshold, quantile):
    """
    Algorithm 2 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]. This
    algorithm captures which columns contain data with similar distributions based on the EMD distance metric.
    The present version is tuned for vertical scaling using as many parallel processes as the available number
    of cpu cores.
    :param columns: The columns of the database
    :param threshold: The conservative global EMD cutoff threshold described in [1]
    :param quantile: The number of quantiles required in the quantile-EMD's metric calculation
    :return: Returns the distribution clusters of the database
    """
    p = Pool(num_of_cpus)  # Create a pool of processes

    A = transform_dict(dict(p.map(process_emd, column_combinations(columns, quantile))))  # Compute EMD in parallel

    graph = nx.Graph()

    for i in range(len(columns)):
        name_i = columns[i].get_long_name()
        theta = compute_cutoff_threshold(A[name_i], threshold)
        Nc = [(name_i, i['c']) for i in A[name_i] if i['e'] <= theta]
        graph.add_edges_from(Nc)

    connected_components = list(nx.connected_components(graph))

    return connected_components


def compute_attributes(columns, DC, theta, quantile):
    """
    Part of Algorithm 4 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al.[1]
    This algorithm creates the attribute graph of the distribution clusters computed in algorithm 2.
    The present version is tuned for vertical scaling using as many parallel processes as the available number
    of cpu cores.
    :param columns: The columns of the database
    :param DC: The distribution clusters computed in algorithm 2
    :param theta: The conservative global EMD cutoff threshold described in [1]
    :param quantile: The number of quantiles required in the quantile-intersection EMD's metric calculation
    :return: The attribute graph of the distribution clusters
    """
    p = Pool(num_of_cpus)

    I = transform_dict(dict(p.map(process_intersection_emd, column_combinations(columns, quantile))))

    GA = dict()
    E = np.zeros((len(DC), len(DC)))

    for i in range(len(DC)):
        c_i: Column = next(filter(lambda x: x.get_long_name() == DC[i], columns))
        name_i = c_i.get_long_name()

        cutoff_i = compute_cutoff_threshold(I[name_i], theta)

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


########################################################################################################################
"""The functions bellow are exact copies of those in clustering.discovery (date: 15/11/2019)"""


def correlation_clustering_gurobi(vertexes, edges):
    import gurobipy as grb

    opt_model = grb.Model(name="MIP Model")

    set_u = vertexes
    set_v = vertexes
    set_w = vertexes

    x_vars = {(i, j): opt_model.addVar(vtype=grb.GRB.INTEGER, lb=0, ub=1, name="{0}-{1}".format(i, j))
              for i in set_u for j in set_v}
    constraints = {(i, j, k): opt_model.addConstr(lhs=x_vars[i, k],
                                                  sense=grb.GRB.LESS_EQUAL,
                                                  rhs=x_vars[i, j] + x_vars[j, k],
                                                  name="constraint_{0}_{1}_{2}".format(i, j, k))
                   for i in set_u for j in set_v for k in set_w}

    sum1 = grb.quicksum(x_vars[i, j] for i in set_u for j in set_v if edges[i][j] == 1)
    sum2 = grb.quicksum(1 - x_vars[i, j] for i in set_u for j in set_v if edges[i][j] == -1)

    opt_model.ModelSense = grb.GRB.MINIMIZE
    opt_model.setObjective(sum1 + sum2)

    opt_model.optimize()

    result = dict()

    for v in opt_model.getVars():
        result[v.varName] = v.x

    return result


def correlation_clustering_pulp(vertexes, edges):
    import pulp as plp

    opt_model = plp.LpProblem(name="MIP Model")

    set_u = vertexes
    set_v = vertexes
    set_w = vertexes

    x_vars = {(i, j): plp.LpVariable(cat=plp.LpInteger, lowBound=0, upBound=1, name="{0}--{1}".format(i, j))
              for i in set_u for j in set_v}

    constraints = {(i, j, k): plp.LpConstraint(e=x_vars[i, k],
                                               sense=plp.LpConstraintLE,
                                               rhs=x_vars[i, j] + x_vars[j, k],
                                               name="constraint_{0}_{1}_{2}".format(i, j, k))
                   for i in set_u for j in set_v for k in set_w}

    sum1 = plp.lpSum(x_vars[i, j] for i in set_u for j in set_v if edges[i][j] == 1)
    sum2 = plp.lpSum(1 - x_vars[i, j] for i in set_u for j in set_v if edges[i][j] == -1)

    opt_model.sense = plp.LpMinimize
    opt_model.setObjective(sum1 + sum2)

    opt_model.solve()

    result = dict()

    for v in opt_model.variables():
        result[v.name] = v.varValue

    return result


def process_correlation_clustering_result(result):
    clusters = [k for (k, v) in result.items() if v == 0]
    return np.extract(list(map(lambda x: False if x.split('__')[0] == x.split('__')[1] else True, clusters)), clusters)
