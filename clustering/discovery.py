from tqdm import tqdm
import numpy as np
from clustering import emd_utils


def compute_cutoff_threshold(C, threshold):
    t = dict()
    t['e'] = threshold
    t['c'] = 0
    C.append(t)
    C = sorted(C, key=lambda i: i['e'])
    cutoff = 0
    gap = 0.0
    i = 0
    while i < len(C)-1 and C[i + 1]['e'] <= threshold:
        if gap < (C[i + 1]['e'] - C[i]['e']):
            gap = C[i + 1]['e'] - C[i]['e']
            cutoff = C[i]['e']
        i += 1

    return cutoff


def get_neighbors(C, cutoff):
    return [i['c'] for i in C if i['e'] <= cutoff]


def compute_distribution_clusters(columns, threshold, quantile):
    graph = dict()
    A = dict()

    for i in tqdm(range(0, len(columns))):
        name_i = columns[i].get_long_name()
        for j in tqdm(range(i + 1, len(columns))):
            name_j = columns[j].get_long_name()
            e = emd_utils.quantile_emd(columns[i], columns[j], quantile)

            item_j = dict()
            item_j['e'] = e
            item_j['c'] = name_j
            if columns[i] not in A:
                A[columns[i]] = []
            A[columns[i]].append(item_j)

            item_i = dict()
            item_i['e'] = e
            item_i['c'] = name_i
            if columns[j] not in A:
                A[columns[j]] = []
            A[columns[j]].append(item_i)

        graph[name_i] = set()

    print("Compute cutoff threshold and neighbors")
    for i in range(len(columns)):
        name_i = columns[i].get_long_name()
        print(name_i)
        theta = compute_cutoff_threshold(A[columns[i]], threshold)
        print(A[columns[i]])
        print(theta)
        print('\n')
        Nc = get_neighbors(A[columns[i]], theta)
        graph[columns[i].get_long_name()].update(Nc)

    return graph


def bfs(graph, start):
    visited, queue = set(), [start]
    while queue:
        vertex = queue.pop(0)
        if vertex not in visited:
            visited.add(vertex)
            queue.extend(graph[vertex] - visited)
    return visited


def get_connected_components(distribution_clusters):
    connected_components = list()
    for i in list(distribution_clusters.keys()):
        components = list(bfs(distribution_clusters, i))

        if len(components) > 1:
            if len(connected_components) == 0:
                connected_components.append(components)
            elif not is_in_list(components, connected_components):
                connected_components.append(components)

    return connected_components


def is_in_list(small_list, big_list):
    for lst in big_list:
        result = all(elm in lst for elm in small_list)
        if result:
            return result
    return False


def compute_attributes(columns, DC, theta, quantile):
    GA = dict()
    I = dict()
    E = np.zeros((len(DC), len(DC)))

    for i in tqdm(range(len(DC))):
        c_i = next(filter(lambda x: x.get_long_name() == DC[i], columns))
        for j in tqdm(range(i + 1, len(DC))):
            c_j = next(filter(lambda x: x.get_long_name() == DC[j], columns))
            e = emd_utils.intersection_emd(c_i, c_j, quantile)

            item_j = dict()
            item_j['e'] = e
            item_j['c'] = c_j.get_long_name()
            if c_i not in I:
                I[c_i] = []
            I[c_i].append(item_j)

            item_i = dict()
            item_i['e'] = e
            item_i['c'] = c_i.get_long_name()
            if c_j not in I:
                I[c_j] = []
            I[c_j].append(item_i)

        print(I[c_i])
        cutoff_i = compute_cutoff_threshold(I[c_i], theta)
        print(cutoff_i)
        Nc = get_neighbors(I[c_i], cutoff_i)
        print(Nc)
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


