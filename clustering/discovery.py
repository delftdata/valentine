import tqdm as tqdm
import numpy as np
from clustering import Emd


def compute_cutoff_threshold(C, threshold):
    t = {}
    t['e'] = threshold + 0.001
    t['c'] = 0
    C.append(t)
    C = sorted(C, key=lambda i: i['e'])
    cutoff = 0
    gap = 0.0
    i = 0
    while C[i + 1]['e'] <= threshold:
        if gap < (C[i + 1]['e'] - C[i]['e']):
            gap = C[i + 1]['e'] - C[i]['e']
            cutoff = C[i]['e']
        i += 1

    return cutoff


def get_neighbors(C, cutoff):
    return [i['c'] for i in C if i['e'] <= cutoff]


def compute_distribution_clusters(data, columns, threshold):
    graph = {}
    A = {}

    for i in range(0, len(columns)):
        print(i)
        for j in range(i + 1, len(columns)):
            print('\t'+str(j))
            e = Emd.quantile_emd(data[columns[i]], data[columns[j]])
            item_j = {}
            item_j['e'] = e
            item_j['c'] = columns[j]
            if columns[i] not in A:
                A[columns[i]] = []
            A[columns[i]].append(item_j)

            item_i = {}
            item_i['e'] = e
            item_i['c'] = columns[i]
            if columns[j] not in A:
                A[columns[j]] = []
            A[columns[j]].append(item_i)
        graph[columns[i]] = set()

    for i in range(len(columns)):
        theta = compute_cutoff_threshold(A[columns[i]], threshold)
        Nc = get_neighbors(A[columns[i]], theta)
        graph[columns[i]].update(Nc)

    return graph


def bfs(graph, start):
        visited, queue = set(), [start]
        while queue:
            vertex = queue.pop(0)
            if vertex not in visited:
                visited.add(vertex)
                queue.extend(graph[vertex] - visited)
        return visited


def compute_attributes(data, DC, theta):
        GA = {}
        I = {}
        E = np.zeros((len(DC), len(DC)))
        M = []

        for i in tqdm(range(len(DC))):
            for j in tqdm(range(i + 1, len(DC))):
                e = Emd.intersection_emd(data[DC[i]], data[DC[j]])
                item_j = {}
                item_j['e'] = e
                item_j['c'] = DC[j]

                if DC[i] not in I:
                    I[DC[i]] = []
                I[DC[i]].append(item_j)

                item_i = {}
                item_i['e'] = e
                item_i['c'] = DC[i]

                if DC[j] not in I:
                    I[DC[j]] = []
                I[DC[j]].append(item_i)
            print(I[DC[i]])
            cutoff_i = compute_cutoff_threshold(I[DC[i]], theta)
            print(cutoff_i)
            Nc = get_neighbors(I[DC[i]], cutoff_i)
            print(Nc)
            for Cj in Nc:
                E[i][DC.index(Cj)] = 1
            GA[DC[i]] = set()
        M = E + np.dot(E, E)
        for i in range(len(DC)):
            for j in range(len(DC)):
                if M[i][j] == 0:
                    GA[DC[i]].update(['-' + DC[j]])
                else:
                    GA[DC[i]].update([DC[j]])
        return GA
