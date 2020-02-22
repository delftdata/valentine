import sys
import time

import numpy as np
from algorithms.sem_prop.knowledgerepr import api_gindex

"""
Compile g-index library with:
g++ -dynamiclib -o graph_index.so GraphIndex.cpp
"""

pairs_src = []
pairs_tgt = []


def path_benchmark(max_hops=0):
    measurements = []
    for src, tgt in zip(pairs_src, pairs_tgt):
        s = time.time()
        api_gindex.path_query(src, tgt, 1, max_hops)
        e = time.time()
        measurements.append((e-s))
    return measurements


def get_stats(list_measurements):
    m = np.asarray(list_measurements)
    avg = np.average(m)
    median = np.percentile(m, 50)
    p95 = np.percentile(m, 95)
    p99 = np.percentile(m, 99)
    return avg, median, p95, p99


def main(path):
    print("Deserialising graph...")
    api_gindex.deserialize_graph(path)
    print("Deserialising graph...OK")
    nnodes = api_gindex.get_num_nodes()
    print("Num nodes: " + str(nnodes))
    nedges = api_gindex.get_num_edges()
    print("Num edges: " + str(nnodes))

    measurements_mh2 = path_benchmark(max_hops=2)
    measurements_mh5 = path_benchmark(max_hops=5)

    stats_mh2 = get_stats(measurements_mh2)
    stats_mh5 = get_stats(measurements_mh5)

    print("graph: " + str(path))
    print("max_hops 2")
    print(str(stats_mh2))
    print("max_hops 5")
    print(str(stats_mh5))


if __name__ == "__main__":
    print("Benchmark G-Index")

    path = sys.argv[0]

    main(path)
