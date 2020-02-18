import time

from algorithms.sem_prop.knowledgerepr.lite_graph import EdgeType
from algorithms.sem_prop.knowledgerepr.lite_graph import LiteGraph

if __name__ == "__main__":
    g = LiteGraph()

    num_nodes = 2000

    for i in range(num_nodes):
        g.add_node(i)

    # complete graph

    for i in range(num_nodes):
        for j in range(num_nodes):
            if i != j:
                g.add_undirected_edge(i, j, EdgeType.SCHEMA_SIM)

    for i in range(num_nodes):
        for j in range(num_nodes):
            if i != j:
                g.add_undirected_edge(i, j, EdgeType.CONTENT_SIM)

    for i in range(num_nodes):
        for j in range(num_nodes):
            if i != j:
                g.add_undirected_edge(i, j, EdgeType.PKFK)

    print("Nodes: " + str(g._node_count))
    print("Edges: " + str(g._edge_count))

    time.sleep(10)