import networkx as nx

from . import Policy
from .node_pair import NodePair


class PropagationGraph:
    """
    Class for constructing a Propagation Graph from two input graphs.
    """

    def __init__(self, graph1, graph2, policy):
        self.graph1 = graph1
        self.graph2 = graph2
        # policy corresponds to the policy used to compute propagation coefficients
        self.policy = policy

    @staticmethod
    def __inverse_label_values(labels, m=1.0):
        for key, value in labels.items():
            labels[key] = m / value

    def __add_propagation_edges(self, c_graph, p_graph, node, case_in):

        if case_in:
            edges = c_graph.in_edges(node)
        else:
            edges = c_graph.out_edges(node)

        labels = {}
        for e in edges:
            edge_data = c_graph.get_edge_data(e[0], e[1])

            label = edge_data.get("label")

            if label in labels:
                labels[label] += 1.0
            else:
                labels[label] = 1.0

        self.__inverse_label_values(labels)

        for e in edges:
            edge_data = c_graph.get_edge_data(e[0], e[1])

            label = edge_data.get("label")

            if case_in:
                p_graph.add_edge(e[1], e[0], weight=labels[label])
            else:
                p_graph.add_edge(e[0], e[1], weight=labels[label])

        return p_graph

    def __construct_connectivity_graph(self):
        # initialize the connectivity graph
        c_g = nx.DiGraph()

        for e1 in self.graph1.edges():
            for e2 in self.graph2.edges():
                l1 = self.graph1.get_edge_data(e1[0], e1[1])
                l2 = self.graph2.get_edge_data(e2[0], e2[1])
                # if the labels of both edges are equal then add a new pair of nodes in p_g
                if l1.get("label") == l2.get("label"):
                    np1 = NodePair(e1[0], e2[0])
                    c_g.add_node(np1)
                    np2 = NodePair(e1[1], e2[1])
                    c_g.add_node(np2)
                    c_g.add_edge(np1, np2, label=l1.get("label"))
        return c_g

    @staticmethod
    def __create_label_dicts(graph1, graph2, node):

        in_labels1 = {}
        out_labels1 = {}

        in_labels2 = {}
        out_labels2 = {}

        for e in graph1.in_edges(node.node1):
            edge_data = graph1.get_edge_data(e[0], e[1])

            label = edge_data.get("label")

            if label in in_labels1:
                in_labels1[label] += 1.0
            else:
                in_labels1[label] = 1.0

        for e in graph2.in_edges(node.node2):
            edge_data = graph2.get_edge_data(e[0], e[1])

            label = edge_data.get("label")

            if label in in_labels2:
                in_labels2[label] += 1.0
            else:
                in_labels2[label] = 1.0

        for e in graph1.out_edges(node.node1):
            edge_data = graph1.get_edge_data(e[0], e[1])

            label = edge_data.get("label")

            if label in out_labels1:
                out_labels1[label] += 1.0
            else:
                out_labels1[label] = 1.0

        for e in graph2.out_edges(node.node2):
            edge_data = graph2.get_edge_data(e[0], e[1])

            label = edge_data.get("label")

            if label in out_labels2:
                out_labels2[label] += 1.0
            else:
                out_labels2[label] = 1.0

        return in_labels1, in_labels2, out_labels1, out_labels2

    def construct_graph(self):
        c_g = self.__construct_connectivity_graph()
        p_g = self.__initialize_graph(c_g)

        if self.policy is Policy.INVERSE_PRODUCT:
            return self.__construct_inverse_product(c_g, p_g)
        return self.__construct_inverse_average(c_g, p_g)

    @staticmethod
    def __initialize_graph(c_g):
        p_g = nx.DiGraph()
        p_g.add_nodes_from(c_g.nodes())
        return p_g

    def __construct_inverse_product(self, c_g, p_g):
        for node in p_g.nodes():
            p_g = self.__add_propagation_edges(c_g, p_g, node, case_in=True)
            p_g = self.__add_propagation_edges(c_g, p_g, node, case_in=False)
        return p_g

    def __construct_inverse_average(self, c_g, p_g):
        for n in p_g.nodes():
            in_labels, out_labels = self.__compute_labels(n)

            self.__inverse_label_values(in_labels, m=2.0)
            self.__inverse_label_values(out_labels, m=2.0)

            self.__add_in_edges(c_g, p_g, n, in_labels)
            self.__add_out_edges(c_g, p_g, n, out_labels)

        return p_g

    def __compute_labels(self, n):
        if n.node1 in self.graph1.nodes():
            g1, g2 = self.graph1, self.graph2
        else:
            g1, g2 = self.graph2, self.graph1

        in1, in2, out1, out2 = self.__create_label_dicts(g1, g2, n)

        in_labels = self.__merge_dicts(in1, in2)
        out_labels = self.__merge_dicts(out1, out2)

        return in_labels, out_labels

    @staticmethod
    def __merge_dicts(d1, d2):
        result = d1.copy()
        for k, v in d2.items():
            result[k] = result.get(k, 0) + v
        return result

    @staticmethod
    def __add_in_edges(c_g, p_g, node, in_labels):
        for u, v in c_g.in_edges(node):
            label = c_g.get_edge_data(u, v).get("label")
            p_g.add_edge(v, u, weight=in_labels[label])

    @staticmethod
    def __add_out_edges(c_g, p_g, node, out_labels):
        for u, v in c_g.out_edges(node):
            label = c_g.get_edge_data(u, v).get("label")
            p_g.add_edge(u, v, weight=out_labels[label])
