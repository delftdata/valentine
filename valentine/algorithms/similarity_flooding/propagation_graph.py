import networkx as nx

from .node_pair import NodePair


class PropagationGraph:
    """
        Class for constructing a Propagation Graph from two input graphs.
    """

    def __init__(self, graph1, graph2, policy):
        self.graph1 = graph1
        self.graph2 = graph2
        self.policy = policy  # policy corresponds to the policy used to compute propagation coefficients

    @staticmethod
    def __inverse_label_values(labels, m=1.0):
        for key, value in labels.items():
            labels[key] = m/value

    def __add_propagation_edges(self, c_graph, p_graph, node, case_in):

        if case_in:
            edges = c_graph.in_edges(node)
        else:
            edges = c_graph.out_edges(node)

        labels = dict()
        for e in edges:
            edge_data = c_graph.get_edge_data(e[0], e[1])

            label = edge_data.get('label')

            if label in labels.keys():
                labels[label] += 1.0
            else:
                labels[label] = 1.0

        self.__inverse_label_values(labels)

        for e in edges:
            edge_data = c_graph.get_edge_data(e[0], e[1])

            label = edge_data.get('label')

            if case_in:
                p_graph.add_edge(e[1], e[0], weight=labels[label])
            else:
                p_graph.add_edge(e[0], e[1], weight=labels[label])

        return p_graph

    def __construct_connectivity_graph(self):
        c_g = nx.DiGraph()  # initialize the connectivity graph

        for e1 in self.graph1.edges():
            for e2 in self.graph2.edges():
                l1 = self.graph1.get_edge_data(e1[0], e1[1])
                l2 = self.graph2.get_edge_data(e2[0], e2[1])
                # if the labels of both edges are equal then add a new pair of nodes in p_g
                if l1.get('label') == l2.get('label'):
                    np1 = NodePair(e1[0], e2[0])
                    c_g.add_node(np1)
                    np2 = NodePair(e1[1], e2[1])
                    c_g.add_node(np2)
                    c_g.add_edge(np1, np2, label=l1.get('label'))
        return c_g

    @staticmethod
    def __create_label_dicts(graph1, graph2, node):

        in_labels1 = {}
        out_labels1 = {}

        in_labels2 = {}
        out_labels2 = {}

        for e in graph1.in_edges(node.node1):
            edge_data = graph1.get_edge_data(e[0], e[1])

            label = edge_data.get('label')

            if label in in_labels1.keys():
                in_labels1[label] += 1.0
            else:
                in_labels1[label] = 1.0

        for e in graph2.in_edges(node.node2):
            edge_data = graph2.get_edge_data(e[0], e[1])

            label = edge_data.get('label')

            if label in in_labels2.keys():
                in_labels2[label] += 1.0
            else:
                in_labels2[label] = 1.0

        for e in graph1.out_edges(node.node1):
            edge_data = graph1.get_edge_data(e[0], e[1])

            label = edge_data.get('label')

            if label in out_labels1.keys():
                out_labels1[label] += 1.0
            else:
                out_labels1[label] = 1.0

        for e in graph2.out_edges(node.node2):
            edge_data = graph2.get_edge_data(e[0], e[1])

            label = edge_data.get('label')

            if label in out_labels2.keys():
                out_labels2[label] += 1.0
            else:
                out_labels2[label] = 1.0

        return in_labels1, in_labels2, out_labels1, out_labels2

    def construct_graph(self):

        c_g = self.__construct_connectivity_graph()

        p_g = nx.DiGraph()  # initialize the similarity propagation graph

        for n in c_g.nodes():
            p_g.add_node(n)
        # inverse product strategy for computing propagation coefficients as described in the paper
        if self.policy == 'inverse_product':
            for node in p_g.nodes():
                p_g = self.__add_propagation_edges(c_g, p_g, node, case_in=True)
                p_g = self.__add_propagation_edges(c_g, p_g, node, case_in=False)
        # inverse average strategy for computing propagation coefficients as described in the paper
        elif self.policy == 'inverse_average':
            for n in p_g.nodes():

                if n.node1 in self.graph1.nodes():
                    in_labels1, in_labels2, out_labels1, out_labels2 = self.__create_label_dicts(self.graph1,
                                                                                                 self.graph2, n)
                else:
                    in_labels1, in_labels2, out_labels1, out_labels2 = self.__create_label_dicts(self.graph2,
                                                                                                 self.graph1, n)

                in_labels = in_labels1.copy()
                out_labels = out_labels1.copy()

                for key in in_labels2:
                    if key in in_labels.keys():
                        in_labels[key] += in_labels2[key]
                    else:
                        in_labels[key] = in_labels2[key]

                for key in out_labels2:
                    if key in out_labels.keys():
                        out_labels[key] += out_labels2[key]
                    else:
                        out_labels[key] = out_labels2[key]

                self.__inverse_label_values(in_labels, m=2.0)
                self.__inverse_label_values(out_labels, m=2.0)

                for e in c_g.in_edges(n):
                    edge_data = c_g.get_edge_data(e[0], e[1])

                    label = edge_data.get('label')

                    p_g.add_edge(e[1], e[0], weight=in_labels[label])

                for e in c_g.out_edges(n):
                    edge_data = c_g.get_edge_data(e[0], e[1])

                    label = edge_data.get('label')

                    p_g.add_edge(e[0], e[1], weight=out_labels[label])
        else:

            print("Wrong policy!")
            return {}

        return p_g
