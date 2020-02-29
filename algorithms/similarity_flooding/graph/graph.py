import networkx as nx

from algorithms.similarity_flooding.graph.node import Node
from data_loader.schema_loader import SchemaLoader


class Graph:
    """
    Class for describing a graph.
    """
    def __init__(self, schema_loader: SchemaLoader):
        self.graph = nx.DiGraph()
        self.schema = schema_loader.schema
        self.relation_name = schema_loader.schema_name
        self.create_graph()

    def create_graph(self):
        table = "Table"
        column = "Column"
        column_type = "ColumnType"

        table_node = Node(table, self.relation_name)
        column_node = Node(column, self.relation_name)
        col_type_node = Node(column_type, self.relation_name)

        self.graph.add_node(table_node)
        self.graph.add_node(column_node)
        self.graph.add_node(col_type_node)
        unique_id = 1

        parent_node = Node("NodeID" + str(unique_id), self.relation_name)
        relation_node = Node(self.relation_name, self.relation_name)

        self.graph.add_node(parent_node)
        self.graph.add_node(relation_node)
        self.graph.add_edge(parent_node, relation_node, label='name')
        self.graph.add_edge(parent_node, table_node, label='type')

        for attribute in self.schema.keys():

            unique_id += 1

            element_node = Node("NodeID" + str(unique_id), self.relation_name)

            self.graph.add_node(element_node)
            self.graph.add_edge(element_node, column_node, label='type')
            self.graph.add_edge(parent_node, element_node, label='column')

            attribute_node = Node(attribute, self.relation_name)

            self.graph.add_edge(element_node, attribute_node, label='name')

            if self.graph.has_node(Node(self.schema[attribute]['type'], self.relation_name)):
                self.graph.add_edge(element_node,
                                    [n for n in self.graph.predecessors(Node(self.schema[attribute]['type'],
                                                                             self.relation_name))][0], label='SQLtype')
            else:
                previous_id = unique_id
                unique_id += 1
                element_node = Node("NodeID" + str(unique_id), self.relation_name)

                self.graph.add_node(element_node)
                self.graph.add_edge(element_node, col_type_node, label='type')

                typename_node = Node(self.schema[attribute]['type'], self.relation_name)

                self.graph.add_node(typename_node)
                self.graph.add_edge(element_node, typename_node, label='name')
                self.graph.add_edge(Node("NodeID" + str(previous_id), self.relation_name),
                                    element_node, label='SQLtype')
