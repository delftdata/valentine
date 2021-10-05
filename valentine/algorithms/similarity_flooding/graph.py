import networkx as nx

from .node import Node
from ...data_sources.base_table import BaseTable
from . import TABLE, COLUMN, COLUMN_TYPE


class Graph:

    def __init__(self, schema: BaseTable):
        self.graph = nx.DiGraph()
        self.schema = schema

        # create table, column and column_type nodes
        self.table_node = Node(TABLE, self.schema.name)
        self.column_node = Node(COLUMN, self.schema.name)
        self.col_type_node = Node(COLUMN_TYPE, self.schema.name)

        # add them to the graph
        self.graph.add_node(self.table_node)
        self.graph.add_node(self.column_node)
        self.graph.add_node(self.col_type_node)

        self.unique_id = 1  # unique identifiers to be used for each node

        # add table node to the graph
        self.tbl_node = Node("NodeID" + str(self.unique_id), self.schema.name)
        attribute_node = Node(self.schema.name, self.schema.name)
        self.graph.add_node(self.tbl_node)
        self.graph.add_edge(self.tbl_node, attribute_node, label='name')
        self.graph.add_edge(self.tbl_node, self.table_node, label='type')

        # construct rest of graph
        self.create_graph()

    def __create_node(self, column, type_node=False, attribute_node=False):

        if type_node:
            node = Node(column.data_type, self.schema.name)
        elif attribute_node:
            node = Node(column.name, self.schema.name)
        else:
            node = Node("NodeID" + str(self.unique_id), self.schema.name)
        node.add_long_name(self.schema.name, self.schema.unique_identifier, column.name, column.unique_identifier)
        return node

    def add_and_connect(self, column):
        self.unique_id += 1
        table = self.schema
        clm_node = self.__create_node(column)
        attribute_node = self.__create_node(column, attribute_node=True)
        self.graph.add_node(clm_node)
        self.graph.add_edge(clm_node, self.column_node, label='type')
        self.graph.add_edge(self.tbl_node, clm_node, label='column')
        self.graph.add_edge(clm_node, attribute_node, label='name')

        if self.graph.has_node(Node(column.data_type, table.name)):
            self.graph.add_edge(clm_node, [n for n in self.graph.predecessors(Node(column.data_type,
                                                                                   table.name))][0],
                                label='SQLtype')
        else:
            previous_id = self.unique_id
            self.unique_id += 1
            clm_node = self.__create_node(column)
            self.graph.add_node(clm_node)
            self.graph.add_edge(clm_node, self.col_type_node, label='type')
            typename_node = self.__create_node(column, type_node=True)
            self.graph.add_node(typename_node)
            self.graph.add_edge(clm_node, typename_node, label='name')
            self.graph.add_edge(Node("NodeID" + str(previous_id), table.name), clm_node, label='SQLtype')

    def create_graph(self):
        for column in self.schema.get_columns():
            self.add_and_connect(column)



