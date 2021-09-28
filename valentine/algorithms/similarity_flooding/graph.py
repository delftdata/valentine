import networkx as nx

from .node import Node
from ...data_sources.base_db import BaseDB


class Graph:

    def __init__(self, schema: BaseDB):
        self.graph = nx.DiGraph()
        self.__schema = schema
        self.schema_name = schema.name
        self.create_graph()

    def create_graph(self):
        database = "Database"
        table = "Table"
        column = "Column"
        column_type = "ColumnType"

        database_node = Node(database, self.schema_name)
        table_node = Node(table, self.schema_name)
        column_node = Node(column, self.schema_name)
        col_type_node = Node(column_type, self.schema_name)

        self.graph.add_node(database_node)
        self.graph.add_node(table_node)
        self.graph.add_node(column_node)
        self.graph.add_node(col_type_node)
        unique_id = 1

        db_node = Node("NodeID" + str(unique_id), self.schema_name)  # DB
        relation_node = Node(self.schema_name, self.schema_name)

        self.graph.add_node(db_node)
        self.graph.add_node(relation_node)
        self.graph.add_edge(db_node, relation_node, label='name')
        self.graph.add_edge(db_node, database_node, label='type')  # DB

        for table in self.__schema.get_tables().values():
            unique_id += 1
            tbl_node = Node("NodeID" + str(unique_id), self.schema_name)
            attribute_node = Node(table.name, self.schema_name)
            self.graph.add_node(tbl_node)
            self.graph.add_edge(database_node, table_node, label='type')
            self.graph.add_edge(db_node, tbl_node, label='table')
            self.graph.add_edge(tbl_node, attribute_node, label='name')
            for column in table.get_columns():
                unique_id += 1
                clm_node = Node("NodeID" + str(unique_id), table.name)
                clm_node.add_long_name(table.name, table.unique_identifier, column.name, column.unique_identifier)
                attribute_node = Node(column.name, table.name)
                attribute_node.add_long_name(table.name, table.unique_identifier, column.name, column.unique_identifier)
                self.graph.add_node(clm_node)
                self.graph.add_edge(clm_node, column_node, label='type')
                self.graph.add_edge(tbl_node, clm_node, label='column')
                self.graph.add_edge(clm_node, attribute_node, label='name')

                if self.graph.has_node(Node(column.data_type, table.name)):
                    self.graph.add_edge(clm_node, [n for n in self.graph.predecessors(Node(column.data_type,
                                                                                           table.name))][0],
                                        label='SQLtype')
                else:
                    previous_id = unique_id
                    unique_id += 1
                    clm_node = Node("NodeID" + str(unique_id), table.name)
                    clm_node.add_long_name(table.name, table.unique_identifier, column.name, column.unique_identifier)
                    self.graph.add_node(clm_node)
                    self.graph.add_edge(clm_node, col_type_node, label='type')
                    typename_node = Node(column.data_type, table.name)
                    typename_node.add_long_name(table.name, table.unique_identifier, column.name,
                                                column.unique_identifier)
                    self.graph.add_node(typename_node)
                    self.graph.add_edge(clm_node, typename_node, label='name')
                    self.graph.add_edge(Node("NodeID" + str(previous_id), table.name), clm_node, label='SQLtype')
