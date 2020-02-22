import networkx as nx
from collections import defaultdict
from algorithms.sem_prop.knowledgerepr.fieldnetwork import FieldNetwork
from algorithms.sem_prop.api.apiutils import Relation


class Report:

    def __init__(self, network: FieldNetwork):
        self.__network = network
        self.__num_tables = 0
        self.__num_columns = 0
        self.__num_schema_sim_relations = 0
        self.__num_content_sim_relations = 0
        self.__num_pkfk_relations = 0
        self.compute_all_statistics()

    @property
    def num_tables(self):
        return self.__num_tables

    @property
    def num_columns(self):
        return self.__num_columns

    @property
    def num_schema_sim_relations(self):
        return self.__num_schema_sim_relations

    @property
    def num_content_sim_relations(self):
        return self.__num_content_sim_relations

    @property
    def num_pkfk_relations(self):
        return self.__num_pkfk_relations

    @property
    def order(self):
        return self.__network.order()

    def top_connected_fields(self, topk):
        return self.__network.fields_degree(topk)

    def compute_all_statistics(self):
        self.__num_columns = self.__network.graph_order()
        self.__num_tables = self.__network.get_number_tables()
        # FIXME: Failing due to cardinality being attached as float to graph nodes ??
        # relations = graph.edges(keys=True)
        content_sim_relations_gen = self.__network.enumerate_relation(
            Relation.CONTENT_SIM)
        # FIXME: counting twice (both directions), so /2. Once edges works, we
        # can modify it
        total_content_sim_relations = len([x for x in content_sim_relations_gen])
        self.__num_content_sim_relations = total_content_sim_relations

        schema_sim_relations_gen = self.__network.enumerate_relation(
            Relation.SCHEMA_SIM)
        total_schema_sim_relations = len([x for x in schema_sim_relations_gen])
        self.__num_schema_sim_relations = total_schema_sim_relations

        pkfk_relations_gen = self.__network.enumerate_relation(Relation.PKFK)
        total_pkfk_relations = len([x for x in pkfk_relations_gen])
        self.__num_pkfk_relations = total_pkfk_relations

        return self

    def print_content_sim_relations(self):
        self.__network.print_relations(Relation.CONTENT_SIM)

    def print_schema_sim_relations(self):
        self.__network.print_relations(Relation.SCHEMA_SIM)

    def print_pkfk_relations(self):
        self.__network.print_relations(Relation.PKFK)

    def print_all_indexed_tables(self):
        tables_set = set()
        for db_name, source_name, field_name, data_type in self.__network.iterate_values():
            tables_set.add(source_name)
        for el in tables_set:
            print(str(el))
        return tables_set

    def print_all_columns_of_type(self, type):
        col_set = set()
        for db_name, source_name, field_name, data_type in self.__network.iterate_values():
            print(str(data_type))
            if data_type == type:
                col_set.add((db_name, source_name, field_name))
        return col_set