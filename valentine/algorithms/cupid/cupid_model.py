from typing import Dict, Tuple

from .schema_tree import SchemaTree
from .tree_match import tree_match, recompute_wsim, mapping_generation_leaves
from ..base_matcher import BaseMatcher
from ...data_sources.base_table import BaseTable


class Cupid(BaseMatcher):
    # Optimal parameters suggested in the paper
    def __init__(self,
                 leaf_w_struct=0.2,
                 w_struct=0.2,
                 th_accept=0.7,
                 th_high=0.6,
                 th_low=0.35,
                 c_inc=1.2,
                 c_dec=0.9,
                 th_ns=0.7,
                 parallelism=1):
        self.__leaf_w_struct = float(leaf_w_struct)
        self.__w_struct = float(w_struct)
        self.__th_accept = float(th_accept)
        self.__th_high = float(th_high)
        self.__th_low = float(th_low)
        self.__c_inc = float(c_inc)
        self.__c_dec = float(c_dec)
        self.__th_ns = float(th_ns)
        self.__parallelism = int(parallelism)
        self.__categories = set()
        self.__schemata = dict()  # schema name:str, schema_tree

    def get_matches(self,
                    source_input: BaseTable,
                    target_input: BaseTable) -> Dict[Tuple[Tuple[str, str], Tuple[str, str]], float]:
        self.__add_data("DB__"+source_input.name, source_input)
        self.__add_data("DB__"+target_input.name, target_input)
        source_tree = self.__get_schema_by_name("DB__"+source_input.name)
        target_tree = self.__get_schema_by_name("DB__"+target_input.name)
        sims = tree_match(source_tree, target_tree, self.__categories, self.__leaf_w_struct, self.__w_struct,
                          self.__th_accept, self.__th_high, self.__th_low, self.__c_inc, self.__c_dec,
                          self.__th_ns, self.__parallelism)
        new_sims = recompute_wsim(source_tree, target_tree, sims)
        matches = mapping_generation_leaves(source_tree, target_tree, new_sims, self.__th_accept)
        return matches

    def __add_data(self,
                   schema_name: str,
                   table: BaseTable):
        if schema_name not in self.__schemata.keys():
            self.__schemata[schema_name] = SchemaTree(schema_name)

        schema_level_node = self.__schemata[schema_name].get_node(schema_name)

        # Add table
        self.__schemata[schema_name].add_node(table_name=table.name, table_guid=table.unique_identifier,
                                              data_type="Table", parent=schema_level_node)

        table_level_node = self.__schemata[schema_name].get_node(table.name)

        columns = table.get_columns()

        # Add columns
        for column in columns:
            self.__schemata[schema_name].add_node(table_name=table.name, table_guid=table.unique_identifier,
                                                  column_name=column.name, column_guid=column.unique_identifier,
                                                  data_type=column.data_type, parent=table_level_node)
            self.__categories.add(column.data_type)

    def __get_schema_by_name(self,
                             schema_name):
        return self.__schemata[schema_name]
