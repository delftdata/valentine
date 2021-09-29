from .schema_tree import SchemaTree
from .tree_match import tree_match, recompute_wsim, mapping_generation_leaves
from ..base_matcher import BaseMatcher
from ...data_sources.base_table import BaseTable

# TODO (1) Find a way to get the nltk corpora in the setup.py
# TODO (2) Replace lambdas with list comprehension

class Cupid(BaseMatcher):
    # Optimal parameters suggested in the paper
    def __init__(self, leaf_w_struct=0.2, w_struct=0.2, th_accept=0.7, th_high=0.6, th_low=0.35, c_inc=1.2, c_dec=0.9,
                 th_ns=0.7, parallelism=1):
        self.leaf_w_struct = float(leaf_w_struct)
        self.w_struct = float(w_struct)
        self.th_accept = float(th_accept)
        self.th_high = float(th_high)
        self.th_low = float(th_low)
        self.c_inc = float(c_inc)
        self.c_dec = float(c_dec)
        self.th_ns = float(th_ns)
        self.parallelism = int(parallelism)
        self.categories = set()
        self.schemata = dict()  # schema name:str, schema_tree

    def get_matches(self, source_input: BaseTable, target_input: BaseTable):
        for table in source_input.get_tables().values():
            self.add_data("DB__"+source_input.name, table)
        for table in target_input.get_tables().values():
            self.add_data("DB__"+target_input.name, table)
        source_tree = self.get_schema_by_name("DB__"+source_input.name)
        target_tree = self.get_schema_by_name("DB__"+target_input.name)
        sims = tree_match(source_tree, target_tree, self.categories, self.leaf_w_struct, self.w_struct, self.th_accept,
                          self.th_high, self.th_low, self.c_inc, self.c_dec, self.th_ns, self.parallelism)
        new_sims = recompute_wsim(source_tree, target_tree, sims)
        source_id = source_input.db_belongs_uid if isinstance(source_input, BaseTable) \
            else source_input.unique_identifier
        target_id = target_input.db_belongs_uid if isinstance(target_input, BaseTable) \
            else target_input.unique_identifier
        matches = mapping_generation_leaves(source_id, target_id, source_tree, target_tree, new_sims, self.th_accept)
        return matches

    def add_data(self, schema_name: str, table: BaseTable):
        if schema_name not in self.schemata.keys():
            self.schemata[schema_name] = SchemaTree(schema_name)

        schema_level_node = self.schemata[schema_name].get_node(schema_name)

        # Add table
        self.schemata[schema_name].add_node(table_name=table.name, table_guid=table.unique_identifier,
                                            data_type="Table", parent=schema_level_node)

        table_level_node = self.schemata[schema_name].get_node(table.name)

        columns = table.get_columns()

        # Add columns
        for column in columns:
            self.schemata[schema_name].add_node(table_name=table.name, table_guid=table.unique_identifier,
                                                column_name=column.name, column_guid=column.unique_identifier,
                                                data_type=column.data_type, parent=table_level_node)
            self.categories.add(column.data_type)

    def get_schema_by_name(self, schema_name):
        return self.schemata[schema_name]

    def print_data(self):
        for schema_tree in self.schemata.values():
            schema_tree.print_schema_tree()
