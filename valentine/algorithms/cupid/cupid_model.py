from ...data_sources.base_table import BaseTable
from ..base_matcher import BaseMatcher
from .schema_tree import SchemaTree
from .tree_match import mapping_generation_leaves, recompute_wsim, tree_match


class Cupid(BaseMatcher):
    """Python implementation of the Cupid schema matching algorithm.

    Cupid combines linguistic similarity of column names with structural
    similarity derived from schema tree shape, following the paper
    *Generic Schema Matching with Cupid* (Madhavan et al., VLDB 2001).

    Parameters
    ----------
    leaf_w_struct : float, optional
        Weight of structural similarity at leaf level, in ``[0, 1]``
        (default: ``0.2``).
    w_struct : float, optional
        Weight of structural similarity at inner-node level, in ``[0, 1]``
        (default: ``0.2``).
    th_accept : float, optional
        Acceptance similarity threshold for the final mapping, in ``[0, 1]``
        (default: ``0.7``).
    th_high : float, optional
        High-confidence threshold used during structural propagation,
        in ``[0, 1]`` (default: ``0.6``).
    th_low : float, optional
        Low-confidence threshold used during structural propagation,
        in ``[0, 1]`` (default: ``0.35``).
    c_inc : float, optional
        Positive reinforcement coefficient for matching children
        (must be ``> 0``, default: ``1.2``).
    c_dec : float, optional
        Negative reinforcement coefficient for non-matching children
        (must be ``> 0``, default: ``0.9``).
    th_ns : float, optional
        Name-similarity threshold, in ``[0, 1]`` (default: ``0.7``).
    process_num : int, optional
        Number of worker processes (must be ``>= 1``, default: ``1``).
    """

    def __init__(
        self,
        leaf_w_struct: float = 0.2,
        w_struct: float = 0.2,
        th_accept: float = 0.7,
        th_high: float = 0.6,
        th_low: float = 0.35,
        c_inc: float = 1.2,
        c_dec: float = 0.9,
        th_ns: float = 0.7,
        process_num: int = 1,
    ):
        self.__leaf_w_struct = float(leaf_w_struct)
        self.__w_struct = float(w_struct)
        self.__th_accept = float(th_accept)
        self.__th_high = float(th_high)
        self.__th_low = float(th_low)
        self.__c_inc = float(c_inc)
        self.__c_dec = float(c_dec)
        self.__th_ns = float(th_ns)
        self.__process_num = int(process_num)

        for name, val in [
            ("leaf_w_struct", self.__leaf_w_struct),
            ("w_struct", self.__w_struct),
            ("th_accept", self.__th_accept),
            ("th_high", self.__th_high),
            ("th_low", self.__th_low),
            ("th_ns", self.__th_ns),
        ]:
            if not 0.0 <= val <= 1.0:
                raise ValueError(f"{name} must be between 0.0 and 1.0, got {val}")
        for name, val in [("c_inc", self.__c_inc), ("c_dec", self.__c_dec)]:
            if val <= 0:
                raise ValueError(f"{name} must be positive, got {val}")
        if self.__process_num < 1:
            raise ValueError(f"process_num must be >= 1, got {self.__process_num}")

        self.__categories = set()
        self.__schemata = {}

    def get_matches(self, source_input: BaseTable, target_input: BaseTable) -> dict:
        self.__add_data("DB__" + source_input.name, source_input)
        self.__add_data("DB__" + target_input.name, target_input)
        source_tree = self.__get_schema_by_name("DB__" + source_input.name)
        target_tree = self.__get_schema_by_name("DB__" + target_input.name)
        sims = tree_match(
            source_tree,
            target_tree,
            self.__categories,
            self.__leaf_w_struct,
            self.__w_struct,
            self.__th_accept,
            self.__th_high,
            self.__th_low,
            self.__c_inc,
            self.__c_dec,
            self.__th_ns,
            self.__process_num,
        )
        new_sims = recompute_wsim(source_tree, target_tree, sims)
        matches = mapping_generation_leaves(source_tree, target_tree, new_sims, self.__th_accept)
        return matches

    def __add_data(self, schema_name: str, table: BaseTable):
        if schema_name not in self.__schemata:
            self.__schemata[schema_name] = SchemaTree(schema_name)

        schema_level_node = self.__schemata[schema_name].get_node(schema_name)

        # Add table
        self.__schemata[schema_name].add_node(
            table_name=table.name,
            table_guid=table.unique_identifier,
            data_type="Table",
            parent=schema_level_node,
        )

        table_level_node = self.__schemata[schema_name].get_node(table.name)

        columns = table.get_columns()

        # Add columns
        for column in columns:
            self.__schemata[schema_name].add_node(
                table_name=table.name,
                table_guid=table.unique_identifier,
                column_name=column.name,
                column_guid=column.unique_identifier,
                data_type=column.data_type,
                parent=table_level_node,
            )
            self.__categories.add(column.data_type)

    def __get_schema_by_name(self, schema_name):
        return self.__schemata[schema_name]
