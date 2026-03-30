from __future__ import annotations

from ...data_sources.base_table import BaseTable
from ..base_matcher import BaseMatcher
from ..match import Match
from .combination import average
from .matchers import COMA_OPT_INST_MATCHERS, COMA_OPT_MATCHERS
from .schema import SchemaGraph
from .selection import select_both_multiple


class ComaPy(BaseMatcher):
    """
    Pure Python implementation of the COMA 3.0 schema matching algorithm.

    Supports two strategies:
    - COMA_OPT (schema-only): uses name, path, leaves, and parents matchers
    - COMA_OPT_INST (schema + instance): adds TF-IDF instance matching

    This is a drop-in replacement for the Java-based Coma class.
    """

    def __init__(self, max_n: int = 0, use_instances: bool = False):
        self.__max_n = int(max_n)
        self.__use_instances = use_instances

    def get_matches(
        self, source_input: BaseTable, target_input: BaseTable
    ) -> dict[tuple[tuple[str, str], tuple[str, str]], float]:
        # Build schema graphs
        source_graph = SchemaGraph.from_table(source_input)
        target_graph = SchemaGraph.from_table(target_input)

        # Select strategy
        complex_matchers = COMA_OPT_INST_MATCHERS if self.__use_instances else COMA_OPT_MATCHERS

        # Compute all-pairs similarity matrix
        sim_matrix: dict[tuple, float] = {}
        for e1 in source_graph.columns:
            for e2 in target_graph.columns:
                scores = [cm.compute(e1, e2, source_graph, target_graph) for cm in complex_matchers]
                sim_matrix[(e1, e2)] = average(scores)

        # Apply selection (COMA_OPT uses delta=0.01, threshold=0.0)
        selected = select_both_multiple(
            sim_matrix,
            source_graph.columns,
            target_graph.columns,
            max_n=self.__max_n,
            delta=0.01,
            threshold=0.0,
        )

        # Format output
        output: dict[tuple[tuple[str, str], tuple[str, str]], float] = {}
        for (e1, e2), sim in selected.items():
            match = Match(
                target_table_name=target_input.name,
                target_column_name=e2.name,
                source_table_name=source_input.name,
                source_column_name=e1.name,
                similarity=float(sim),
            )
            output.update(match.to_dict)

        return output
