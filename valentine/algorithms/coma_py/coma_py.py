from __future__ import annotations

from ...data_sources.base_table import BaseTable
from ..base_matcher import BaseMatcher
from ..match import Match
from .combination import average
from .matchers import build_matchers
from .schema import SchemaGraph
from .selection import select_both_multiple
from .similarity.tfidf import TfidfCorpus


class ComaPy(BaseMatcher):
    """
    Pure Python implementation of the COMA 3.0 schema matching algorithm.

    COMA (COmbination of MAtching algorithms) works by composing multiple
    individual matchers — each targeting a different aspect of schema or data
    similarity — and combining their scores. This implementation faithfully
    reproduces the matching pipeline from the original Java COMA 3.0 Community
    Edition, without requiring a Java runtime.

    **Schema matchers** (enabled by ``use_schema=True``, the default):

    - *Name*: trigram (Dice) similarity on column names.
    - *Path*: trigram similarity on dot-separated schema paths.
    - *Leaves*: name similarity across all leaf-level columns.
    - *Parents*: structural similarity via parent-level leaf comparison.

    **Instance matcher** (enabled by ``use_instances=True``):

    - *TF-IDF cosine similarity*: each cell value is treated as a document,
      a global IDF is computed across all columns of both tables, and
      per-column similarity is aggregated using a max-matching Dice formula.

    The two groups can be used independently or together. At least one of
    ``use_schema`` or ``use_instances`` must be enabled.

    After computing all-pairs similarity scores, a selection step filters
    results using bidirectional best-match logic (DIR_BOTH) controlled by
    ``max_n``, ``delta``, and ``threshold``.

    This class will be renamed to ``Coma`` in valentine v1.0.0, replacing
    the current Java-based ``Coma`` wrapper.

    Parameters
    ----------
    max_n : int, optional
        Maximum number of matches to keep per column (0 = unlimited).
    use_instances : bool, optional
        Enable TF-IDF instance-based matching (default: False).
    use_schema : bool, optional
        Enable schema-based matching (default: True).
    delta : float, optional
        Fraction from the best score within which matches are kept.
        For example, 0.15 keeps all matches scoring within 15%% of the
        best match for that column (default: 0.15).
    threshold : float, optional
        Absolute minimum similarity score to keep a match (default: 0.0).
    """

    def __init__(
        self,
        max_n: int = 0,
        use_instances: bool = False,
        use_schema: bool = True,
        delta: float = 0.15,
        threshold: float = 0.0,
    ):
        if not use_schema and not use_instances:
            raise ValueError("At least one of use_schema or use_instances must be True")
        self.__max_n = int(max_n)
        self.__use_instances = use_instances
        self.__use_schema = use_schema
        self.__delta = delta
        self.__threshold = threshold

    def get_matches(
        self, source_input: BaseTable, target_input: BaseTable
    ) -> dict[tuple[tuple[str, str], tuple[str, str]], float]:
        # Build schema graphs
        source_graph = SchemaGraph.from_table(source_input)
        target_graph = SchemaGraph.from_table(target_input)

        # Build global TF-IDF corpus if instances are needed
        corpus = None
        if self.__use_instances:
            all_column_instances = [
                col.instances for col in source_graph.columns + target_graph.columns
            ]
            corpus = TfidfCorpus(all_column_instances)

        complex_matchers = build_matchers(
            corpus,
            use_schema=self.__use_schema,
            use_instances=self.__use_instances,
        )

        # Compute all-pairs similarity matrix
        sim_matrix: dict[tuple, float] = {}
        for e1 in source_graph.columns:
            for e2 in target_graph.columns:
                scores = [cm.compute(e1, e2, source_graph, target_graph) for cm in complex_matchers]
                sim_matrix[(e1, e2)] = average(scores)

        selected = select_both_multiple(
            sim_matrix,
            source_graph.columns,
            target_graph.columns,
            max_n=self.__max_n,
            delta=self.__delta,
            threshold=self.__threshold,
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
