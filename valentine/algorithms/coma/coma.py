from __future__ import annotations

from itertools import combinations

from ...data_sources.base_table import BaseTable
from ..base_matcher import BaseMatcher
from ..match import ColumnPair, Match
from .combination import average
from .matchers import build_matchers
from .schema import SchemaGraph
from .selection import select_both_multiple
from .similarity.tfidf import TfidfCorpus


class Coma(BaseMatcher):
    """
    Pure Python implementation of the COMA 3.0 schema matching algorithm.

    COMA (COmbination of MAtching algorithms) works by composing multiple
    individual matchers — each targeting a different aspect of schema or data
    similarity — and combining their scores. This implementation faithfully
    reproduces the matching pipeline from the original Java COMA 3.0 Community
    Edition.

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

    When matching more than two tables via :meth:`get_matches_batch`, the
    TF-IDF corpus is built once from **all** tables, giving each pair the
    benefit of global IDF statistics.

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
        if self.__max_n < 0:
            raise ValueError(f"max_n must be >= 0, got {self.__max_n}")
        self.__use_instances = use_instances
        self.__use_schema = use_schema
        self.__delta = float(delta)
        self.__threshold = float(threshold)
        if not 0.0 <= self.__delta <= 1.0:
            raise ValueError(f"delta must be between 0.0 and 1.0, got {self.__delta}")
        if not 0.0 <= self.__threshold <= 1.0:
            raise ValueError(f"threshold must be between 0.0 and 1.0, got {self.__threshold}")

    def get_matches(
        self, source_input: BaseTable, target_input: BaseTable
    ) -> dict[ColumnPair, float]:
        self._match_details: dict[ColumnPair, dict[str, float]] = {}
        source_graph = SchemaGraph.from_table(source_input)
        target_graph = SchemaGraph.from_table(target_input)

        corpus = None
        if self.__use_instances:
            all_column_instances = [
                col.instances for col in source_graph.columns + target_graph.columns
            ]
            corpus = TfidfCorpus(all_column_instances)

        return self._match_pair(source_graph, target_graph, source_input, target_input, corpus)

    def get_matches_batch(self, tables: list[BaseTable]) -> dict[ColumnPair, float]:
        """Match all table pairs with a single global TF-IDF corpus.

        Building the corpus from all tables at once gives better IDF
        statistics than building a separate corpus per pair.
        """
        self._match_details: dict[ColumnPair, dict[str, float]] = {}
        graphs = [(table, SchemaGraph.from_table(table)) for table in tables]

        # Build one global TF-IDF corpus from all tables
        corpus = None
        if self.__use_instances:
            all_column_instances = [col.instances for _, graph in graphs for col in graph.columns]
            corpus = TfidfCorpus(all_column_instances)

        matches: dict[ColumnPair, float] = {}
        for (t1, g1), (t2, g2) in combinations(graphs, 2):
            matches.update(self._match_pair(g1, g2, t1, t2, corpus))

        return matches

    def _match_pair(
        self,
        source_graph: SchemaGraph,
        target_graph: SchemaGraph,
        source_input: BaseTable,
        target_input: BaseTable,
        corpus: TfidfCorpus | None,
    ) -> dict[ColumnPair, float]:
        complex_matchers = build_matchers(
            corpus,
            use_schema=self.__use_schema,
            use_instances=self.__use_instances,
        )

        # Compute all-pairs similarity matrix and collect per-matcher details
        sim_matrix: dict[tuple, float] = {}
        details_matrix: dict[tuple, dict[str, float]] = {}
        for e1 in source_graph.columns:
            for e2 in target_graph.columns:
                scores = []
                pair_details = {}
                for cm in complex_matchers:
                    score = cm.compute(e1, e2, source_graph, target_graph)
                    scores.append(score)
                    pair_details[cm.name] = score
                sim_matrix[(e1, e2)] = average(scores)
                details_matrix[(e1, e2)] = pair_details

        selected = select_both_multiple(
            sim_matrix,
            source_graph.columns,
            target_graph.columns,
            max_n=self.__max_n,
            delta=self.__delta,
            threshold=self.__threshold,
        )

        # Format output and populate match_details
        output: dict[ColumnPair, float] = {}
        for (e1, e2), sim in selected.items():
            match = Match(
                target_table_name=target_input.name,
                target_column_name=e2.name,
                source_table_name=source_input.name,
                source_column_name=e1.name,
                similarity=float(sim),
            )
            match_dict = match.to_dict
            output.update(match_dict)
            # Store per-matcher details keyed by the ColumnPair
            col_pair = next(iter(match_dict))
            self._match_details[col_pair] = details_matrix[(e1, e2)]

        return output
