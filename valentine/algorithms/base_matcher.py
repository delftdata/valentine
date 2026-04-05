from __future__ import annotations

from abc import ABC, abstractmethod
from itertools import combinations

from ..data_sources.base_table import BaseTable
from .match import ColumnPair


class BaseMatcher(ABC):
    @abstractmethod
    def get_matches(
        self, source_input: BaseTable, target_input: BaseTable
    ) -> dict[ColumnPair, float]:
        """Match columns between two tables.

        Parameters
        ----------
        source_input : BaseTable
            The source table.
        target_input : BaseTable
            The target table.

        Returns
        -------
        dict
            Mapping of :class:`ColumnPair` to similarity score.
        """
        raise NotImplementedError

    def get_matches_batch(self, tables: list[BaseTable]) -> dict[ColumnPair, float]:
        """Match columns across all unique pairs of tables.

        The default implementation calls :meth:`get_matches` for each pair
        independently. Algorithms that benefit from a holistic view of all
        tables (e.g. global TF-IDF corpus, global distribution ranks) can
        override this method.

        Parameters
        ----------
        tables : list[BaseTable]
            Two or more tables to match.

        Returns
        -------
        dict
            Combined matches across all pairs.
        """
        matches: dict[ColumnPair, float] = {}
        for t1, t2 in combinations(tables, 2):
            matches.update(self.get_matches(t1, t2))
        return matches

    @property
    def match_details(self) -> dict[ColumnPair, dict[str, float]]:
        """Per-pair score breakdowns from the most recent match call.

        Returns a mapping from :class:`ColumnPair` to a dictionary of
        ``{matcher_name: score}`` showing how each sub-matcher contributed
        to the final similarity. Empty by default; override in subclasses
        that combine multiple matchers (e.g. Coma).
        """
        return getattr(self, "_match_details", {})
