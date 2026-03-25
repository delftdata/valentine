from abc import ABC, abstractmethod

from ..data_sources.base_table import BaseTable


class BaseMatcher(ABC):
    @abstractmethod
    def get_matches(
        self, source_input: BaseTable, target_input: BaseTable
    ) -> dict[tuple[tuple[str, str], tuple[str, str]], float]:
        """
        Get the column matches from a schema matching algorithm
        :returns List of matches
        """
        raise NotImplementedError
