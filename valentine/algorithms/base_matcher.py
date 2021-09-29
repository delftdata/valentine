from abc import ABC, abstractmethod
from typing import List, Dict

from ..data_sources.base_table import BaseTable


class BaseMatcher(ABC):

    @abstractmethod
    def get_matches(self, source_input: BaseTable, target_input: BaseTable) -> List[Dict]:
        """
        Get the column matches from a schema matching algorithm
        :returns List of matches
        """
        raise NotImplementedError
