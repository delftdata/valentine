from abc import ABC, abstractmethod
from typing import List, Dict, Union

from ..data_sources.base_db import BaseDB
from ..data_sources.base_table import BaseTable


class BaseMatcher(ABC):

    @abstractmethod
    def get_matches(self, source_input: Union[BaseDB, BaseTable], target_input: Union[BaseDB, BaseTable]) -> List[Dict]:
        """
        Get the column matches from a schema matching algorithm
        :returns List of matches
        """
        raise NotImplementedError
