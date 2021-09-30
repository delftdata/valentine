from abc import ABC, abstractmethod
from typing import Dict, List

import pandas as pd

from .base_column import BaseColumn


class BaseTable(ABC):
    """
    Abstract class representing a table
    """

    @property
    @abstractmethod
    def unique_identifier(self) -> object:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_columns(self) -> List[BaseColumn]:
        raise NotImplementedError

    @abstractmethod
    def get_df(self) -> pd.DataFrame:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        raise NotImplementedError

    def get_guid_column_lookup(self) -> Dict[str, object]:
        return {column.name:  column.unique_identifier for column in self.get_columns()}
