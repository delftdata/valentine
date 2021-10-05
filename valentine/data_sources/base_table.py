from abc import ABC, abstractmethod
from typing import Dict, List

import pandas as pd

from .base_column import BaseColumn
from .utils import is_date


class BaseTable(ABC):
    """
    Abstract class representing a table
    """

    def __str__(self):
        __str: str = "\tTable: " + self.name + "  |  " + str(self.unique_identifier) + "\n"
        for column in self.get_columns():
            __str = __str + str(column.__str__())
        return __str

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

    @staticmethod
    def get_data_type(data: list, d_type: str) -> str:
        new_d_type = ""
        if len(data) != 0:
            if d_type == "object":
                if is_date(data[0]):
                    new_d_type = "date"
                else:
                    new_d_type = "varchar"
            elif d_type.startswith("int"):
                new_d_type = "int"
            elif d_type.startswith("float"):
                new_d_type = "float"
        else:
            if d_type == "object":
                new_d_type = "varchar"
            else:
                new_d_type = d_type
        return new_d_type
