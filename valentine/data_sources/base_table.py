from abc import ABC, abstractmethod

import pandas as pd

from .base_column import BaseColumn
from .utils import is_date


class BaseTable(ABC):
    """
    Abstract class representing a table
    """

    def __str__(self):
        __str: str = f"\tTable: {self.name}  |  {self.unique_identifier}\n"
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
    def get_columns(self) -> list[BaseColumn]:
        raise NotImplementedError

    @abstractmethod
    def get_df(self) -> pd.DataFrame:
        raise NotImplementedError

    def get_instances_df(self) -> pd.DataFrame:
        """Return the DataFrame used for instance-based sampling."""
        return self.get_df()

    def get_instances_columns(self) -> list[BaseColumn]:
        """Return columns built from the instance-sampled DataFrame."""
        return self.get_columns()

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        raise NotImplementedError

    def get_guid_column_lookup(self) -> dict[str, object]:
        return {column.name: column.unique_identifier for column in self.get_columns()}

    @staticmethod
    def get_data_type(data: list, d_type: str) -> str:
        # ``d_type`` is the string form of ``column.dtype``. Pandas has
        # three textual categories worth handling here: the legacy
        # ``object`` dtype, the nullable ``string`` dtype, and the
        # modern ``str`` dtype (pandas 2.1+). All three should be
        # treated as candidate text (falling back to ``date`` only
        # when the first value parses as a date).
        text_like = d_type in ("object", "string", "str")
        new_d_type = ""
        if len(data) != 0:
            if text_like:
                if is_date(data[0]):
                    new_d_type = "date"
                else:
                    new_d_type = "varchar"
            elif d_type.startswith("int"):
                new_d_type = "int"
            elif d_type.startswith("float"):
                new_d_type = "float"
        elif text_like:
            new_d_type = "varchar"
        else:
            new_d_type = d_type
        return new_d_type
