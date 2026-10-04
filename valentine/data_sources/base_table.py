from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .base_column import BaseColumn
from .utils import is_date


class BaseTable(ABC):
    """
    Abstract class representing a table.

    Subclasses wrap a concrete frame type (pandas DataFrame, Polars
    DataFrame, etc.) and expose its contents through a uniform
    column-oriented interface that the matching algorithms consume.
    """

    def __str__(self):
        __str: str = f"\tTable: {self.name}  |  {self.unique_identifier}\n"
        for column in self.get_columns():
            __str = __str + str(column.__str__())
        return __str

    @property
    @abstractmethod
    def unique_identifier(self) -> object:
        """Stable identifier used internally to key per-table state."""
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        """Table name. Becomes ``source_table``/``target_table`` in emitted `ColumnPair` instances."""
        raise NotImplementedError

    @abstractmethod
    def get_columns(self) -> list[BaseColumn]:
        """All columns in the table."""
        raise NotImplementedError

    @abstractmethod
    def get_df(self) -> Any:
        """Return the underlying frame object (pandas/Polars/…)."""
        raise NotImplementedError

    def get_instances_df(self) -> Any:
        """Return the frame used for instance-based sampling. Defaults to `get_df`."""
        return self.get_df()

    def get_instances_columns(self) -> list[BaseColumn]:
        """Return columns built from the instance-sampled frame. Defaults to `get_columns`."""
        return self.get_columns()

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        """Whether the table has zero rows."""
        raise NotImplementedError

    def get_guid_column_lookup(self) -> dict[str, object]:
        """Return a ``{column_name: column.unique_identifier}`` lookup."""
        return {column.name: column.unique_identifier for column in self.get_columns()}

    @staticmethod
    def get_data_type(data: list, d_type: str) -> str:
        """Map a dtype string to a canonical Valentine type.

        Recognises text-like types from both pandas (``object``,
        ``string``, ``str``) and Polars (``Utf8``, ``String``,
        ``Categorical``), as well as numeric and date categories.
        """
        text_like = d_type.lower() in (
            "object",
            "string",
            "str",
            "utf8",
            "categorical",
            "boolean",
            "bool",
        )
        new_d_type = ""
        if len(data) != 0:
            if text_like:
                if is_date(data[0]):
                    new_d_type = "date"
                else:
                    new_d_type = "varchar"
            elif d_type.lower().startswith("int") or d_type.lower().startswith("uint"):
                new_d_type = "int"
            elif d_type.lower().startswith("float") or d_type.lower() == "decimal":
                new_d_type = "float"
            elif d_type.lower() in ("date", "datetime", "time"):
                new_d_type = "date"
        elif text_like:
            new_d_type = "varchar"
        else:
            new_d_type = d_type
        return new_d_type
