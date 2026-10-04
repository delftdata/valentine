from abc import ABC, abstractmethod


class BaseColumn(ABC):
    """Abstract class representing a single column.

    A ``BaseColumn`` knows its name, its values, and its detected data
    type. Implement this alongside `BaseTable`
    to plug a non-DataFrame backend into Valentine's matchers.
    """

    def __str__(self):
        return f"\t\tColumn: {self.name} <{self.data_type}>  |  {self.unique_identifier}\n"

    @property
    @abstractmethod
    def unique_identifier(self) -> object:
        """Stable identifier used internally to key per-column state."""
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        """Column name."""
        raise NotImplementedError

    @property
    @abstractmethod
    def data_type(self) -> str:
        """Detected type: one of ``"varchar"``, ``"int"``, ``"float"``, or ``"date"``."""
        raise NotImplementedError

    @property
    @abstractmethod
    def data(self) -> list:
        """The column's values."""
        raise NotImplementedError

    @property
    def size(self) -> int:
        """Number of elements in `data`."""
        return len(self.data)

    @property
    def is_empty(self) -> bool:
        """``True`` when `size` is ``0``."""
        return self.size == 0
