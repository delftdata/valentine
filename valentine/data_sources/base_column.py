from abc import ABC, abstractmethod


class BaseColumn(ABC):
    """
    Abstract class representing a column
    """

    def __str__(self):
        return f"\t\tColumn: {self.name} <{self.data_type}>  |  {self.unique_identifier}\n"

    @property
    @abstractmethod
    def unique_identifier(self) -> object:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def data_type(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def data(self) -> list:
        raise NotImplementedError

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def is_empty(self) -> bool:
        return self.size == 0
