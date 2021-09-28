from abc import ABC, abstractmethod


class BaseColumn(ABC):
    """
    Abstract class representing a column
    """

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

    def __str__(self):
        return "\t\tColumn: " + self.name + " <" + self.data_type + ">  |  " + str(self.unique_identifier) + "\n"
