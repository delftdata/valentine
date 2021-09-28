from ..base_column import BaseColumn


class LocalFSColumn(BaseColumn):

    def __init__(self, column_name: str, data: list, d_type: str, table_guid: tuple):
        self.__column_name = column_name
        self.__data = data
        self.__d_type = d_type
        self.__table_guid = table_guid

    @property
    def unique_identifier(self) -> str:
        return self.__table_guid[0] + "_" + self.__table_guid[1] + ":" + self.__column_name

    @property
    def name(self):
        return self.__column_name

    @property
    def data_type(self):
        return self.__d_type

    @property
    def data(self) -> list:
        return self.__data
