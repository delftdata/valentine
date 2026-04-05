import pandas as pd

from ..base_column import BaseColumn
from ..base_table import BaseTable
from .dataframe_column import DataframeColumn


class DataframeTable(BaseTable):
    def __init__(self, df: pd.DataFrame, name: str, instance_sample_size: int | None = 1000):
        if instance_sample_size is not None and instance_sample_size < 0:
            raise ValueError(
                f"instance_sample_size must be >= 0 or None, got {instance_sample_size}"
            )
        self.__table_name = name
        self.__columns = {}
        self.__instance_columns = {}
        self.__df = df
        self.__instance_sample_size = instance_sample_size
        self.__instances_df: pd.DataFrame | None = None

    @property
    def unique_identifier(self) -> str:
        return self.__table_name

    @property
    def name(self) -> str:
        return self.__table_name

    def get_columns(self) -> list[BaseColumn]:
        if not self.__columns:
            self.__columns = self.__build_columns_from_df(self.__df)
        return list(self.__columns.values())

    def get_column_names(self) -> list[str]:
        if not self.__columns:
            self.__columns = self.__build_columns_from_df(self.__df)
        return list(self.__columns.keys())

    def get_df(self) -> pd.DataFrame:
        return self.__df

    def get_instances_df(self) -> pd.DataFrame:
        if self.__instance_sample_size is None:
            return self.__df
        if self.__instance_sample_size == 0:
            return self.__df.iloc[0:0]
        if self.__instances_df is None:
            self.__instances_df = self.__build_instances_df(self.__instance_sample_size)
        return self.__instances_df

    def get_instances_columns(self) -> list[BaseColumn]:
        if not self.__instance_columns:
            instances_df = self.get_instances_df()
            self.__instance_columns = self.__build_columns_from_df(instances_df)
        return list(self.__instance_columns.values())

    @property
    def is_empty(self) -> bool:
        return self.__df.empty

    def __build_columns_from_df(self, df: pd.DataFrame) -> dict[str, BaseColumn]:
        columns: dict[str, BaseColumn] = {}
        for column_name, column_data in df.items():
            data = list(column_data.dropna().values)
            d_type = self.get_data_type(data, str(column_data.dtype))
            columns[column_name] = DataframeColumn(
                column_name, data, d_type, self.unique_identifier
            )
        return columns

    def __build_instances_df(self, max_rows: int) -> pd.DataFrame:
        rows = []
        for idx, row in self.__df.iterrows():
            has_value = False
            for value in row.values:
                if pd.notna(value) and str(value) != "":
                    has_value = True
                    break
            if has_value:
                rows.append(idx)
                if len(rows) >= max_rows:
                    break
        if not rows:
            return self.__df.iloc[0:0]
        return self.__df.loc[rows]
