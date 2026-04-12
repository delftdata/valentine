from __future__ import annotations

import polars as pl

from ..base_column import BaseColumn
from ..base_table import BaseTable
from .polars_column import PolarsColumn


class PolarsTable(BaseTable):
    """A :class:`BaseTable` backed by a Polars DataFrame.

    Drop-in replacement for :class:`DataframeTable`; all matching
    algorithms work identically because they consume the
    :class:`BaseTable` / :class:`BaseColumn` interface, not the
    underlying frame type.

    Parameters
    ----------
    df : pl.DataFrame
        The Polars DataFrame to wrap.
    name : str
        A human-readable table name.
    instance_sample_size : int | None
        Maximum number of non-empty rows kept for instance-based
        matchers. ``None`` keeps all rows, ``0`` keeps none.
    """

    def __init__(
        self,
        df: pl.DataFrame,
        name: str,
        instance_sample_size: int | None = 1000,
    ):
        if instance_sample_size is not None and instance_sample_size < 0:
            raise ValueError(
                f"instance_sample_size must be >= 0 or None, got {instance_sample_size}"
            )
        self.__table_name = name
        self.__df = df
        self.__instance_sample_size = instance_sample_size
        self.__columns: dict[str, BaseColumn] = {}
        self.__instance_columns: dict[str, BaseColumn] = {}
        self.__instances_df: pl.DataFrame | None = None

    @property
    def unique_identifier(self) -> str:
        return self.__table_name

    @property
    def name(self) -> str:
        return self.__table_name

    def get_columns(self) -> list[BaseColumn]:
        if not self.__columns:
            self.__columns = self.__build_columns(self.__df)
        return list(self.__columns.values())

    def get_column_names(self) -> list[str]:
        if not self.__columns:
            self.__columns = self.__build_columns(self.__df)
        return list(self.__columns.keys())

    def get_df(self) -> pl.DataFrame:
        return self.__df

    def get_instances_df(self) -> pl.DataFrame:
        if self.__instance_sample_size is None:
            return self.__df
        if self.__instance_sample_size == 0:
            return self.__df.head(0)
        if self.__instances_df is None:
            self.__instances_df = self.__build_instances_df(self.__instance_sample_size)
        return self.__instances_df

    def get_instances_columns(self) -> list[BaseColumn]:
        if not self.__instance_columns:
            instances_df = self.get_instances_df()
            self.__instance_columns = self.__build_columns(instances_df)
        return list(self.__instance_columns.values())

    @property
    def is_empty(self) -> bool:
        return len(self.__df) == 0

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def __build_columns(self, df: pl.DataFrame) -> dict[str, BaseColumn]:
        columns: dict[str, BaseColumn] = {}
        for col_name in df.columns:
            series = df[col_name]
            data = series.drop_nulls().to_list()
            d_type = self.get_data_type(data, str(series.dtype))
            columns[col_name] = PolarsColumn(col_name, data, d_type, self.unique_identifier)
        return columns

    def __build_instances_df(self, max_rows: int) -> pl.DataFrame:
        # Keep only rows that have at least one non-null, non-empty value.
        # Polars makes this easy: a row is "non-empty" when not ALL
        # columns are null. We then take the first ``max_rows``.
        mask = pl.lit(False)
        for col_name in self.__df.columns:
            col = self.__df[col_name]
            non_null = col.is_not_null()
            # For string columns, additionally exclude empty strings.
            if col.dtype in (pl.Utf8, pl.String):
                non_null = non_null & (col != "")
            mask = mask | non_null
        filtered = self.__df.filter(mask)
        return filtered.head(max_rows)
