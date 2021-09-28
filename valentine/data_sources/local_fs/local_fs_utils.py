import pandas as pd

from ...utils.utils import get_encoding, get_delimiter


def get_columns_from_local_fs_csv_file(table_path: str):
    return pd.read_csv(table_path, nrows=0).columns.tolist()


def get_pandas_df_from_local_fs_csv_file(table_path: str):
    return pd.read_csv(table_path,
                       index_col=False,
                       encoding=get_encoding(table_path),
                       sep=get_delimiter(table_path),
                       on_bad_lines='warn',
                       encoding_errors='ignore')
