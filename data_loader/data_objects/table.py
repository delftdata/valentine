import pandas as pd

from data_loader.data_objects.column import Column


class Table:
    def __init__(self, name: str, df: pd.DataFrame):
        self.name = name
        self.columns = self.init_columns(df)

    def init_columns(self, df: pd.DataFrame):
        columns = dict()
        for (column_name, column_data) in df.iteritems():
            columns[column_name] = Column(column_name, list(column_data.values), self.name)
        return columns

    def get_data(self):
        data = []
        for column in self.columns.values():
            data.extend(column.data)
        return data

    def get_column_data(self, column_name):
        return self.columns[column_name].data
