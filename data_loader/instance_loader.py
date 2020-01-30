import pandas as pd

from data_loader.base_loader import BaseLoader
from data_loader.data_objects.table import Table


class InstanceLoader(BaseLoader):

    def __init__(self, data, schema=None):
        super().__init__(data, schema)
        self.table = None
        self.load_instances()

    def load_instances(self):
        assert self.data_path, 'Data path not set for Instance Loader'
        table_df = pd.read_csv(self.data_path, index_col=False).fillna('')
        table_name = self.data_path.split("/")[-1].split(".")[0]
        self.table = Table(table_name, table_df)

    @property
    def column_names(self):
        return list(map(lambda column: column.long_name, self.table.columns.values()))
