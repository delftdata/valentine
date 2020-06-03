import pandas as pd

from data_loader.base_loader import BaseLoader
from data_loader.data_objects.table import Table
from utils.utils import get_encoding, get_delimiter


class InstanceLoader(BaseLoader):
    """
    A class used to represent instance only data loader

    Attributes
    ----------
    table : Table
        The table inside the InstanceLoader

    Methods
    -------
    load_instances()
        Function that loads the data instances

    column_names()
        Returns the original data instances
    """

    def __init__(self, data: str, schema: str = None):
        """
        Parameters
        ----------
        data : str
            The path to the data file
        schema : str
            The path to the schema file
        """
        super().__init__(data, schema)
        self.table = None
        self.load_instances()

    def load_instances(self):
        """ Function that loads the data instances """
        assert self.data_path, 'Data path not set for Instance Loader'
        table_df = pd.read_csv(self.data_path,
                               index_col=False,
                               encoding=get_encoding(self.data_path),
                               sep=get_delimiter(self.data_path)).fillna('')
        table_name = self.data_path.split("/")[-1].split(".")[0]
        self.table = Table(table_name, table_df)

    @property
    def column_names(self):
        """
        Function that returns the loader's column names

        Returns
        -------
        list
            All the column names
        """
        return list(map(lambda column: column.long_name, self.table.columns.values()))
