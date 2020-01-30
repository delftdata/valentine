import numpy as np
import pickle

from data_loader.data_objects.column import Column


class CorrelationClusteringColumn(Column):
    """
    A class used to represent a column of a table

    Attributes
    ----------
    __long_name : str
        a string containing (table_name + '__' + column_name)
    __name : str
        the name of the column
    data : list
        the data contained in the column
    __data_type : str
        the data type of the column
    quantiles : int
        the number of quantiles used in the histogram creation
    cardinality : int
        the cardinality of the column
    quantile_histogram : QuantileHistogram
        the quantile histogram representation of the column using the sorted ranks of the data

    Methods
    -------
    get_histogram()
        Returns the quantile histogram of the column

    get_original_name()
        Returns the column name

    get_original_data()
        Returns the original data instances

    get_long_name()
        Returns the compound name of the column (table_name + '_' + column_name)

    get_data_type()
        Returns the data type of the column
    """
    def __init__(self, name: str, data: list, table_name: str, quantiles: int):
        """
        Parameters
        ----------
        name : str
            The name of the column
        data : list
            The data instances of the column
        source_name : str
            The name of the table
        data_type: str
            The data type of the column
        quantiles: int
            The number of quantiles of the column's quantile histogram
        """
        super().__init__(name, data, table_name)
        self.quantiles = quantiles
        self.ranks = self.get_global_ranks(self.data)
        self.cardinality = len(set(data))
        self.quantile_histogram = None

    def get_histogram(self):
        """Returns the quantile histogram of the column"""
        return self.quantile_histogram

    def get_original_data(self):
        """Returns the original data instances"""
        return self.data

    @staticmethod
    def convert_data_type(string: str):
        try:
            f = float(string)
            if f.is_integer():
                return int(f)
            return f
        except ValueError:
            return string

    def get_global_ranks(self, column: list):
        with open('cache/global_ranks/ranks.pkl', 'rb') as pkl_file:
            global_ranks: dict = pickle.load(pkl_file)
            ranks = np.array(sorted([global_ranks[self.convert_data_type(x)] for x in column]))
            return ranks
