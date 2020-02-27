import numpy as np
import pickle

from data_loader.data_objects.column import Column
from utils.utils import convert_data_type


class CorrelationClusteringColumn(Column):
    """
    A class used to represent a column of a table in the Correlation Clustering algorithm

    Attributes
    ----------
    data : list
        The data contained in the column
    quantiles : int
        The number of quantiles used in the histogram creation
    ranks : list
        A list containing the ranks of the column
    quantile_histogram : QuantileHistogram
        The quantile histogram representation of the column using the sorted ranks of the data

    Methods
    -------
    get_histogram()
        Returns the quantile histogram of the column

    get_original_data()
        Returns the original data instances
    """

    def __init__(self, name: str, data: list, table_name: str, dataset_name: str, quantiles: int):
        """
        Parameters
        ----------
        name : str
            The name of the column
        data : list
            The data instances of the column
        table_name : str
            The name of the table
        dataset_name : str
            The name of the dataset
        quantiles: int
            The number of quantiles of the column's quantile histogram
        """
        super().__init__(name, data, table_name)
        self.quantiles = quantiles
        self.dataset_name = dataset_name
        self.ranks = self.get_global_ranks(self.data, self.dataset_name)
        self.quantile_histogram = None

    def get_histogram(self):
        """Returns the quantile histogram of the column"""
        return self.quantile_histogram

    def get_original_data(self):
        """Returns the original data instances"""
        return self.data

    @staticmethod
    def get_global_ranks(column: list, dataset_name: str):
        """
        Function that gets the column data, reads the pickled global ranks and produces a ndarray that contains the
        ranks of the data .

        Parameters
        ----------
        column : list
            The column data
        dataset_name : str
            The name of the dataset

        Returns
        -------
        ndarray
            The ndarray that contains the ranks of the data
        """
        with open('cache/global_ranks/' + dataset_name + '.pkl', 'rb') as pkl_file:
            global_ranks: dict = pickle.load(pkl_file)
            ranks = np.array(sorted([global_ranks[convert_data_type(x)] for x in column]))
            return ranks
