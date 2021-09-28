import numpy as np
import pickle

from ...data_sources.base_column import BaseColumn
from ...utils.utils import convert_data_type, get_project_root


class CorrelationClusteringColumn(BaseColumn):
    """
    A class used to represent a column of a table in the Correlation Clustering algorithm

    Attributes
    ----------
    data : list
        The data contained in the column
    quantiles : int
        The number of quantiles used in the histogram creation
    __ranks : list
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
    def __init__(self, name: str, column_uid: str, data: list,
                 table_name: str, table_guid: str, quantiles: int, uuid: str):
        """
        Parameters
        ----------
        name : str
            The name of the column
        data : list
            The data instances of the column
        table_name : str
            The name of the table
        quantiles: int
            The number of quantiles of the column's quantile histogram
        """
        self.__name = name
        self.__uid = column_uid
        self.__data = data
        self.__table_name = table_name
        self.__table_guid = table_guid
        self.__quantiles = quantiles
        self.task_uuid = uuid
        self.__ranks = self.get_global_ranks(self.__data, self.task_uuid)
        self.quantile_histogram = None

    @property
    def unique_identifier(self) -> object:
        return self.__uid

    @property
    def name(self) -> str:
        return self.__name

    @property
    def data_type(self) -> str:
        return "varchar"

    @property
    def data(self) -> list:
        return self.__data

    @property
    def table_name(self) -> str:
        return self.__table_name

    @property
    def long_name(self):
        return self.table_name, self.__table_guid, self.name, self.unique_identifier

    @property
    def ranks(self):
        return self.__ranks

    @staticmethod
    def get_global_ranks(column: list, task_uuid: str):
        """
        Function that gets the column data, reads the pickled global ranks and produces a ndarray that contains the
        ranks of the data .

        Parameters
        ----------
        column : list
            The column data
        task_uuid : str
            The unique identifier of the task

        Returns
        -------
        ndarray
            The ndarray that contains the ranks of the data
        """
        with open(f'{get_project_root()}/algorithms/distribution_based/cache/global_ranks/{task_uuid}/ranks.pkl',
                  'rb') as pkl_file:
            global_ranks: dict = pickle.load(pkl_file)
            ranks = np.array(sorted([global_ranks[dt_x] for x in column
                                     if (dt_x := convert_data_type(x)) in global_ranks]))
            return ranks
