class BaseColumn:
    def __init__(self, column_name: str, data: list, table_name: str, data_type: str = 'str'):
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
        self.long_name = table_name + '__' + column_name
        self.name = column_name
        self.data = list(filter(lambda d: d != '', data))  # remove the empty strings
        self.data_type = data_type
