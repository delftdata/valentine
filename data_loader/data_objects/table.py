import pandas as pd

from data_loader.data_objects.column import Column


class Table:
    """
    A class describing a relational table

    Attributes
    ----------
    name : str
        The name of the table
    columns : dict
        A dictionary containing column_names and their Column objects

    Methods
    -------
    init_columns(df) : dict
        Function that initializes the Column objects that belong to the table
    get_data() : list
        Function that returns all the table's data i.e. the Column objects
    get_column_data() : Column
        Function that returns a column of the table based on the given column name

    """
    def __init__(self, name: str, df: pd.DataFrame):
        """
        Parameters
        ----------
        name : str
            The table name
        df : pd.DataFrame
            The DataFrame containing the table's data
        """
        self.name = name
        self.columns = self.init_columns(df)
        self.df = df

    def init_columns(self, df: pd.DataFrame):
        """
        Function that initializes the Column objects that belong to the table

        Parameters
        ----------
        df : pd.DataFrame
            The pandas DataFrame representation of the table
        Returns
        -------
        dict
            A dictionary containing column_names and their Column objects
        """
        columns = dict()
        for (column_name, column_data) in df.iteritems():
            columns[column_name] = Column(column_name, list(column_data.values), self.name)
        return columns

    def get_data(self):
        """
        Function that returns all the table's data i.e. the Column objects

        Returns
        -------
        list
            A list containing all the Column objects of the table
        """
        data = []
        for column in self.columns.values():
            data.extend(column.data)
        return data

    def get_column_data(self, column_name):
        """
        Function that returns a column of the table based on the given column name

        Parameters
        ----------
        column_name : str
            The column name

        Returns
        -------
        Column
            The Column object attached to the column name
        """
        return self.columns[column_name].data

    @property
    def as_df(self):
        return self.df
