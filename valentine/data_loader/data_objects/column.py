
class Column:
    """
    A class used to represent a generic column of a relational table

     Attributes
    ----------
    table_name : str
        The name of the table that the column belongs
    name : str
        The name of the column
    data : list
        The data inside the column with all the NaN values removed
    size : int
        The size of the column

    Methods
    -------
    long_name() : tuple
        Function that returns a synthetic name of a column for the purposes of a unique identifier
    """

    def __init__(self, name: str, data: list, table_name: str):
        """
        Parameters
        ----------
        name : str
            The name of the column
        data : list
            The data inside the column
        table_name : str
            The name of the table that the column belongs
        """
        self.table_name = table_name
        self.name = name
        self.data = list(filter(lambda d: d != '', data))  # remove the NaN values
        self.size = len(data)

    @property
    def long_name(self):
        """
        Function that returns a synthetic name of a column for the purposes of a unique identifier

        Returns
        -------
        tuple
            The unique identifier of the column (table_name, column_name)
        """
        return self.table_name, self.name
