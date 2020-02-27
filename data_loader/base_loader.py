from abc import ABC


class BaseLoader(ABC):
    """
    An abstract class that is the base of a data loader
    """

    def __init__(self, data: str = None, schema: str = None):
        """
        Parameters
        ----------
        data : str
            The path of the column data
        schema : str
            The path of the schema data
        """
        self.data_path = data
        self.schema_path = schema
