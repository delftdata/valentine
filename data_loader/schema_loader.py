import json

from data_loader.base_loader import BaseLoader


class SchemaLoader(BaseLoader):
    """
     A class used to represent schema only data loader

    Attributes
    ----------
    schema_name : str
        The name of the table
    schema  : dict
        The schema represented a dictionary with keys the column names and values the data types

    Methods
    -------
    load_instances()
        Function that loads the data instances

    column_names()
        Returns the original data instances
    """

    def __init__(self, schema: str, data=None):
        """
        Parameters
        ----------
        data : str
            The path to the data file
        schema : str
            The path to the schema file
        """
        super().__init__(data, schema)
        self.schema_name = self.schema_path.split("/")[-1].split(".")[0]
        self.schema: dict = {}
        self.load_schema()

    def load_schema(self):
        """  Function that loads the schema """
        with open(self.schema_path, "r") as schema_file:
            self.schema = json.load(schema_file)

    @property
    def column_names(self):
        """
        Function that returns the column names of the schema

        Returns
        -------
        list
            The column names
        """
        return list(self.schema.keys())

    @property
    def column_name_type_pairs(self):
        """
        Function that returns all the column type/name pairs of the schema
        Returns
        -------
        list
            The column type/name pairs
        """
        return [(column, props["type"]) for column, props in self.schema.items()]
