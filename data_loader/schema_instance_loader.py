from data_loader.instance_loader import InstanceLoader
from data_loader.schema_loader import SchemaLoader


class SchemaInstanceLoader(SchemaLoader, InstanceLoader):
    """
    A class used to represent a combined loader (SchemaInstanceLoader), both schema and instance
    """

    def __init__(self, data: str, schema: str):
        """
        Parameters
        ----------
        data : str
            The path to the data file
        schema : str
            The path to the schema file
        """
        super(SchemaLoader, self).__init__(data=data, schema=None)
        super(InstanceLoader, self).__init__(data=None, schema=schema)
