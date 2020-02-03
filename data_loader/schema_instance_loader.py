from data_loader.instance_loader import InstanceLoader
from data_loader.schema_loader import SchemaLoader


class SchemaInstanceLoader(SchemaLoader, InstanceLoader):
    def __init__(self, data, schema):
        super(SchemaLoader, self).__init__(data=data, schema=None)
        super(InstanceLoader, self).__init__(data=None, schema=schema)
