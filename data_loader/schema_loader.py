import json

from data_loader.base_loader import BaseLoader


class SchemaLoader(BaseLoader):

    def __init__(self, schema, data=None):
        super().__init__(data, schema)
        self.schema_name = self.schema_path.split("/")[-1].split(".")[0]
        self.schema = None
        self.load_schema()

    def load_schema(self):
        with open(self.schema_path, "r") as schema_file:
            self.schema = json.load(schema_file)
