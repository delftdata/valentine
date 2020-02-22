import json

from data_loader.base_loader import BaseLoader


class SchemaLoader(BaseLoader):

    def __init__(self, schema, data=None):
        super().__init__(data, schema)
        self.schema_name = self.schema_path.split("/")[-1].split(".")[0]
        self.schema: dict = {}
        self.load_schema()

    def load_schema(self):
        with open(self.schema_path, "r") as schema_file:
            self.schema = json.load(schema_file)

    @property
    def column_names(self):
        return list(self.schema.keys())

    @property
    def column_name_type_pairs(self):
        return [(column, props["type"]) for column, props in self.schema.items()]
