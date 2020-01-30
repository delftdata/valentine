from abc import ABC


class BaseLoader(ABC):

    def __init__(self, data=None, schema=None):
        self.data_path = data
        self.schema_path = schema
