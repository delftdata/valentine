class Node:
    """
        Class for describing a node of a graph.
    """

    def __init__(self, name, db):
        self.name = name
        self.long_name = None
        self.db = db

    def add_long_name(self, table_name, table_guid, column_name, column_guid):
        self.long_name = (table_name, table_guid, column_name, column_guid)

    def __eq__(self, other):

        if isinstance(other, Node):
            return self.name == other.name and self.db == other.db

        return False

    def __hash__(self):
        return hash(self.name)
