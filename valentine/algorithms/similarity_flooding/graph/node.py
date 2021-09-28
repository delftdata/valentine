class Node:
    """
    Class for describing a node of a graph.
    """

    def __init__(self, name, db):
        self.name = name
        self.db = db

    def __eq__(self, other):

        if isinstance(other, Node):
            return self.name == other.name and self.db == other.db

        return False

    def __hash__(self):
        return hash(self.name)
