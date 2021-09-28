class NodePair:
    """
    Class for describing a map pair in the propagation graph
    """

    def __init__(self, node1, node2):
        self.node1 = node1
        self.node2 = node2

    def __eq__(self, other):

        if isinstance(other, NodePair):
            return (self.node1 == other.node1 and self.node2 == other.node2) or \
                   (self.node1 == other.node2 and self.node2 == other.node1)

    def __hash__(self):
        return hash(self.node1.name + self.node2.name)