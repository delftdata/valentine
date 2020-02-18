from collections import defaultdict
from enum import Enum

from bitarray import bitarray

num_edge_types = 4


class EdgeType(Enum):
    SCHEMA_SIM = 0
    CONTENT_SIM = 1
    PKFK = 2
    SEMANTIC = 3


class LiteGraph:

    def __init__(self):
        self._node_count = 0
        self._edge_count = 0
        self._node_index = dict()

    def add_node(self, nid):
        if nid not in self._node_index:
            self._node_index[nid] = defaultdict(bitarray)
            self._node_count += 1

    def add_edge(self, source, target, type):
        if source == target:
            return
        type = type.value
        self.add_node(source)
        self.add_node(target)
        if target not in self._node_index[source]:
            edge_type = bitarray(num_edge_types)
            edge_type.setall(False)
            edge_type[type] = True
            self._node_index[source][target] = edge_type
            self._edge_count += 1
        elif self._node_index[source][target][type] is False:
            self._node_index[source][target][type] = True
            self._edge_count += 1

    def add_undirected_edge(self, source, target, type):
        self.add_edge(source, target, type)
        self.add_edge(target, source, type)

    def neighbors(self, nid, type):
        type = type.value
        n = []
        nodes = self._node_index[nid]
        for target, edge_type in nodes.items():
            if edge_type[type]:
                n.append(target)
        return n