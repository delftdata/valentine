from enum import Enum

from algorithms.sem_prop.api.annotation import MRS
from algorithms.sem_prop.api.apiutils import DRS
from algorithms.sem_prop.api.apiutils import Hit
from algorithms.sem_prop.api.apiutils import OP
from algorithms.sem_prop.api.apiutils import Relation
from algorithms.sem_prop.knowledgerepr.gindexekg import GIndexEKG
from algorithms.sem_prop.knowledgerepr.inmemoryekg import InMemoryEKG


class BackEndType(Enum):
    IN_MEMORY = 0
    POSTGRES = 1
    JANUS = 2
    NEO = 3
    VIRTUOSO = 4
    G_INDEX = 5


class EKGapi:

    def __init__(self, backend_type: BackEndType, config=None):
        self.backend_type = backend_type
        if self.backend_type == BackEndType.IN_MEMORY:
            self.backend = InMemoryEKG(config)
        elif self.backend_type == BackEndType.G_INDEX:
            self.backend = GIndexEKG(config)

    @staticmethod
    def get_op_from_relation(relation):
        if relation == Relation.CONTENT_SIM:
            return OP.CONTENT_SIM
        if relation == Relation.ENTITY_SIM:
            return OP.ENTITY_SIM
        if relation == Relation.PKFK:
            return OP.PKFK
        if relation == Relation.SCHEMA:
            return OP.TABLE
        if relation == Relation.SCHEMA_SIM:
            return OP.SCHEMA_SIM
        if relation == Relation.MEANS_SAME:
            return OP.MEANS_SAME
        if relation == Relation.MEANS_DIFF:
            return OP.MEANS_DIFF
        if relation == Relation.SUBCLASS:
            return OP.SUBCLASS
        if relation == Relation.SUPERCLASS:
            return OP.SUPERCLASS
        if relation == Relation.MEMBER:
            return OP.MEMBER
        if relation == Relation.CONTAINER:
            return OP.CONTAINER

    """
    WRITE OPS
    """

    def init(self, fields: (int, str, str, str, int, int, str)):
        self.backend.init_meta_schema(fields)

    def add_node(self, nid, cardinality=None):
        self.backend.add_field(nid)

    def add_nodes(self, list_of_fields):
        self.backend.add_fields(list_of_fields)

    def add_edge(self, node_src, node_target, relation, score):
        self.backend.add_relation(node_src, node_target, relation, score)

    """
    READ OPS
    """

    def neighbors_id(self, hit: Hit, relation: Relation) -> DRS:
        return self.backend.neighbors_id(hit, relation)

    def md_neighbors_id(self, hit: Hit, md_neighbors: MRS, relation: Relation) -> DRS:
        return self.backend.md_neighbors_id(hit, md_neighbors, relation)

    """
    ANALYTICAL OPS
    """

    def topk_nodes_by_degree(self, topk):
        self.backend.topk_nodes_by_degree(topk)

    def enumerate_edges_of_type(self, relation):
        self.backend.enumerate_edges_of_type(relation)

    """
    UTILS
    """

    def print_edges_of_type(self, relation):
        self.backend.print_edges_of_type(relation)