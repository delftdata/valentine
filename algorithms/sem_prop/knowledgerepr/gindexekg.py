from ctypes import *

from algorithms.sem_prop.api.annotation import MRS
from algorithms.sem_prop.api.apiutils import DRS
from algorithms.sem_prop.api.apiutils import Hit
from algorithms.sem_prop.api.apiutils import Relation
from algorithms.sem_prop.knowledgerepr import EKGapi
from algorithms.sem_prop.knowledgerepr.ekgstore.pg_store import PGStore

global gI
gI = None

def load_and_config_library(libname):
    global gI
    gI = cdll.LoadLibrary(libname)
    gI.neighbors.argtypes = [POINTER(POINTER(c_int32))]
    gI.neighbors.restype = int
    gI.all_paths.argtypes = [POINTER(POINTER(c_int32))]
    gI.all_paths.restype = int
    gI.release_array.argtypes = [POINTER(c_int32)]
    gI.release_array.restype = None
    gI.serialize_graph_to_disk.argtypes = [c_char_p]
    gI.serialize_graph_to_disk.restype = None


def get_gI_path(path: str):
    p = c_char_p(path.encode('utf-8'))
    return p


class GIndexEKG(EKGapi):

    def __init__(self, config=None):
        self.backend_type = EKGapi.BackEndType.G_INDEX
        self.pg = PGStore(db_ip=config.host,
                          db_port=config.port,
                          db_name=config.db_name,
                          db_user=config.db_user,
                          db_passwd=config.db_passwd)

    """
    WRITE OPS
    """

    def init(self, fields: (int, str, str, str, int, int, str)):
        print("Building schema relation...")
        for (nid, db_name, sn_name, fn_name, total_values, unique_values, data_type) in fields:
            #self.__id_names[nid] = (db_name, sn_name, fn_name, data_type)
            #self.__source_ids[sn_name].append(nid)
            uniqueness_ratio = None
            if float(total_values) > 0:
                uniqueness_ratio = float(unique_values) / float(total_values)
            self.pg.new_node(node_id=nid, uniqueness_ratio=uniqueness_ratio)
        print("Building schema relation...OK")

    def add_node(self, nid, uniqueness_ratio=None):
        self.pg.new_node(node_id=nid, uniqueness_ratio=uniqueness_ratio)

    def add_edge(self, node_src, node_target, relation, score):
        self.pg.new_edge(source_node_id=node_src, target_node_id=node_target, relation_type=relation, weight=score)

    """
    READ OPS
    """

    def neighbors_id(self, hit: Hit, relation: Relation) -> DRS:
        # nid = None
        # if isinstance(hit, Hit):
        #     nid = str(hit.nid)
        # if isinstance(hit, str):
        #     nid = hit
        # nid = str(nid)
        # data = []
        # neighbours = self.pg.connected_to(nid)
        # for k, v in neighbours.items():
        #     if relation in v:
        #         score = v[relation]['score']
        #         (db_name, source_name, field_name, data_type) = self.__id_names[k]
        #         data.append(Hit(k, db_name, source_name, field_name, score))
        # op = self.get_op_from_relation(relation)
        # o_drs = DRS(data, Operation(op, params=[hit]))
        # return o_drs
        return

    def md_neighbors_id(self, hit: Hit, md_neighbors: MRS, relation: Relation) -> DRS:
        return

    """
    ANALYTICAL OPS
    """

    def topk_nodes_by_degree(self, topk):
        return

    def enumerate_edges_of_type(self, relation):
        return

    """
    UTILS
    """

    def print_edges_of_type(self, relation):
        return


if __name__ == "__main__":

    load_and_config_library("graph_index.so")

    # examples on how to use gI

    nodes = gI.get_num_nodes()
    print("Nodes: " + str(nodes))
    edges = gI.get_num_edges()
    print("Edges: " + str(edges))

    output = POINTER(c_int32)()

    output_size = gI.all_paths(output, 1, 345, 1, 30)

    print("Python output: ")
    for idx in range(output_size):
        print(str(output[idx]))

    gI.release_array(output)

    path = get_gI_path("whatever_path")

    gI.serialize_graph_to_disk(path)
