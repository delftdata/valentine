import time
from ctypes import *

clib = cdll.LoadLibrary("graph_index.so")
clib.neighbors.argtypes = [POINTER(POINTER(c_int32))]
clib.neighbors.restype = int
clib.all_paths.argtypes = [POINTER(POINTER(c_int32))]
clib.all_paths.restype = int
clib.release_array.argtypes = [POINTER(c_int32)]
clib.release_array.restype = None
clib.serialize_graph_to_disk.argtypes = [c_char_p]
clib.serialize_graph_to_disk.restype = None


def deserialize_graph(path_str):
    path = c_char_p(path_str.encode('utf-8'))
    clib.deserialize_graph(path)


def serialize_graph(path_str):
    clib.serialize_graph_to_disk(path_str)


def get_num_nodes():
    nodes = clib.get_num_nodes()
    return nodes


def get_num_edges():
    edges = clib.get_num_edges()
    return edges


def neighbor_query(src_id, type):
    s = time.time()
    array = POINTER(c_int32)()
    size = clib.neighbors(array, src_id, type)
    results = [array[idx] for idx in range(size)]
    clib.release_array(array)
    e = time.time()
    print(str(e-s) + "s")
    return results


def path_query(src_id, tgt_id, type, max_hops):
    s = time.time()
    output = POINTER(c_int32)()
    output_size = clib.all_paths(output, src_id, tgt_id, type, max_hops)
    results = [output[idx] for idx in range(output_size)]
    total_paths = 0
    for idx in range(output_size):
        el = output[idx]
        if el == -1:
            total_paths += 1
    clib.release_array(output)
    e = time.time()
    print("total paths: " + str(total_paths))
    print(str(e-s) + "s")
    return results
