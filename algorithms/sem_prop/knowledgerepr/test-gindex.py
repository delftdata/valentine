import time
from ctypes import *

mylib = cdll.LoadLibrary("graph_index.so")
mylib.neighbors.argtypes = [POINTER(POINTER(c_int32))]
mylib.neighbors.restype = int
mylib.all_paths.argtypes = [POINTER(POINTER(c_int32))]
mylib.all_paths.restype = int
mylib.release_array.argtypes = [POINTER(c_int32)]
mylib.release_array.restype = None



path = c_char_p("/Users/ra-mit/Downloads/graph-1.csv".encode('utf-8'))
mylib.deserialize_graph(path)


#import networkx as nx
#
#nodes = 1000
#edge_probability = 0.3  # fairly populated graph
#random_g = nx.fast_gnp_random_graph(nodes, edge_probability)
#
#for src_id, tgt_id in random_g.edges():
#    mylib.add_node(src_id)
#    mylib.add_node(tgt_id)
#    mylib.add_edge(src_id, tgt_id, 1)
#
#mylib.serialize_graph_to_disk(path)
#
#exit()

nodes = mylib.get_num_nodes()
print("Nodes: " + str(nodes))
edges = mylib.get_num_edges()
print("Edges: " + str(edges))

# PATH QUERY
stime = time.time()

output = POINTER(c_int32)()

output_size = mylib.all_paths(output, 1, 0, 0, 30)

print("Python output, total paths: " + str(output_size))
for idx in range(output_size):
    print(str(output[idx]))

mylib.release_array(output)

etime = time.time()
print("Total time: " + str(etime-stime))

# NEIGHBOR QUERY
array = POINTER(c_int32)()

size = mylib.neighbors(array, 0, 0)

print("size: " + str(size))
print("Neighbors: ")

for idx in range(size):
    print(str(array[idx]))

mylib.release_array(array)

exit()


def path_query(src_id, tgt_id, type, max_hops):
    stime = time.time()
    output = POINTER(c_int32)()
    output_size = mylib.all_paths(output, src_id, tgt_id, type, max_hops)

    print("Python output, total paths: " + str(output_size))
    for idx in range(output_size):
        print(str(output[idx]))

    mylib.release_array(output)

    etime = time.time()
    print("Total time: " + str(etime - stime))


def neighbor_query(src_id, type):
    array = POINTER(c_int32)()

    size = mylib.neighbors(array, src_id, type)

    print("size: " + str(size))
    print("Neighbors: " + str(array))

    for idx in range(size):
        print(str(array[idx]))

    mylib.release_array(array)


path = c_char_p("random_graph_test.txt".encode('utf-8'))

mylib.serialize_graph_to_disk(path)

print("DONE!")
exit()

#mylib.add_node(77)
#mylib.add_node(78)
#mylib.add_node(79)
#
#mylib.add_edge(77, 78, 1)
#mylib.add_edge(77, 79, 1)

array = POINTER(c_int32)()

size = mylib.neighbors(array, 77, 1)

print("size: " + str(size))
print("Neighbors: " + str(array))

for idx in range(size):
    print(str(array[idx]))

mylib.release_array(array)

nodes = mylib.get_num_nodes()
print(str(nodes))

print("now strings")

mylib.serialize_graph_to_disk.argtypes = [c_char_p]
mylib.serialize_graph_to_disk.restype = None

path = c_char_p("here_for_test.txt".encode('utf-8'))

mylib.serialize_graph_to_disk(path)


