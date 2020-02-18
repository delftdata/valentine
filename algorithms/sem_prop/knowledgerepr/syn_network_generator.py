from algorithms.sem_prop.api.apiutils import Relation
from algorithms.sem_prop.knowledgerepr.fieldnetwork import FieldNetwork
from algorithms.sem_prop.knowledgerepr.fieldnetwork import serialize_network


def generate_network_with(num_nodes=10, num_nodes_per_table=2, num_schema_sim=5, num_content_sim=5, num_pkfk=2):

    def node_generator():
        for i in range(num_nodes):
            for j in range(num_nodes_per_table):
                table_name = "synt" + str(i)
                element = (i, "syndb", table_name, "synf" + str(i), 100, 50, "T")
                yield element

    def gen_pairs_relation(source, num_relations):

        def create_multiple_conn_per_node(num_conn_per_node):
            for n in range(num_nodes):  # for each node
                for i in range(num_conn_per_node):  # num relations
                    next_node = (n + i + 1) % num_nodes
                    yield n, next_node

        def create_conn_every_x_node(source_idx, jump_size):
            src = source_idx
            for i in range(num_relations):
                next_node = (src + jump_size) % num_nodes
                yield src, next_node
                src = next_node

        if num_relations > num_nodes:
            #  Create multiple connections per node (roughly even)
            num_conn_per_node = int(num_relations / num_nodes)
            gen = create_multiple_conn_per_node(num_conn_per_node)
            for x in gen:
                yield x
        elif num_relations <= num_nodes:
            jump_size = int(num_nodes / num_relations)
            gen = create_conn_every_x_node(source, jump_size)
            for x in gen:
                yield x


    # Skeleton, columns and tables
    fn = FieldNetwork()
    node_g = node_generator()
    fn.init_meta_schema(node_g)

    # num schema sim
    gen_schema_sim = gen_pairs_relation(0, num_schema_sim)
    for src, trg in gen_schema_sim:
        #print(str(src) + " - " + str(trg))
        fn.add_relation(src, trg, Relation.SCHEMA_SIM, 0.2)

    # num content sim
    gen_schema_sim = gen_pairs_relation(1, num_content_sim)
    for src, trg in gen_schema_sim:
        fn.add_relation(src, trg, Relation.CONTENT_SIM, 0.5)

    # num pkfk
    gen_schema_sim = gen_pairs_relation(2, num_pkfk)
    for src, trg in gen_schema_sim:
        fn.add_relation(src, trg, Relation.PKFK, 0.8)

    return fn

if __name__ == "__main__":
    print("Synthetic Network Generator")

    ex = generate_network_with(num_nodes=1000, num_nodes_per_table=10, num_schema_sim=2000, num_content_sim=1500,
                               num_pkfk=500)

    path = "/Users/ra-mit/development/discovery_proto/syn/test1/"
    serialize_network(ex, path)

    ex = generate_network_with(num_nodes=10000, num_nodes_per_table=10, num_schema_sim=20000, num_content_sim=15000,
                               num_pkfk=5000)

    path = "/Users/ra-mit/development/discovery_proto/syn/test2/"
    serialize_network(ex, path)

    #ex._visualize_graph()
