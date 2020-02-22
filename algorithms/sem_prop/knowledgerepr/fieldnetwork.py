import operator
import os
from collections import defaultdict

import matplotlib.pyplot as plt
import networkx as nx

from algorithms.sem_prop.api.annotation import MRS
from algorithms.sem_prop.api.apiutils import DRS
from algorithms.sem_prop.api.apiutils import Hit
from algorithms.sem_prop.api.apiutils import OP
from algorithms.sem_prop.api.apiutils import Operation
from algorithms.sem_prop.api.apiutils import Relation
from algorithms.sem_prop.api.apiutils import compute_field_id


def build_hit(sn, fn):
    nid = compute_field_id(sn, fn)
    return Hit(nid, sn, fn, -1)


class FieldNetwork:
    # The core graph
    __G = nx.MultiGraph()
    __id_names = dict()
    __source_ids = defaultdict(list)

    def __init__(self, graph=None, id_names=None, source_ids=None):
        if graph is None:
            self.__G = nx.MultiGraph()
        else:
            self.__G = graph
            self.__id_names = id_names
            self.__source_ids = source_ids

    def graph_order(self):
        return len(self.__id_names.keys())

    def get_number_tables(self):
        return len(self.__source_ids.keys())

    def iterate_ids(self):
        for k, _ in self.__id_names.items():
            yield k

    def iterate_ids_text(self):
        for k, v in self.__id_names.items():
            (db_name, source_name, field_name, data_type) = v
            if data_type == 'T':
                yield k

    def iterate_values(self) -> (str, str, str, str):
        for _, v in self.__id_names.items():
            yield v

    def get_fields_of_source(self, source) -> [int]:
        return self.__source_ids[source]

    def get_data_type_of(self, nid):
        _, _, _, data_type = self.__id_names[nid]
        return data_type

    def get_info_for(self, nids):
        info = []
        try:
            for nid in nids:
                db_name, source_name, field_name, data_type = self.__id_names[nid]
                info.append((nid, db_name, source_name, field_name))
        except KeyError as e:
            print(e)
        return info

    def get_hits_from_info(self, info):
        hits = [Hit(nid, db_name, s_name, f_name, 0) for nid, db_name, s_name, f_name in info]
        return hits

    def get_hits_from_table(self, table) -> [Hit]:
        nids = self.get_fields_of_source(table)
        info = self.get_info_for(nids)
        hits = [Hit(nid, db_name, s_name, f_name, 0) for nid, db_name, s_name, f_name in info]
        return hits

    def get_cardinality_of(self, node_id):
        c = self.__G.nodes[node_id]
        card = c['cardinality']
        if card is None:
            return 0  # no cardinality is like card 0
        return card

    def _get_underlying_repr_graph(self):
        return self.__G

    def _get_underlying_repr_id_to_field_info(self):
        return self.__id_names

    def _get_underlying_repr_table_to_ids(self):
        return self.__source_ids

    def _visualize_graph(self):
        nx.draw(self.__G)
        plt.show()

    def init_meta_schema(self, fields: (int, str, str, str, int, int, str)):
        """
        Creates a dictionary of id -> (dbname, sourcename, fieldname)
        and one of:
        sourcename -> id
        Then it also initializes the graph with all the nodes, e.g., ids and the cardinality
        for these, if any.
        :param fields:
        :return:
        """
        print("Building schema relation...")
        for (nid, db_name, sn_name, fn_name, total_values, unique_values, data_type) in fields:
            self.__id_names[nid] = (db_name, sn_name, fn_name, data_type)
            self.__source_ids[sn_name].append(nid)
            cardinality_ratio = None
            if float(total_values) > 0:
                cardinality_ratio = float(unique_values) / float(total_values)
            self.add_field(nid, cardinality_ratio)
        print("Building schema relation...OK")

    def add_field(self, nid, cardinality=None):
        """
        Creates a graph node for this field and adds it to the graph
        :param nid: the id of the node (a hash of dbname, sourcename and fieldname
        :param cardinality: the cardinality of the values of the node, if any
        :return: the newly added field node
        """
        self.__G.add_node(nid, cardinality=cardinality)
        return nid

    def add_fields(self, list_of_fields):
        """
        Creates a list of graph nodes from the list of fields and adds them to the graph
        :param list_of_fields: list of (source_name, field_name) tuples
        :return: the newly added list of field nodes
        """
        nodes = []
        for nid, sn, fn in list_of_fields:
            n = Hit(nid, sn, fn, -1)
            nodes.append(n)
        self.__G.add_nodes_from(nodes)
        return nodes

    def add_relation(self, node_src, node_target, relation, score):
        """
        Adds or updates the score of relation for the edge between node_src and node_target
        :param node_src: the source node
        :param node_target: the target node
        :param relation: the type of relation (edge)
        :param score: the numerical value of the score
        :return:
        """
        score = {'score': score}
        self.__G.add_edge(node_src, node_target, relation, score=score)

    def fields_degree(self, topk):
        degree = nx.degree(self.__G)
        sorted_degree = sorted(degree.items(), key=operator.itemgetter(1))
        sorted_degree.reverse()
        topk_nodes = sorted_degree[:topk]
        return topk_nodes

    def enumerate_relation(self, relation, as_str=True):
        seen_pairs = set()
        for nid in self.iterate_ids():
            db_name, source_name, field_name, data_type = self.__id_names[nid]
            hit = Hit(nid, db_name, source_name, field_name, 0)
            neighbors = self.neighbors_id(hit, relation)
            for n2 in neighbors:
                if not (n2.nid, nid) in seen_pairs:
                    seen_pairs.add((nid, n2.nid))
                    if as_str:
                        string = str(hit) + " - " + str(n2)
                        yield string
                    else:
                        yield hit, n2

    def print_relations(self, relation):
        total_relationships = 0
        if relation == Relation.CONTENT_SIM:
            for x in self.enumerate_relation(Relation.CONTENT_SIM):
                total_relationships += 1
                print(x)
        if relation == Relation.SCHEMA_SIM:
            for x in self.enumerate_relation(Relation.SCHEMA):
                total_relationships += 1
                print(x)
        if relation == Relation.PKFK:
            for x in self.enumerate_relation(Relation.PKFK):
                total_relationships += 1
                print(x)
        print("Total " + str(relation) + " relations: " + str(total_relationships))

    def get_op_from_relation(self, relation):
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

    def neighbors_id(self, hit: Hit, relation: Relation) -> DRS:
        if isinstance(hit, Hit):
            nid = str(hit.nid)
        if isinstance(hit, str):
            nid = hit
        nid = str(nid)
        data = []
        neighbours = self.__G[nid]
        for k, v in neighbours.items():
            if relation in v:
                score = v[relation]['score']
                (db_name, source_name, field_name, data_type) = self.__id_names[k]
                data.append(Hit(k, db_name, source_name, field_name, score))
        op = self.get_op_from_relation(relation)
        o_drs = DRS(data, Operation(op, params=[hit]))
        return o_drs

    def md_neighbors_id(self, hit: Hit, md_neighbors: MRS, relation: Relation) -> DRS:
        if isinstance(hit, Hit):
            nid = str(hit.nid)
        if isinstance(hit, str):
            nid = hit
        nid = str(nid)
        data = []
        score = 1.0 # TODO: return more meaningful score results
        for hit in md_neighbors:
            k = hit.target if hit.target != nid else hit.source
            (db_name, source_name, field_name, data_type) = self.__id_names[k]
            data.append(Hit(k, db_name, source_name, field_name, score))
        op = self.get_op_from_relation(relation)
        o_drs = DRS(data, Operation(op, params=[hit]))
        return o_drs

    def find_path_hit(self, source, target, relation, max_hops=5):

        def assemble_field_path_provenance(o_drs, path, relation):
            src = path[0]
            tgt = path[-1]
            origin = DRS([src], Operation(OP.ORIGIN))
            o_drs.absorb_provenance(origin)
            prev_c = src
            for c in path[1:-1]:
                nxt = DRS([c], Operation(OP.PKFK, params=[prev_c]))
                o_drs.absorb_provenance(nxt)
                prev_c = c
            sink = DRS([tgt], Operation(OP.PKFK, params=[prev_c]))
            o_drs = o_drs.absorb(sink)
            return o_drs

        def deep_explore(candidates, target_group, already_visited, path, max_hops):
            """
            Recursively depth-first explore the graph, checking if candidates are in target_group
            Returns (boolean, [])
            """
            local_max_hops = max_hops

            if local_max_hops == 0:
                return False

            # first check membership
            for c in candidates:
                if c in target_group:
                    path.insert(0, c)
                    return True

            # if not, then we explore these individually
            for c in candidates:
                if c in already_visited:
                    continue  # next candidate
                else:
                    already_visited.append(c)  # add candidate to set of already visited

                next_level_candidates = [x for x in self.neighbors_id(c, relation)]  # get next set of candidates

                if len(next_level_candidates) == 0:
                    continue
                next_max_hops = local_max_hops - 1  # reduce one level depth and go ahead
                success = deep_explore(next_level_candidates, target_group, already_visited, path, next_max_hops)
                if success:
                    path.insert(0, c)
                    return True
            return False  # if all nodes were already visited

        # maximum number of hops
        max_hops = 5

        o_drs = DRS([], Operation(OP.NONE))  # Carrier of provenance

        # TODO: same src == trg, etc

        path = []

        success = deep_explore([source], [target], [], path, max_hops)
        if success:
            o_drs = assemble_field_path_provenance(o_drs, path, relation)
            return o_drs
        else:
            return DRS([], Operation(OP.NONE))

    def find_path_table(self, source: str, target: str, relation, api, max_hops=3, lean_search=False):

        def assemble_table_path_provenance(o_drs, paths, relation):

            for path in paths:
                src, src_sibling = path[0]
                assert (src_sibling is None)  # sibling of source should be None, as source is an origin
                tgt, tgt_sibling = path[-1]
                origin = DRS([src], Operation(OP.ORIGIN))
                o_drs.absorb_provenance(origin)
                prev_c = src
                for c, sibling in path[1:-1]:
                    nxt = DRS([sibling], Operation(OP.PKFK, params=[prev_c]))
                    o_drs.absorb_provenance(nxt)
                    if c.nid != sibling.nid:  # avoid loop on head nodes of the graph
                        linker = DRS([c], Operation(OP.TABLE, params=[sibling]))
                        o_drs.absorb_provenance(linker)
                    prev_c = c
                sink = DRS([tgt_sibling], Operation(OP.PKFK, params=[prev_c]))

                #The join path at the target has None sibling
                if tgt is not None and tgt_sibling is not None and tgt.nid != tgt_sibling.nid:
                    o_drs = o_drs.absorb_provenance(sink)
                    linker = DRS([tgt], Operation(OP.TABLE, params=[tgt_sibling]))
                    o_drs.absorb(linker)
                else:
                    o_drs = o_drs.absorb(sink)
            return o_drs

        def check_membership(c, paths):
            for p in paths:
                for (s, sibling) in p:
                    if c.source_name == s.source_name:
                        return True
            return False

        def append_to_paths(paths, c):
            new_paths = []
            for p in paths:
                new_path = []
                new_path.extend(p)
                new_path.append(c)
                new_paths.append(new_path)
            return new_paths

        def get_table_neighbors(hit, relation, paths):
            results = []
            direct_neighbors = self.neighbors_id(hit, relation)

            # Rewriting results - filtering out results that are in the same table as the input. Rewriting prov
            direct_neighbors_list = [neigh for neigh in direct_neighbors if neigh.source_name != hit.source_name]
            op = self.get_op_from_relation(relation)
            direct_neighbors = DRS(direct_neighbors_list, Operation(op, params=[hit]))

            # FIXME: filter out already seen nodes here
            for n in direct_neighbors:
                if not check_membership(n, paths):
                    if lean_search:
                        t_neighbors = api._drs_from_table_hit_lean_no_provenance(n)
                    else:
                        t_neighbors = api.drs_from_table_hit(n)  # Brought old API
                    # t_neighbors = api.make_drs(n)  # XXX: this won't take all table neighbors, only the input one
                    results.extend([(x, n) for x in t_neighbors])
            return results  # note how we include hit as sibling of x here

        def dfs_explore(sources, targets, max_hops, paths):

            target_nid_set = set([x.nid for x in targets])

            # Check if sources have reached targets
            for (s, sibling) in sources:
                if s.nid in target_nid_set:  # faster than using the generic __eq__ of Hit
                #if s in targets:
                    # Append successful paths to found_paths
                    # T1.A join T2.B, and T2.C may join with other tables T3.D
                    # get_table_neighbors returns next_candidates (s, sibling) (C,B)
                    # in case T2 is the target add to the path (sibling, sibling)
                    # Otherwise (C,B)
                    if s.source_name == targets[0].source_name:
                        next_paths = append_to_paths(paths, (sibling, sibling))
                    else:
                        next_paths = append_to_paths(paths, (s, sibling))
                    found_paths.extend(next_paths)
                    return True

            # Check if no more hops are allowed:
            if max_hops == 0:
                return False  # not found path

            # Get next set of candidates and keep exploration
            for (s, sibling) in sources:
                next_candidates = get_table_neighbors(s, relation, paths)  # updated paths to test membership
                # recursive on new candidates, one fewer hop and updated paths
                if len(next_candidates) == 0:
                    continue
                next_paths = append_to_paths(paths, (s, sibling))
                dfs_explore(next_candidates, targets, max_hops - 1, next_paths)

        o_drs = DRS([], Operation(OP.NONE))  # Carrier of provenance

        # TODO: same src == trg, etc

        # src_drs = api.drs_from_table(source)
        # trg_drs = api.drs_from_table(target)
        src_drs = api.make_drs(source)
        trg_drs = api.make_drs(target)

        found_paths = []
        candidates = [(x, None) for x in src_drs]  # tuple carrying candidate and same-table attribute

        paths = [[]]  # to carry partial paths

        dfs_explore(candidates, [x for x in trg_drs], max_hops, paths)

        # for p in found_paths:
        #     print(p)

        o_drs = assemble_table_path_provenance(o_drs, found_paths, relation)

        return o_drs


def serialize_network_to_csv(network, path):
    nodes = set()
    G = network._get_underlying_repr_graph()
    with open(path + "edges.csv", 'w') as f:
        for src, tgt in G.edges_iter(data=False):
            s = str(src) + "," + str(tgt) + "," + "1\n"
            nodes.add(src)
            nodes.add(tgt)
            f.write(s)
    with open(path + "nodes.csv", 'w') as f:
        for n in nodes:
            s = str(n) + "," + "node\n"
            f.write(s)

def serialize_network(network, path):
    """
    Serialize the meta schema index
    :param network:
    :param path:
    :return:
    """
    G = network._get_underlying_repr_graph()
    id_to_field_info = network._get_underlying_repr_id_to_field_info()
    table_to_ids = network._get_underlying_repr_table_to_ids()

    # Make sure we create directory if this does not exist
    path = path + '/'  # force separator
    os.makedirs(os.path.dirname(path), exist_ok=True)

    nx.write_gpickle(G, path + "graph.pickle")
    nx.write_gpickle(id_to_field_info, path + "id_info.pickle")
    nx.write_gpickle(table_to_ids, path + "table_ids.pickle")


def deserialize_network(path):
    G = nx.read_gpickle(path + "graph.pickle")
    id_to_info = nx.read_gpickle(path + "id_info.pickle")
    table_to_ids = nx.read_gpickle(path + "table_ids.pickle")
    network = FieldNetwork(G, id_to_info, table_to_ids)
    return network


if __name__ == "__main__":
    print("Field Network")
