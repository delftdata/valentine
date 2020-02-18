import matplotlib.pyplot as plt
from collections import namedtuple
from collections import defaultdict
from collections import OrderedDict
from enum import Enum
import binascii
import networkx as nx
import time
import numpy as np
from bitarray import bitarray
import sys

python_version = (sys.version_info[0], sys.version_info[1], sys.version_info[2])

global_origin_id = 0

BaseHit = namedtuple(
    'Hit', 'nid, db_name, source_name, field_name, score')


class Hit(BaseHit):

    def __hash__(self):
        hsh = int(self.nid)
        return hsh

    def __eq__(self, other):
        target_type = type(other)
        if target_type == int:
            if self.nid == other:
                return True
        elif target_type == Hit:
            if self.nid == other.nid:
                return True
        elif other != None and self.nid == other.nid:
            return True
        return False

    def __eq__2(self, other):
        """
        XXX: probably safe to remove
        :param other:
        :return:
        """
        if isinstance(other, int):  # cover the case when id is provided directly
            if self.nid == other:
                return True
        elif isinstance(other, Hit):  # cover the case of comparing a Node with a Hit
            if self.nid == other.nid:
                return True
        elif other != None and self.nid == other.nid:  # cover the case of comparing two nodes
            return True
        return False

    def __dict__(self):
        if python_version == (3, 5, 0):
            return OrderedDict(zip(self._fields, self))
        else:
            return self._asdict()

    def __str__(self):
        return self.__repr__()


def compute_field_id(db_name, source_name, field_name):
    string = db_name + source_name + field_name
    nid = binascii.crc32(bytes(string, encoding="UTF-8"))
    return str(nid)


class Relation(Enum):
    SCHEMA = 0
    SCHEMA_SIM = 1
    CONTENT_SIM = 2
    ENTITY_SIM = 3
    PKFK = 5
    INCLUSION_DEPENDENCY = 6
    MEANS_SAME = 10
    MEANS_DIFF = 11
    SUBCLASS = 12
    SUPERCLASS = 13
    MEMBER = 14
    CONTAINER = 15

    def from_metadata(self):
        if self == \
                Relation.MEANS_SAME or self == \
                Relation.MEANS_DIFF or self == \
                Relation.SUBCLASS or self == \
                Relation.SUPERCLASS or self == \
                Relation.MEMBER or self == \
                Relation.CONTAINER:
            return True
        else:
            return False


class OP(Enum):
    NONE = 0  # for initial creation of DRS
    ORIGIN = 1
    KW_LOOKUP = 2
    SCHNAME_LOOKUP = 3
    SCHEMA_SIM = 4
    TABLE = 5
    CONTENT_SIM = 6
    PKFK = 7
    ENTITY_SIM = 8
    ENTITY_LOOKUP = 9
    MEANS_SAME = 10
    MEANS_DIFF = 11
    SUBCLASS = 12
    SUPERCLASS = 13
    MEMBER = 14
    CONTAINER = 15


class Operation:

    def __init__(self, op: OP, params=None):
        self._op = op
        self._params = params

    @property
    def op(self):
        return self._op

    @property
    def params(self):
        return self._params


class DRSMode(Enum):
    FIELDS = 0
    TABLE = 1


class Provenance:
    """
    Nodes are Hit (only). Origin nodes are given a special Hit object too.
    """

    def __init__(self, data, operation):
        self._p_graph = nx.MultiDiGraph()
        op = operation.op
        params = operation.params
        self.populate_provenance(data, op, params)
        # cache for leafs and heads
        self._cached_leafs_and_heads = (None, None)

    def prov_graph(self):
        self.invalidate_leafs_heads_cache()  # for safety invalidate cache
        return self._p_graph

    def swap_p_graph(self, new):
        self.invalidate_leafs_heads_cache()  # for safety invalidate cache
        self._p_graph = new

    def populate_provenance(self, data, op, params):
        if op == OP.NONE:
            # This is a carrier DRS, skip
            return
        elif op == OP.ORIGIN:
            for element in data:
                self._p_graph.add_node(element)
        # We check operations that come with parameters
        elif op == OP.SCHNAME_LOOKUP or op == OP.ENTITY_LOOKUP or op == OP.KW_LOOKUP:
            global global_origin_id
            hit = Hit(global_origin_id, params[0], params[0], params[0], -1)
            global_origin_id += 1
            self._p_graph.add_node(hit)
            for element in data:  # now we connect the new node to data with the op
                self._p_graph.add_node(element)
                self._p_graph.add_edge(hit, element, op)
        else:  # This all come with a Hit parameter
            hit = params[0]  # get the hit that comes with the op otherwise
            self._p_graph.add_node(hit)  # we add the param
            for element in data:  # now we connect the new node to data with the op
                self._p_graph.add_node(element)
                self._p_graph.add_edge(hit, element, op)
                self.invalidate_leafs_heads_cache()

    def get_leafs_and_heads(self):
        # Compute leafs and heads
        if self._cached_leafs_and_heads[0] is not None and self._cached_leafs_and_heads[1] is not None:
            return self._cached_leafs_and_heads[0], self._cached_leafs_and_heads[1]
        leafs = []
        heads = []
        for node in self._p_graph.nodes():
            pre = self._p_graph.predecessors(node)
            suc = self._p_graph.successors(node)
            no_cycles = set(pre) - set(suc)
            pre = list(no_cycles)
            if len(pre) == 0 and len(suc) == 0:
                # FIXME
                continue  # this should not happen anyway, what does it mean?
            if len(pre) == 0:
                leafs.append(node)
            if len(suc) == 0:
                heads.append(node)
        #self._cached_leafs_and_heads = (leafs, heads)
        return leafs, heads

    def invalidate_leafs_heads_cache(self):
        self._cached_leafs_and_heads = (None, None)

    def compute_paths_from_origin_to(self, a: Hit, leafs=None, heads=None):
        if leafs is None and heads is None:
            leafs, heads = self.get_leafs_and_heads()
        all_paths = []
        for l in leafs:
            paths = nx.all_simple_paths(self._p_graph, l, a)
            all_paths.extend(paths)
        return all_paths

    def compute_all_paths(self, leafs=None, heads=None):
        if leafs is None and heads is None:
            leafs, heads = self.get_leafs_and_heads()
        all_paths = []
        for h in heads:
            paths = self.compute_paths_with(h)
            all_paths.extend(paths)
        return all_paths

    def compute_paths_with(self, a: Hit, leafs=None, heads=None):
        """
        Given a node, a, in the provenance graph, return all paths that contain it.
        :param a:
        :return:
        """
        # FIXME: refactor with compute_paths_from_origin and all that
        if leafs is None and heads is None:
            leafs, heads = self.get_leafs_and_heads()
        all_paths = []
        if a in leafs:
            for h in heads:
                paths = nx.all_simple_paths(self._p_graph, a, h)
                all_paths.extend(paths)
        elif a in heads:
            for l in leafs:
                paths = nx.all_simple_paths(self._p_graph, l, a)
                all_paths.extend(paths)
        else:
            upstreams = []
            for l in leafs:
                paths = nx.all_simple_paths(self._p_graph, l, a)
                upstreams.extend(paths)
            downstreams = []
            for h in heads:
                paths = nx.all_simple_paths(self._p_graph, a, h)
                downstreams.extend(paths)

            if len(downstreams) > len(upstreams):
                for d in downstreams:
                    for u in upstreams:
                        all_paths.append(u + d)
            else:
                for u in upstreams:
                    for d in downstreams:
                        all_paths.append(u + d)
        return all_paths

    def explain_path(self, p: [Hit]):
        """
        Given a path in the provenance graph, traverse it, checking the edges that connect nodes and forming
        a story that explains how p is a result.
        :param p:
        :return:
        """
        def get_name_from_hit(h: Hit):
            name = h.source_name + ":" + h.field_name
            return name

        def get_string_from_edge_info(edge_info):
            string = ""
            for k, v in edge_info.items():
                string = string + str(k) + " ,"
            return string

        explanation = ""

        slice_range = lambda a: a + 1  # pairs
        for idx in range(len(p)):
            if (idx + 1) < len(p):
                pair = p[idx::slice_range(idx)]
                src, trg = pair
                explanation = explanation + get_name_from_hit(src) + " -> "
                edge_info = self._p_graph[src][trg]
                explanation = explanation + get_string_from_edge_info(edge_info) + " -> " \
                    + get_name_from_hit(trg) + '\n'
        return explanation


class DRS:

    class RankingCriteria(Enum):
        CERTAINTY = 0
        COVERAGE = 1

    def __init__(self, data, operation, lean_drs=False):
        self._data = data
        if not lean_drs:
            self._provenance = Provenance(data, operation)
        self._table_view = []
        self._idx = 0
        self._idx_table = 0
        self._mode = DRSMode.FIELDS
        # Ranking related variables
        self._ranked = False
        self._rank_data = defaultdict(dict)
        self._ranking_criteria = None
        self._chosen_rank = []
        self._origin_values_coverage = dict()

    def __iter__(self):
        return self

    def __next__(self):
        # Iterating fields mode
        if self._mode == DRSMode.FIELDS:
            if self._idx < len(self._data):
                self._idx += 1
                return self._data[self._idx - 1]
            else:
                self._idx = 0
                raise StopIteration
        # Iterating in table mode
        elif self._mode == DRSMode.TABLE:
            #  Lazy initialization of table view
            if len(self._table_view) == 0:
                table_set = set()
                for h in self._data:
                    t = h.source_name
                    table_set.add(t)
                self._table_view = list(table_set)
            if self._idx_table < len(self._table_view):
                self._idx_table += 1
                return self._table_view[self._idx_table - 1]
            else:
                self._idx_table = 0
                raise StopIteration

    def __dict__(self):
        '''
        prepares a dictionary to return for jsonification with the api
        '''
        mode = self.mode  # save state
        sources = {}
        edges = []
        self.set_fields_mode()

        # order fields under sources
        for x in self:
            table = x.source_name

            # create a new source_res if necessary
            if not sources.get(table, None):
                source_res = x.__dict__()
                sources[table] = {
                    'source_res': source_res,
                    'field_res': []}

            sources[table]['field_res'].append(x.__dict__())

        # convert edges into a dict
        for edge in self.get_provenance().prov_graph().edges():
            origin = edge[0].__dict__()
            destination = edge[1].__dict__()
            edges.append((origin, destination))

        self._mode = mode
        return {'sources': sources, 'edges': edges}

    @property
    def data(self):
        return self._data

    def set_data(self, data):
        self._data = list(data)
        self._table_view = []
        self._idx = 0
        self._idx_table = 0
        self._mode = DRSMode.FIELDS
        self._ranked = False
        return self

    @property
    def mode(self):
        return self._mode

    def size(self):
        return len(self.data)

    def get_provenance(self):
        return self._provenance

    """
    Provenance functions
    """

    def debug_print(self):
        len_data = len(self.data)
        total_nodes_provenance = len(self._provenance.prov_graph().nodes())
        print("Total data: " + str(len_data))
        print("Total nodes prov graph: " + str(total_nodes_provenance))

    def visualize_provenance(self, labels=False):
        if labels:
            nx.draw_networkx(self.get_provenance().prov_graph())
        else:
            nx.draw(self.get_provenance().prov_graph())
        plt.show()

    def absorb_provenance(self, drs, annotate_and_edges=False, annotate_or_edges=False):
        """
        Merge provenance of the input parameter into self, *not* the data.
        :param drs:
        :return:
        """
        def annotate_union_edges(label):
            # Find nodes that intersect (those that will contain add_edges)
            my_data = set(self.data)
            merging_data = set(drs.data)
            disjoint = my_data.intersection(
                merging_data)  # where a union is created
            # Now find the incoming edges to these nodes in each of the drs's
            node_and_edges = []
            for el in disjoint:
                input_edges1 = self._provenance.prov_graph().in_edges(el, data=True)
                input_edges2 = drs._provenance.prov_graph().in_edges(el, data=True)
                node_and_edges.append((input_edges1, input_edges2))
            # Now locate the nodes in the merged prov graph and annotate edges
            # with AND
            for input1, input2 in node_and_edges:
                for src1, tar1, dic1 in input1:
                    # this is the edge information
                    edge_data = merge[src1][tar1]
                    for e in edge_data:  # we iterate over each edge
                        # we assign the new metadata as data assigned to the
                        # edge
                        edge_data[e][label] = 1
                for src2, tar2, dic2 in input2:
                    edge_data = merge[src2][tar2]
                    for e in edge_data:
                        edge_data[e][label] = 1

        # Reset ranking
        self._ranked = False
        # Get prov graph of merging
        prov_graph_of_merging = drs.get_provenance().prov_graph()
        # Compose into my prov graph
        merge = nx.compose(self._provenance.prov_graph(),
                           prov_graph_of_merging)

        if annotate_and_edges:
            annotate_union_edges('AND')

        if annotate_or_edges:
            annotate_union_edges('OR')

        # Rewrite my prov graph to the new merged one and return
        self._provenance.swap_p_graph(merge)
        return self

    def absorb(self, drs):
        """
        Merge the input parameter DRS into self, by extending provenance appropriately and appending data
        :param drs:
        :return:
        """
        # Reset ranking
        self._ranked = False
        # Set union merge data
        merging_data = set(drs.data)
        my_data = set(self.data)
        new_data = merging_data.union(my_data)
        self.set_data(list(new_data))
        # Merge provenance
        self.absorb_provenance(drs)
        return self

    """
    Set operations
    """

    def intersection(self, drs):
        # Reset ranking
        self._ranked = False
        result = DRS([], Operation(OP.NONE))
        new_data = []
        # FIXME: There are more efficient ways of doing this
        if drs.mode == DRSMode.TABLE:
            merging_tables = [(hit.source_name, hit) for hit in drs.data]
            my_tables = [(hit.source_name, hit) for hit in self.data]
            for table, hit_ext in merging_tables:
                for t, hit_in in my_tables:
                    if table == t:
                        new_data.append(hit_ext)
                        new_data.append(hit_in)
        elif drs.mode == DRSMode.FIELDS:
            merging_data = set(drs.data)
            my_data = set(self.data)
            new_data = list(merging_data.intersection(my_data))
        # We set the new data into our DRS again
        # self.set_data(new_data)
        result.set_data(new_data)
        # Merge provenance
        # FIXME: perhaps we need to do some garbage collection of the prov graph at some point
        # FIXME: or alternatively perform a more fine-grained merging
        # self.absorb_provenance(drs, annotate_and_edges=True)
        result.absorb_provenance(self, annotate_and_edges=True)
        result.absorb_provenance(drs, annotate_and_edges=True)
        return result

    def union(self, drs):
        # Reset ranking
        self._ranked = False
        result = DRS([], Operation(OP.NONE))
        merging_data = set(drs.data)
        my_data = set(self.data)
        new_data = merging_data.union(my_data)
        # self.set_data(list(new_data))
        result.set_data(list(new_data))
        # Merge provenance
        # FIXME: perhaps we need to do some garbage collection of the prov
        # graph at some point
        # self.absorb_provenance(drs)
        result.absorb_provenance(self)
        result.absorb_provenance(drs)
        return result

    def set_difference(self, drs):
        # Reset ranking
        self._ranked = False
        result = DRS([], Operation(OP.NONE))
        merging_data = set(drs.data)
        my_data = set(self.data)
        new_data = my_data - merging_data
        # self.set_data(list(new_data))
        result.set_data(list(new_data))
        # Merge provenance
        # FIXME: perhaps we need to do some garbage collection of the prov
        # graph at some point
        # self.absorb_provenance(drs)
        result.absorb_provenance(self)
        result.absorb_provenance(drs)
        return result


    """
    Mode configuration functions
    """

    def set_fields_mode(self):
        self._mode = DRSMode.FIELDS

    def set_table_mode(self):
        self._mode = DRSMode.TABLE

    """
    Path functions
    """

    def paths(self):
        """
        Returns all paths contained in the provenance graph
        :return:
        """
        paths = self._provenance.compute_all_paths()
        return paths

    def path(self, a: Hit):
        """
        Return all paths that contain a
        :param a:
        :return:
        """
        paths = self._provenance.compute_paths_with(a)
        return paths

    """
    Query Provenance functions
    """

    def why_id(self, a: int) -> [Hit]:
        """
        Given the id of a Hit, explain what were the initial results that lead to this result appearing here
        :param a:
        :return:
        """
        hit = None
        for x in self._data:
            if x.nid == a:
                hit = x
        return self.why(hit)

    def why(self, a: Hit) -> [Hit]:
        """
        Given a result, explain what were the initial results that lead to this result appearing here
        :param a:
        :return:
        """
        # Make sure a is in data
        if a not in self.data:
            print("The result does not exist")
            return

        # Calculate paths from a to leafs, in reverse order and return the
        # leafs.
        paths = self._provenance.compute_paths_from_origin_to(a)
        origins = set()
        for p in paths:
            origins.add(p[0])
        return list(origins)

    def how_id(self, a: int) -> [Hit]:
        """
        Given the id of a Hit, explain what were the initial results that lead to this result appearing here
        :param a:
        :return:
        """
        hit = None
        for x in self._data:
            if x.nid == a:
                hit = x
        return self.how(hit)

    def how(self, a: Hit) -> [str]:
        """
        Given a result, explain how this result ended up forming part of the output
        :param a:
        :return:
        """
        # Make sure a is in data
        if a not in self.data:
            print("The result does not exist")
            return

        # Calculate paths from a to leafs, in reverse order and return the
        # leafs.
        paths = self._provenance.compute_paths_from_origin_to(a)
        explanations = []
        for p in paths:
            explanation = self._provenance.explain_path(p)
            explanations.append(explanation)
        return explanations

    """
    Ranking functions
    """

    def _compute_certainty_scores(self, aggr_strategy=None):
        """
        FIXME: scores being part of nodes instead of edges mean we cannot implement the
        best aggregation method. Only one temporal one that works ok in practice.
        :param aggregation:
        :return:
        """

        def decide_path(pg, ns, nodes_already_visited):
            """
            Given n score_paths, it chooses one according to an aggregation strategy
            :param pg:
            :return:
            """
            if aggr_strategy is None:
                max_score = 0
                for n in ns:
                    nodes_already_visited.add(n)
                    score = get_score_continuous_path(pg, n, nodes_already_visited)
                    if score > max_score:
                        max_score = score
            # FIXME: plug in here the logic for the other aggr strategies
            return score

        def get_score_continuous_path(pg, src, nodes_already_visited):
            """
            Traverse graph from src forming a score path
            :param pg:
            :param src:
            :return:
            """
            current_score = float(src.score)
            ns = [x for x in pg.neighbors(src) if x not in nodes_already_visited]  # skip already visited nodes
            #ns = [x for x in pg.neighbors(src)]
            if len(ns) == 1:
                nodes_already_visited.add(ns[0])
                current_score = current_score + get_score_continuous_path(pg, ns[0], nodes_already_visited)
            elif len(ns) > 1:
                current_score = current_score + decide_path(pg, ns, nodes_already_visited)
            # Last option: when there are no neighbors, we simply return
            # current_store
            return current_score

        # We reverse the provenance graph
        pg = self._provenance.prov_graph().reverse()
        nodes_already_visited = set()
        # Compute recursively the certainty score for every result
        for el in self.data:
            if el not in nodes_already_visited:
                score = get_score_continuous_path(pg, el, nodes_already_visited)
                self._rank_data[el]['certainty_score'] = score

    def _compute_coverage_scores(self):
        # Get total number of ORIGIN elements FIXME: (not KW, etc)
        (leafs, _) = self._provenance.get_leafs_and_heads()
        total_number = len(leafs)

        i = 0
        for origin in leafs:
            # Assign index to original values
            self._origin_values_coverage[origin] = i
            i += 1

        for el in self.data:
            # initialize coverage set to False
            coverage_set = bitarray(len(leafs))
            coverage_set.setall(False)
            # Get covered elements
            elements = self.why(el)
            for element_covered in elements:
                idx = self._origin_values_coverage[element_covered]
                coverage_set[idx] = True
            coverage = float(len(elements)) / float(total_number)
            self._rank_data[el]['coverage_score'] = (coverage, coverage_set)

    def compute_ranking_scores(self):

        st = time.time()
        self._compute_certainty_scores()
        et = time.time()
        print("Time to compute certainty scores: " + str(et - st))
        st = time.time()
        self._compute_coverage_scores()
        et = time.time()
        print("Time to compute coverage scores: " + str(et - st))

        self._ranked = True

    def rank_certainty(self):
        """
        Ranks the current results in DRS with respect to certainty criteria. It will rank columns or tables
        :return:
        """
        if self._ranked is False:
            self.compute_ranking_scores()

        elements = []
        for el, score_dict in self._rank_data.items():
            if 'certainty_score' in score_dict:
                value = (el, score_dict['certainty_score'])
            else:
                value = (el, 0)  # no certainty score is like 0
            elements.append(value)
        elements = sorted(elements, key=lambda a: a[1], reverse=True)
        self._data = [el for (el, score) in elements]  # save data in order
        self._ranking_criteria = self.RankingCriteria.CERTAINTY
        self._chosen_rank = elements  # store ranked data with scores for debugging/inspection

        return self

    def rank_coverage(self):
        """
        Ranks the current results in DRS with respect to coverage criteria. It will rank columns or tables
        TODO: basic implementation
        :return:
        """
        if self._ranked is False:
            self.compute_ranking_scores()

        elements = []
        for el, score_dict in self._rank_data.items():
            if 'coverage_score' in score_dict:
                value = (el, score_dict['coverage_score'])
            else:
                value = (el, 0)  # no coverage score is like 0
            elements.append(value)
        elements = sorted(elements, key=lambda a: a[1][0], reverse=True)
        self._data = [el for (el, score) in elements]  # save data in order
        self._ranking_criteria = self.RankingCriteria.COVERAGE
        self._chosen_rank = elements  # store ranked data with scores for debugging/inspection

        return self

    def rank_certainty_include_coverage(self):
        """
        TODO: future work
        :return:
        """
        return

    """
    Convenience functions
    """
    # TODO: all these functions were written on-demand. Some refactoring would help a lot here

    def print_tables(self):
        mode = self.mode  # save state
        self.set_table_mode()
        for x in self:
            print(x)
        self._mode = mode  # recover state

    def print_columns(self):
        mode = self.mode  # save state
        self.set_fields_mode()
        seen_nid = dict()
        for x in self:
            if x not in seen_nid:
                print(x)
            seen_nid[x] = 0  # just use as set
        self._mode = mode  # recover state

    def print_tables_with_scores(self):

        def aggr_certainty_table(group_by_table):
            ranked_list = []
            # First aggregate bitsets
            for x, score in self._chosen_rank:
                new_score = score
                if x.source_name in group_by_table:
                    old_score = group_by_table[x.source_name]
                    new_score = old_score + new_score
                group_by_table[x.source_name] = new_score
            for table, score_value in group_by_table.items():
                value = (table, score_value)
                ranked_list.append(value)
            ranked_list = sorted(ranked_list, key=lambda a: a[1], reverse=True)
            return ranked_list

        def aggr_coverage_table(group_by_table):
            ranked_list = []
            # First aggregate bitsets
            for x, score in self._chosen_rank:
                coverage_score, new_coverage_set = score
                if x.source_name in group_by_table:
                    old_coverage_set = group_by_table[x.source_name]
                    new_coverage_set = new_coverage_set | old_coverage_set
                group_by_table[x.source_name] = new_coverage_set
            for table, bitset in group_by_table.items():
                new_score = bitset.count() / bitset.length()
                value = (table, new_score)
                ranked_list.append(value)
            ranked_list = sorted(ranked_list, key=lambda a: a[1], reverse=True)
            return ranked_list

        mode = self.mode  # save state
        self.set_fields_mode()
        group_by_table = dict()
        if self._ranking_criteria == self.RankingCriteria.CERTAINTY:
            ranked_list = aggr_certainty_table(group_by_table)
        elif self._ranking_criteria == self.RankingCriteria.COVERAGE:
            ranked_list = aggr_coverage_table(group_by_table)

        for x in ranked_list:
            print(x)
        self._mode = mode  # recover state

    def print_columns_with_scores(self):
        mode = self.mode
        self.set_fields_mode()
        seen_nid = dict()
        for el, score in self._chosen_rank:
            if el not in seen_nid:
                print(str(el) + " -> " + str(score))
            seen_nid[el] = 0
        self._mode = mode

    def pretty_print_columns(self):
        mode = self.mode  # save state
        self.set_fields_mode()
        seen_nid = dict()
        for x in self:
            if x not in seen_nid:
                string = "DB: {0:20} TABLE: {1:30} FIELD: {2:30}".format(x.db_name, x.source_name, x.field_name)
                #string = "DB: " + x.db_name + "\t\t SOURCE: " + x.source_name + "\t\t FIELD: " + x.field_name
                print(string)
            seen_nid[x] = 0
        self._mode = mode  # recover state

    def pretty_print_columns_with_scores(self):
        mode = self.mode  # save state
        self.set_fields_mode()
        seen_nid = dict()
        for x, score in self._chosen_rank:
            if x not in seen_nid:
                string = "DB: {0:20} TABLE: {1:30} FIELD: {2:30} SCORE: {3:10}".format(x.db_name, x.source_name, x.field_name, str(score))
                #string = "DB: " + x.db_name + "\t\t SOURCE: " + x.source_name + "\t FIELD: " + \
                #    x.field_name + "\t SCORE: " + str(score)
                print(string)
            seen_nid[x] = 0
        self._mode = mode  # recover state


if __name__ == "__main__":

    test = DRS([1, 2, 3])

    for el in test:
        print(str(el))

    print(test.mode)
    test.set_table_mode()
    print(test.mode)
