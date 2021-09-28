from typing import Union

import Levenshtein as Lv
import math

from .graph import Graph
from .node_pair import NodePair
from .propagation_graph import PropagationGraph
from ..match import Match
from ..base_matcher import BaseMatcher
from ...data_sources.base_db import BaseDB
from ...data_sources.base_table import BaseTable


class SimilarityFlooding(BaseMatcher):

    def __init__(self, coeff_policy='inverse_average', formula='formula_c'):
        self.coeff_policy = coeff_policy
        self.formula = formula  # formula used to update similarities of map-pairs as shown in page 10 of the paper
        self.graph1 = None
        self.graph2 = None
        self.propagation_graph = None
        self.initial_map = None
        self.source_guid = None
        self.target_guid = None

    def get_matches(self, source_input: Union[BaseDB, BaseTable], target_input: Union[BaseDB, BaseTable]):
        self.graph1 = Graph(source_input).graph
        self.graph2 = Graph(target_input).graph
        self.calculate_initial_mapping()
        self.source_guid = source_input.db_belongs_uid if isinstance(source_input, BaseTable) \
            else source_input.unique_identifier
        self.target_guid = target_input.db_belongs_uid if isinstance(target_input, BaseTable) \
            else target_input.unique_identifier
        matches = self.fixpoint_computation(100, 0.001)

        filtered_matches = self.filter_map(matches)

        return self.format_output(filtered_matches)

    def calculate_initial_mapping(self):
        self.initial_map = {}

        for n1 in self.graph1.nodes():
            for n2 in self.graph2.nodes():
                if n1.name[0:6] == "NodeID" or n2.name[0:6] == "NodeID":
                    self.initial_map[NodePair(n1, n2)] = 0.0
                else:
                    similarity = Lv.ratio(n1.name, n2.name)
                    self.initial_map[NodePair(n1, n2)] = similarity

    def fixpoint_computation(self, num_iter, residual_diff):

        p_g_builder = PropagationGraph(self.graph1, self.graph2, self.coeff_policy)

        p_g = p_g_builder.construct_graph()

        if self.formula == 'basic':  # using the basing formula

            previous_map = self.initial_map.copy()
            next_map = {}
            for i in range(0,num_iter):
                max_map = 0
                for n in p_g.nodes():
                    map_sim = previous_map[n]
                    
                    for e in p_g.in_edges(n):
                        edge_data = p_g.get_edge_data(e[0], e[1])
                        
                        weight = edge_data.get('weight')
                        
                        map_sim += weight*previous_map[e[0]]
                        
                    if map_sim > max_map:
                        max_map = map_sim
                    
                    next_map[n] = map_sim
                for key in next_map.keys():
                    next_map[key] = next_map[key]/max_map

                # residual vector
                residual_vector = {key: math.pow(previous_map.get(key, 0) - next_map.get(key, 0), 2)
                                   for key in set(previous_map) | set(next_map)}

                euc_len = math.sqrt(sum(residual_vector.values()))  # compute euclidean length of residual vector

                if euc_len <= residual_diff:  # check whether the algo has converged
                    break

                previous_map = next_map.copy()
                next_map = {}
        elif self.formula == 'formula_a':  # using formula A
            previous_map = self.initial_map.copy()
            next_map = {}
            for _ in range(0, num_iter):
                max_map = 0
                for n in p_g.nodes():
                    map_sim = self.initial_map[n]

                    for e in p_g.in_edges(n):
                        edge_data = p_g.get_edge_data(e[0], e[1])

                        weight = edge_data.get('weight')

                        map_sim += weight*previous_map[e[0]]

                    if map_sim > max_map:
                        max_map = map_sim

                    next_map[n] = map_sim
                for key in next_map.keys():
                    next_map[key] = next_map[key]/max_map

                # residual vector
                residual_vector = {key: math.pow(previous_map.get(key, 0) - next_map.get(key, 0),2)
                                   for key in set(previous_map) | set(next_map)}

                euc_len = math.sqrt(sum(residual_vector.values()))  # compute euclidean length of residual vector

                if euc_len <= residual_diff:  # check whether the algo has converged
                    break

                previous_map = next_map.copy()
                next_map = {}
        elif self.formula == 'formula_b':  # using formula B
            next_map = {}
            max_map = 0
            for n in p_g.nodes():
                map_sim = 0

                for e in p_g.in_edges(n):
                    edge_data = p_g.get_edge_data(e[0], e[1])

                    weight = edge_data.get('weight')

                    map_sim += weight*self.initial_map[e[0]]

                if map_sim > max_map:
                    max_map = map_sim

                next_map[n] = map_sim
            for key in next_map.keys():
                next_map[key] = next_map[key]/max_map
            previous_map = next_map.copy()
            next_map = {}

            for i in range(0, num_iter-1):
                max_map = 0
                for n in p_g.nodes():
                    map_sim = 0

                    for e in p_g.in_edges(n):
                        edge_data = p_g.get_edge_data(e[0],e[1])

                        weight = edge_data.get('weight')

                        map_sim += weight*(previous_map[e[0]]+self.initial_map[e[0]])

                    if map_sim > max_map:
                        max_map = map_sim

                    next_map[n] = map_sim
                for key in next_map.keys():
                    next_map[key] = next_map[key]/max_map

                # residual vector
                residual_vector = {key: math.pow(previous_map.get(key, 0) - next_map.get(key, 0),2)
                                   for key in set(previous_map) | set(next_map)}

                euc_len = math.sqrt(sum(residual_vector.values()))  # compute euclidean length of residual vector

                if euc_len <= residual_diff:  # check whether the algo has converged
                    break

                previous_map = next_map.copy()
                next_map = {}
        elif self.formula == 'formula_c':  # using formula C which is claimed to be the best one
            next_map = {}
            max_map = 0
            for n in p_g.nodes():
                map_sim = self.initial_map[n]

                for e in p_g.in_edges(n):
                    edge_data = p_g.get_edge_data(e[0], e[1])

                    weight = edge_data.get('weight')

                    map_sim += weight*self.initial_map[e[0]]

                if map_sim > max_map:
                    max_map = map_sim

                next_map[n] = map_sim
            for key in next_map.keys():
                next_map[key] = next_map[key]/max_map
            previous_map = next_map.copy()
            next_map = {}

            for i in range(0, num_iter-1):
                max_map = 0
                for n in p_g.nodes():
                    map_sim = previous_map[n]

                    for e in p_g.in_edges(n):
                        edge_data = p_g.get_edge_data(e[0], e[1])

                        weight = edge_data.get('weight')

                        map_sim += self.initial_map[e[0]] + weight*(previous_map[e[0]]+self.initial_map[e[0]])

                    if map_sim > max_map:
                        max_map = map_sim

                    next_map[n] = map_sim
                for key in next_map.keys():
                    next_map[key] = next_map[key]/max_map

                # residual vector
                residual_vector = {key: math.pow(previous_map.get(key, 0) - next_map.get(key, 0),2)
                                   for key in set(previous_map) | set(next_map)}

                euc_len = math.sqrt(sum(residual_vector.values()))  # compute euclidean length of residual vector

                if euc_len <= residual_diff:  # check whether the algo has converged
                    break

                previous_map = next_map.copy()
                next_map = {}
        else:
            print("Wrong formula option!")
            return {}

        return previous_map  # the dictionary storing the final similarities of map pairs

    def filter_map(self, prev_map):

        """
        Function that filters the matching results, so that only pairs of columns remain
        :param prev_map: the matching results of the iterative algorithm
        :return: the filtered matchings
        """

        filtered_map = prev_map.copy()

        for key in prev_map.keys():

            flag = False
            if key.node1.name[0:6] == 'NodeID':

                if key.node1 in self.graph1.nodes():

                    for e in self.graph1.out_edges(key.node1):

                        if e[1].name == 'Column':
                            flag = True

                            break
                else:

                    for e in self.graph2.out_edges(key.node1):

                        if e[1].name == 'Column':
                            flag = True

                            break
            else:

                del filtered_map[key]
                continue

            if flag:

                flag = False

                if key.node2.name[0:6] == 'NodeID':

                    if key.node2 in self.graph1.nodes():

                        for e in self.graph1.out_edges(key.node2):

                            if e[1].name == 'Column':
                                flag = True

                                break
                    else:
                        for e in self.graph2.out_edges(key.node2):

                            if e[1].name == 'Column':
                                flag = True

                                break
            else:

                del filtered_map[key]
                continue

            if not flag:

                del filtered_map[key]

        return filtered_map

    def print_results(self, matches):

        """

        :param matches: dictionary holding the match similarities of map pairs

        """

        sortedmaps = {k: v for k, v in sorted(matches.items(), key=lambda item: item[1])}

        for key in sortedmaps.keys():
            name1 = key.node1.name
            if key.node1.name[0:6] == 'NodeID':
                name1 = "[" + key.node1.name + "=>"
                if key.node1 in self.graph1.nodes():
                    for e in self.graph1.out_edges(key.node1):
                        edge_data = self.graph1.get_edge_data(e[0], e[1])
                        print("1) This is e[1].name: ", e[1].name)
                        name1 += edge_data.get('label') + ":" + e[1].name + ", "
                else:
                    for e in self.graph2.out_edges(key.node1):
                        edge_data = self.graph2.get_edge_data(e[0], e[1])
                        print("2) This is e[1].name: ", e[1].name)
                        name1 += edge_data.get('label') + e[1].name + ", "
                name1 += ']'

            name2 = key.node2.name
            if key.node2.name[0:6] == 'NodeID':
                name2 = "[" + key.node2.name + "=>"
                if key.node2 in self.graph1.nodes():
                    for e in self.graph1.out_edges(key.node2):
                        edge_data = self.graph1.get_edge_data(e[0], e[1])
                        print("3) This is e[1].name: ", e[1].name)
                        name2 += edge_data.get('label') + ":" + e[1].name + ", "
                else:
                    for e in self.graph2.out_edges(key.node2):
                        edge_data = self.graph2.get_edge_data(e[0], e[1])
                        print("4) This is e[1].name: ", e[1].name)
                        name2 += edge_data.get('label') + ":" + e[1].name + ", "
                name2 += ']'
            print(name1 + "-" + name2 + ":" + str(sortedmaps[key]))

    @staticmethod
    def filter_n_to_1_matches(matches):

        matches_n_to_1 = dict()
        nodes_left = set()

        for np in matches.keys():

            nodes_left.add(np.node1)

        for nd in nodes_left:

            max_sim = 0
            max_node = 0

            for np in matches.keys():

                if nd == np.node1:

                    if matches[np] > max_sim:

                        max_sim = matches[np]
                        max_node = np

            matches_n_to_1[max_node] = max_sim

        return matches_n_to_1

    def format_output(self, matches):
        output = []
        sorted_maps = {k: v for k, v in sorted(matches.items(), key=lambda item: -item[1])}
        for key in sorted_maps.keys():
            s_long_name, t_long_name = self.get_node_name(key)
            similarity = sorted_maps[key]
            s_t_name, s_t_guid, s_c_name, s_c_guid = s_long_name
            t_t_name, t_t_guid, t_c_name, t_c_guid = t_long_name
            match = Match(self.target_guid, t_t_name, t_t_guid, t_c_name, t_c_guid,
                          self.source_guid, s_t_name, s_t_guid, s_c_name, s_c_guid,
                          float(similarity))
            output.append(match.to_dict)
        return output

    def get_node_name(self, key):
        return self.get_attribute_tuple(key.node1), self.get_attribute_tuple(key.node2)

    def get_attribute_tuple(self, node):
        column_name = None
        if node in self.graph1.nodes():
            for e in self.graph1.out_edges(node):
                links = self.graph1.get_edge_data(e[0], e[1])
                if links.get('label') == "name":
                    column_name = e[1].long_name
        else:
            for e in self.graph2.out_edges(node):
                links = self.graph2.get_edge_data(e[0], e[1])
                if links.get('label') == "name":
                    column_name = e[1].long_name
        return column_name
