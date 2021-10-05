from typing import Dict, Tuple

import Levenshtein as Lv
import math

from .graph import Graph
from .node_pair import NodePair
from .propagation_graph import PropagationGraph
from ..match import Match
from ..base_matcher import BaseMatcher
from ...data_sources.base_table import BaseTable


class SimilarityFlooding(BaseMatcher):

    def __init__(self, coeff_policy='inverse_average', formula='formula_c'):
        self.__coeff_policy = coeff_policy
        self.__formula = formula  # formula used to update similarities of map-pairs as shown in page 10 of the paper
        self.__graph1 = None
        self.__graph2 = None
        self.__propagation_graph = None
        self.__initial_map = None

    def get_matches(self,
                    source_input: BaseTable,
                    target_input: BaseTable
                    ) -> Dict[Tuple[Tuple[str, str], Tuple[str, str]], float]:
        self.__graph1 = Graph(source_input).graph
        self.__graph2 = Graph(target_input).graph
        self.__calculate_initial_mapping()
        matches = self.__fixpoint_computation(100, 1e-4)

        filtered_matches = self.__filter_map(matches)

        return self.__format_output(filtered_matches)

    def __calculate_initial_mapping(self):
        self.__initial_map = {}

        for n1 in self.__graph1.nodes():
            for n2 in self.__graph2.nodes():
                if n1.name[0:6] == "NodeID" or n2.name[0:6] == "NodeID":
                    self.__initial_map[NodePair(n1, n2)] = 0.0
                else:
                    similarity = Lv.ratio(n1.name, n2.name)
                    self.__initial_map[NodePair(n1, n2)] = similarity

    @staticmethod
    def __get_euc_residual_vector(previous_map, next_map):
        # residual vector
        residual_vector = {key: math.pow(previous_map.get(key, 0) - next_map.get(key, 0), 2)
                           for key in set(previous_map) | set(next_map)}
        return math.sqrt(sum(residual_vector.values())) # compute euclidean length of residual vector

    def __get_next_map(self, previous_map, p_graph, formula):
        next_map = dict()

        max_map = 0
        for n in p_graph.nodes():
            if formula == 'formula_a':
                map_sim = self.__initial_map[n]
            elif formula == 'formula_b':
                map_sim = 0
            else:
                map_sim = previous_map[n]

            for e in p_graph.in_edges(n):
                edge_data = p_graph.get_edge_data(e[0], e[1])

                weight = edge_data.get('weight')

                if formula == 'formula_a' or formula == 'basic':
                    map_sim += weight * previous_map[e[0]]
                elif formula == 'formula_b':
                    map_sim += weight * self.__initial_map[e[0]]
                else:
                    map_sim += self.__initial_map[e[0]] + weight * (previous_map[e[0]] + self.__initial_map[e[0]])

            if map_sim > max_map:
                max_map = map_sim

            next_map[n] = map_sim
        for key in next_map.keys():
            next_map[key] = next_map[key] / max_map

        return next_map

    def __fixpoint_computation(self, num_iter, residual_diff):

        p_g_builder = PropagationGraph(self.__graph1, self.__graph2, self.__coeff_policy)

        p_g = p_g_builder.construct_graph()

        if self.__formula == 'basic':  # using the basing formula

            previous_map = self.__initial_map.copy()

            for _ in range(0, num_iter):
                next_map = self.__get_next_map(previous_map, p_g, self.__formula)

                euc_len = self.__get_euc_residual_vector(previous_map, next_map)

                if euc_len <= residual_diff:  # check whether the algo has converged
                    break

                previous_map = next_map.copy()

        elif self.__formula == 'formula_a':  # using formula A
            previous_map = self.__initial_map.copy()

            for _ in range(0, num_iter):
                next_map = self.__get_next_map(previous_map, p_g, self.__formula)

                euc_len = self.__get_euc_residual_vector(previous_map, next_map)

                if euc_len <= residual_diff:  # check whether the algo has converged
                    break

                previous_map = next_map.copy()

        elif self.__formula == 'formula_b':  # using formula B
            next_map = self.__get_next_map(None, p_g, self.__formula)
            previous_map = next_map.copy()

            for _ in range(0, num_iter-1):
                next_map = self.__get_next_map(previous_map, p_g, self.__formula)

                euc_len = self.__get_euc_residual_vector(previous_map, next_map)

                if euc_len <= residual_diff:  # check whether the algo has converged
                    break

                previous_map = next_map.copy()

        elif self.__formula == 'formula_c':  # using formula C which is claimed to be the best one
            previous_map = self.__initial_map.copy()
            next_map = self.__get_next_map(previous_map, p_g, 'formula_b')
            previous_map = next_map.copy()

            for _ in range(0, num_iter-1):
                next_map = self.__get_next_map(previous_map, p_g, self.__formula)

                euc_len = self.__get_euc_residual_vector(previous_map, next_map)
                 
                if euc_len <= residual_diff:  # check whether the algo has converged
                    break

                previous_map = next_map.copy()

        else:
            print("Wrong formula option!")
            return {}

        return previous_map  # the dictionary storing the final similarities of map pairs

    def __filter_map(self, prev_map):

        """
        Function that filters the matching results, so that only pairs of columns remain
        :param prev_map: the matching results of the iterative algorithm
        :return: the filtered matchings
        """

        filtered_map = prev_map.copy()

        for key in prev_map.keys():

            flag = False
            if key.node1.name[0:6] == 'NodeID':

                if key.node1 in self.__graph1.nodes():

                    for e in self.__graph1.out_edges(key.node1):

                        if e[1].name == 'Column':
                            flag = True

                            break
                else:

                    for e in self.__graph2.out_edges(key.node1):

                        if e[1].name == 'Column':
                            flag = True

                            break
            else:

                del filtered_map[key]
                continue

            if flag:

                flag = False

                if key.node2.name[0:6] == 'NodeID':

                    if key.node2 in self.__graph1.nodes():

                        for e in self.__graph1.out_edges(key.node2):

                            if e[1].name == 'Column':
                                flag = True

                                break
                    else:
                        for e in self.__graph2.out_edges(key.node2):

                            if e[1].name == 'Column':
                                flag = True

                                break
            else:

                del filtered_map[key]
                continue

            if not flag:

                del filtered_map[key]

        return filtered_map


    @staticmethod
    def __filter_n_to_1_matches(matches):

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

    def __format_output(self, matches) -> Dict[Tuple[Tuple[str, str], Tuple[str, str]], float]:
        output = {}
        sorted_maps = {k: v for k, v in sorted(matches.items(), key=lambda item: -item[1])}
        for key in sorted_maps.keys():
            s_long_name, t_long_name = self.__get_node_name(key)
            similarity = sorted_maps[key]
            s_t_name, s_t_guid, s_c_name, s_c_guid = s_long_name
            t_t_name, t_t_guid, t_c_name, t_c_guid = t_long_name
            match = Match(t_t_name, t_c_name,
                          s_t_name, s_c_name,
                          float(similarity))
            output.update(match.to_dict)
        return output

    def __get_node_name(self, key):
        return self.__get_attribute_tuple(key.node1), self.__get_attribute_tuple(key.node2)

    def __get_attribute_tuple(self, node):
        column_name = None
        if node in self.__graph1.nodes():
            for e in self.__graph1.out_edges(node):
                links = self.__graph1.get_edge_data(e[0], e[1])
                if links.get('label') == "name":
                    column_name = e[1].long_name
        else:
            for e in self.__graph2.out_edges(node):
                links = self.__graph2.get_edge_data(e[0], e[1])
                if links.get('label') == "name":
                    column_name = e[1].long_name
        return column_name
