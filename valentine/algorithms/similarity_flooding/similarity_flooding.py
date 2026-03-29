import math

from ...data_sources.base_table import BaseTable
from ..base_matcher import BaseMatcher
from ..match import Match
from .graph import Graph
from .node_pair import NodePair
from .propagation_graph import PropagationGraph
from .string_matcher import (
    compute_idf_weights,
    levenshtein_sim,
    prefix_suffix_tfidf,
    prefix_suffix_tokenized,
)


class SimilarityFlooding(BaseMatcher):
    def __init__(
        self,
        coeff_policy="inverse_average",
        formula="formula_c",
        string_matcher="prefix_suffix",
        tfidf_corpus: list[BaseTable] | None = None,
    ):
        self.__coeff_policy = coeff_policy
        self.__formula = formula
        self.__string_matcher = string_matcher
        self.__tfidf_corpus = tfidf_corpus or []
        self.__graph1 = None
        self.__graph2 = None
        self.__initial_map = None

    def get_matches(
        self, source_input: BaseTable, target_input: BaseTable
    ) -> dict[tuple[tuple[str, str], tuple[str, str]], float]:
        self.__graph1 = Graph(source_input).graph
        self.__graph2 = Graph(target_input).graph
        self.__calculate_initial_mapping()
        matches = self.__fixpoint_computation(100, 1e-4)
        filtered_matches = self.__filter_map(matches)
        return self.__format_output(filtered_matches)

    def __calculate_initial_mapping(self):
        if self.__string_matcher == "prefix_suffix_tfidf":
            # Collect non-NodeID node names from both graphs for IDF computation.
            # Include corpus tables so IDF reflects the full schema vocabulary
            # (e.g. the paper's G2 contains both Employee and Department tables).
            all_nodes = list(self.__graph1.nodes()) + list(self.__graph2.nodes())
            for table in self.__tfidf_corpus:
                all_nodes.extend(Graph(table).graph.nodes())
            all_names = [n.name for n in all_nodes if not n.name.startswith("NodeID")]
            idf_weights = compute_idf_weights(all_names)

            def sim_fn(s1, s2):
                return prefix_suffix_tfidf(s1, s2, idf_weights)

        elif self.__string_matcher == "prefix_suffix":
            sim_fn = prefix_suffix_tokenized
        else:
            sim_fn = levenshtein_sim

        init_map = {}
        for n1 in self.__graph1.nodes():
            n1_name = n1.name
            for n2 in self.__graph2.nodes():
                n2_name = n2.name
                if n1_name.startswith("NodeID") or n2_name.startswith("NodeID"):
                    sim = 0.0
                else:
                    sim = sim_fn(n1_name, n2_name)
                init_map[NodePair(n1, n2)] = sim
        self.__initial_map = init_map

    @staticmethod
    def __get_euc_residual_vector(previous_map, next_map):
        keys = set(previous_map) | set(next_map)
        return math.sqrt(sum((previous_map.get(k, 0) - next_map.get(k, 0)) ** 2 for k in keys))

    def __get_next_map(self, previous_map, p_graph, formula):
        next_map = {}
        max_val = 0
        init_map = self.__initial_map
        for n in p_graph.nodes():
            if formula == "formula_a":
                map_sim = init_map[n]
            elif formula == "formula_b":
                map_sim = 0
            elif formula == "formula_c":
                map_sim = init_map[n] + previous_map[n]
            else:  # basic
                map_sim = previous_map[n]
            for e in p_graph.in_edges(n):
                w = p_graph.get_edge_data(e[0], e[1]).get("weight")
                if formula in ("formula_a", "basic"):
                    map_sim += w * previous_map[e[0]]
                elif formula == "formula_b":
                    map_sim += w * (init_map[e[0]] + previous_map.get(e[0], 0))
                elif formula == "formula_c":
                    map_sim += w * (init_map[e[0]] + previous_map[e[0]])
            max_val = max(max_val, map_sim)
            next_map[n] = map_sim
        inv_max = 1.0 / max_val if max_val > 0 else 1.0
        for k in next_map:
            next_map[k] *= inv_max
        return next_map

    def __fixpoint_computation(self, num_iter, residual_diff):
        p_g = PropagationGraph(self.__graph1, self.__graph2, self.__coeff_policy).construct_graph()

        def iterate(previous_map, formula, iters):
            for _ in range(iters):
                next_map = self.__get_next_map(previous_map, p_g, formula)
                if self.__get_euc_residual_vector(previous_map, next_map) <= residual_diff:
                    return next_map
                previous_map = next_map.copy()
            return previous_map

        if self.__formula in ("basic", "formula_a", "formula_b", "formula_c"):
            return iterate(self.__initial_map.copy(), self.__formula, num_iter)

        print("Wrong formula option!")
        return {}

    def __filter_map(self, prev_map):
        filtered = prev_map.copy()
        g1_nodes = self.__graph1.nodes()
        g1_out_edges = self.__graph1.out_edges
        g2_out_edges = self.__graph2.out_edges

        for key in prev_map:
            n1 = key.node1
            n2 = key.node2
            if not n1.name.startswith("NodeID"):
                filtered.pop(key, None)
                continue

            edges = g1_out_edges(n1) if n1 in g1_nodes else g2_out_edges(n1)
            if not any(e[1].name == "Column" for e in edges):
                filtered.pop(key, None)
                continue

            if not n2.name.startswith("NodeID"):
                filtered.pop(key, None)
                continue

            edges = g1_out_edges(n2) if n2 in g1_nodes else g2_out_edges(n2)
            if not any(e[1].name == "Column" for e in edges):
                filtered.pop(key, None)

        return filtered

    def __format_output(self, matches) -> dict[tuple[tuple[str, str], tuple[str, str]], float]:
        output = {}
        sorted_maps = sorted(matches.items(), key=lambda item: -item[1])
        for key, sim in sorted_maps:
            s_long_name, t_long_name = self.__get_node_name(key)
            s_t_name, _, s_c_name, _ = s_long_name
            t_t_name, _, t_c_name, _ = t_long_name
            match = Match(t_t_name, t_c_name, s_t_name, s_c_name, float(sim))
            output.update(match.to_dict)
        return output

    def __get_node_name(self, key):
        return self.__get_attribute_tuple(key.node1), self.__get_attribute_tuple(key.node2)

    def __get_attribute_tuple(self, node):
        g1_nodes = self.__graph1.nodes()
        g1_out_edges = self.__graph1.out_edges
        g2_out_edges = self.__graph2.out_edges
        if node in g1_nodes:
            edges = g1_out_edges(node)
            get_data = self.__graph1.get_edge_data
        else:
            edges = g2_out_edges(node)
            get_data = self.__graph2.get_edge_data
        for e in edges:
            if get_data(e[0], e[1]).get("label") == "name":
                return e[1].long_name
        return None
