import math
import random

import numpy as np

from tqdm import tqdm

import warnings

try:
    import networkx as nx
    NX_NOT_FOUND = False
except ModuleNotFoundError:
    warnings.warn('NetworkX not found. Graph conversion unavailable')
    NX_NOT_FOUND = True


class Node:
    """
        Cell class used to describe the nodes that build the graph.
    """
    def __init__(self, name, type, node_class, numeric):
        # if type not in ['cell', 'rid', 'attr']:
        #     raise ValueError('Type unrecognized.')
        self.random_neigh = None
        self.neighbors = dict()
        self.neighbor_names = []
        self.n_similar = 1
        self.name = str(name)
        self.type = type
        self.similar_tokens = [name]
        self.similar_distance = [1.0]
        self.startfrom = []
        self.isfirst = self.isroot = self.isappear = bool(0)
        self.node_class = {}
        self._extract_class(node_class)
        self.numeric = numeric

    def _extract_class(self, node_type):
        bb = '{:03b}'.format(node_type)
        for i, _ in enumerate(['isfirst', 'isroot', 'isappear']):
            self.node_class[_] = bool(int(bb[i]))

    def set_frequency(self, frequency):
        self.frequency = frequency

    def _prepare_aliased_randomizer(self, weights):
        """
        Implemented according to the alias method.
        :param weights:
        :return: Aliased randomizer
        """
        N = len(weights)
        if N == 0:
            raise ValueError('Node {} has no neighbors. Check the input dataset. '.format(self.name))
        avg = sum(weights) / N
        aliases = [(1, None)] * N
        smalls = ((i, w / avg) for i, w in enumerate(weights) if w < avg)
        bigs = ((i, w / avg) for i, w in enumerate(weights) if w >= avg)
        small, big = next(smalls, None), next(bigs, None)
        while big and small:
            aliases[small[0]] = (small[1], big[0])
            big = (big[0], big[1] - (1 - small[1]))
            if big[1] < 1:
                small = big
                big = next(bigs, None)
            else:
                small = next(smalls, None)

        # def weighted_random():
        #     r = random.random() * N
        #     i = int(r)
        #     odds, alias = aliases[i]
        #     if (r - i) > odds:
        #         return self.neighbor_names[alias]
        #     else:
        #         return self.neighbor_names[i]
        #     # return alias if (r - i) > odds else i
        #
        # return weighted_random

    def get_random_start(self):
        if len(self.startfrom) > 0:
            return np.random.choice(self.startfrom, size=1)[0]
        else:
            return self.name

    def get_weighted_random_neighbor(self):
        return self.random_neigh()

    def get_random_neighbor(self):
        return np.random.choice(self.neighbor_names, size=1)[0]
        # return random.choice(self.neighbor_names)

    def add_neighbor(self, neighbor, weight):
        if neighbor not in self.neighbors:
            self.neighbors[neighbor.name] = weight
            if neighbor.node_class['isfirst']:
                self.startfrom.append(neighbor.name)
        else:
            if neighbor in self.neighbors and self.neighbors[neighbor.name] != weight:
                raise ValueError('Duplicate edge {} {} found, weights are different.'.format(self.name, neighbor))

    def get_random_replacement(self):
        return random.choices(self.similar_tokens, weights=self.similar_distance, k=1)[0]

    def normalize_neighbors(self):
        self.neighbor_names = np.array(list(self.neighbors.keys()))
        # self.neighbor_frequencies = np.array(list(self.neighbors.values()))
        self.random_neigh = self._prepare_aliased_randomizer(np.array(list(self.neighbors.values())))
        self.startfrom = np.array(self.startfrom)
        self.neighbors = None

    def rebuild(self):
        raise NotImplementedError
        if np.nan in self.left:
            self.left.remove(np.nan)
        if '' in self.left:
            self.left.remove('')
        self.left = list(self.left)

        if np.nan in self.right:
            self.right.remove(np.nan)
        if '' in self.right:
            self.right.remove('')
        self.right = list(self.right)

        if len(self.similar_distance) > 1:
            candidates_replacement = self.similar_distance[1:]
            sum_cand = sum(candidates_replacement)
            if sum_cand >= 1:
                candidates_replacement = np.array(candidates_replacement) / sum_cand * (1-self.p_stay)
            else:
                candidates_replacement = np.array(candidates_replacement) * sum_cand * (1-self.p_stay)
        else:
            candidates_replacement = []
        self.similar_distance = [1-sum(candidates_replacement)] + list(candidates_replacement)
        self.n_similar = len(self.similar_tokens)

    def add_similar(self, other, distance):
        self.similar_tokens.append(other)
        self.similar_distance.append(distance)


class Edge:
    def __init__(self, node_from, node_to, weight_forward=1, weight_back=1):
        self.node_from = node_from
        self.node_to = node_to
        self.weight_forward = weight_forward
        self.weight_back = weight_back

class Graph:
    def compute_n_sentences(self, sentence_length, factor=1000):
        """Compute the default number of sentences according to the rule of thumb:
        n_sentences = n_nodes * representation_factor // sentence_length

        :param sentence_length: target sentence length
        :param factor: "desired" number of occurrences of each node
        :return: n_sentences
        """
        n = len(self.nodes)*factor//sentence_length
        print('# Computing default number of sentences.\n{} sentences will be generated.'.format(n))
        return n

    def add_edge(self, node_from, node_to, weight_forward, weight_back=None):
        if weight_back is None:
            e = Edge(node_from.name, node_to.name, weight_forward)
        else:
            e = Edge(node_from.name, node_to.name, weight_forward, weight_back)
        l1 = len(self.edges)
        self.edges.add(e)
        l2 = len(self.edges)
        if l2 > l1:
            node_from.add_neighbor(node_to, weight_forward)
            if weight_back is not None:
                node_to.add_neighbor(node_from, weight_back)


    def add_similarities(self, sim_list):
        for row in sim_list:
            this = row[0]
            other = row[1]
            if this not in self.nodes or other not in self.nodes:
                continue
            distance = row[2]
            try:
                self.nodes[this].add_similar(other, distance)
                self.nodes[other].add_similar(this, distance)
            except KeyError:
                pass
        for t in self.nodes:
            self.nodes[t].rebuild()

    def get_node_list(self):
        return list(self.nodes.keys())

    def get_graph(self):
        return self

    def _get_node_type(self, node):
        for pre in self.node_classes:
            if node.startswith(pre + '__'):
                return pre
        raise ValueError('Node {} does not have a recognized prefix. '
                         'Currently recognized node_classes:\n'.format(node, ' '.join(self.node_classes)))

    def _check_flatten(self):
        if self.to_flatten != 'all':
            for _ in self.to_flatten:
                if _[:] not in self.node_classes:
                    raise ValueError('Unknown to-flatten type {}.'.format(_))

    def _extract_prefix(self, prefixes):
        valid = False
        for prefix in prefixes:
            prefix_properties, pref = prefix.split('__')
            strnum = prefix_properties[1]
            rwclass = prefix_properties[0]
            if int(rwclass) not in range(8):
                raise ValueError('Unknown class {}'.format(rwclass))
            else:
                self.node_classes[pref] = int(rwclass)
                if int(rwclass) >= 4:
                    self.possible_first.append(pref)
            if int(rwclass)%2 == 1:
                # self.isappear.append(prefix)
                valid = True
            if strnum not in ['#', '$']:
                raise ValueError('Unknown type prefix {}'.format(strnum))
            else:
                self.node_is_numeric[pref] = True if strnum == '#' else False
        if not valid:
            raise ValueError('No node class with "isappear"==True is present. '
                             'All random walks will be empty. Terminating. ')

    def __init__(self, edgelist, prefixes, sim_list=None, flatten=None):
        """Data structure used to represent dataframe df as a graph. The data structure contains a list of all nodes
        in the graph, built according to the parameters passed to the function.

        :param df: dataframe to convert into graph
        :param sim_list: optional, list of pairs of similar values
        :param smoothing_method: one of {no, smooth, inverse_smooth, log, inverse}
        :param flatten: if present and different from "all", expand the strings of all nodes whose type is in the list.
        """
        if flatten is None:
            flatten = []
        self.nodes = {}
        self.edges = set()
        self.node_classes = {}
        self.node_is_numeric = {}
        self.isappear = []
        self.to_flatten = flatten
        self.cell_list = []
        self.possible_first = []

        self._extract_prefix(prefixes)
        self._check_flatten()
        self.uniform = True

        if flatten == 'all':
            print('# Flatten = all, all strings will be expanded.')
            self.to_flatten = self.node_classes
        elif len(self.to_flatten) > 0:
            print('# Expanding columns {}.'.format(self.to_flatten))
        else:
            print('# All values will be tokenized. ')

        # pbar = tqdm()
        for line in tqdm(edgelist, desc='Loading edgelist_file.'):
            n1 = line[0]
            n2 = line[1]

            if n1 is np.nan or n2 is np.nan:
                raise ValueError('{} or {} are NaNs'.format(n1,n2))

            to_link = []

            if len(line) == 2:
                w1 = w2 = 1

            elif len(line) == 4:
                w1 = line[2]
                w2 = line[3]

            elif len(line) == 3:
            # unidirectional edge
                w1 = line[2]
                w2 = None
            else:
                raise ValueError('Line {} does not contain the correct number of values'.format(line))

            if w1 != w2 or w2 is None:
                self.uniform = False

            for _n in [n1,n2]:
                tl = []
                try:
                    float_c = float(_n)
                    if math.isnan(float_c):
                        continue
                    node_name = str(_n)
                except ValueError:
                    node_name = str(_n)
                except OverflowError:
                    node_name = str(_n)

                node_prefix = self._get_node_type(node_name)
                # npr, nn = node_name.split('__', maxsplit=1)
                if node_prefix in self.to_flatten:
                    # node_prefix = node_name.split('__', maxsplit=1)[0]
                    valsplit = node_name.split('_')
                    for idx, val in enumerate(valsplit):
                        if idx == 0 and val in self.node_classes or val == '':
                            continue
                        nn = node_prefix + '__' + val
                        if nn not in self.nodes:
                            node = Node(nn, node_prefix, node_class=self.node_classes[node_prefix],
                                        numeric=False)
                            self.nodes[nn] = node
                    tl += [self.nodes[node_prefix + '__' + _] for ii, _ in enumerate(valsplit)
                           if (ii > 0 and _ != '')]
                    # to_link.append()
                # else:
                    # node_prefix = node_name.split('__', maxsplit=1)[0]

                if node_name not in self.nodes:
                    node = Node(node_name, node_prefix, node_class=self.node_classes[node_prefix],
                                numeric=self.node_is_numeric[node_prefix])
                    self.nodes[node_name] = node
                tl.append(self.nodes[node_name])
                # to_link.append([])
                to_link.append(set(tl))

            for _1 in to_link[0]:
                for _2 in to_link[1]:
                    if _1 != _2:
                        self.add_edge(_1, _2, w1, w2)

        to_delete = []
        for node_name in tqdm(self.nodes, desc='Prepare aliased randomizer for each node'):
            if self.nodes[node_name].node_class['isroot']:
                self.cell_list.append(node_name)
            if len(self.nodes[node_name].neighbors) == 0:
                raise ValueError('Node {} has no neighbors'.format(node_name))
                # to_delete.append(n)
            else:
                self.nodes[node_name].normalize_neighbors()
        print('')
        for node_name in to_delete:
            self.nodes.pop(node_name)
        # self.edges = None  # remove the edges list since to save memory
        if sim_list:
            self.add_similarities(sim_list)


    def convert_to_nx(self):
        if NX_NOT_FOUND:
            raise ImportError('NetworkX not found.')
        else:
            nxg = nx.Graph()
            for edge in self.edges:
                nxg.add_edge(edge.node_from, edge.node_to)
            return nxg