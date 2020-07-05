import math
import random
import warnings

import numpy as np

from tqdm import tqdm

class Node:
    """
        Cell class used to describe the nodes that build the graph.
    """
    def __init__(self, name, type, frequency, p_stay=0.5):
        if type not in ['cell', 'rid', 'attr']:
            raise ValueError('Type unrecognized.')
        self.random_neigh = None
        self.right = set([])
        self.left = set([])
        self.neighbors = dict()
        self.neighbor_names = []
        self.neighbor_frequencies = []
        self.n_similar = 1
        self.p_stay = p_stay
        self.frequency = frequency
        self.name = str(name)
        self.type = type
        self.similar_tokens = [name]
        self.similar_distance = [1.0]

    def _prepare_aliased_randomizer(self, weights):
        '''Implemented according to the alias method.

        :param weights:
        :return: Aliased randomizer
        '''
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

        def weighted_random():
            r = random.random() * N
            i = int(r)
            odds, alias = aliases[i]
            if (r - i) > odds:
                return self.neighbor_names[alias]
            else:
                return self.neighbor_names[i]
            # return alias if (r - i) > odds else i

        return weighted_random

    def get_weighted_random_neighbor(self):
        return self.random_neigh()

    def get_random_neighbor(self):
        return random.choice(self.neighbor_names)

    def add_neighbor(self, neighbor):
        if neighbor not in self.neighbors:
            self.neighbors[neighbor.name] = neighbor.frequency

    def normalize_neighbors(self):
        self.left = list(self.left)
        self.right = list(self.right)
        self.neighbor_names = np.array(list(self.neighbors.keys()))
        self.neighbor_frequencies = np.array(list(self.neighbors.values()))
        self.random_neigh = self._prepare_aliased_randomizer(self.neighbor_frequencies)

    def add_left(self, item):
        self.left.add(item)

    def add_right(self, item):
        self.right.add(item)

    def update_left(self, item, old=None):
        if old and old in self.left:
            self.left.remove(old)
        tmp_set = set(self.left)
        tmp_set.add(item)
        self.left = tmp_set

    def update_right(self, item, old=None):
        if old and old in self.right:
            self.right.remove(old)
        tmp_set = set(self.right)
        tmp_set.add(item)
        self.right = tmp_set

    def rebuild(self):
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

    def get_next(self, direction):
        if self.type == 'rid':
            if direction == 'left':
                raise ValueError('Error: in rid token, cannot go left.')
        if self.type == 'attr':
            if direction == 'right':
                raise ValueError('Error: in attr token, cannot go right.')
        if direction == 'left':
            return random.choice(self.left)
        if direction == 'right':
            return random.choice(self.right)

    def get_random_rid(self):
        if self.type != 'cell':
            raise ValueError('Error: in rid token, cannot go left.')
        else:
            return random.choice(self.left)

    def get_random_cid(self):
        if self.type != 'cell':
            raise ValueError('Error: in rid token, cannot go left.')
        else:
            return random.choice(list(self.right))


class Edge:
    def __init__(self, node_from, node_to):
        self.node_from = node_from
        self.node_to = node_to
        # self.weight = weight


class Graph:

    @staticmethod
    def f_no_smoothing():
        return 1.0

    @staticmethod
    def smooth_exp(x, eps=0.01, target=10, k=0.5):
        t = (eps / (1 - k)) ** (1 / (1 - target))
        y = (1 - k) * t ** (-x + 1) + k
        return y

    @staticmethod
    def inverse_smooth(x, s):
        y = 1 / 2 * (-(1 + s) ** (1 - x) + 2)
        return y

    @staticmethod
    def inverse_freq(freq):
        return 1/freq

    @staticmethod
    def log_freq(freq, base=10):
        return 1 / (math.log(freq, base) + 1)

    def smooth_freq(self, freq, eps=0.01):
        if self.smoothing_method == 'smooth':
            return self.smooth_exp(freq, eps, self.smoothing_target)
        if self.smoothing_method == 'inverse_smooth':
            return self.inverse_smooth(freq, self.smoothing_k)
        elif self.smoothing_method == 'log':
            return self.log_freq(freq, self.smoothing_target)
        elif self.smoothing_method == 'inverse':
            return self.inverse_freq(freq)
        elif self.smoothing_method == 'no':
            return self.f_no_smoothing()

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

    def add_edge(self, node_from, node_to):
        e = Edge(node_from.name, node_to.name)
        l1 = len(self.edges)
        self.edges.add(e)
        l2 = len(self.edges)
        if l2 > l1:
            node_from.add_neighbor(node_to)
            node_to.add_neighbor(node_from)

    def init_nodes(self, frequencies):
        for node in frequencies:
            try:
                float_c = float(node)
                if math.isnan(float_c):
                    continue
                cell_value = str(int(float_c))
            except ValueError:
                cell_value = str(node)
            except OverflowError:
                cell_value = str(node)
            if cell_value.startswith('idx_'):
                cell_value = 'dfc_' + cell_value

            smoothed_f = self.smooth_freq(frequencies[node])
            self.nodes[cell_value] = Node(cell_value, 'cell', frequency=smoothed_f)

    def init_nodes_flatten(self, frequencies, flatten):
        for node in frequencies:
            try:
                float_c = float(node)
                if math.isnan(float_c):
                    continue
                cell_value = str(int(float_c))
            except ValueError:
                cell_value = str(node)
            except OverflowError:
                cell_value = str(node)
            # smoothed_f = self.smooth_freq(frequencies[node])
            if cell_value.startswith('idx_'):
                cell_value = 'dfc_' + cell_value

            smoothed_f = self.smooth_freq(frequencies[node])

            if flatten:
                for split in cell_value.split('_'):
                    self.nodes[split] = Node(split, 'cell', frequency=1)
            else:
                self.nodes[cell_value] = Node(cell_value, 'cell', frequency=smoothed_f)

    def init_rids(self, df):
        n_rows = len(df)
        for idx in range(n_rows):
            s_idx = 'idx_{}'.format(idx)
            self.nodes[s_idx] = Node(s_idx, 'rid', frequency=1)
            self.rid_list.append(s_idx)

    def init_cids(self, df):
        for c in range(len(df.columns)):
            col = df.columns[c]
            if col not in self.cid_list:
                if col in self.nodes:
                    col = 'dfcol_' + col
                    columns = list(df.columns)
                    columns[c] = col
                    df.columns = columns
                self.nodes[col] = Node(col, 'attr', frequency=1)
            self.cid_list.append(col)

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

    def get_struct(self):
        return self, self.cell_list, self.rid_list, self.nodes

    def get_graph(self):
        return self

    def get_node_neighbors(self, node):
        return self.nodes[node].neighbor_names, self.nodes[node].neighbor_frequencies

    def _parse_smoothing_method(self, smoothing_method):
        if smoothing_method.startswith('smooth'):
            smooth_split = smoothing_method.split(',')
            if len(smooth_split) == 3:
                self.smoothing_method, self.smoothing_k, self.smoothing_target = smooth_split
                self.smoothing_k = float(self.smoothing_k)
                self.smoothing_target = float(self.smoothing_target)
                if not 0 <= self.smoothing_k <= 1:
                    raise ValueError('Smoothing k must be in range [0,1], current k = {}'.format(self.smoothing_k))
                if self.smoothing_target < 1:
                    raise ValueError('Smoothing target must be > 1, current target = {}'.format(self.smoothing_target))
            elif len(smooth_split) == 1:
                self.smoothing_method = 'smooth'
                self.smoothing_k = 0.2
                self.smoothing_target = 200
            else:
                raise ValueError('Unknown smoothing parameters.')
        if smoothing_method.startswith('inverse_smooth'):
            smooth_split = smoothing_method.split(',')
            if len(smooth_split) == 2:
                self.smoothing_method, self.smoothing_k = smooth_split
                self.smoothing_k = float(self.smoothing_k)
            elif len(smooth_split) == 1:
                self.smoothing_method = 'inverse_smooth'
                self.smoothing_k = 0.1
            else:
                raise ValueError('Unknown smoothing parameters.')
        elif smoothing_method.startswith('log'):
            log_split = smoothing_method.split(',')
            if len(log_split) == 2:
                self.smoothing_method, self.smoothing_target = log_split
                self.smoothing_target = float(self.smoothing_target)
                if self.smoothing_target <= 1:
                    raise ValueError('Log base must be > 1, current base = {}'.format(self.smoothing_target))
            elif len(log_split) == 1:
                self.smoothing_method = 'log'
                self.smoothing_target = 10
            else:
                raise ValueError('Unknown smoothing parameters.')
        elif smoothing_method.startswith('piecewise'):
            piecewise_split = smoothing_method.split(',')
            if len(piecewise_split) == 2:
                self.smoothing_method, self.smoothing_target = piecewise_split
                self.smoothing_target = float(self.smoothing_target)
                self.smoothing_k = 10
            elif len(piecewise_split) == 3:
                self.smoothing_method, self.smoothing_target, self.smoothing_k = piecewise_split
                self.smoothing_target = float(self.smoothing_target)
                self.smoothing_k = float(self.smoothing_k)
            elif len(piecewise_split) == 1:
                self.smoothing_method = self.smoothing_method
                self.smoothing_target = 20
                self.smoothing_k = 10
            else:
                raise ValueError('Unknown smoothing parameters. ')
        else:
            self.smoothing_method = smoothing_method

    def __init__(self, df, sim_list=None, smoothing_method='smooth', flatten=True):
        """Data structure used to represent dataframe df as a graph. The data structure contains a list of all nodes
        in the graph, built according to the parameters passed to the function.

        :param df: dataframe to convert into graph
        :param sim_list: optional, list of pairs of similar values
        :param smoothing_method: one of {no, smooth, inverse_smooth, log, inverse}
        :param flatten: if set to True, spread multi-word tokens over multiple nodes. If set to false, all unique cell
        values will be merged in a single node.
        """
        self.rid_list = []
        self.cell_list = set()
        self.cid_list = []
        self.nodes = {}
        self.edges = set()
        self._parse_smoothing_method(smoothing_method)
        df = df.fillna('')

        values, counts = np.unique(df.values.ravel(), return_counts=True)  # Count unique values to find word frequency.

        frequencies = dict(zip(values, counts))

        frequencies.pop('', None)
        frequencies.pop(np.nan, None)

        if flatten:
            print('# Flatten = True, expanding strings.')
        else:
            print('# Flatten = False, all strings will be tokenized.')
        self.init_nodes_flatten(frequencies, flatten)
        self.init_rids(df)
        self.init_cids(df)
        count_rows = 1
        # for row in df.itertuples(index=True):
        for idx, r in tqdm(df.iterrows()):
            # Create a node for the current row id.
            rid = 'idx_' + str(idx)
            row = r.dropna()
            for col in df.columns:
                cell_value = row[col]
                if cell_value == '':
                    continue
                try:
                    float_c = float(cell_value)
                    if math.isnan(float_c):
                        continue
                    cell_value = str(int(float_c))
                except ValueError:
                    cell_value = str(row[col])
                except OverflowError:
                    cell_value = str(row[col])

                c = col
                if flatten:
                    valsplit = cell_value.split('_')
                else:
                    valsplit = [cell_value]
                for split in valsplit:
                    if split not in self.nodes:
                        raise KeyError('{} not found in self.nodes'.format(split))
                    self.add_edge(self.nodes[rid], self.nodes[split])
                    self.add_edge(self.nodes[split], self.nodes[rid])
                    self.add_edge(self.nodes[c], self.nodes[split])
                    self.add_edge(self.nodes[split], self.nodes[c])

                    self.nodes[split].add_left(rid)
                    self.nodes[split].add_right(c)
                    self.nodes[rid].add_right(split)
                    self.nodes[c].add_left(split)

                    self.cell_list.add(str(split))
            # print('\r# {:60} {:0.1f} - {:}/{:} tuples'.format('Loading tuples in graph',
            #       count_rows/len(df) * 100, count_rows, len(df)), end='')
            count_rows += 1

        print('')
        count_nodes = 1
        to_delete = []
        for node in tqdm(self.nodes):
            if len(self.nodes[node].neighbors) == 0:
                warnings.warn('Node {} has no neighbors'.format(node))
                to_delete.append(node)
            else:
                self.nodes[node].normalize_neighbors()
                # print('\r# {:60} {:0.1f} - {:}/{:} nodes'.format('Normalizing node weights:',
                #                                                    count_nodes / len(self.nodes) * 100,
                #                                                    count_nodes, len(self.nodes)), end='')
            count_nodes += 1
        print('')
        for node in to_delete:
            self.nodes.pop(node)
        # self.edges = None  # remove the edges list since to save memory
        if sim_list:
            self.add_similarities(sim_list)
