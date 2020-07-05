import math
import pandas as pd

import numpy as np

from collections import Counter
from algorithms.embdi.EmbDI.graph import Graph

import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', required=True, type=str, help='Path to input csv file to translate.')
    parser.add_argument('-o', '--output_file', required=True, type=str, help='Path to output edgelist_file.')

    return parser.parse_args()

class EdgeList:

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


    def __init__(self, df, smoothing_method='no', flatten=False):
        """Data structure used to represent dataframe df as a graph. The data structure contains a list of all nodes
        in the graph, built according to the parameters passed to the function.

        :param df: dataframe to convert into graph
        :param sim_list: optional, list of pairs of similar values
        :param smoothing_method: one of {no, smooth, inverse_smooth, log, inverse}
        :param flatten: if set to True, spread multi-word tokens over multiple nodes. If set to false, all unique cell
        values will be merged in a single node.
        """
        self._parse_smoothing_method(smoothing_method)
        # df = df.fillna('')
        self.edgelist  = []

        self.pref = ['3#__tn', '3$__tt','5$__idx', '1$__cid']

        numeric_columns = []

        for col in df.columns:
            try:
            # if True:
                df[col].dropna(axis=0).astype(float).astype(str)
                numeric_columns.append(col)
            except ValueError:
                pass

        # values, counts = np.unique(df.values.ravel(), return_counts=True)  # Count unique values to find word frequency.
        if flatten:
            split_values = []
            for val in df.values.ravel().tolist():
                try:
                    split = val.split('_')

                except AttributeError:
                    split = [str(val)]
                split_values += split
            frequencies = dict(Counter(split_values))

            frequencies.pop('', None)
            frequencies.pop(np.nan, None)

        else:
            frequencies = dict(Counter(df.values.ravel().tolist()))

            frequencies.pop('', None)
            frequencies.pop(np.nan, None)

        count_rows = 1
        #with open(edgefile, 'w') as fp:
        #fp.write(','.join(prefixes) + '\n')
        for idx, r in df.iterrows():
            rid = 'idx__' + str(idx)

            row = r.dropna()
            # Create a node for the current row id.
            for col in df.columns:
                try:
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

                    if flatten:
                        valsplit = cell_value.split('_')
                    else:
                        valsplit = [cell_value]
                    for split in valsplit:
                        try:
                            smoothed_f = self.smooth_freq(frequencies[split])
                        except KeyError:
                            smoothed_f = 1
                        n1 = rid
                        if col in numeric_columns:
                            n2 = 'tn__' + split
                        else:
                            n2 = 'tt__' + split

                        w1 = 1
                        w2 = smoothed_f
                        self.edgelist.append((n1, n2, w1, w2))
                        #edgerow = '{},{},{},{}\n'.format(n1, n2, w1, w2)

                        #fp.write(edgerow)
                        if col in numeric_columns:
                            n1 = 'tn__' + split
                        else:
                            n1 = 'tt__' + split

                        n2 = 'cid__' + col
                        w1 = smoothed_f
                        w2 = 1
                        self.edgelist.append((n1, n2, w1, w2))

                        #edgerow = '{},{},{},{}\n'.format(n1, n2, w1, w2)

                        #fp.write(edgerow)
                except KeyError:
                    continue

                print('\r# {:0.1f} - {:}/{:} tuples'.format(count_rows/len(df) * 100, count_rows, len(df)), end='')
                count_rows += 1

        print('')

    def get_edgelist(self):
        return  self.edgelist

    def get_pref(self):
        return self.pref

if __name__ == '__main__':
    args = parse_args()
    # dsname = sys.argv[1]
    # dfpath = 'pipeline/datasets/{}/{}-master.csv'.format(dsname, dsname)
    dfpath = args.input_file
    df = pd.read_csv(dfpath)

    edgefile = args.output_file
    # edgefile = 'pipeline/edgelists/{}-edges-norm.txt'.format(dsname)

    pref = ['3#__tn', '3$__tt','5$__idx', '1$__cid']

    el = EdgeList(df, edgefile)

    g = Graph(el.get_edgelist(), prefixes=el.pref)
