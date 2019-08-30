import math
import numbers
import string
from operator import itemgetter

import nltk
import numpy as np
from pyemd.emd import emd
from scipy.spatial.distance import pdist, squareform
from scipy.stats import stats
from pybloom import ScalableBloomFilter

TABLE = str.maketrans({key: ' ' for key in string.punctuation})


def quantile_emd(column1, column2, is_tokenized=False, quantile=256):
    if ~is_tokenized:
        column1 = process_data(column1)
        column2 = process_data(column2)

    # get the unique values
    set1 = set(column1)
    set2 = set(column2)

    # compute the union of the 2 columns
    set_union = list(set1.union(set2))

    # sort the values in lexicographic/numeric order
    numeric_values = np.array([x for x in set_union if isinstance(x, numbers.Number)]).astype(np.object)
    string_values = np.array(list(filter(lambda x: x not in numeric_values, set_union)))
    sorted_set = np.append(numeric_values, string_values, axis=0)

    # rank the sorted values
    wmap = {key: i for (i, key) in enumerate(sorted_set)}

    # get the ranks for each column
    ranks1 = np.array(itemgetter(*list(set1))(wmap))
    ranks2 = np.array(itemgetter(*list(set2))(wmap))

    # check if the ranks contain more than 1 value and sort the lists
    if len(ranks1.shape) > 0:
        ranks1 = sorted(ranks1)
        l1 = len(ranks1)
    else:
        l1 = 1

    if len(ranks2.shape) > 0:
        ranks2 = sorted(ranks2)

    # get the bin edges by using 1-quantile
    bin_edges1 = stats.mstats.mquantiles(ranks1, np.array(range(0, l1 + 1, quantile)) / l1)

    # compute the histogram for both columns
    hist1, bins1 = np.histogram(ranks1, bins=bin_edges1)
    hist2, bins2 = np.histogram(ranks2, bins=bin_edges1)

    # find the distance matrix between each word
    wmap = {key: int(i / quantile) for (i, key) in enumerate(sorted_set)}
    ranks = np.array(list(set(wmap.values())))
    ranks_l = len(ranks)
    D = pdist(ranks.reshape(-1, 1), 'minkowski', p=1.)

    return emd(hist1 / ranks_l, hist2 / ranks_l, squareform(D)) / ranks_l


def intersection_emd(column1, column2, bloom_filter=False):
    c1 = process_data(column1)
    c2 = process_data(column2)

    # compute the intersection of the 2 columns
    if bloom_filter:
        set_intersection = bloom_filter_intersection(c1, c2)
    else:
        set_intersection = list(set(c1).intersection(set(c2)))

    if len(set_intersection) == 0:
        return math.inf

    e1 = quantile_emd(c1, set_intersection, True)
    e2 = quantile_emd(c2, set_intersection, True)

    return (e1 + e2) / 2


def tokenize(column):
    c_type = column.dtype.type

    if (c_type is str) or (c_type is np.str_) or (c_type is np.object_):
        c = column.astype(str)
        c = np.chararray.translate(c, TABLE)
        c = np.char.lower(c)
        c = [nltk.word_tokenize(token) for token in c]
        return np.concatenate(c).ravel()

    return column


def make_numpy(column):
    if type(column) is not np.ndarray:
        return np.array(column)

    return column


def process_data(column):
    return tokenize(make_numpy(column))


def bloom_filter_intersection(column1, column2):
    c1 = list(set(process_data(column1)))
    c2 = list(set(process_data(column2)))
    bloom_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
    [bloom_filter.add(x) for x in c1]
    
    return np.extract(list(map(lambda x: x in bloom_filter, c2)), c2)
