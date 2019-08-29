import math
import numbers
import string
from operator import itemgetter

import nltk
import numpy as np
from pyemd.emd import emd
from scipy.spatial.distance import pdist, squareform
from scipy.stats import stats


TABLE = str.maketrans({key: ' ' for key in string.punctuation})


def quantile_emd(column1, column2, quantile=256):
    if type(column1) is not np.ndarray:
        column1 = np.array(column1)
    if type(column2) is not np.ndarray:
        column2 = np.array(column2)

    # if data is string, process it (apply lowercase, remove punctuation and tokenize the strings)
    column1 = __tokenize(column1)
    column2 = __tokenize(column2)

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

    e = emd(hist1 / ranks_l, hist2 / ranks_l, squareform(D)) / ranks_l

    return e


def intersection_emd(column1, column2):
    if type(column1) is not np.ndarray:
        column1 = np.array(column1)
    if type(column2) is not np.ndarray:
        column2 = np.array(column2)

    # if data is string, process it (apply lowercase, remove punctuation and tokenize the strings)
    column1 = __tokenize(column1)
    column2 = __tokenize(column2)

    # get the unique values
    set1 = set(column1)
    set2 = set(column2)

    # compute the union of the 2 columns
    set_intersection = list(set1.intersection(set2))

    if len(set_intersection) == 0:
        return math.inf

    e1 = quantile_emd(column1, set_intersection)
    e2 = quantile_emd(column2, set_intersection)

    return (e1 + e2) / 2


def __tokenize(column):
    if (type(column[0]) is str) or (type(column[0]) is np.str_):
        column = column.astype(str)
        column = np.chararray.translate(column, TABLE)
        column = np.char.lower(column)
        column = [nltk.word_tokenize(token) for token in column]
        column = np.concatenate(column).ravel()
    return column
