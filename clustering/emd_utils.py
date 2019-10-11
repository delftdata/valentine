import math
import numbers
from operator import itemgetter

import numpy as np
from pyemd.emd import emd, emd_samples
from scipy.spatial.distance import pdist, squareform
from scipy import stats
from pybloom import ScalableBloomFilter


def quantile_emd(column1, column2, quantile=256, intersection=False):
    """
    Computes the quantile Earth Mover's Distance (EMD)

    :param column1: First column to be compared
    :type column1: column_model.Column
    :param column2: Second column to be compared
    :type column2: column_model.Column
    :param quantile: How granular the data should be
    :type quantile: int
    :param intersection: Default False. True if the method is called from intersection_emd
    :return: float
    """

    column_data1 = column1.get_tokens()
    # if the function is called from intersection_emd, then the second column is already a list of tokens
    if intersection:
        column_data2 = column2
    else:
        column_data2 = column2.get_tokens()

    # get the unique values
    set1 = set(column_data1)
    set2 = set(column_data2)

    # compute the union of the 2 columns
    set_union = list(set1.union(set2))

    # Some columns contain both numeric and string tokens that can't be sorted all together
    # Get the numeric values and sort them ascending
    numeric_values = np.array([x for x in set_union if isinstance(x, numbers.Number)]).astype(np.object)
    numeric_values = np.sort(numeric_values)

    # Get the string tokens and sort them ascending
    string_values = np.array(list(filter(lambda x: x not in numeric_values, set_union)))
    string_values = np.sort(string_values)

    # Form a new sorted list that contains first the numbers and then the strings
    sorted_set = np.append(numeric_values, string_values, axis=0)

    # rank the sorted values
    wmap = {key: i for (i, key) in enumerate(sorted_set)}

    # get the ranks for each column
    ranks1 = np.array(itemgetter(*list(set1))(wmap))
    ranks2 = np.array(itemgetter(*list(set2))(wmap))

    # check if the ranks contain more than 1 value and sort the lists
    # python throws an error if you try to sort an empty list
    if len(ranks1.shape) > 0:
        ranks1 = sorted(ranks1)
        l1 = len(ranks1)
    else:
        l1 = 1

    if len(ranks2.shape) > 0:
        ranks2 = sorted(ranks2)

    # get the bin edges by using N-quantile
    bin_edges1 = stats.mstats.mquantiles(ranks1, np.array(range(0, l1 + 1, quantile)) / l1)

    # compute the quantile histogram
    hist1, bins1 = np.histogram(ranks1, bins=bin_edges1)

    # use the edges from the quantile histogram to find the number of values in each bin (hist2)
    hist2, bins2 = np.histogram(ranks2, bins=bin_edges1)

    # use quantiles to create new ranks
    wmap = {key: int(i / quantile) for (i, key) in enumerate(sorted_set)}
    ranks = np.array(list(set(wmap.values())))
    ranks_l = len(ranks)

    # find the distance matrix between each word
    # D = pdist(ranks.reshape(-1, 1), 'minkowski', p=1.)

    # if one of the histograms is empty, it means that they don't have any values in common, but
    # emd will return 0 which is the perfect match, so instead I am returning math.inf
    if len(hist1) == 0 or len(hist2) == 0:
        return math.inf
    # return emd(hist1 / ranks_l, hist2 / ranks_l, squareform(D)) / ranks_l
    return emd_samples(hist1 / ranks_l, hist2 / ranks_l)


def intersection_emd(c1, c2, quantile, bloom_filter=False):
    """
    Computes the intersection EMD

    :param c1: First column
    :type c1: column_model.Column
    :param c2: Second column
    :type c2: column_model.Column
    :param quantile: How granular the data should be
    :type quantile: int
    :param bloom_filter: Default False. True if the data is large.
    :return: float
    """

    column_data1 = c1.get_tokens()
    column_data2 = c2.get_tokens()

    # for large data, bloom filter is recommended
    if bloom_filter:
        set_intersection = __bloom_filter_intersection(column_data1, column_data2)
    else:
        set_intersection = list(set(column_data1).intersection(set(column_data2)))

    # if the intersection is empty, then they don't have any values in common
    if len(set_intersection) == 0:
        return math.inf

    e1 = quantile_emd(c1, set_intersection, quantile, intersection=True)
    e2 = quantile_emd(c2, set_intersection, quantile, intersection=True)

    return (e1 + e2) / 2


def __bloom_filter_intersection(column1, column2):
    bloom_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
    # add the first column in the bloom filter
    [bloom_filter.add(x) for x in column1]

    # return the common values
    return np.extract(list(map(lambda x: x in bloom_filter, column2)), column2)
