import math
import scipy.stats as ss
from scipy.stats import wasserstein_distance
import numpy as np

from clustering_scale.column_model_scale import Column
from clustering_scale.quantile_histogram.histogram import QuantileHistogram


def quantile_emd(column1: Column, column2: Column, quantiles: int = 256):
    """
    Computes the Earth Mover's Distance (EMD) over two column quantile histograms

    If the argument `quantiles` isn't passed in, the default of the paper
    "Automatic Discovery of Attributes in Relational Databases" is used which is 256.

    Parameters
    ---------
    column1 : Column
        The first column
    column2 : Column
        The second column that we create its quantile histogram by doing a linear scan over the first's
    quantiles: int, optional
        The number of quantiles that the histograms are split on (default is 256)

    Returns
    -------
    float
        the EMD value between column1 and column2
    """
    histogram1 = column1.get_histogram()
    histogram2 = QuantileHistogram(column2.get_long_name(),
                                   column2.get_original_data(),
                                   min(column2.cardinality, quantiles),
                                   reference_hist=histogram1)

    if histogram2.is_empty:
        del histogram2
        return math.inf
    else:
        # i = list(range(1, quantiles + 1))
        p1 = histogram1.probabilities
        # print("Probabilities of H1: ", histogram1.name, round(sum(p1), 2))
        p2 = histogram2.probabilities
        # print("Probabilities of H2: ", histogram2.name, round(sum(p2), 2))
        e = wasserstein_distance(v_values=list(histogram1.buckets.keys()), v_weights=p1,
                                 u_values=list(histogram2.buckets.keys()), u_weights=p2) \
            / \
            (column1.size + column2.size)
        del histogram2
        return e


# def bucket_emd(hist1: QuantileHistogram, hist2: QuantileHistogram, quantiles: int):
#     e = 0
#
#     # hist2.print_histogram()
#     for (v1, w1), (v2, w2) in zip(hist1.bucket_generator(), hist2.bucket_generator()):
#         if len(v2) != 0:
#             if(hist1.name=="customer__c_nationkey" and hist2.name=="asia_customer__c_nationkey"):
#                 v1, v2 = get_new_ranks(v1, v2)
#                 # w1, w2 = normalize_bucket_weights(w1, w2)
#                 print(v1, w1)
#                 print(v2, w2)
#
#             e = e + wasserstein_distance(v_values=v1, v_weights=w1, u_values=v2, u_weights=w2)
#         else:
#             e = e + sum(w1)
#     return e / quantiles


# def bucket_emd(hist1: QuantileHistogram, hist2: QuantileHistogram, quantiles: int):
#     i = list(range(1, quantiles+1))
#     ww1 = []
#     ww2 = []
#     for (v1, w1), (v2, w2) in zip(hist1.bucket_generator(), hist2.bucket_generator()):
#         if len(v2) != 0:
#             ww2.append(0.0)
#         else:
#             ww2.append(sum(w2))
#         ww1.append(sum(w1))
#
#     print(ww1)
#     print(ww2)
#     print(i)
#     e = wasserstein_distance(v_values=i, v_weights=ww1, u_values=i, u_weights=ww2)
#     return e



def normalize_bucket_weights(w1, w2):
    idx1 = len(w1)
    idx2 = idx1 + len(w2)
    w = np.array(np.concatenate([w1, w2]))
    w = w / w.sum()
    w1 = w[0:idx1]
    w2 = w[idx1:idx2]
    return w1, w2


def get_new_ranks(v1, v2):
    idx1 = len(v1)
    idx2 = idx1 + len(v2)
    v = ss.rankdata(np.concatenate([v1, v2]), method='dense')
    v1 = v[0:idx1]
    v2 = v[idx1:idx2]
    return v1, v2


def intersection_emd(column1: Column, column2: Column, quantiles: int = 256):
    """
    Computes the intersection Earth Mover's Distance (EMD) over two column quantile histograms as described in
    "Automatic Discovery of Attributes in Relational Databases"

    Intersection_EMD(C, C') = (EMD(C, C∩C') + EMD(C', C∩C'))/2.

    If the argument `quantiles` isn't passed in, the default of the paper
    "Automatic Discovery of Attributes in Relational Databases" is used which is 256.

    Parameters
    ---------
    column1 : Column
        The first column
    column2 : Column
        The second column
    quantiles: int, optional
        The number of quantiles that the histograms are split on (default is 256)

    Returns
    -------
    float
        the intersection EMD value between column1 and column2
    """
    common_elements = set(list(column1.get_original_data())).intersection(set(list(column2.get_original_data())))

    # If the two columns do not share any common elements return inf
    if len(common_elements) == 0:
        return math.inf

    intersection = [x for x in list(column1.get_original_data()) + list(column2.get_original_data())
                    if x in common_elements]  # The intersection of the two columns

    intersection_column = Column("_", intersection, "_", "_", quantiles)

    e1 = quantile_emd(column1, intersection_column, quantiles)
    e2 = quantile_emd(column2, intersection_column, quantiles)

    del common_elements, intersection, intersection_column

    return (e1 + e2) / 2
