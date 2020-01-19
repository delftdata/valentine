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
                                   column2.get_original_data(), quantiles, reference_hist=histogram1)

    if histogram2.is_empty:
        del histogram2
        return math.inf
    else:
        e = bucket_emd(histogram1, histogram2, quantiles) / (column1.cardinality + column2.cardinality)
        del histogram2
        return e


def bucket_emd(hist1: QuantileHistogram, hist2: QuantileHistogram, quantiles: int):
    e = 0
    for (v1, w1), (v2, w2) in zip(hist1.bucket_generator(), hist2.bucket_generator()):
        if len(v2) != 0:
            e = e + wasserstein_distance(v_values=v1, v_weights=w1, u_values=v2, u_weights=w2)
        else:
            e = e + sum(w1)
    return e / quantiles


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