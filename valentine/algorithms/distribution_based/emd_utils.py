import math
from pyemd import emd

from .column_model import CorrelationClusteringColumn
from .quantile_histogram import QuantileHistogram


def quantile_emd(column1: CorrelationClusteringColumn,
                 column2: CorrelationClusteringColumn,
                 quantiles: int = 256):
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
    if column1.size == 0 or column2.size == 0:
        return math.inf

    histogram1 = column1.quantile_histogram
    histogram2 = QuantileHistogram(column2.long_name, column2.ranks, column2.size, quantiles,
                                   reference_hist=histogram1)
    if histogram2.is_empty:
        return math.inf
    h1 = histogram1.get_values/histogram1.get_values.sum()
    h2 = histogram2.get_values/histogram2.get_values.sum()
    return emd(h1, h2, histogram1.dist_matrix)


def intersection_emd(column1: CorrelationClusteringColumn,
                     column2: CorrelationClusteringColumn,
                     tmp_folder_path: str,
                     quantiles: int = 256):
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
    tmp_folder_path: str
        The path of the temporary folder that will serve as a cache for the run
    quantiles: int, optional
        The number of quantiles that the histograms are split on (default is 256)

    Returns
    -------
    float
        the intersection EMD value between column1 and column2
    """
    common_elements = set(list(column1.data)).intersection(set(list(column2.data)))

    # If the two columns do not share any common elements return inf
    if len(common_elements) == 0:
        return math.inf

    # The intersection of the two columns
    intersection = [x for x in list(column1.data) + list(column2.data)
                    if x in common_elements]
    intersection_column = CorrelationClusteringColumn(
        "", f"Intersection of {column1.long_name} {column2.long_name}",
        intersection, "", "", tmp_folder_path)

    e1 = quantile_emd(column1, intersection_column, quantiles)
    e2 = quantile_emd(column2, intersection_column, quantiles)

    del common_elements, intersection, intersection_column

    return (e1 + e2) / 2
