import math

from ot import emd2

from .bloom_filter import BloomFilter
from .column_model import CorrelationClusteringColumn
from .quantile_histogram import QuantileHistogram


def quantile_emd(
    column1: CorrelationClusteringColumn,
    column2: CorrelationClusteringColumn,
    quantiles: int = 256,
):
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
    histogram2 = QuantileHistogram(
        column2.long_name,
        column2.ranks,
        column2.size,
        quantiles,
        reference_hist=histogram1,
    )
    if histogram2.is_empty:
        return math.inf
    h1 = histogram1.get_values / histogram1.get_values.sum()
    h2 = histogram2.get_values / histogram2.get_values.sum()
    return emd2(h1, h2, histogram1.dist_matrix)


def intersection_emd(
    column1: CorrelationClusteringColumn,
    column2: CorrelationClusteringColumn,
    tmp_folder_path: str,
    quantiles: int = 256,
    use_bloom_filters: bool = False,
):
    """
    Computes the intersection Earth Mover's Distance (EMD) over two column quantile histograms as described in
    "Automatic Discovery of Attributes in Relational Databases"

    Intersection_EMD(C, C') = (EMD(C, C∩C') + EMD(C', C∩C'))/2.

    When ``use_bloom_filters`` is True, the intersection is approximated using Bloom filters
    as described in Section 4 of the paper. This avoids materializing the full intersection
    set and is beneficial for very large columns, at the cost of possible false positives.

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
    use_bloom_filters: bool, optional
        If True, use Bloom filters for approximate intersection (default is False)

    Returns
    -------
    float
        the intersection EMD value between column1 and column2
    """
    if use_bloom_filters:
        return _intersection_emd_bloom(column1, column2, tmp_folder_path, quantiles)
    return _intersection_emd_exact(column1, column2, tmp_folder_path, quantiles)


def _intersection_emd_exact(
    column1: CorrelationClusteringColumn,
    column2: CorrelationClusteringColumn,
    tmp_folder_path: str,
    quantiles: int,
) -> float:
    """Compute intersection EMD using exact set intersection."""
    common_elements = set(column1.data).intersection(set(column2.data))

    if len(common_elements) == 0:
        return math.inf

    # Per the paper (Definition 8): EMD_∩(C,C') = (EMD(C, C∩C') + EMD(C', C∩C')) / 2
    # Create separate intersection columns filtered from each source column's data,
    # so that each half of the formula compares against its own column's distribution
    # of common values.
    intersection1 = [x for x in column1.data if x in common_elements]
    intersection_column1 = CorrelationClusteringColumn(
        "",
        f"Intersection of {column1.long_name} {column2.long_name}",
        intersection1,
        "",
        "",
        tmp_folder_path,
    )

    intersection2 = [x for x in column2.data if x in common_elements]
    intersection_column2 = CorrelationClusteringColumn(
        "",
        f"Intersection of {column2.long_name} {column1.long_name}",
        intersection2,
        "",
        "",
        tmp_folder_path,
    )

    e1 = quantile_emd(column1, intersection_column1, quantiles)
    e2 = quantile_emd(column2, intersection_column2, quantiles)

    return (e1 + e2) / 2


def _intersection_emd_bloom(
    column1: CorrelationClusteringColumn,
    column2: CorrelationClusteringColumn,
    tmp_folder_path: str,
    quantiles: int,
) -> float:
    """
    Compute intersection EMD using Bloom filters for approximate intersection.

    Per Section 4 of the paper: build a Bloom filter for each column, then scan
    the other column probing the filter to approximate the intersection. This may
    introduce false positives (values incorrectly considered common), but never
    false negatives.
    """
    # Build Bloom filters for both columns
    bf1 = BloomFilter.from_iterable(column1.data, len(column1.data))
    bf2 = BloomFilter.from_iterable(column2.data, len(column2.data))

    # Approximate intersection: scan each column and probe the other's Bloom filter
    intersection1 = [x for x in column1.data if x in bf2]
    intersection2 = [x for x in column2.data if x in bf1]

    # If neither column has values that pass the other's filter, no intersection
    if len(intersection1) == 0 and len(intersection2) == 0:
        return math.inf

    # Handle edge cases where only one direction found matches
    if len(intersection1) == 0:
        intersection1 = intersection2
    if len(intersection2) == 0:
        intersection2 = intersection1

    intersection_column1 = CorrelationClusteringColumn(
        "",
        f"BloomIntersection of {column1.long_name} {column2.long_name}",
        intersection1,
        "",
        "",
        tmp_folder_path,
    )
    intersection_column2 = CorrelationClusteringColumn(
        "",
        f"BloomIntersection of {column2.long_name} {column1.long_name}",
        intersection2,
        "",
        "",
        tmp_folder_path,
    )

    e1 = quantile_emd(column1, intersection_column1, quantiles)
    e2 = quantile_emd(column2, intersection_column2, quantiles)

    return (e1 + e2) / 2
