import pickle
from collections.abc import Iterable, Sequence
from functools import lru_cache
from itertools import combinations
from pathlib import Path
from typing import Any

from ...data_sources.base_column import BaseColumn
from ...utils.utils import convert_data_type
from .column_model import CorrelationClusteringColumn
from .emd_utils import intersection_emd, quantile_emd
from .quantile_histogram import QuantileHistogram


def compute_cutoff_threshold(matrix_c: list, threshold: float) -> float:
    """
    Algorithm 1 of the paper "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]
    This algorithm computes the threshold of a column that determines if any other column is to be considered
    its neighbour.

    Parameters
    ---------
    matrix_c : list
        A list containing dicts of EMD/ColumnName pairs
    threshold : float
        The conservative global EMD cutoff threshold described in [1]

    Returns
    -------
    float
        The cutoff threshold of the input column
    """
    matrix_c.append({"e": threshold, "c": 0})
    matrix_c.sort(key=lambda k: k["e"])
    cutoff = 0.0
    gap = 0.0
    i = 0
    while i < len(matrix_c) - 1 and matrix_c[i + 1]["e"] <= threshold:
        if gap < (matrix_c[i + 1]["e"] - matrix_c[i]["e"]):
            gap = matrix_c[i + 1]["e"] - matrix_c[i]["e"]
            cutoff = matrix_c[i]["e"]
        i += 1
    return cutoff


def column_combinations(
    columns: list[tuple[Any, Any, Any, Any]],
    quantiles: int,
    tmp_folder_path: str,
    intersection: bool = False,
    use_bloom_filters: bool = False,
) -> Iterable[tuple]:
    """
    All the unique pairwise combinations between columns (Algorithm 2, lines 3-8 of the paper).

    Generates ALL unique column pairs, including same-table pairs, so that distribution
    clusters and attribute graphs are built with full pairwise information. Cross-table
    filtering is applied later in the output ranking stage.

    Parameters
    ---------
    columns : list
        A list that contains all the column names
    quantiles : int
        The number of quantiles that the histograms are split on
    tmp_folder_path: str
        The path of the temporary folder that will serve as a cache for the run
    intersection : bool, optional
        If true do the intersection EMD else the normal EMD
    use_bloom_filters : bool, optional
        If true use Bloom filters for approximate intersection (default is False)

    Returns
    -------
    tuple
        A tuple with ((column_name1, column_name2), quantiles, intersection, tmp_folder_path, use_bloom_filters)
    """
    for ci, cj in combinations(columns, 2):
        yield (ci, cj), quantiles, intersection, tmp_folder_path, use_bloom_filters


def process_emd(tup: tuple) -> tuple[tuple[Any, Any], float]:
    """
    Function defining a single quantile_emd process between two columns.

    Parameters
    ---------
    tup : tuple
        A tuple with ((column_name1, column_name2), quantiles, intersection, tmp_folder_path, use_bloom_filters)

    Returns
    -------
    tuple
        a dictionary entry {k: joint key of the column combination, v: quantile_emd calculation}
    """
    name_i, name_j, k, quantile, intersection, tmp_folder_path, use_bloom_filters = (
        unwrap_process_input_tuple(tup)
    )
    tn_i, _, cn_i, _ = name_i
    tn_j, _, cn_j, _ = name_j
    c1 = read_from_cache(f"{make_filename_safe(tn_i)}_{make_filename_safe(cn_i)}", tmp_folder_path)
    c2 = read_from_cache(f"{make_filename_safe(tn_j)}_{make_filename_safe(cn_j)}", tmp_folder_path)
    if intersection:
        return k, intersection_emd(c1, c2, tmp_folder_path, quantile, use_bloom_filters)
    return k, quantile_emd(c1, c2, quantile)


@lru_cache(maxsize=512)
def read_from_cache(file_name: str, tmp_folder_path: str) -> CorrelationClusteringColumn:
    """
    Function that reads from a pickle file lru cache a column after pre-processing

    Parameters
    ----------
    file_name: str
        The file name that contains the
    tmp_folder_path: str
        The path of the temporary folder that will serve as a cache for the run

    Returns
    -------
    CorrelationClusteringColumn
        The preprocessed column
    """
    return get_column_from_store(file_name, tmp_folder_path)


def unwrap_process_input_tuple(
    tup: tuple,
) -> tuple[
    tuple[Any, Any, Any, Any],
    tuple[Any, Any, Any, Any],
    tuple[Any, Any],
    int,
    bool,
    str,
    bool,
]:
    """
    Helper function that unwraps a tuple to its components and creates a unique key for the column combination

    Parameters
    ---------
    tup : tuple
        the tuple to unwrap
    """
    names, quantile, intersection, tmp_folder_path, use_bloom_filters = tup
    name_i, name_j = names
    k = (name_i, name_j)
    return name_i, name_j, k, quantile, intersection, tmp_folder_path, use_bloom_filters


def insert_to_dict(dc: dict[Any, list[dict[str, Any]]], k: Any, v: dict[str, Any]) -> None:
    """
    Helper function that instantiates a list to a dictionary key if it is not present and then appends an
    EMD/ColumnName pair to it

    Parameters
    ---------
    dc : dict
        the dictionary
    k : str
        the key
    v : dict
         EMD/ColumnName pair
    """
    if k not in dc:
        dc[k] = []
    dc[k].append(v)


def transform_dict(dc: dict[tuple[Any, Any], float]) -> dict[Any, list[dict[str, Any]]]:
    """
    Helper function that transforms a dict with composite column combination keys to a dict with column keys and
    values EMD/ColumnName pairs in a sorted list (ascending based on the EMD value)

    Parameters
    ---------
    dc : dict
        the dictionary
    """
    tmp_dict: dict[Any, list[dict[str, Any]]] = {}
    append = insert_to_dict
    for (k1, k2), v in dc.items():
        append(tmp_dict, k1, {"e": v, "c": k2})
        append(tmp_dict, k2, {"e": v, "c": k1})
    return tmp_dict


def process_columns(tup: tuple) -> None:
    """
    Process a pandas dataframe column to a column_model_scale.Column

    Parameters
    ---------
    tup : tuple
        tuple containing the information of the column to be processed
    """
    (
        column_name,
        column_uid,
        data,
        source_name,
        source_guid,
        quantiles,
        tmp_folder_path,
    ) = tup
    Path(tmp_folder_path).mkdir(parents=True, exist_ok=True)
    column = CorrelationClusteringColumn(
        column_name, column_uid, data, source_name, source_guid, tmp_folder_path
    )
    if column.size > 0:
        column.quantile_histogram = QuantileHistogram(
            column.long_name, column.ranks, column.size, quantiles
        )
    with Path(
        Path(tmp_folder_path)
        / f"{make_filename_safe(column.table_name)}_{make_filename_safe(column.name)}.pkl"
    ).open(
        "wb",
    ) as output:
        pickle.dump(column, output, pickle.HIGHEST_PROTOCOL)
    del column


def parallel_cutoff_threshold(
    tup: tuple,
) -> list[tuple[tuple[Any, Any, Any, Any], tuple[Any, Any, Any, Any]]]:
    """
    Process the cutoff threshold in parallel for each column

    Parameters
    ---------
    tup : tuple
        tuple containing the information of the column to be processed
    """
    matrix_a, column, threshold = tup
    name_i = column.long_name
    theta = compute_cutoff_threshold(matrix_a[name_i], threshold)
    n_c = [(name_i, i["c"]) for i in matrix_a[name_i] if i["e"] <= theta]
    return n_c


def ingestion_column_generator(
    columns: Sequence[BaseColumn],
    table_name: str,
    table_guid: object,
    quantiles: int,
    tmp_folder_path: str,
) -> Iterable[tuple[str, object, Sequence[Any], str, object, int, str]]:
    """
    Generator of incoming pandas dataframe columns
    """
    for column in columns:
        if not column.is_empty:
            yield (
                column.name,
                column.unique_identifier,
                column.data,
                table_name,
                table_guid,
                quantiles,
                tmp_folder_path,
            )


def cuttoff_column_generator(
    matrix_a: dict,
    columns: list[tuple[str, str, str, str]],
    threshold: float,
    tmp_folder_path: str,
) -> Iterable[tuple[dict, CorrelationClusteringColumn, float]]:
    """
    Generator of columns for the cutoff threshold computation
    """
    for column_name in columns:
        tn_i, _, cn_i, _ = column_name
        f_name = f"{make_filename_safe(tn_i)}_{make_filename_safe(cn_i)}"
        column = read_from_cache(f_name, tmp_folder_path)
        yield matrix_a, column, threshold


def generate_global_ranks(data: list, tmp_folder_path: str) -> None:
    """
    Function that creates a pickle file with the global ranks of all the values inside the database.

    Parameters
    ----------
    data : list
        All the values from every column
    tmp_folder_path: str
        The path of the temporary folder that will serve as a cache for the run
    """
    Path(tmp_folder_path).mkdir(parents=True, exist_ok=True)
    ranks = _compute_ranks(set(data))
    with Path(Path(tmp_folder_path) / "ranks.pkl").open("wb") as output:
        pickle.dump(ranks, output, pickle.HIGHEST_PROTOCOL)


def _compute_ranks(corpus: set) -> dict[Any, int]:
    """
    Compute global ranks for all unique values using type-aware sorting.

    Per Section 2.3 of the paper: numeric values are sorted numerically,
    string values are sorted lexicographically. Numbers are ranked first,
    followed by strings, so that different data types naturally separate
    into different distribution clusters in Phase 1.

    Parameters
    ----------
    corpus: set
        The corpus (all the unique values from every column)

    Returns
    -------
    dict
        The ranks in the form of k: value, v: the rank of the value
    """
    numeric_values: list[int | float] = []
    string_values: list[str] = []

    for val in corpus:
        converted = convert_data_type(str(val))
        if isinstance(converted, (int, float)):
            numeric_values.append(converted)
        else:
            string_values.append(converted)

    numeric_values.sort()
    string_values.sort()

    ranks: dict[Any, int] = {}
    rank = 1
    for val in numeric_values:
        ranks[val] = rank
        rank += 1
    for val in string_values:
        ranks[val] = rank
        rank += 1

    return ranks


def get_column_from_store(file_name: str, tmp_folder_path: str) -> CorrelationClusteringColumn:
    file_path = Path(tmp_folder_path) / f"{file_name}.pkl"
    with Path(file_path).open("rb") as pkl_file:
        data = pickle.load(pkl_file)
    return data


@lru_cache(maxsize=4096)
def make_filename_safe(file_name: str) -> str:
    return "".join(c for c in file_name if c.isalpha() or c.isdigit() or c == " ").rstrip()
