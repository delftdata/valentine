from sortedcontainers import SortedList

from clustering_scale.emd_utils import quantile_emd, intersection_emd


def column_combinations(columns, quantile):
    """
    To balance the load of each process, instead of giving as input one iteration of the emd's calculation outer-loop
    which is unbalanced. This function gives as input (in a lazy way) one column combination.
    :param columns: The columns of the database
    :param quantile: The number of quantiles for the quantile-EMD calculation
    :return: A tuple with ((column_name1, column_name1), (column1, column2), #quantiles)
    """
    c = len(columns)
    c_i = 0
    while c_i < c:
        name_i = columns[c_i].get_long_name()
        c_j = c_i + 1
        while c_j < c:
            name_j = columns[c_j].get_long_name()
            yield (name_i, name_j), (columns[c_i].get_tokens(), columns[c_j].get_tokens()), quantile
            c_j = c_j + 1
        c_i = c_i + 1


def process_emd(tup):
    """
    Function defining a single quantile_emd process between two columns.
    :param tup: Tuple containing (column1, column2, unique joint key of the column combination, #quantiles)
    :return: a dictionary entry {k: joint key of the column combination, v: quantile_emd calculation}
    """
    c1, c2, k, quantile = unwrap_process_input_tuple(tup)
    return k, quantile_emd(c1, c2, quantile)


def process_intersection_emd(tup):
    """
    Function defining a single intersection_quantile_emd process between two columns.
    :param tup: Tuple containing (column1, column2, unique joint key of the column combination, #quantiles)
    :return: a dictionary entry {k: joint key of the column combination, v: intersection_quantile_emd calculation}
    """
    c1, c2, k, quantile = unwrap_process_input_tuple(tup)
    return k, intersection_emd(c1, c2, quantile)


def unwrap_process_input_tuple(tup):
    """Helper function that unwraps a tuple to its components and creates a unique key for the column combination"""
    indexes, cols, quantile = tup
    c1, c2 = cols
    name_i, name_j = indexes
    k = str(name_i) + "|" + str(name_j)
    return c1, c2, k, quantile


def transform_dict(dc: dict):
    """Helper function that transforms a dict with composite column combination keys to a dict with column keys and
    values EMD/ColumnName pairs in a sorted list (ascending based on the EMD value)"""
    tmp_dict = dict()
    for k, v in dc.items():
        k1, k2 = k.split("|")
        v1 = {'e': v, 'c': k2}
        v2 = {'e': v, 'c': k1}
        insert_to_dict(tmp_dict, k1, v1)
        insert_to_dict(tmp_dict, k2, v2)
    return tmp_dict


def insert_to_dict(dc: dict, k, v):
    """Helper function that instantiates a sorted list to a dictionary key if it is not present and then appends an
    EMD/ColumnName pair to it"""
    if k not in dc:
        dc[k] = SortedList(key=lambda e: e['e'])
    dc[k].add(v)