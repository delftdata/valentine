import re
import json

from definitions import ROOT_DIR

pk_fk: dict = {'c_custkey': ['o_custkey'],
               'o_orderkey': ['l_orderkey'],
               'n_nationkey': ['c_nationkey', 's_nationkey'],
               'p_partkey': ['ps_partkey', 'l_partkey'],
               'r_regionkey': ['n_regionkey'],
               'ps_partkey': ['l_partkey'],
               'ps_suppkey': ['l_suppkey'],
               's_suppkey': ['ps_suppkey', 'l_suppkey']
               }


def rule1(column1: str, column2: str):
    """
    A primary/foreign key
    """
    if (column1 in pk_fk.keys() and column2 in pk_fk[column1]) or \
            (column2 in pk_fk.keys() and column1 in pk_fk[column2]):
        return True
    return False


def rule2(column1: str, column2: str):
    """
    Two foreign keys referring to the same primary key
    """
    for fks_of_pk in pk_fk.values():
        if column1 in fks_of_pk and column2 in fks_of_pk:
            return True
    return False


def rule3_4(column1: str, column2: str):
    """
    3) A column in a view and the corresponding column in the base table
    4) Two columns in two views but from the same column in the base table
    """
    if column1 == column2:
        return True
    else:
        return False


def rule5():
    return False


def gold_standard(match: str):
    table1, column1, table2, column2 = get_columns_tables_from_match(match)
    return rule1(column1, column2) or rule2(column1, column2) or rule3_4(column1, column2) or rule5()


def get_columns_tables_from_match(match: str):
    match_obj = re.match(r'(.*)__(.*)__(.*)__(.*)', match)
    return match_obj.group(1), match_obj.group(2), match_obj.group(3), match_obj.group(4)


def calculate_accuracy(clusters: dict):
    total = 0.0
    correct = 0.0
    for matches in clusters.values():
        for match in matches:
            if gold_standard(match):
                correct = correct + 1.0
            total = total + 1.0
    print("Accuracy: ", (correct/total)*100, "%")


def check_matches_json(file_path: str):
    with open(ROOT_DIR + "/" + file_path) as json_file:
        clusters = json.load(json_file)
        calculate_accuracy(clusters)
