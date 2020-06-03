from pathlib import Path
import math
import chardet
import csv
import os


def is_sorted(dictionary: dict):
    """
    Function that checks if a dict is sorted (true if sorted false if not)

    Parameters
    ----------
    dictionary: dict
        The dict to check

    Returns
    -------
    bool
        If the dict is sorted or not
    """
    prev = None
    for value in dictionary.values():
        if prev is None:
            prev = value
        else:
            if prev > value:
                return False
    return True


def convert_data_type(value: str):
    """
    Takes a value as a string and return it with its proper datatype attached

    Parameters
    ----------
    value: str
        The value that we want to find its datatype as a string

    Returns
    -------
    The value with the proper datatype
    """
    try:
        f = float(value)
        if f.is_integer():
            return int(f)
        return f
    except ValueError:
        return value


def get_project_root():
    """ Return the root of the project as a string """
    return str(Path(__file__).parent.parent)


def get_relative_path(path: str):
    """ Return the relative path of a file from the project root """
    return os.path.relpath(path, get_project_root())


def create_folder(path: str):
    """ Create a folder on the given path """
    if not os.path.exists(path):
        os.makedirs(path)


def one_to_one_matches(matches: dict):
    """
    A filter that takes a dict of column matches and returns a dict of 1 to 1 matches. The filter works in the following
    way: At first it gets the median similarity of the set of the values and removes all matches
    that have a similarity lower than that. Then from what remained it matches columns for me highest similarity
    to the lowest till the columns have at most one match.

    Parameters
    ----------
    matches : dict
        The ranked list of matches

    Returns
    -------
    dict
        The ranked list of matches after the 1 to 1 filter
    """
    set_match_values = set(matches.values())

    if len(set_match_values) < 2:
        return matches

    matched = dict()

    for key in matches.keys():
        matched[key[0]] = False
        matched[key[1]] = False

    median = list(set_match_values)[math.ceil(len(set_match_values)/2)]

    matches1to1 = dict()

    for key in matches.keys():
        if (not matched[key[0]]) and (not matched[key[1]]):
            similarity = matches.get(key)
            if similarity >= median:
                matches1to1[key] = similarity
                matched[key[0]] = True
                matched[key[1]] = True
            else:
                break
    return matches1to1


def get_table_from_dataset_path(ds_path: str):
    """ Returns the table name from the dataset path """
    return ds_path.split(".")[0].split("/")[-1]


def get_encoding(ds_path: str) -> str:
    """ Returns the encoding of the file """
    with open(ds_path, 'rb') as f:
        return chardet.detect(f.read())['encoding']


def get_delimiter(ds_path: str) -> str:
    """ Returns the encoding of the csv file """
    with open(ds_path) as f:
        first_line = f.readline()
        s = csv.Sniffer()
        return s.sniff(first_line).delimiter
