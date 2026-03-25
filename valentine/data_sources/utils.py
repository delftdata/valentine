import csv
from pathlib import Path

import chardet
from dateutil.parser import parse


def get_encoding(ds_path: str) -> str:
    """Returns the encoding of the file"""
    test_str = b""
    number_of_lines_to_read = 500
    count = 0
    with Path(ds_path).open("rb") as f:
        line = f.readline()
        while line and count < number_of_lines_to_read:
            test_str = test_str + line
            count += 1
            line = f.readline()
        result = chardet.detect(test_str)
    if result["encoding"] == "ascii":
        return "utf-8"
    return result["encoding"]


def get_delimiter(ds_path: Path) -> str:
    """Returns the delimiter of the csv file"""
    with Path(ds_path).open() as f:
        first_line = f.readline()
        s = csv.Sniffer()
        return str(s.sniff(first_line).delimiter)


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.
    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(str(string), fuzzy=fuzzy)
    except Exception:
        return False
    else:
        return True
