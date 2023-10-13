from pathlib import Path


def is_sorted(matches: dict):
    prev = None
    for value in matches.values():
        if prev is None:
            prev = value
        else:
            if prev > value:
                return False
    return True


def convert_data_type(string: str):
    try:
        f = float(string)
        if f.is_integer():
            return int(f)
        return f
    except ValueError:
        return string


def normalize_distance(dist: int,
                       str1: str,
                       str2: str):
    """
    Function that returns a normalized similarity score between two strings given their distance

    Parameters
    ----------
    dist : int
        The distance between the two strings (hamming, levenshtein or damerau levenshtein)
    str1: str
        The first string
    str2: str
        The second string
    """

    return 1 - dist/max(max(len(str1), len(str2)), 1)


def get_project_root():
    return str(Path(__file__).parent.parent)
