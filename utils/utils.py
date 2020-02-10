from pathlib import Path
import os


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


def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent


def create_folder(path: str):
    if not os.path.exists(path):
        os.makedirs(path)
