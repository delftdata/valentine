from pathlib import Path
import math
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


def get_project_root():
    return str(Path(__file__).parent.parent)


def create_folder(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def one_to_one_matches(matches: dict):
    if len(matches) < 2:
        return matches

    matched = dict()

    for key in matches.keys():
        matched[key[0]] = False
        matched[key[1]] = False

    median = list(matches.values())[math.ceil(len(matches)/2)]

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


def get_table_from_dataset_path(ds_path):
    return ds_path.split(".")[0].split("/")[-1]
