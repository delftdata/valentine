def is_sorted(matches: dict):
    prev = None
    for value in matches.values():
        if prev is None:
            prev = value
        elif prev > value:
            return False
    return True


def convert_data_type(string: str):
    try:
        f = float(string)
        if f.is_integer():
            return int(f)
    except ValueError:
        return string
    else:
        return f
