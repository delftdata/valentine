def convert_data_type(string: str):
    try:
        f = float(string)
        if f.is_integer():
            return int(f)
    except ValueError:
        return string
    else:
        return f
