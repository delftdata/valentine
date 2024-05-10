import csv

import chardet
import numpy as np
from dateutil.parser import parse


def get_encoding(ds_path: str) -> str:
    """ Returns the encoding of the file """
    test_str = b''
    number_of_lines_to_read = 500
    count = 0
    with open(ds_path, 'rb') as f:
        line = f.readline()
        while line and count < number_of_lines_to_read:
            test_str = test_str + line
            count += 1
            line = f.readline()
        result = chardet.detect(test_str)
    if result['encoding'] == 'ascii':
        return 'utf-8'
    else:
        return result['encoding']


def get_delimiter(ds_path: str) -> str:
    """ Returns the delimiter of the csv file """
    with open(ds_path) as f:
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
        return True
    except Exception:
        return False


def add_noise_to_df_column(df, column_name, noise_level):
    """
    Adds noise to a specified column in a DataFrame.

    Parameters:
    - df (pd.DataFrame): The DataFrame containing the column to which noise will be added.
    - column_name (str): The name of the column to which noise will be added.
    - noise_level (float): The level of noise to be added. For numerical columns, this indicates the standard deviation
                            of the Gaussian noise. For string columns, it represents the probability of permuting the
                            characters of each string.

    Returns:
    - pd.DataFrame: The DataFrame with noise added to the specified column.
    """
    if df[column_name].dtype in ["int64", "float64"]:
        noise = np.random.normal(0, noise_level, df[column_name].shape[0])
        df[column_name] = df[column_name] + noise
    elif df[column_name].dtype == "object":
        for _ in range(df[column_name].shape[0]):
            if np.random.rand() < noise_level:
                df[column_name] = df[column_name].apply(lambda x: ''.join(np.random.permutation(list(str(x)))))
    return df

# if __name__ == "__main__":
#     add_noise_to_df_column(pd.DataFrame({'a': [1, 2, 3], 'b': ['abcdefg', 'hijklmn', 'opqrst']}), 'b', 0.99)
