from collections import defaultdict
from time import time

import nltk
import numpy as np
import string

nltk.download('punkt')


TABLE = str.maketrans({key: ' ' for key in string.punctuation})


def tokenize(column):
    c_type = column.dtype.type

    if (c_type is str) or (c_type is np.str_) or (c_type is np.object_):
        c = column.astype(str)
        c = np.chararray.translate(c, TABLE)
        c = np.char.lower(c)
        c = [nltk.word_tokenize(token) for token in c]
        return np.concatenate(c).ravel()

    return column


def make_numpy(column):
    if type(column) is not np.ndarray:
        return np.array(column)

    return column


def process_data(column):
    numpy_column = make_numpy(column)
    return tokenize(numpy_column)


class Column:
    def __init__(self, name, data, source_name):
        """
        :param name: Column name
        :param data: Column data instances
        :param source_name: The table name
        """

        self.__long_name = source_name + '_' + name
        self.__name = name
        self.__data = data
        self.tokens = []
        self.quantile_histogram = defaultdict(defaultdict)

    def get_original_name(self):
        return self.__name

    def get_original_data(self):
        return self.__data

    def get_long_name(self):
        return self.__long_name

    def get_tokens(self):
        return self.tokens

    def process_data(self):
        st = time()
        self.tokens = process_data(self.__data)
        et = time()
        print("\tProcess column %s took: %.2f" % (self.__name, et-st))




