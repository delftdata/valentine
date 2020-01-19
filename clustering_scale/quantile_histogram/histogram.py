import scipy.stats as ss
from numpy import ndarray
import numpy as np
from clustering_scale.quantile_histogram.bucket import Bucket
import pickle


class QuantileHistogram(object):
    """
    A class used to represent an equi depth quantile histogram

     e.g. Quantile histogram with 256 buckets

    |------||------||------| ... |------|
        1       2       3    ...    256


    Attributes
    ----------
    buckets : dict
        a dictionary containing the buckets of the histogram {bucket_id: Bucket}


    Methods
    -------
    create_histogram(column: list, quantiles: int)
        Creates an equi depth quantile histogram for the specified column with the specified amount of quantiles

    create_histogram_from_reference(column: list)
        Creates an equi depth quantile histogram for the specified column based on the reference histogram

    get_buckets()
        Returns the histogram's buckets

    chunks(lst: ndarray, n: int)
        Splits a list into n equal sized chunks

    get_sorted_ranks(column: list)
         A UNIX-like "sort -n" function that works for mixed columns that might contain both numbers and strings.
        The strings are sorted first in lexicographic order and then the numbers in numeric and then it returns the
        ranks of the sorted data

    copy_buckets_without_values()
        Copies the bucket boundaries but not the values

     add_value_to_bucket(value: float)
        Adds a value into a bucket

     normalize_buckets(column_size: int)
        Normalizes the frequencies inside the buckets based on the column size

     print_histogram()
        Prints the histogram

     flatten_histogram()
        Flattens the histogram by creating two lists with all the bucket values with their weights
    """
    def __init__(self, name: str, column: list, quantiles: int, reference_hist=None):
        """
        Parameters
        ----------
        column : list
            the column's data
        quantiles : int
            the number of quantiles
        reference_hist : QuantileHistogram, optional
            the reference histogram that provides the bucket boundaries
        """
        self.name = name
        self.buckets = dict()
        if reference_hist is None:
            self.create_histogram(column, quantiles)
        else:
            self.reference_hist = reference_hist
            self.create_histogram_from_reference(column)

    def create_histogram(self, column: list, quantiles: int):
        """
        Creates an equi depth quantile histogram for the specified column with the specified amount of quantiles

        Parameters
        ---------
        column : list
            the column's data
        quantiles : int
            the number of quantiles
        """
        bucket_count = 0
        column_size = len(column)
        for chunk in self.chunks(self.get_sorted_ranks(column), quantiles):
        # for chunk in self.chunks(self.get_global_ranks(column), quantiles):
            self.buckets[bucket_count] = Bucket(chunk, column_size)
            bucket_count = bucket_count + 1
        self.normalize_buckets(self.size)

    def create_histogram_from_reference(self, column: list):
        """
        Creates an equi depth quantile histogram for the specified column based on the reference histogram

        Parameters
        ---------
        column : list
            the column's data
        """
        self.copy_buckets_without_values()
        for rank in self.get_sorted_ranks(column):
        # for rank in self.get_global_ranks(column):
            self.add_value_to_bucket(rank)
        self.normalize_buckets(self.size)

    def get_buckets(self):
        """Returns the histogram's buckets"""
        return self.buckets

    @staticmethod
    def chunks(lst: ndarray, n: int):
        """
        Splits a list into n equal sized chunks

        Parameters
        ---------
        lst : ndarray
            the list that is to be split into chunks
        n : int
            the number of chunks
        """
        return np.array_split(lst, n)

    @staticmethod
    def get_sorted_ranks(column: list):
        """
        A UNIX-like "sort -n" function that works for mixed columns that might contain both numbers and strings.
        The strings are sorted first in lexicographic order and then the numbers in numeric and then it returns the
        ranks of the sorted data

        Parameters
        ---------
        column : list
            the column data to get the ranks from

        Returns
        -------
        ndarray
            yields a chuck of the list at a time
        """
        # return sorted(ss.rankdata(list(map(lambda x: float(x) if type(x) is int else x, column)), method='dense'))
        return ss.rankdata(sorted(map(lambda x: float(x) if type(x) is int else x, list(column)),
                                  key=lambda item: ([str, float].index(type(item)), item)), method='dense')

    @staticmethod
    def get_global_ranks(column: list):
        with open('cache/global_ranks/ranks.pkl', 'rb') as pkl_file:
            global_ranks = pickle.load(pkl_file)
            ranks = sorted([global_ranks[x]for x in column])
            return ranks

    def copy_buckets_without_values(self):
        """Copies the bucket boundaries but not the values"""
        for bucket_idx, bucket in self.reference_hist.get_buckets().items():
            self.buckets[bucket_idx] = Bucket()
            self.buckets[bucket_idx].set_lower_bound(bucket.lower_bound)
            self.buckets[bucket_idx].set_upper_bound(bucket.upper_bound)

    def add_value_to_bucket(self, value: float):
        """
        Adds a value into a bucket

        Parameters
        ---------
        value : float
            the value to add to the bucket
        """
        if self.buckets[0].lower_bound > value or self.buckets[len(self.buckets) - 1].upper_bound < value:
            return

        for bucket_idx, bucket in self.buckets.items():
            if bucket.lower_bound <= value <= bucket.upper_bound:
                bucket.add_value(value)
                return

    def normalize_buckets(self, column_size: int):
        """
        Normalizes the frequencies inside the buckets based on the column size

        Parameters
        ---------
        column_size : int
            the size of the column
        """
        for bucket_idx, bucket in self.get_buckets().items():
            bucket.normalize_bucket(column_size)

    def print_histogram(self):
        """Prints the histogram"""
        print("Number of buckets: ", len(self.buckets.keys()))
        for k in self.buckets.keys():
            print("Bucket "+str(k)+" with size: "+str(self.buckets[k].size)+" with contents: ")
            print(self.buckets[k].contents)

    def bucket_generator(self):
        for bucket in self.buckets.values():
            yield np.array(bucket.items), np.array(bucket.weights)

    def flatten_histogram(self):
        """Flattens the histogram by creating two lists with all the bucket values with their weights"""
        contents = []
        weights = []
        for _, bucket in self.buckets.items():
            for c, w in bucket.contents.items():
                contents.append(c)
                weights.append(w)
        return contents, weights

    @property
    def size(self):
        size = 0
        for bucket in self.buckets.values():
            size = size + bucket.size
        return size

    @property
    def probabilities(self):
        probabilities = []
        for bucket in self.buckets.values():
            probabilities.append(sum(bucket.weights))
        return probabilities

    @property
    def is_empty(self):
        for bucket in self.buckets.values():
            if bucket.size != 0:
                return False
        return True
