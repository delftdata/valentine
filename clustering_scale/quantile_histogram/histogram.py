from numpy import ndarray
import numpy as np
import scipy.stats as ss


class QuantileHistogram(object):
    """
    A class used to represent an equi depth quantile histogram

     e.g. Quantile histogram with 256 buckets

    |------||------||------| ... |------|
        1       2       3    ...    256


    Attributes
    ----------



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
    def __init__(self, name: str, ranks: ndarray, normalization: int, quantiles: int, reference_hist=None):
        """
        Parameters
        ----------
        ranks : list
            the column's ranked data
        quantiles : int
            the number of quantiles
        reference_hist : QuantileHistogram, optional
            the reference histogram that provides the bucket boundaries
        """
        self.bucket_boundaries = {}
        self.bucket_values = {}
        self.name = name
        self.normalization_factor = normalization
        self.quantiles = quantiles
        self.dist_matrix = self.calc_dist_matrix()

        if reference_hist is None:
            self.add_buckets(ranks.min(), ss.mstats.mquantiles(ranks, np.array(list(range(1, quantiles + 1))) / quantiles))
            self.add_values(ranks)
        else:
            self.bucket_boundaries = reference_hist.bucket_boundaries
            self.add_values(ranks)

    @property
    def get_values(self):
        return np.array(list(self.bucket_values.values()))

    @property
    def is_empty(self):
        return np.sum(self.get_values) == 0

    def add_buckets(self, min_val, bb):
        self.bucket_boundaries[0] = (min_val, bb[0])
        i = 0
        while i < len(bb) - 1:
            self.bucket_boundaries[i+1] = (bb[i], bb[i+1])
            i = i + 1
        # self.bucket_boundaries[i] = (self.bucket_boundaries[i-1][1], math.inf)

    def add_values(self, values, norm=True):
        for i in range(len(self.bucket_boundaries.values())):
            self.bucket_values[i] = 0.0
        for value in values:
            idx = self.bucket_binary_search(value)
            if idx != -1:
                self.bucket_values[idx] = self.bucket_values[idx] + 1.0
        if norm:
            self.normalize_values()

    def normalize_values(self):
        self.bucket_values = {k: v / self.normalization_factor for k, v in self.bucket_values.items()}

    def bucket_binary_search(self, x):
        lelf = 0
        right = self.quantiles - 1
        while lelf <= right:
            mid = lelf + (right - lelf) // 2
            if self.bucket_boundaries[mid][0] <= x <= self.bucket_boundaries[mid][1]:
                return mid
            elif self.bucket_boundaries[mid][1] < x:
                lelf = mid + 1
            else:
                right = mid - 1
        return -1

    def calc_dist_matrix(self):
        q = np.array(list(range(1, self.quantiles + 1))) / self.quantiles
        dist = []
        for i in q:
            temp = []
            for j in q:
                temp.append(abs(i - j))
            dist.append(temp)
        return np.array(dist)
