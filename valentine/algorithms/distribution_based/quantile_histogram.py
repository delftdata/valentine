from numpy import ndarray
import numpy as np
import scipy.stats as ss
import math


class QuantileHistogram:
    """
    A class used to represent an equi-depth quantile histogram

    Attributes
    ----------
    name : str
        The column name
    ranks : ndarray
        The column's ranked data
    normalization : int
        The number that normalizes the histogram values
    quantiles : int
        The number of quantiles
    reference_hist : QuantileHistogram, optional
        The reference histogram that provides the bucket boundaries

    Methods
    -------
    get_values()
        Returns the histogram's bucket values (ranks)

    is_empty()
        Returns if the histogram is empty

    add_buckets(min_val: int, bb: list)
        Create the buckets with the given bucket boundaries

    add_values(values, norm=True)
        Add all values to buckets

    normalize_values()
        Normalize the bucket values based on the normalization factor

    bucket_binary_search(x)
        Find in which bucket the specific value falls into using binary search

    calc_dist_matrix()
         Compute the distance matrix between all buckets.
    """

    def __init__(self, name: tuple, ranks: ndarray, normalization: int, quantiles: int, reference_hist=None):
        """
        Parameters
        ----------
        name : tuple
            The column name (table_name, column_name)
        ranks : ndarray
            The column's ranked data
        normalization : int
            The number that normalizes the histogram values
        quantiles : int
            The number of quantiles
        reference_hist : QuantileHistogram, optional
            The reference histogram that provides the bucket boundaries
        """
        self.bucket_boundaries = {}
        self.bucket_values = {}
        self.name = name
        self.normalization_factor = normalization
        self.quantiles = quantiles
        self.dist_matrix = self.calc_dist_matrix()
        if reference_hist is None:
            self.add_buckets(ranks.min(initial=math.inf),
                             ss.mstats.mquantiles(ranks, np.array(list(range(1, quantiles + 1))) / quantiles))
            self.add_values(ranks)
        else:
            self.bucket_boundaries = reference_hist.bucket_boundaries
            self.add_values(ranks)

    @property
    def get_values(self):
        """
        Returns the histogram's bucket values (ranks)

        Returns
        -------
        ndarray
            The values inside the histogram
        """
        return np.array(list(self.bucket_values.values()))

    @property
    def is_empty(self):
        """
        Returns if the histogram is empty

        Returns
        -------
        bool
            True if the histogram is empty false if it is not
        """
        return np.sum(self.get_values) == 0

    def add_buckets(self, min_val: int, bb: list):
        """
        Create the buckets with the given bucket boundaries

        Parameters
        ----------
        min_val: int
            The minimum value of the histogram
        bb: list
            List containing the bucket boundaries
        """
        self.bucket_boundaries[0] = (min_val, bb[0])
        i = 0
        while i < len(bb) - 1:
            self.bucket_boundaries[i+1] = (bb[i], bb[i+1])
            i = i + 1

    def add_values(self, values, norm=True):
        """
        Add all values to buckets

        Parameters
        ----------
        values: ndarray
            The ranks to be added to the histogram
        norm: bool, optional
            Normalize the bucket values or not
        """
        for i in range(len(self.bucket_boundaries.values())):
            self.bucket_values[i] = 0.0
        for value in values:
            idx = self.bucket_binary_search(value)
            if idx != -1:
                self.bucket_values[idx] = self.bucket_values[idx] + 1.0
        if norm:
            self.normalize_values()

    def normalize_values(self):
        """
        Normalize the bucket values based on the normalization factor
        """
        self.bucket_values = {k: v / self.normalization_factor for k, v in self.bucket_values.items()}

    def bucket_binary_search(self, x):
        """
        Find in which bucket the specific value falls into using binary search
        Parameters
        ----------
        x: int
            The input value

        Returns
        -------
        int
            The bucket index that the value falls into or -1 if it does not fit in anyone
        """
        left = 0
        right = self.quantiles - 1
        while left <= right:
            mid = left + (right - left) // 2
            if self.bucket_boundaries[mid][0] <= x <= self.bucket_boundaries[mid][1]:
                return mid
            elif self.bucket_boundaries[mid][1] < x:
                left = mid + 1
            else:
                right = mid - 1
        return -1

    def calc_dist_matrix(self):
        """
        Compute the distance matrix between all buckets.
        E.g. with 256 buckets the matrix will be 256x256

        Returns
        -------
        ndarray
            The distances between the buckets
        """
        q = np.array(list(range(1, self.quantiles + 1))) / self.quantiles
        dist = []
        for i in q:
            temp = []
            for j in q:
                temp.append(abs(i - j))
            dist.append(temp)
        return np.array(dist)
