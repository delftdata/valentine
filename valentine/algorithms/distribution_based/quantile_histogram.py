from functools import lru_cache
from statistics import quantiles

import numpy as np
from numpy import ndarray


@lru_cache(maxsize=32)
def _bucket_distance_matrix(n_buckets: int) -> ndarray:
    """Return the |i - j| / n distance matrix between bucket centres.

    The matrix only depends on the number of buckets, not on column data,
    so it can be computed once per ``n_buckets`` value and shared across
    every ``QuantileHistogram`` in a run. The previous implementation
    rebuilt this 256x256 matrix in a Python double-loop on every histogram
    construction, which dominated the DistributionBased profile.
    """
    q = np.arange(1, n_buckets + 1, dtype=np.float64) / n_buckets
    return np.abs(q[:, None] - q[None, :])


class QuantileHistogram:
    """
    A class used to represent an equi-depth quantile histogram

    Attributes
    ----------
    name : str
        The column name

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
    """

    __slots__ = (
        "_lower_bounds",
        "_min_val",
        "_upper_bounds",
        "bucket_boundaries",
        "bucket_values",
        "dist_matrix",
        "n_buckets",
        "name",
        "normalization_factor",
        "quantiles",
    )

    def __init__(
        self,
        name: tuple,
        ranks: ndarray,
        normalization: int,
        n_quantiles: int,
        reference_hist=None,
    ):
        """
        Parameters
        ----------
        name : tuple
            The column name (table_name, column_name)
        ranks : ndarray
            The column's ranked data
        normalization : int
            The number that normalizes the histogram values
        n_quantiles : int
            The number of quantiles
        reference_hist : QuantileHistogram, optional
            The reference histogram that provides the bucket boundaries
        """
        self.bucket_boundaries = {}
        self.bucket_values = {}
        self.name = name
        self.normalization_factor = normalization
        self.quantiles = n_quantiles
        if reference_hist is None:
            bucket = (
                [round(q, 3) for q in quantiles(ranks, n=self.quantiles + 1, method="inclusive")]
                if len(ranks) > 1
                else ranks
            )

            self.add_buckets(min(ranks), sorted(set(bucket)))
            self.n_buckets = len(self.bucket_boundaries)
            # Precompute the vectorised search arrays from the boundaries
            # dict so ``add_values`` can use numpy.searchsorted instead of
            # a python-level binary search per value. We store BOTH the
            # lower and upper bounds so the original binary-search
            # predicate ``lower <= x <= upper`` can be reproduced exactly.
            self._upper_bounds = np.array(
                [self.bucket_boundaries[i][1] for i in range(self.n_buckets)], dtype=np.float64
            )
            self._lower_bounds = np.array(
                [self.bucket_boundaries[i][0] for i in range(self.n_buckets)], dtype=np.float64
            )
            self._min_val = float(self.bucket_boundaries[0][0])
            self.add_values(ranks)
            # Only the "reference" histogram's dist_matrix is ever read
            # by ``emd2`` (see ``quantile_emd``), so we materialise it
            # here. Histograms built against a reference reuse it for
            # free and skip this work entirely.
            self.dist_matrix = _bucket_distance_matrix(self.n_buckets)
        else:
            self.bucket_boundaries = reference_hist.bucket_boundaries
            self.n_buckets = reference_hist.n_buckets
            # Reuse the reference histogram's cached search arrays.
            self._upper_bounds = reference_hist._upper_bounds
            self._lower_bounds = reference_hist._lower_bounds
            self._min_val = reference_hist._min_val
            self.add_values(ranks)
            self.dist_matrix = reference_hist.dist_matrix

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
            self.bucket_boundaries[i + 1] = (bb[i], bb[i + 1])
            i = i + 1

    def add_values(self, values, norm=True):
        """
        Add all values to buckets (vectorised).

        Parameters
        ----------
        values: ndarray
            The ranks to be added to the histogram
        norm: bool, optional
            Normalize the bucket values or not
        """
        n = self.n_buckets
        arr = np.asarray(values, dtype=np.float64)
        if arr.size == 0:
            counts = np.zeros(n, dtype=np.float64)
        else:
            # Reproduce the original binary-search predicate
            # ``lower[idx] <= x <= upper[idx]``. ``searchsorted(upper,
            # x, side='left')`` gives the smallest idx with
            # upper[idx] >= x; if additionally lower[idx] <= x the
            # value belongs to bucket ``idx``. Otherwise it falls in
            # the gap (which should not happen for contiguous buckets,
            # but handles the reference-histogram case where a new
            # column's values may land outside the training range).
            idx = np.searchsorted(self._upper_bounds, arr, side="left")
            in_range = idx < n
            safe_idx = np.where(in_range, idx, 0)
            valid = in_range & (arr >= self._lower_bounds[safe_idx])
            counts = np.bincount(idx[valid], minlength=n).astype(np.float64)
            if counts.size > n:
                counts = counts[:n]
        if norm and self.normalization_factor:
            counts /= self.normalization_factor
        self.bucket_values = {i: float(counts[i]) for i in range(n)}
