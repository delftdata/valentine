import hashlib
import math
from collections.abc import Iterable


class BloomFilter:
    """
    A Bloom filter for approximate set membership testing.

    Used in intersection EMD computation (Section 4 of the paper) to approximate
    the intersection between two columns without materializing the full intersection.
    This trades a small false positive rate for reduced computation on large columns.

    Parameters
    ----------
    expected_elements : int
        The expected number of elements to be inserted.
    false_positive_rate : float
        The desired false positive probability (default 0.01 = 1%).
    """

    def __init__(self, expected_elements: int, false_positive_rate: float = 0.01):
        if expected_elements <= 0:
            expected_elements = 1
        self._size = self._optimal_size(expected_elements, false_positive_rate)
        self._num_hashes = self._optimal_num_hashes(self._size, expected_elements)
        self._bit_array = bytearray(self._size)

    def add(self, item) -> None:
        """Add an item to the filter."""
        for i in range(self._num_hashes):
            idx = self._hash(item, i) % self._size
            self._bit_array[idx] = 1

    def __contains__(self, item) -> bool:
        """Test whether an item is (probably) in the filter."""
        return all(
            self._bit_array[self._hash(item, i) % self._size] for i in range(self._num_hashes)
        )

    @classmethod
    def from_iterable(
        cls, data: Iterable, expected_elements: int, false_positive_rate: float = 0.01
    ) -> "BloomFilter":  # noqa
        """Build a BloomFilter from an iterable of items."""
        bf = cls(expected_elements, false_positive_rate)
        for item in data:
            bf.add(item)
        return bf

    @staticmethod
    def _hash(item, seed: int) -> int:
        h = hashlib.sha256(f"{seed}:{item}".encode()).digest()
        return int.from_bytes(h[:8], "big")

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """Optimal bit array size: m = -n * ln(p) / (ln2)^2."""
        return max(1, int(-n * math.log(p) / (math.log(2) ** 2)))

    @staticmethod
    def _optimal_num_hashes(m: int, n: int) -> int:
        """Optimal number of hash functions: k = (m/n) * ln2."""
        return max(1, int((m / n) * math.log(2)))
