import unittest

import pytest

from tests import d1_path
from valentine.data_sources.utils import get_encoding, get_delimiter, is_date
from valentine.utils.utils import is_sorted, convert_data_type, normalize_distance


class TestUtils(unittest.TestCase):

    def test_is_sorted(self):
        sorted_dict = {"k1": 1, "k2": 2, "k3": 3}
        assert is_sorted(sorted_dict)
        unsorted_dict = {"k1": 2, "k2": 1, "k3": 3}
        assert not is_sorted(unsorted_dict)

    def test_convert_data_type(self):
        float_str = "1.1"
        assert isinstance(convert_data_type(float_str), float)
        int_str = "1"
        assert isinstance(convert_data_type(int_str), int)
        str_str = "test"
        assert isinstance(convert_data_type(str_str), str)

    def test_get_encoding(self):
        assert get_encoding(d1_path) == "utf-8"

    def test_get_delimiter(self):
        assert get_delimiter(d1_path) == ","

    def test_is_date(self):
        date_str = "2019-04-26 18:03:50.941332"
        assert is_date(date_str)

    def test_normalize_distance_many_cases(self):
        cases = [
            # identical strings
            (0, "abc", "abc", 1.0),
            # completely different, distance == max length
            (3, "abc", "xyz", 0.0),
            # partial similarity
            (1, "abc", "axc", 1 - 1/3),
            # different lengths, distance smaller than max length
            (2, "abcd", "ab", 1 - 2/4),
            # both empty strings → max(len1, len2) = 0 → denominator becomes 1
            (0, "", "", 1 - 0/1),
            # one empty, one non-empty, distance equals length of non-empty
            (3, "", "abc", 1 - 3/3),
            # distance greater than max length (still valid mathematically)
            (5, "abc", "", 1 - 5/3),
            # another mixed case
            (2, "kitten", "sitting", 1 - 2/7),
        ]

        for dist, s1, s2, expected in cases:
            with self.subTest(dist=dist, str1=s1, str2=s2):
                result = normalize_distance(dist, s1, s2)
                self.assertAlmostEqual(result, expected)
