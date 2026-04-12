import unittest

from tests import d1_path
from valentine.data_sources.utils import get_delimiter, get_encoding, is_date
from valentine.utils.utils import convert_data_type


class TestUtils(unittest.TestCase):
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
