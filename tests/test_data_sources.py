import tempfile
import unittest
from pathlib import Path

import pandas as pd

from valentine.data_sources.base_column import BaseColumn
from valentine.data_sources.base_table import BaseTable
from valentine.data_sources.utils import get_delimiter, get_encoding, is_date

# ---- Minimal concrete implementations for the ABCs ----


class DummyColumn(BaseColumn):
    def __init__(self, uid: object, name: str, dtype: str, data: list[object]):
        self._uid = uid
        self._name = name
        self._dtype = dtype
        self._data = data

    @property
    def unique_identifier(self) -> object:
        return self._uid

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_type(self) -> str:
        return self._dtype

    @property
    def data(self) -> list:
        return self._data


class DummyTable(BaseTable):
    def __init__(self, uid: object, name: str, columns: list[BaseColumn], df: pd.DataFrame):
        self._uid = uid
        self._name = name
        self._columns = columns
        self._df = df

    @property
    def unique_identifier(self) -> object:
        return self._uid

    @property
    def name(self) -> str:
        return self._name

    def get_columns(self) -> list[BaseColumn]:
        return self._columns

    def get_df(self) -> pd.DataFrame:
        return self._df

    @property
    def is_empty(self) -> bool:
        return self._df.empty


# ---- Tests ----


class TestBaseColumnTableAndUtils(unittest.TestCase):
    def setUp(self):
        self.col1 = DummyColumn(uid=1, name="a", dtype="int64", data=[1, 2, 3])
        self.col2 = DummyColumn(uid=2, name="b", dtype="object", data=["2020-01-01", "x"])
        self.df = pd.DataFrame({"a": [1, 2, 3], "b": ["2020-01-01", "x", "y"]})
        self.table = DummyTable(uid="T1", name="tbl", columns=[self.col1, self.col2], df=self.df)

    def test_basecolumn_str_size_empty(self):
        s = str(self.col1)
        self.assertIn("Column:", s)
        self.assertIn("<int64>", s)
        self.assertIn("|  1", s)
        self.assertEqual(self.col1.size, 3)
        self.assertFalse(self.col1.is_empty)

        empty_col = DummyColumn(uid=3, name="c", dtype="float64", data=[])
        self.assertEqual(empty_col.size, 0)
        self.assertTrue(empty_col.is_empty)
        self.assertIn("<float64>", str(empty_col))

    def test_basetable_str_and_lookup(self):
        s = str(self.table)
        self.assertIn("Table: tbl", s)
        self.assertIn("Column: a", s)
        self.assertIn("Column: b", s)
        lookup = self.table.get_guid_column_lookup()
        self.assertEqual(lookup, {"a": 1, "b": 2})
        self.assertFalse(self.table.is_empty)

    def test_basetable_get_data_type(self):
        self.assertEqual(BaseTable.get_data_type([1], "int64"), "int")
        self.assertEqual(BaseTable.get_data_type([1.2], "float64"), "float")
        self.assertEqual(BaseTable.get_data_type(["2020-01-01"], "object"), "date")
        self.assertEqual(BaseTable.get_data_type(["hello"], "object"), "varchar")
        self.assertEqual(BaseTable.get_data_type([], "object"), "varchar")
        self.assertEqual(BaseTable.get_data_type([], "float64"), "float64")

    def test_is_date(self):
        self.assertTrue(is_date("2020-12-31"))
        self.assertTrue(is_date(20200101))  # will be str()'d
        self.assertFalse(is_date("not-a-date"))
        self.assertTrue(is_date("Mon, 5 Jan 2015", fuzzy=True))

    def test_get_delimiter_and_encoding(self):
        with tempfile.TemporaryDirectory() as d:
            # delimiter: comma
            p_comma = Path(d) / "comma.csv"
            with Path(p_comma).open("w", encoding="utf-8") as f:
                f.write("a,b,c\n1,2,3\n")
            self.assertEqual(get_delimiter(p_comma), ",")

            # delimiter: semicolon
            p_sc = Path(d) / "semi.csv"
            with Path(p_sc).open("w", encoding="utf-8") as f:
                f.write("a;b;c\n1;2;3\n")
            self.assertEqual(get_delimiter(p_sc), ";")

            # encoding: ASCII -> returns utf-8
            p_ascii = Path(d) / "ascii.txt"
            with Path(p_ascii).open("wb") as f:
                f.write(b"just ascii lines\nsecond line\n")
            self.assertEqual(get_encoding(p_ascii), "utf-8")

            # encoding: non-ascii (latin-1 with 'é')
            p_latin1 = Path(d) / "latin1.txt"
            with Path(p_latin1).open("wb") as f:
                f.write("caf\u00e9\n".encode("latin-1"))
            enc = get_encoding(p_latin1)
            self.assertIsInstance(enc, str)
            self.assertNotEqual(enc.lower(), "ascii")
