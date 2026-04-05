import math
import unittest
from collections.abc import Mapping

from tests import df1, df2
from valentine import valentine_match
from valentine.algorithms import ColumnPair, JaccardDistanceMatcher
from valentine.algorithms.matcher_results import MatcherResults
from valentine.metrics import Precision


class TestMatcherResults(unittest.TestCase):
    def setUp(self):
        self.matches = valentine_match([df1, df2], JaccardDistanceMatcher())
        self.ground_truth = [
            ("emp_id", "employee_number"),
            ("fname", "first_name"),
            ("lname", "last_name"),
            ("dept", "department"),
            ("annual_salary", "compensation"),
            ("hire_date", "start_date"),
            ("office_loc", "work_location"),
        ]

    def test_is_mapping(self):
        assert isinstance(self.matches, Mapping)

    def test_is_not_mutable_dict(self):
        # MatcherResults should not support mutation
        assert not hasattr(self.matches, "update")
        assert not hasattr(self.matches, "pop")

    def test_get_metrics(self):
        metrics = self.matches.get_metrics(self.ground_truth)
        assert all(x in metrics for x in ("Precision", "Recall", "F1Score"))

        metrics_specific = self.matches.get_metrics(self.ground_truth, metrics={Precision()})
        assert "Precision" in metrics_specific

    def test_one_to_one(self):
        m = self.matches
        n = len(m)
        assert n > 0

        # Build a new MatcherResults with duplicate (lower-score) entries
        extended = dict(m)
        for pair in list(m):
            dup = ColumnPair(
                pair.source_table,
                pair.source_column,
                pair.target_table,
                pair.target_column + "foo",
            )
            extended[dup] = m[pair] / 2
        m = MatcherResults(extended)

        assert len(m) == 2 * n

        m_one_to_one = m.one_to_one()
        # one_to_one should remove duplicates, returning fewer entries
        assert len(m_one_to_one) <= n
        assert len(m_one_to_one) < len(m)

        # None of the lower-similarity "foo" entries should survive
        for pair in m_one_to_one:
            assert not pair.target_column.endswith("foo")

        # Cache resets on new instance
        m_entry = MatcherResults(dict(m))
        assert m_entry._cached_one_to_one is None

        # Add a new entry with distinct columns
        ext2 = dict(m_entry)
        ext2[ColumnPair("extra_src", "BLA", "extra_tgt", "BLA")] = 0.7214057
        m_entry = MatcherResults(ext2)

        m_entry_one_to_one = m_entry.one_to_one()
        assert m_one_to_one != m_entry_one_to_one

        # All remaining values should be above the median
        median = sorted(m_entry.values(), reverse=True)[math.ceil(len(m_entry) / 2)]
        for k in m_entry_one_to_one:
            assert m_entry_one_to_one[k] >= median

    def test_take_top_percent(self):
        take_0_percent = self.matches.take_top_percent(0)
        assert len(take_0_percent) == 0

        n = len(self.matches)
        take_40_percent = self.matches.take_top_percent(40)
        assert len(take_40_percent) == math.ceil(n * 0.4)

        take_100_percent = self.matches.take_top_percent(100)
        assert len(take_100_percent) == len(self.matches)

    def test_take_top_n(self):
        take_none = self.matches.take_top_n(0)
        assert len(take_none) == 0

        take_some = self.matches.take_top_n(2)
        assert len(take_some) == 2

        take_all = self.matches.take_top_n(len(self.matches))
        assert len(take_all) == len(self.matches)

        take_more_than_all = self.matches.take_top_n(len(self.matches) + 1)
        assert len(take_more_than_all) == len(self.matches)

    def test_copy(self):
        copy = self.matches.get_copy()
        assert copy is not self.matches
        assert dict(copy) == dict(self.matches)

    def test_column_pair_named_access(self):
        """ColumnPair fields are accessible by name."""
        pair = next(iter(self.matches))
        assert isinstance(pair, ColumnPair)
        assert isinstance(pair.source_table, str)
        assert isinstance(pair.source_column, str)
        assert isinstance(pair.target_table, str)
        assert isinstance(pair.target_column, str)
        assert pair.source == (pair.source_table, pair.source_column)
        assert pair.target == (pair.target_table, pair.target_column)
