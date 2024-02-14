import unittest
import math

from tests import df1, df2
from valentine.algorithms.matcher_results import MatcherResults
from valentine.algorithms import JaccardDistanceMatcher
from valentine.metrics import Precision
from valentine import valentine_match


class TestMatcherResults(unittest.TestCase):
    def setUp(self):
        self.matches = valentine_match(df1, df2, JaccardDistanceMatcher())
        self.ground_truth = [
            ('Cited by', 'Cited by'),
            ('Authors', 'Authors'),
            ('EID', 'EID')
        ]

    def test_dict(self):
        assert isinstance(self.matches, dict)

    def test_get_metrics(self):
        metrics = self.matches.get_metrics(self.ground_truth)
        assert all([x in metrics for x in {"Precision", "Recall", "F1Score"}])

        metrics_specific = self.matches.get_metrics(self.ground_truth, metrics={Precision()})
        assert "Precision" in metrics_specific

    def test_one_to_one(self):
        m = self.matches

        # Add multiple matches per column
        pairs = list(m.keys())
        for (ta, ca), (tb, cb) in pairs:
            m[((ta, ca), (tb, cb + 'foo'))] = m[((ta, ca), (tb, cb))] / 2

        # Verify that len gets corrected from 6 to 3
        m_one_to_one = m.one_to_one()
        assert len(m_one_to_one) == 3 and len(m) == 6

        # Verify that none of the lower similarity "foo" entries made it
        for (ta, ca), (tb, cb) in pairs:
            assert ((ta, ca), (tb, cb + 'foo')) not in m_one_to_one

        # Verify that the cache resets on a new MatcherResults instance
        m_entry = MatcherResults(m)
        assert m_entry._cached_one_to_one is None

        # Add one new entry with lower similarity
        m_entry[(('table_1', 'BLA'), ('table_2', 'BLA'))] = 0.7214057

        # Verify that the new one_to_one is different from the old one
        m_entry_one_to_one = m_entry.one_to_one()
        assert m_one_to_one != m_entry_one_to_one

        # Verify that all remaining values are above the median
        median = sorted(list(m_entry.values()), reverse=True)[math.ceil(len(m_entry)/2)]
        for k in m_entry_one_to_one:
            assert m_entry_one_to_one[k] >= median

    def test_take_top_percent(self):
        take_0_percent = self.matches.take_top_percent(0)
        assert len(take_0_percent) == 0

        take_40_percent = self.matches.take_top_percent(40)
        assert len(take_40_percent) == 2

        take_100_percent = self.matches.take_top_percent(100)
        assert len(take_100_percent) == len(self.matches)

    def test_take_top_n(self):
        take_none = self.matches.take_top_n(0)
        assert len(take_none) == 0

        take_some = self.matches.take_top_n(2)
        assert len(take_some) == 2

        take_all = self.matches.take_top_n(len(self.matches))
        assert len(take_all) == len(self.matches)

        take_more_than_all = self.matches.take_top_n(len(self.matches)+1)
        assert len(take_more_than_all) == len(self.matches)

    def test_copy(self):
        assert self.matches.get_copy() is not self.matches