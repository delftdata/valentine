import unittest

import math
from valentine.metrics.metrics import get_tp_fn, get_fp, recall, one_to_one_matches, precision
from copy import deepcopy 

matches = {
    (('table_1', 'Cited by'), ('table_2', 'Cited by')): 0.8374313,
    (('table_1', 'Authors'), ('table_2', 'Authors')): 0.83498037,
    (('table_1', 'EID'), ('table_2', 'EID')): 0.8214057,
}

ground_truth = [
    ('Cited by', 'Cited by'),
    ('Authors', 'Authors'),
    ('EID', 'EID')
]


class TestMetrics(unittest.TestCase):

    def test_one_to_one(self):
        m = deepcopy(matches)

        # Add multiple matches per column
        pairs = list(m.keys())
        for ((ta, ca), (tb, cb)) in pairs:
            m[((ta, ca), (tb, cb + 'foo'))] = m[((ta, ca), (tb, cb))] / 2

        # Verify that len gets corrected to 3
        m_one_to_one = one_to_one_matches(m)
        assert len(m_one_to_one) == 3 and len(m) == 6

        # Verify that none of the lower similarity "foo" entries made it
        for ((ta, ca), (tb, cb)) in pairs:
            assert ((ta, ca), (tb, cb + 'foo')) not in m_one_to_one

        # Add one new entry with lower similarity
        m_entry = deepcopy(matches)
        m_entry[(('table_1', 'BLA'), ('table_2', 'BLA'))] = 0.7214057

        m_entry_one_to_one = one_to_one_matches(m_entry)

        # Verify that all remaining values are above the median
        median = sorted(set(m_entry.values()))[math.ceil(len(m_entry)/2)]
        for k in m_entry_one_to_one:
            assert m_entry_one_to_one[k] >= median
