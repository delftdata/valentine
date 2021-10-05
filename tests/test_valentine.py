import unittest

from valentine import valentine_match, valentine_metrics
from tests import df1, df2
from valentine.algorithms import Coma


class TestValentine(unittest.TestCase):

    def test_match(self):
        matches = valentine_match(df1, df2, Coma(strategy="COMA_OPT_INST"))
        assert len(matches) > 0

    def test_metrics(self):
        matches = valentine_match(df1, df2, Coma(strategy="COMA_OPT_INST"))
        golden_standard = [('Cited by', 'Cited by'),
                           ('Authors', 'Authors'),
                           ('EID', 'EID')]
        metrics = valentine_metrics.all_metrics(matches, golden_standard)
        assert metrics['recall_at_sizeof_ground_truth'] == 1.0
