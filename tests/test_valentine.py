import unittest

from valentine.data_sources import DataframeTable

from valentine import valentine_match, valentine_metrics, NotAValentineMatcher
from tests import df1, df2
from valentine.algorithms import Coma


class TestValentine(unittest.TestCase):

    def test_match(self):
        assert not DataframeTable(df1, name='df1_name').is_empty
        assert not DataframeTable(df2, name='df2_name').is_empty
        matches = valentine_match(df1, df2, Coma(strategy="COMA_OPT_INST"))
        assert len(matches) > 0
        try:
            valentine_match(df1, df2, None)
        except NotAValentineMatcher:
            pass
        else:
            assert False

    def test_metrics(self):
        matches = valentine_match(df1, df2, Coma(strategy="COMA_OPT_INST"))
        golden_standard = [('Cited by', 'Cited by'),
                           ('Authors', 'Authors'),
                           ('EID', 'EID')]
        metrics = valentine_metrics.all_metrics(matches, golden_standard)
        assert metrics['recall_at_sizeof_ground_truth'] == 1.0
