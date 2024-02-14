import unittest

from valentine.data_sources import DataframeTable

from valentine import valentine_match, valentine_match_batch, NotAValentineMatcher
from tests import df1, df2
from valentine.algorithms import JaccardDistanceMatcher


class TestValentine(unittest.TestCase):

    def test_match(self):
        assert not DataframeTable(df1, name='df1_name').is_empty
        assert not DataframeTable(df2, name='df2_name').is_empty
        matches = valentine_match(df1, df2, JaccardDistanceMatcher())
        assert len(matches) > 0
        try:
            valentine_match(df1, df2, None)
        except NotAValentineMatcher:
            pass
        else:
            assert False

    def test_batch_generator(self):
        n = 3

        def generate_df1():
            for _ in range(n):
                yield df1

        def generate_df2():
            for _ in range(n):
                yield df2

        matches = valentine_match_batch(generate_df1(), generate_df2(), JaccardDistanceMatcher())
        assert len(matches) > 0

    def test_batch_list(self):
        matches = valentine_match_batch([df1, df1, df1], [df2, df2, df2], JaccardDistanceMatcher())
        assert len(matches) > 0

    def test_batch_names(self):
        matches = valentine_match_batch([df1, df1], [df2, df2], JaccardDistanceMatcher(), ['ta1', 'tb1'], ['ta2', 'tb2'])
        assert len(matches) > 0