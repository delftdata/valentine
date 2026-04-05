import unittest

import pytest

from tests import df1, df2
from valentine import InvalidMatcherError, NotAValentineMatcher, valentine_match
from valentine.algorithms import JaccardDistanceMatcher


class TestValentine(unittest.TestCase):
    def test_match_pair(self):
        matches = valentine_match([df1, df2], JaccardDistanceMatcher())
        assert len(matches) > 0

    def test_match_invalid_matcher(self):
        with pytest.raises(InvalidMatcherError):
            valentine_match([df1, df2], None)

    def test_match_invalid_matcher_old_alias(self):
        # Backward compat: old exception name still works
        with pytest.raises(NotAValentineMatcher):
            valentine_match([df1, df2], None)

    def test_match_too_few_dfs(self):
        with pytest.raises(ValueError, match="At least 2"):
            valentine_match([df1], JaccardDistanceMatcher())

    def test_match_with_names(self):
        matches = valentine_match(
            [df1, df2],
            JaccardDistanceMatcher(),
            df_names=["source", "target"],
        )
        assert len(matches) > 0
        for pair in matches:
            assert pair.source_table in ("source", "target")
            assert pair.target_table in ("source", "target")

    def test_match_multiple_tables(self):
        matches = valentine_match(
            [df1, df2, df1],
            JaccardDistanceMatcher(),
            df_names=["alpha", "beta", "gamma"],
        )
        assert len(matches) > 0

    def test_match_names_length_mismatch(self):
        with pytest.raises(ValueError, match="Length of df_names"):
            valentine_match(
                [df1, df2],
                JaccardDistanceMatcher(),
                df_names=["only_one"],
            )

    def test_match_generator(self):
        def gen():
            yield df1
            yield df2

        matches = valentine_match(gen(), JaccardDistanceMatcher())
        assert len(matches) > 0
