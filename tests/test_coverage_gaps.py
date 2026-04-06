"""Targeted tests to cover edge cases & validation paths across the package.

Kept in a dedicated module so that coverage-driven additions don't pollute
behaviour-focused test files.
"""

import pandas as pd
import pytest

from tests import df1, df2
from valentine import InvalidMatcherError, valentine_match
from valentine.algorithms import (
    Coma,
    Cupid,
    DistributionBased,
    Formula,
    JaccardDistanceMatcher,
    Policy,
    SimilarityFlooding,
    StringMatcher,
)
from valentine.algorithms.match import ColumnPair
from valentine.algorithms.matcher_results import MatcherResults
from valentine.data_sources.dataframe.dataframe_table import DataframeTable

# -- MatcherResults dunder & transformation coverage ------------------------


class TestMatcherResultsInternals:
    def setup_method(self):
        self.data = {
            ColumnPair("s", "a", "t", "a"): 0.9,
            ColumnPair("s", "b", "t", "b"): 0.8,
            ColumnPair("s", "c", "t", "c"): 0.7,
            ColumnPair("s", "d", "t", "d"): 0.6,
            ColumnPair("s", "e", "t", "e"): 0.5,
            ColumnPair("s", "f", "t", "f"): 0.4,
        }
        self.details = {k: {"NameCM": v} for k, v in self.data.items()}
        self.results = MatcherResults(self.data, details=self.details)

    def test_repr_truncates(self):
        r = repr(self.results)
        assert r.startswith("MatcherResults({")
        assert "..." in r  # more than 5 entries

    def test_repr_short(self):
        small = MatcherResults({ColumnPair("s", "a", "t", "a"): 0.9})
        assert "..." not in repr(small)

    def test_eq_matcher_results(self):
        other = MatcherResults(dict(self.data))
        assert self.results == other

    def test_eq_plain_dict(self):
        # __eq__ supports comparison against a dict of the same contents
        assert self.results == dict(sorted(self.data.items(), key=lambda x: x[1], reverse=True))

    def test_eq_unrelated(self):
        assert (self.results == 42) is False

    def test_details_property_and_get(self):
        assert self.results.details == self.details
        pair = next(iter(self.results))
        assert self.results.get_details(pair) == {"NameCM": self.data[pair]}
        missing = ColumnPair("s", "missing", "t", "missing")
        assert self.results.get_details(missing) is None

    def test_details_empty_when_none(self):
        bare = MatcherResults(dict(self.data))
        assert bare.details == {}
        assert bare.get_details(next(iter(bare))) is None

    def test_one_to_one_with_explicit_threshold(self):
        result = self.results.one_to_one(threshold=0.7)
        # Only entries >= 0.7 survive the explicit threshold path
        assert all(score >= 0.7 for score in result.values())
        assert len(result) == 3

    def test_one_to_one_identical_scores(self):
        # Less than two distinct values -> early return branch
        flat = MatcherResults(
            {
                ColumnPair("s", "a", "t", "a"): 0.5,
                ColumnPair("s", "b", "t", "b"): 0.5,
            }
        )
        assert len(flat.one_to_one()) == len(flat)

    def test_filter(self):
        result = self.results.filter(min_score=0.75)
        assert len(result) == 2
        assert all(score >= 0.75 for score in result.values())
        # Details are filtered alongside data
        for pair in result:
            assert pair in result.details

    def test_get_copy_independent(self):
        copy = self.results.get_copy()
        assert copy == self.results
        assert copy is not self.results


# -- Algorithm parameter validation -----------------------------------------


class TestMatcherValidation:
    def test_coma_needs_at_least_one_mode(self):
        with pytest.raises(ValueError, match="use_schema or use_instances"):
            Coma(use_schema=False, use_instances=False)

    def test_coma_negative_max_n(self):
        with pytest.raises(ValueError, match="max_n"):
            Coma(max_n=-1)

    def test_coma_bad_delta(self):
        with pytest.raises(ValueError, match="delta"):
            Coma(delta=1.5)

    def test_coma_bad_threshold(self):
        with pytest.raises(ValueError, match="threshold"):
            Coma(threshold=-0.1)

    def test_cupid_bad_threshold(self):
        with pytest.raises(ValueError, match="th_accept"):
            Cupid(th_accept=1.5)

    def test_cupid_bad_c_inc(self):
        with pytest.raises(ValueError, match="c_inc"):
            Cupid(c_inc=0)

    def test_cupid_bad_process_num(self):
        with pytest.raises(ValueError, match="process_num"):
            Cupid(process_num=0)

    def test_distribution_based_bad_quantiles(self):
        with pytest.raises(ValueError, match="quantiles"):
            DistributionBased(quantiles=0)

    def test_distribution_based_bad_threshold1(self):
        with pytest.raises(ValueError, match="threshold1"):
            DistributionBased(threshold1=1.5)

    def test_distribution_based_bad_threshold2(self):
        with pytest.raises(ValueError, match="threshold2"):
            DistributionBased(threshold2=-0.1)

    def test_distribution_based_bad_process_num(self):
        with pytest.raises(ValueError, match="process_num"):
            DistributionBased(process_num=0)

    def test_jaccard_bad_threshold(self):
        with pytest.raises(ValueError, match="threshold_dist"):
            JaccardDistanceMatcher(threshold_dist=1.5)

    def test_jaccard_bad_process_num(self):
        with pytest.raises(ValueError, match="process_num"):
            JaccardDistanceMatcher(process_num=0)


# -- Similarity Flooding batch & string matcher coverage -------------------


class TestSimilarityFloodingBatch:
    def test_batch_with_tfidf_string_matcher(self):
        """Global IDF path in get_matches_batch."""
        matcher = SimilarityFlooding(
            coeff_policy=Policy.INVERSE_AVERAGE,
            formula=Formula.FORMULA_C,
            string_matcher=StringMatcher.PREFIX_SUFFIX_TFIDF,
        )
        matches = valentine_match([df1, df2], matcher, df_names=["src", "tgt"])
        assert len(matches) > 0

    def test_batch_with_levenshtein(self):
        matcher = SimilarityFlooding(string_matcher=StringMatcher.LEVENSHTEIN)
        matches = valentine_match([df1, df2], matcher, df_names=["src", "tgt"])
        assert len(matches) > 0

    def test_single_pair_tfidf_with_corpus(self):
        matcher = SimilarityFlooding(
            string_matcher=StringMatcher.PREFIX_SUFFIX_TFIDF,
        )
        matches = matcher.get_matches(
            DataframeTable(df1, name="src"),
            DataframeTable(df2, name="tgt"),
        )
        assert len(matches) > 0


# -- DataframeTable edge cases ---------------------------------------------


class TestDataframeTable:
    def test_negative_sample_size_rejected(self):
        with pytest.raises(ValueError, match="instance_sample_size"):
            DataframeTable(df1, name="t", instance_sample_size=-5)

    def test_sample_size_zero_returns_empty_frame(self):
        table = DataframeTable(df1, name="t", instance_sample_size=0)
        assert table.get_instances_df().empty

    def test_sample_size_none_returns_full_frame(self):
        table = DataframeTable(df1, name="t", instance_sample_size=None)
        assert len(table.get_instances_df()) == len(df1)

    def test_empty_dataframe(self):
        empty = pd.DataFrame({"a": [], "b": []})
        table = DataframeTable(empty, name="empty")
        assert table.is_empty
        assert table.get_instances_df().empty

    def test_get_column_names_builds_columns(self):
        table = DataframeTable(df1, name="t")
        names = table.get_column_names()
        assert len(names) > 0
        # Second call hits the cached path
        assert table.get_column_names() == names

    def test_sampling_with_all_nan_rows(self):
        df = pd.DataFrame({"a": [None, 1.0, None], "b": [None, 2.0, None]})
        table = DataframeTable(df, name="mixed", instance_sample_size=10)
        instances = table.get_instances_df()
        assert len(instances) == 1

    def test_sampling_with_all_empty_rows(self):
        df = pd.DataFrame({"a": [None, None], "b": [None, None]})
        table = DataframeTable(df, name="empties", instance_sample_size=10)
        assert table.get_instances_df().empty


# -- valentine_match edge cases --------------------------------------------


class TestValentineMatchEdges:
    def test_too_many_default_names(self):
        # >26 DataFrames without explicit names should error
        dfs = [df1.copy() for _ in range(27)]
        with pytest.raises(ValueError, match="26"):
            valentine_match(dfs, JaccardDistanceMatcher())

    def test_invalid_matcher_raises(self):
        with pytest.raises(InvalidMatcherError):
            valentine_match([df1, df2], "not a matcher")
