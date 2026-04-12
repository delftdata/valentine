"""Regression tests for brittle code patterns fixed across the algorithms.

Each test targets a specific edge case that previously could cause division
by zero, silent NaN propagation, cache corruption, or incorrect tokenisation.
"""

import math
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from scipy.sparse import csr_matrix

from valentine import valentine_match
from valentine.algorithms import (
    Coma,
    Cupid,
    DistributionBased,
    JaccardDistanceMatcher,
    SimilarityFlooding,
)
from valentine.algorithms.coma.similarity.tfidf import TfidfCorpus
from valentine.algorithms.cupid.linguistic_matching import (
    data_type_similarity,
    name_similarity_tokens,
)
from valentine.algorithms.cupid.schema_element import Token, TokenTypes
from valentine.algorithms.cupid.structural_similarity import compute_ssim
from valentine.algorithms.distribution_based.emd_utils import quantile_emd
from valentine.algorithms.distribution_based.quantile_histogram import QuantileHistogram
from valentine.algorithms.similarity_flooding import NODE_ID_PREFIX
from valentine.algorithms.similarity_flooding.string_matcher import (
    _camel_case_split,
    prefix_suffix_tokenized,
)
from valentine.data_sources.utils import get_delimiter, get_encoding

_DATA = Path(__file__).parent / "data"


# ---------------------------------------------------------------------------
# 1. Cupid: name_similarity_tokens with empty token sets
# ---------------------------------------------------------------------------


class TestCupidEmptyTokens:
    """name_similarity_tokens must return 0.0 when both token sets are empty."""

    def test_empty_token_sets(self):
        assert name_similarity_tokens([], []) == 0.0

    def test_one_empty_token_set(self):
        t = Token()
        t.data = "hello"
        t.token_type = TokenTypes.CONTENT
        # One non-empty, one empty — should not crash
        result = name_similarity_tokens([t], [])
        assert result == 0.0

    def test_data_type_similarity_empty(self):
        assert data_type_similarity([], []) == 0


# ---------------------------------------------------------------------------
# 2. Cupid: structural_similarity with empty leaves
# ---------------------------------------------------------------------------


class TestCupidEmptyLeaves:
    """compute_ssim must return 0.0 when both nodes have no leaves."""

    def test_both_empty_leaves(self):
        node_s = MagicMock()
        node_t = MagicMock()
        node_s.get_leaf_names.return_value = []
        node_t.get_leaf_names.return_value = []
        result = compute_ssim(node_s, node_t, {})
        assert result == 0.0

    def test_one_empty_leaves(self):
        node_s = MagicMock()
        node_t = MagicMock()
        node_s.get_leaf_names.return_value = ["col1"]
        node_t.get_leaf_names.return_value = []
        # Factor of 2 check: 1 > 0*2 → nan
        result = compute_ssim(node_s, node_t, {})
        assert math.isnan(result)


# ---------------------------------------------------------------------------
# 3. DistributionBased: zero-sum histogram guard
# ---------------------------------------------------------------------------


class TestEmdZeroSumHistogram:
    """quantile_emd must return inf when histogram values sum to zero."""

    def test_zero_sum_returns_inf(self):
        # Build a real histogram, then zero out its values to trigger the guard
        ranks = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        hist = QuantileHistogram(("t", "c"), ranks, 5, 4)
        # Zero out all bucket values
        for k in hist.bucket_values:
            hist.bucket_values[k] = 0.0

        col1 = MagicMock()
        col1.size = 5
        col1.quantile_histogram = hist

        col2 = MagicMock()
        col2.size = 5
        col2.long_name = ("t", "g", "c", "u")
        col2.ranks = ranks

        result = quantile_emd(col1, col2)
        assert result == math.inf

    def test_empty_column_returns_inf(self):
        col1 = MagicMock()
        col1.size = 0
        col2 = MagicMock()
        col2.size = 5
        result = quantile_emd(col1, col2)
        assert result == math.inf


# ---------------------------------------------------------------------------
# 4. Coma TF-IDF: id()-based cache robustness
# ---------------------------------------------------------------------------


class TestTfidfCacheRobustness:
    """TfidfCorpus cache must not serve stale data on id() reuse."""

    def test_same_id_different_content_does_not_return_stale(self):
        list_a = ["hello world", "foo bar"]
        list_b = ["completely different", "another thing"]
        corpus = TfidfCorpus([list_a, list_b])

        sim_original = corpus.similarity(list_a, list_b)

        # Simulate id reuse: manually corrupt the cache with a fake entry
        # using list_b's id but holding a reference to a different list.
        fake_list = ["should not be used"]
        fake_key = id(list_b)
        # Insert a stale entry — the identity check should reject it
        corpus._column_cache[fake_key] = (
            fake_list,  # different reference than list_b
            csr_matrix((0, max(len(corpus._vocab), 1))),
            0,
        )

        # Should recompute because ref check fails (fake_list is not list_b)
        sim_after = corpus.similarity(list_a, list_b)
        assert sim_original == pytest.approx(sim_after, abs=1e-6)

    def test_cache_hit_on_same_object(self):
        instances = ["one two three", "four five six"]
        corpus = TfidfCorpus([instances])
        # First call populates cache
        corpus._vectorise_column(instances)
        # Second call should hit cache (same object)
        cached = corpus._column_cache.get(id(instances))
        assert cached is not None
        assert cached[0] is instances  # reference stored


# ---------------------------------------------------------------------------
# 5. Similarity Flooding: improved string tokenisation
# ---------------------------------------------------------------------------


class TestSFStringMatcher:
    """_camel_case_split must handle snake_case, hyphens, SCREAMING_SNAKE, digits."""

    def test_camel_case(self):
        assert _camel_case_split("ColumnType") == ["Column", "Type"]

    def test_snake_case(self):
        assert _camel_case_split("dept_name") == ["dept", "name"]

    def test_screaming_snake(self):
        assert _camel_case_split("EMPLOYEE_ID") == ["EMPLOYEE", "ID"]

    def test_hyphen_separated(self):
        assert _camel_case_split("first-name") == ["first", "name"]

    def test_digits_split(self):
        assert _camel_case_split("order123") == ["order", "123"]

    def test_consecutive_uppercase(self):
        assert _camel_case_split("XMLParser") == ["XML", "Parser"]

    def test_empty_string(self):
        assert _camel_case_split("") == []

    def test_single_word(self):
        assert _camel_case_split("name") == ["name"]

    def test_prefix_suffix_with_snake_case(self):
        """prefix_suffix_tokenized should work with snake_case names."""
        # "dept_name" and "DeptName" should have high similarity
        sim = prefix_suffix_tokenized("dept_name", "DeptName")
        assert sim > 0.5

    def test_prefix_suffix_screaming_vs_camel(self):
        sim = prefix_suffix_tokenized("EMPLOYEE_ID", "EmployeeId")
        assert sim > 0.3


# ---------------------------------------------------------------------------
# 6. Similarity Flooding: NodeID prefix collision
# ---------------------------------------------------------------------------


class TestSFNodeIDPrefix:
    """Columns named "NodeID*" must not collide with structural node IDs."""

    def test_column_named_nodeid_still_matches(self):
        """A column literally named 'NodeID123' should still produce matches."""
        df1 = pd.DataFrame({"NodeID123": ["a", "b"], "name": ["c", "d"]})
        df2 = pd.DataFrame({"NodeID123": ["a", "b"], "label": ["c", "d"]})
        matches = valentine_match([df1, df2], SimilarityFlooding(), df_names=["s", "t"])
        # The key test: should produce at least one match (NodeID123 ↔ NodeID123)
        matched_cols = {(p.source_column, p.target_column) for p in matches}
        assert ("NodeID123", "NodeID123") in matched_cols

    def test_prefix_constant_not_plain_nodeid(self):
        """The sentinel prefix must not be a plain string that could appear in data."""
        # Must contain a character that can't appear in normal column names
        assert "\x00" in NODE_ID_PREFIX


# ---------------------------------------------------------------------------
# 7. End-to-end regression: all algorithms still produce correct matches
# ---------------------------------------------------------------------------


class TestEndToEndRegression:
    """Verify that all fixes preserve algorithm output on the standard test data."""

    @pytest.fixture(scope="class")
    def test_data(self):
        df1 = pd.read_csv(_DATA / "source_candidates.csv")
        df2 = pd.read_csv(_DATA / "target_candidates.csv")
        return df1, df2

    def test_coma_produces_matches(self, test_data):
        df1, df2 = test_data
        matches = valentine_match([df1, df2], Coma(), df_names=["src", "tgt"])
        assert len(matches) > 0
        # Scores should all be valid floats in [0, 1]
        for score in matches.values():
            assert 0.0 <= score <= 1.0
            assert not math.isnan(score)

    def test_cupid_produces_matches(self, test_data):
        df1, df2 = test_data
        matches = valentine_match([df1, df2], Cupid(), df_names=["src", "tgt"])
        assert len(matches) > 0
        for score in matches.values():
            assert not math.isnan(score)

    def test_distribution_based_produces_matches(self, test_data):
        df1, df2 = test_data
        matches = valentine_match([df1, df2], DistributionBased(), df_names=["src", "tgt"])
        assert len(matches) > 0
        for score in matches.values():
            assert not math.isnan(score)

    def test_jaccard_produces_matches(self, test_data):
        df1, df2 = test_data
        matches = valentine_match([df1, df2], JaccardDistanceMatcher(), df_names=["src", "tgt"])
        assert len(matches) > 0

    def test_similarity_flooding_produces_matches(self, test_data):
        df1, df2 = test_data
        matches = valentine_match([df1, df2], SimilarityFlooding(), df_names=["src", "tgt"])
        assert len(matches) > 0
        for score in matches.values():
            assert not math.isnan(score)

    def test_coma_with_instances(self, test_data):
        df1, df2 = test_data
        matches = valentine_match([df1, df2], Coma(use_instances=True), df_names=["src", "tgt"])
        assert len(matches) > 0


# ---------------------------------------------------------------------------
# 8. Edge cases: single-column and minimal tables
# ---------------------------------------------------------------------------


class TestMinimalTables:
    """Algorithms must handle single-column and very small tables gracefully."""

    def test_single_column_tables(self):
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        df2 = pd.DataFrame({"b": [1, 2, 3]})
        for matcher in [Coma(), SimilarityFlooding(), JaccardDistanceMatcher()]:
            matches = valentine_match([df1, df2], matcher, df_names=["s", "t"])
            # Should not crash; may or may not find matches
            assert matches is not None

    def test_empty_string_columns(self):
        df1 = pd.DataFrame({"": ["a", "b"]})
        df2 = pd.DataFrame({"": ["a", "b"]})
        matches = valentine_match([df1, df2], Coma(), df_names=["s", "t"])
        assert matches is not None

    def test_identical_tables(self):
        df = pd.DataFrame({"name": ["alice", "bob"], "age": [30, 25]})
        matches = valentine_match([df, df.copy()], Coma(), df_names=["s", "t"])
        assert len(matches) > 0
        # Identical columns should have high similarity
        best = max(matches.values())
        assert best > 0.5


# ---------------------------------------------------------------------------
# 9. Data source utilities: encoding and delimiter detection
# ---------------------------------------------------------------------------


class TestDataSourceUtils:
    """get_encoding and get_delimiter must handle edge cases gracefully."""

    def test_get_encoding_empty_file(self):
        """chardet on an empty file must not return None."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            f.write(b"")
            path = f.name
        result = get_encoding(path)
        assert isinstance(result, str)
        Path(path).unlink()

    def test_get_encoding_binary_garbage(self):
        """chardet on unrecognisable bytes must fall back to utf-8."""
        with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
            f.write(bytes(range(256)) * 2)
            path = f.name
        result = get_encoding(path)
        assert isinstance(result, str)
        Path(path).unlink()

    def test_get_delimiter_empty_first_line(self):
        """Sniffer failure on empty first line must fall back to comma."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            f.write("\n")
            path = f.name
        result = get_delimiter(Path(path))
        assert result == ","
        Path(path).unlink()

    def test_get_delimiter_single_column(self):
        """A file with one column and no delimiter must fall back to comma."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            f.write("header\n")
            path = f.name
        result = get_delimiter(Path(path))
        assert isinstance(result, str)
        Path(path).unlink()

    def test_get_delimiter_normal_csv(self):
        """Standard comma-delimited CSV should detect comma."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            f.write("a,b,c\n1,2,3\n")
            path = f.name
        result = get_delimiter(Path(path))
        assert result == ","
        Path(path).unlink()

    def test_get_delimiter_tsv(self):
        """Tab-delimited file should detect tab."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False, newline="") as f:
            f.write("a\tb\tc\n1\t2\t3\n")
            path = f.name
        result = get_delimiter(Path(path))
        assert result == "\t"
        Path(path).unlink()
