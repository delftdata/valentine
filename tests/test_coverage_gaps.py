"""Targeted tests to cover edge cases & validation paths across the package.

Kept in a dedicated module so that coverage-driven additions don't pollute
behaviour-focused test files.
"""

import numpy as np
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
from valentine.algorithms.coma.similarity.tfidf import TfidfCorpus
from valentine.algorithms.coma.similarity.tokens import tokenize_name, tokens_similarity
from valentine.algorithms.cupid.linguistic_matching import _cached_synsets, get_synonyms
from valentine.algorithms.distribution_based.clustering_utils import (
    _COLUMN_STORE,
    _compute_ranks,
    generate_global_ranks,
    ingestion_column_generator,
    process_columns,
)
from valentine.algorithms.distribution_based.column_model import (
    CorrelationClusteringColumn,
    clear_global_ranks_cache,
)
from valentine.algorithms.distribution_based.quantile_histogram import QuantileHistogram
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


# -- Coma similarity primitive edge cases ----------------------------------


class TestComaSimilarityPrimitives:
    def test_tokens_empty_name_returns_empty_tuple(self):
        assert tokenize_name("") == ()
        assert tokens_similarity("", "anything") == 0.0
        assert tokens_similarity("anything", "") == 0.0

    def test_tokens_dedupes_repeated_token(self):
        # ``aa_aa`` -> ["aa", "aa"]; the second occurrence hits the
        # ``tok in seen`` short-circuit and is dropped.
        assert tokenize_name("aa_aa") == ("aa",)

    def test_tokens_disjoint_returns_zero(self):
        # Two non-empty names with zero token overlap exercise the
        # ``not inter`` early return inside ``_token_jaccard``.
        assert tokens_similarity("foo", "bar") == 0.0

    def test_tokens_abbrev_expansion_matches_full_word(self):
        # ``addr`` expands to ``address``; the two names should overlap
        # via the expanded token even though their raw tokens differ.
        assert tokens_similarity("addr", "address") > 0.0

    def test_tfidf_empty_instances_returns_zero(self):
        corpus = TfidfCorpus([["alpha beta", "alpha gamma"], ["beta gamma"]])
        # Empty instance lists short-circuit before any vectorisation.
        assert corpus.similarity([], ["alpha"]) == 0.0
        assert corpus.similarity(["alpha"], []) == 0.0

    def test_tfidf_empty_corpus_returns_zero(self):
        # Constructing a TfidfCorpus from inputs that tokenise to nothing
        # leaves ``_idf`` empty, which is the third leg of the early-out.
        corpus = TfidfCorpus([[""], [""]])
        assert corpus.similarity(["alpha"], ["alpha"]) == 0.0

    def test_tfidf_column_that_tokenises_to_nothing(self):
        # The corpus has real tokens but one of the queried columns is
        # made up of values that tokenise to the empty string. That
        # exercises the ``n == 0`` branch inside ``_vectorise_column``
        # and the cached-zero return inside ``similarity``.
        corpus = TfidfCorpus([["alpha"], ["beta"]])
        result = corpus.similarity([""], ["alpha"])
        assert result == 0.0
        # Second call hits the per-pair cache.
        assert corpus.similarity([""], ["alpha"]) == 0.0


# -- Jaccard distance edge cases -------------------------------------------


class TestJaccardDistanceEdges:
    def test_empty_inputs_with_non_exact_distance_returns_zero(self):
        # When using a fuzzy distance (not Exact) and one side has no
        # values, the early ``not set1 or not set2`` branch returns 0
        # without invoking rapidfuzz.
        df_left = pd.DataFrame({"a": []})
        df_right = pd.DataFrame({"a": ["x", "y"]})
        matcher = JaccardDistanceMatcher(threshold_dist=0.5)
        results = valentine_match([df_left, df_right], matcher, df_names=["l", "r"])
        # Empty source means there is nothing to match against.
        assert all(score == 0 for score in results.values()) or len(results) == 0


# -- Cupid linguistic helper ------------------------------------------------


class TestCupidLinguisticHelpers:
    def test_get_synonyms_returns_set(self):
        # ``car`` is a guaranteed WordNet entry; both helpers should
        # return a non-empty collection of synsets, with the public
        # helper returning a (deduplicated) set view of the cached tuple.
        cached = _cached_synsets("car")
        assert len(cached) > 0
        result = get_synonyms("car")
        assert isinstance(result, set)
        assert result == set(cached)


# -- DistributionBased internal edge cases ---------------------------------


class TestDistributionBasedInternals:
    def test_quantile_histogram_empty_values(self, tmp_path):
        # Build a reference histogram from a non-trivial column so we
        # can construct an empty histogram against it. The empty branch
        # in ``add_values`` zero-fills the bucket counts without
        # invoking ``searchsorted``.
        ref = QuantileHistogram(("t", "ref"), np.array([1.0, 2.0, 3.0, 4.0, 5.0]), 5, n_quantiles=4)
        empty = QuantileHistogram(
            ("t", "empty"), np.array([]), 1, n_quantiles=4, reference_hist=ref
        )
        assert empty.is_empty
        assert sum(empty.bucket_values.values()) == 0.0

    def test_quantile_histogram_values_outside_reference_range(self):
        ref = QuantileHistogram(("t", "ref"), np.array([1.0, 2.0, 3.0, 4.0, 5.0]), 5, n_quantiles=4)
        # Values strictly above every bucket's upper bound: searchsorted
        # returns ``n``, the ``in_range`` mask drops them all, and the
        # resulting bucket counts are zero.
        outlier = QuantileHistogram(
            ("t", "out"),
            np.array([100.0, 200.0]),
            2,
            n_quantiles=4,
            reference_hist=ref,
        )
        assert outlier.is_empty

    def test_compute_ranks_skips_nan(self):
        # ``_compute_ranks`` walks the corpus and skips entries whose
        # conversion produces NaN. Mixing real numbers with the string
        # ``"nan"`` (which ``convert_data_type`` parses as math.nan)
        # exercises that branch.
        ranks = _compute_ranks({"1", "2", "3", "nan"})
        assert "nan" not in ranks
        assert set(ranks.keys()) >= {1, 2, 3}

    def test_column_model_get_global_ranks_filters_nan_and_unknowns(self, tmp_path):
        # Pre-populate a global ranks pickle that maps just two
        # values, then build a column whose data contains: a known
        # value, an unknown value, and a NaN. The two latter rows hit
        # the two uncovered branches inside ``get_global_ranks``.
        generate_global_ranks(["10", "20"], str(tmp_path))
        clear_global_ranks_cache(str(tmp_path))
        col = CorrelationClusteringColumn(
            "c", "cid", ["10", "999", "nan"], "t", "tid", str(tmp_path)
        )
        # Only the known value survives the filter.
        assert col.data == ["10"]
        # ``data_type`` property is the COMA-side compatibility shim;
        # touching it covers the trivial getter.
        assert col.data_type == "varchar"
        clear_global_ranks_cache(str(tmp_path))

    def test_process_columns_skips_histogram_for_empty_column(self, tmp_path):
        # An all-unknown data column has size 0 after the global-rank
        # filter, so the ``column.size > 0`` branch in process_columns
        # is skipped and no quantile histogram is built.
        generate_global_ranks(["a"], str(tmp_path))
        clear_global_ranks_cache(str(tmp_path))
        process_columns(
            (
                "col",
                "uid",
                ["zzz", "yyy"],  # nothing in the global ranks
                "table",
                "tguid",
                4,
                str(tmp_path),
                False,  # write_pickle=False; in-memory store path
            )
        )
        # The column was registered in the in-memory store with no
        # quantile histogram attached.
        store = _COLUMN_STORE[str(tmp_path)]
        # Single registered column has size 0 and no histogram.
        col = next(iter(store.values()))
        assert col.size == 0
        assert col.quantile_histogram is None
        store.clear()
        clear_global_ranks_cache(str(tmp_path))

    def test_ingestion_generator_skips_empty_columns(self, tmp_path):
        # ``ingestion_column_generator`` filters columns whose
        # ``is_empty`` flag is True. Build a table with both an empty
        # and a non-empty column to hit both legs of that loop branch.
        df = pd.DataFrame({"empty": [None, None], "full": [1, 2]})
        table = DataframeTable(df, name="mixed")
        cols = list(table.get_columns())
        produced = list(
            ingestion_column_generator(cols, "mixed", "guid", 4, str(tmp_path), write_pickle=False)
        )
        # Only the non-empty column survives.
        assert len(produced) == 1
        assert produced[0][0] == "full"
