"""Tests verifying that Polars DataFrames produce the same results as pandas.

All tests are skipped when polars is not installed so that CI environments
without the optional ``polars`` dependency still pass cleanly.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

try:
    import polars as pl

    _HAS_POLARS = True
except ImportError:
    _HAS_POLARS = False
    pl = None  # type: ignore[assignment]

if _HAS_POLARS:
    from valentine import valentine_match
    from valentine.algorithms import (
        Coma,
        Cupid,
        DistributionBased,
        JaccardDistanceMatcher,
        SimilarityFlooding,
    )
    from valentine.data_sources.polars.polars_table import PolarsTable

    _DATA = Path(__file__).parent / "data"
    pl_df1 = pl.read_csv(_DATA / "source_candidates.csv")
    pl_df2 = pl.read_csv(_DATA / "target_candidates.csv")

_skip = pytest.mark.skipif(not _HAS_POLARS, reason="polars not installed")


# -- PolarsTable unit tests ------------------------------------------------


@_skip
class TestPolarsTable:
    def test_basic_properties(self):
        table = PolarsTable(pl_df1, name="src")
        assert table.name == "src"
        assert table.unique_identifier == "src"
        assert not table.is_empty

    def test_empty_frame(self):
        empty = PolarsTable(pl.DataFrame({"a": [], "b": []}), name="empty")
        assert empty.is_empty

    def test_get_columns(self):
        table = PolarsTable(pl_df1, name="src")
        cols = table.get_columns()
        assert len(cols) == len(pl_df1.columns)
        col_names = {c.name for c in cols}
        assert col_names == set(pl_df1.columns)

    def test_column_data_excludes_nulls(self):
        df = pl.DataFrame({"a": [1, None, 3], "b": ["x", "y", None]})
        table = PolarsTable(df, name="t")
        cols = {c.name: c for c in table.get_columns()}
        assert cols["a"].data == [1, 3]
        assert cols["b"].data == ["x", "y"]

    def test_column_data_types(self):
        df = pl.DataFrame(
            {
                "name": ["alice", "bob"],
                "age": [30, 25],
                "salary": [1.0, 2.0],
            }
        )
        table = PolarsTable(df, name="t")
        dtypes = {c.name: c.data_type for c in table.get_columns()}
        assert dtypes["name"] == "varchar"
        assert dtypes["age"] == "int"
        assert dtypes["salary"] == "float"

    def test_instance_sampling(self):
        table = PolarsTable(pl_df1, name="src", instance_sample_size=2)
        inst_cols = table.get_instances_columns()
        # Each column should have at most 2 values
        for col in inst_cols:
            assert len(col.data) <= 2

    def test_instance_sample_none_returns_all(self):
        table = PolarsTable(pl_df1, name="src", instance_sample_size=None)
        inst_df = table.get_instances_df()
        assert len(inst_df) == len(pl_df1)

    def test_instance_sample_zero_returns_empty(self):
        table = PolarsTable(pl_df1, name="src", instance_sample_size=0)
        inst_df = table.get_instances_df()
        assert len(inst_df) == 0

    def test_negative_sample_size_rejected(self):
        with pytest.raises(ValueError, match="instance_sample_size"):
            PolarsTable(pl_df1, name="t", instance_sample_size=-1)

    def test_guid_column_lookup(self):
        table = PolarsTable(pl_df1, name="src")
        lookup = table.get_guid_column_lookup()
        for col_name in pl_df1.columns:
            assert col_name in lookup
            assert lookup[col_name] == f"src:{col_name}"

    def test_str_representation(self):
        table = PolarsTable(pl_df1, name="src")
        s = str(table)
        assert "src" in s


# -- Integration: all matchers with Polars input ---------------------------


@_skip
class TestPolarsMatcherIntegration:
    """Verify that every matcher produces results from Polars DataFrames."""

    def test_coma(self):
        matches = valentine_match([pl_df1, pl_df2], Coma(), df_names=["src", "tgt"])
        assert len(matches) > 0

    def test_coma_with_instances(self):
        matches = valentine_match(
            [pl_df1, pl_df2], Coma(use_instances=True), df_names=["src", "tgt"]
        )
        assert len(matches) > 0

    def test_cupid(self):
        matches = valentine_match([pl_df1, pl_df2], Cupid(), df_names=["src", "tgt"])
        assert len(matches) > 0

    def test_distribution_based(self):
        matches = valentine_match([pl_df1, pl_df2], DistributionBased(), df_names=["src", "tgt"])
        assert len(matches) > 0

    def test_jaccard(self):
        matches = valentine_match(
            [pl_df1, pl_df2], JaccardDistanceMatcher(), df_names=["src", "tgt"]
        )
        assert len(matches) > 0

    def test_similarity_flooding(self):
        matches = valentine_match([pl_df1, pl_df2], SimilarityFlooding(), df_names=["src", "tgt"])
        assert len(matches) > 0


# -- Cross-framework: pandas vs Polars produce same results ----------------


@_skip
class TestPandasPolarsEquivalence:
    """Verify that the same data yields identical match results regardless of
    pandas vs Polars input. This is the key correctness guarantee."""

    @staticmethod
    def _match_keys(results) -> set[tuple]:
        return {(p.source_column, p.target_column) for p in results}

    def _assert_equivalent(self, matcher):
        pd_df1 = pd.read_csv(_DATA / "source_candidates.csv")
        pd_df2 = pd.read_csv(_DATA / "target_candidates.csv")

        pd_matches = valentine_match([pd_df1, pd_df2], matcher, df_names=["src", "tgt"])
        pl_matches = valentine_match([pl_df1, pl_df2], matcher, df_names=["src", "tgt"])

        pd_keys = self._match_keys(pd_matches)
        pl_keys = self._match_keys(pl_matches)
        assert pd_keys == pl_keys, f"Key mismatch: {pd_keys ^ pl_keys}"

        # Scores should be very close (float rounding may differ slightly)
        for pair in pd_matches:
            pd_score = pd_matches[pair]
            pl_score = pl_matches[pair]
            assert abs(pd_score - pl_score) < 0.05, (
                f"{pair}: pandas={pd_score:.4f} vs polars={pl_score:.4f}"
            )

    def test_coma_equivalence(self):
        self._assert_equivalent(Coma())

    def test_coma_with_instances_equivalence(self):
        self._assert_equivalent(Coma(use_instances=True))

    def test_cupid_equivalence(self):
        self._assert_equivalent(Cupid())

    def test_jaccard_equivalence(self):
        self._assert_equivalent(JaccardDistanceMatcher())

    def test_similarity_flooding_equivalence(self):
        self._assert_equivalent(SimilarityFlooding())


# -- Mixed input: pandas + Polars in same call -----------------------------


@_skip
class TestMixedInput:
    def test_pandas_source_polars_target(self):
        pd_df1 = pd.read_csv(_DATA / "source_candidates.csv")
        matches = valentine_match([pd_df1, pl_df2], Coma(), df_names=["src", "tgt"])
        assert len(matches) > 0

    def test_polars_source_pandas_target(self):
        pd_df2 = pd.read_csv(_DATA / "target_candidates.csv")
        matches = valentine_match([pl_df1, pd_df2], Coma(), df_names=["src", "tgt"])
        assert len(matches) > 0
