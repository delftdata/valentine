"""Smoke test: every public symbol referenced in the docs must import.

The docs under ``docs/`` are hand-written markdown, so they don't get
updated automatically when a symbol is renamed or removed. This test
catches that drift by importing every name the docs claim is public.

If you remove or rename a public symbol, either restore the name or
update the docs AND this test in the same commit.
"""

from __future__ import annotations

import inspect
import uuid

import pandas as pd

from valentine import (
    ColumnPair,
    InvalidMatcherError,
    MatcherResults,
    NotAValentineMatcher,
    valentine_match,
)
from valentine.algorithms import (
    BaseMatcher,
    ColumnPair as AlgColumnPair,
    Coma,
    Cupid,
    DistributionBased,
    Formula,
    JaccardDistanceMatcher,
    Policy,
    SimilarityFlooding,
    StringMatcher,
    all_matchers,
    instance_only_algorithms,
    schema_instance_algorithms,
    schema_only_algorithms,
)
from valentine.algorithms.jaccard_distance import StringDistanceFunction
from valentine.algorithms.matcher_results import MatcherResults as MR
from valentine.data_sources import BaseColumn, BaseTable, DataframeColumn, DataframeTable
from valentine.metrics import (
    METRICS_ALL,
    METRICS_CORE,
    METRICS_PRECISION_INCREASING_N,
    METRICS_PRECISION_RECALL,
    F1Score,
    Metric,
    Precision,
    PrecisionTopNPercent,
    Recall,
    RecallAtSizeofGroundTruth,
)


def test_top_level_package_exports():
    # Deprecated alias must still resolve to the current exception class.
    assert NotAValentineMatcher is InvalidMatcherError
    assert callable(valentine_match)
    assert ColumnPair._fields == (
        "source_table",
        "source_column",
        "target_table",
        "target_column",
    )
    assert MatcherResults.__name__ == "MatcherResults"


def test_algorithms_exports():
    for cls in (
        Coma,
        Cupid,
        DistributionBased,
        JaccardDistanceMatcher,
        SimilarityFlooding,
    ):
        assert issubclass(cls, BaseMatcher)

    assert AlgColumnPair is ColumnPair
    assert set(all_matchers) == set(
        schema_only_algorithms + instance_only_algorithms + schema_instance_algorithms
    )
    assert {p.name for p in Policy} >= {"INVERSE_AVERAGE", "INVERSE_PRODUCT"}
    assert {f.name for f in Formula} >= {"BASIC", "FORMULA_A", "FORMULA_B", "FORMULA_C"}
    assert {s.name for s in StringMatcher} >= {
        "PREFIX_SUFFIX",
        "PREFIX_SUFFIX_TFIDF",
        "LEVENSHTEIN",
    }


def test_string_distance_function_enum():
    assert {e.name for e in StringDistanceFunction} >= {
        "Levenshtein",
        "DamerauLevenshtein",
        "Hamming",
        "Jaro",
        "JaroWinkler",
        "Exact",
    }


def test_metrics_exports():
    for cls in (
        Precision,
        Recall,
        F1Score,
        PrecisionTopNPercent,
        RecallAtSizeofGroundTruth,
    ):
        assert issubclass(cls, Metric)

    # Predefined sets must be non-empty and contain Metric instances.
    for s in (
        METRICS_CORE,
        METRICS_ALL,
        METRICS_PRECISION_RECALL,
        METRICS_PRECISION_INCREASING_N,
    ):
        assert s and all(isinstance(m, Metric) for m in s)


def test_data_sources_exports():
    assert issubclass(DataframeTable, BaseTable)
    assert issubclass(DataframeColumn, BaseColumn)


def test_matcher_results_documented_methods():
    """Every MatcherResults method referenced in the docs must exist."""
    for name in (
        "one_to_one",
        "filter",
        "take_top_n",
        "take_top_percent",
        "get_copy",
        "get_metrics",
        "get_details",
        "details",
    ):
        assert hasattr(MR, name), f"MatcherResults.{name} missing"


def test_valentine_match_signature():
    """The `instance_sample_size` parameter documented in the API ref must exist."""
    params = inspect.signature(valentine_match).parameters
    assert "dfs" in params
    assert "matcher" in params
    assert "df_names" in params
    assert "instance_sample_size" in params
    assert params["instance_sample_size"].default == 1000


def test_custom_data_source_example_from_api_docs():
    """Execute the DictTable/DictColumn custom data source example from api.md.

    This is copy-pasted verbatim from ``docs/api.md`` — if the snippet
    stops running, update *both* the test and the docs in the same
    commit.
    """

    class DictColumn(BaseColumn):
        def __init__(self, name: str, data: list, data_type: str = "varchar"):
            self._name = name
            self._data = data
            self._data_type = data_type
            self._guid = str(uuid.uuid4())

        @property
        def name(self) -> str:
            return self._name

        @property
        def unique_identifier(self) -> str:
            return self._guid

        @property
        def data_type(self) -> str:
            return self._data_type

        @property
        def data(self) -> list:
            return self._data

    class DictTable(BaseTable):
        def __init__(self, name: str, columns: dict[str, list]):
            self._name = name
            self._guid = str(uuid.uuid4())
            self._columns = [DictColumn(k, v) for k, v in columns.items()]

        @property
        def name(self) -> str:
            return self._name

        @property
        def unique_identifier(self) -> str:
            return self._guid

        def get_columns(self) -> list[BaseColumn]:
            return self._columns

        def get_df(self) -> pd.DataFrame:
            return pd.DataFrame({c.name: c.data for c in self._columns})

        @property
        def is_empty(self) -> bool:
            return all(len(c.data) == 0 for c in self._columns)

    source = DictTable(
        "hr",
        {"emp_id": [1, 2, 3], "fname": ["a", "b", "c"]},
    )
    target = DictTable(
        "payroll",
        {"employee_number": [1, 2, 3], "first_name": ["a", "b", "c"]},
    )

    raw = Coma().get_matches_batch([source, target])

    # The returned dict must be keyed by ColumnPair and contain at least one match.
    assert raw, "Custom-source example produced no matches"
    for key in raw:
        assert isinstance(key, ColumnPair)
        assert key.source_table == "hr"
        assert key.target_table == "payroll"


def test_end_to_end_example_from_docs():
    """Execute the minimal example that appears in index.md / getting-started.md."""
    df1 = pd.DataFrame({"emp_id": [1, 2, 3], "fname": ["a", "b", "c"]})
    df2 = pd.DataFrame({"employee_number": [1, 2, 3], "first_name": ["a", "b", "c"]})

    matches = valentine_match([df1, df2], Coma())

    # Iteration form documented in the guide pages must work.
    for pair, score in matches.items():
        assert isinstance(score, float)
        _ = pair.source_table, pair.source_column, pair.target_table, pair.target_column

    # Ground truth in the column-name-pair format must compute metrics.
    metrics = matches.get_metrics([("emp_id", "employee_number"), ("fname", "first_name")])
    assert "Precision" in metrics
    assert "Recall" in metrics
    assert "F1Score" in metrics
