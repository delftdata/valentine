"""
Regression tests for the Coma matcher.

Tests use the candidates dataset: a hard case with different column names,
abbreviations, and different value formats (e.g., "NYC" vs "New York").
"""

import time

import pytest

from tests import df1, df2
from valentine import valentine_match
from valentine.algorithms import Coma
from valentine.data_sources import DataframeTable

# --- Dataset setup ---
candidates_src = DataframeTable(df1, name="source")
candidates_tgt = DataframeTable(df2, name="target")

CANDIDATES_GROUND_TRUTH = [
    ("emp_id", "employee_number"),
    ("fname", "first_name"),
    ("lname", "last_name"),
    ("dept", "department"),
    ("annual_salary", "compensation"),
    ("hire_date", "start_date"),
    ("office_loc", "work_location"),
]

# Pairs with enough name overlap for schema-only matching to find
NAME_MATCHABLE = [
    ("fname", "first_name"),
    ("lname", "last_name"),
    ("dept", "department"),
    ("hire_date", "start_date"),
]


# ---- Candidates tests ----


def test_candidates_schema_instance_finds_all_ground_truth():
    """Schema+instance must find all ground truth pairs when names differ."""
    matches = valentine_match([df1, df2], Coma(use_instances=True))

    for src_col, tgt_col in CANDIDATES_GROUND_TRUTH:
        found = any(k.source_column == src_col and k.target_column == tgt_col for k in matches)
        assert found, f"Missing ground truth pair: {src_col} <-> {tgt_col}"


def test_candidates_schema_instance_precision():
    """Schema+instance should achieve perfect precision on this dataset."""
    matches = valentine_match([df1, df2], Coma(use_instances=True))
    metrics = matches.get_metrics(CANDIDATES_GROUND_TRUTH)

    assert metrics["Precision"] == 1.0, f"Precision={metrics['Precision']:.2f}, expected 1.0"


def test_candidates_instance_only_high_f1():
    """Instance-only should achieve high F1 when data values overlap."""
    matches = valentine_match([df1, df2], Coma(use_instances=True, use_schema=False))
    metrics = matches.get_metrics(CANDIDATES_GROUND_TRUTH)

    assert metrics["F1Score"] >= 0.9, f"F1={metrics['F1Score']:.2f}, expected >= 0.9"


def test_candidates_schema_only_finds_name_based_pairs():
    """Schema-only should find pairs where names partially overlap."""
    matches = valentine_match([df1, df2], Coma(use_instances=False))

    for src_col, tgt_col in NAME_MATCHABLE:
        found = any(k.source_column == src_col and k.target_column == tgt_col for k in matches)
        assert found, f"Schema-only should find: {src_col} <-> {tgt_col}"


def test_candidates_schema_only_produces_output():
    """Schema-only must produce some matches."""
    results = Coma(use_instances=False).get_matches(candidates_src, candidates_tgt)
    assert len(results) > 0


def test_candidates_schema_instance_produces_output():
    """Schema+instance must produce some matches."""
    results = Coma(use_instances=True).get_matches(candidates_src, candidates_tgt)
    assert len(results) > 0


def test_candidates_instance_only_produces_output():
    """Instance-only must produce some matches."""
    results = Coma(use_instances=True, use_schema=False).get_matches(candidates_src, candidates_tgt)
    assert len(results) > 0


# ---- Configuration tests ----


def test_no_matchers_raises():
    """Must raise ValueError when both schema and instances are disabled."""
    with pytest.raises(ValueError, match="At least one"):
        Coma(use_schema=False, use_instances=False)


def test_delta_controls_output_count():
    """Smaller delta should produce fewer matches."""
    strict = valentine_match([df1, df2], Coma(use_instances=True, delta=0.01))
    relaxed = valentine_match([df1, df2], Coma(use_instances=True, delta=0.15))

    assert len(relaxed) >= len(strict)


# ---- Performance ----


def test_performance():
    """Coma must complete matching within reasonable time."""
    matcher = Coma(use_instances=True)
    start = time.perf_counter()
    for _ in range(10):
        matcher.get_matches(candidates_src, candidates_tgt)
    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / 10) * 1000
    assert avg_ms < 5000, f"Coma too slow: {avg_ms:.0f}ms per match"
