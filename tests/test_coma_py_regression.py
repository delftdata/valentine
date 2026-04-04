"""
Regression tests for ComaPy.

Tests use two datasets:
- authors1/authors2: easy case with identical column names
- source_candidates/target_candidates: hard case with different column names,
  abbreviations, and different value formats (e.g., "NYC" vs "New York")
"""

import time
from pathlib import Path

import pandas as pd
import pytest

from tests import df1, df2
from valentine import valentine_match
from valentine.algorithms import ComaPy
from valentine.data_sources import DataframeTable

# --- Easy dataset: identical column names ---
authors1 = DataframeTable(df1, name="authors1")
authors2 = DataframeTable(df2, name="authors2")

AUTHORS_EXPECTED = {
    (("authors1", "Authors"), ("authors2", "Authors")),
    (("authors1", "Cited by"), ("authors2", "Cited by")),
    (("authors1", "EID"), ("authors2", "EID")),
}

# --- Hard dataset: different column names, abbreviations, different value formats ---
DATA_DIR = Path(__file__).parent / "data"
_src_df = pd.read_csv(DATA_DIR / "source_candidates.csv")
_tgt_df = pd.read_csv(DATA_DIR / "target_candidates.csv")
candidates_src = DataframeTable(_src_df, name="source")
candidates_tgt = DataframeTable(_tgt_df, name="target")

CANDIDATES_GROUND_TRUTH = [
    ("emp_id", "employee_number"),
    ("fname", "first_name"),
    ("lname", "last_name"),
    ("dept", "department"),
    ("annual_salary", "compensation"),
    ("hire_date", "start_date"),
    ("office_loc", "work_location"),
]


# ---- Authors tests (easy) ----


def test_authors_schema_only():
    """Schema-only must find identical column names with high scores."""
    results = ComaPy(use_instances=False).get_matches(authors1, authors2)

    assert AUTHORS_EXPECTED.issubset(set(results.keys()))
    for key in AUTHORS_EXPECTED:
        assert results[key] > 0.7


def test_authors_schema_instance():
    """Schema+instance on identical columns should produce high scores."""
    results = ComaPy(use_instances=True).get_matches(authors1, authors2)

    assert AUTHORS_EXPECTED.issubset(set(results.keys()))
    for key in AUTHORS_EXPECTED:
        assert results[key] > 0.8


def test_authors_instance_only():
    """Instance-only on identical data should produce near-perfect scores."""
    results = ComaPy(use_instances=True, use_schema=False).get_matches(authors1, authors2)

    assert AUTHORS_EXPECTED.issubset(set(results.keys()))
    for key in AUTHORS_EXPECTED:
        assert results[key] > 0.95


# ---- Candidates tests (hard) ----


def test_candidates_schema_instance_finds_all_ground_truth():
    """Schema+instance must find all ground truth pairs when names differ."""
    matches = valentine_match(_src_df, _tgt_df, ComaPy(use_instances=True))

    for src_col, tgt_col in CANDIDATES_GROUND_TRUTH:
        found = any(k[0][1] == src_col and k[1][1] == tgt_col for k in matches)
        assert found, f"Missing ground truth pair: {src_col} <-> {tgt_col}"


def test_candidates_schema_instance_precision():
    """Schema+instance should achieve perfect precision on this dataset."""
    matches = valentine_match(_src_df, _tgt_df, ComaPy(use_instances=True))
    metrics = matches.get_metrics(CANDIDATES_GROUND_TRUTH)

    assert metrics["Precision"] == 1.0, f"Precision={metrics['Precision']:.2f}, expected 1.0"


def test_candidates_instance_only_high_f1():
    """Instance-only should achieve high F1 when data values overlap."""
    matches = valentine_match(_src_df, _tgt_df, ComaPy(use_instances=True, use_schema=False))
    metrics = matches.get_metrics(CANDIDATES_GROUND_TRUTH)

    assert metrics["F1Score"] >= 0.9, f"F1={metrics['F1Score']:.2f}, expected >= 0.9"


def test_candidates_schema_only_finds_name_based_pairs():
    """Schema-only should find pairs where names partially overlap."""
    matches = valentine_match(_src_df, _tgt_df, ComaPy(use_instances=False))

    # These have partial name overlap and should be found by schema matching
    name_matchable = [
        ("fname", "first_name"),
        ("lname", "last_name"),
        ("dept", "department"),
        ("hire_date", "start_date"),
    ]
    for src_col, tgt_col in name_matchable:
        found = any(k[0][1] == src_col and k[1][1] == tgt_col for k in matches)
        assert found, f"Schema-only should find: {src_col} <-> {tgt_col}"


# ---- Configuration tests ----


def test_no_matchers_raises():
    """Must raise ValueError when both schema and instances are disabled."""
    with pytest.raises(ValueError, match="At least one"):
        ComaPy(use_schema=False, use_instances=False)


def test_delta_controls_output_count():
    """Smaller delta should produce fewer matches."""
    strict = valentine_match(_src_df, _tgt_df, ComaPy(use_instances=True, delta=0.01))
    relaxed = valentine_match(_src_df, _tgt_df, ComaPy(use_instances=True, delta=0.15))

    assert len(relaxed) >= len(strict)


# ---- Performance ----


def test_performance():
    """ComaPy must complete matching within reasonable time."""
    matcher = ComaPy(use_instances=True)
    start = time.perf_counter()
    for _ in range(10):
        matcher.get_matches(authors1, authors2)
    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / 10) * 1000
    assert avg_ms < 5000, f"ComaPy too slow: {avg_ms:.0f}ms per match"
