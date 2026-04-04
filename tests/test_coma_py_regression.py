"""
Regression tests for ComaPy.

Verifies that ComaPy produces the expected column pairs and sensible
similarity scores on the authors1/authors2 test data, and that the
various configuration options work correctly.
"""

import time

import pytest

from tests import df1, df2
from valentine.algorithms import ComaPy
from valentine.data_sources import DataframeTable

d1 = DataframeTable(df1, name="authors1")
d2 = DataframeTable(df2, name="authors2")

# Expected matching pairs for authors data (both modes should find these)
EXPECTED_PAIRS = {
    (("authors1", "Authors"), ("authors2", "Authors")),
    (("authors1", "Cited by"), ("authors2", "Cited by")),
    (("authors1", "EID"), ("authors2", "EID")),
}


def test_coma_py_schema_only_pairs():
    """ComaPy schema-only must find the correct column pairs."""
    matcher = ComaPy(use_instances=False)
    results = matcher.get_matches(d1, d2)

    assert EXPECTED_PAIRS.issubset(set(results.keys())), (
        f"Missing pairs: {EXPECTED_PAIRS - set(results.keys())}"
    )


def test_coma_py_schema_only_scores():
    """ComaPy schema-only scores must be in a reasonable range."""
    matcher = ComaPy(use_instances=False)
    results = matcher.get_matches(d1, d2)

    for key in EXPECTED_PAIRS:
        assert key in results, f"Missing match: {key}"
        # Schema-only scores for identical column names should be high
        assert results[key] > 0.7, f"Score too low for {key}: {results[key]:.4f} (expected > 0.7)"
        assert results[key] <= 1.0, f"Score out of range for {key}: {results[key]:.4f}"


def test_coma_py_schema_instance_pairs():
    """ComaPy schema+instance must find the correct column pairs."""
    matcher = ComaPy(use_instances=True)
    results = matcher.get_matches(d1, d2)

    assert EXPECTED_PAIRS.issubset(set(results.keys())), (
        f"Missing pairs: {EXPECTED_PAIRS - set(results.keys())}"
    )


def test_coma_py_schema_instance_scores():
    """ComaPy schema+instance scores should be high for matching columns."""
    schema_inst = ComaPy(use_instances=True).get_matches(d1, d2)

    for key in EXPECTED_PAIRS:
        assert key in schema_inst, f"Missing match: {key}"
        # Instance matching on identical columns should boost or maintain scores
        assert schema_inst[key] > 0.8, (
            f"Score too low for {key}: {schema_inst[key]:.4f} (expected > 0.8)"
        )


def test_coma_py_instance_only():
    """ComaPy instance-only mode must work and find correct pairs."""
    matcher = ComaPy(use_instances=True, use_schema=False)
    results = matcher.get_matches(d1, d2)

    assert EXPECTED_PAIRS.issubset(set(results.keys())), (
        f"Missing pairs: {EXPECTED_PAIRS - set(results.keys())}"
    )
    for key in EXPECTED_PAIRS:
        # Instance-only on identical columns should produce very high scores
        assert results[key] > 0.95, f"Score too low for {key}: {results[key]:.4f} (expected > 0.95)"


def test_coma_py_no_matchers_raises():
    """ComaPy must raise ValueError when both schema and instances are disabled."""
    with pytest.raises(ValueError, match="At least one"):
        ComaPy(use_schema=False, use_instances=False)


def test_coma_py_performance():
    """ComaPy must complete matching within reasonable time."""
    matcher = ComaPy(use_instances=True)
    start = time.perf_counter()
    for _ in range(10):
        matcher.get_matches(d1, d2)
    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / 10) * 1000
    # Must complete in under 5 seconds per match
    assert avg_ms < 5000, f"ComaPy too slow: {avg_ms:.0f}ms per match"
