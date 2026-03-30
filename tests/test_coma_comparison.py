"""
Comprehensive comparison of Java COMA vs Python ComaPy.

Runs both implementations side-by-side on the same data and reports:
- Matched column pairs (agreement and disagreement)
- Per-pair similarity score differences
- Aggregate accuracy statistics (MAE, max error)
- Performance (wall-clock time per match)

Requires Java to be installed. Skips gracefully if Java is unavailable.
"""

import subprocess
import time

import pytest

from tests import df1, df2
from valentine.algorithms import Coma, ComaPy
from valentine.data_sources import DataframeTable

d1 = DataframeTable(df1, name="authors1")
d2 = DataframeTable(df2, name="authors2")

_java_available = True
try:
    subprocess.check_output(["java", "-version"], stderr=subprocess.DEVNULL)
except FileNotFoundError, subprocess.CalledProcessError:
    _java_available = False

requires_java = pytest.mark.skipif(not _java_available, reason="Java not installed")


def _run_java(use_instances: bool) -> dict[tuple, float]:
    matcher = Coma(use_instances=use_instances)
    return matcher.get_matches(d1, d2)


def _run_python(use_instances: bool) -> dict[tuple, float]:
    matcher = ComaPy(use_instances=use_instances)
    return matcher.get_matches(d1, d2)


def _format_key(key: tuple) -> str:
    (t1, c1), (t2, c2) = key
    return f"{t1}.{c1} <-> {t2}.{c2}"


def _compare(java_results: dict, py_results: dict, label: str) -> dict:
    """Compare two result dicts and return a summary dict."""
    java_keys = set(java_results.keys())
    py_keys = set(py_results.keys())

    common = java_keys & py_keys
    java_only = java_keys - py_keys
    py_only = py_keys - java_keys

    # Per-pair score comparison
    diffs = []
    for key in sorted(common, key=_format_key):
        java_score = java_results[key]
        py_score = py_results[key]
        diffs.append(
            {
                "pair": _format_key(key),
                "java": java_score,
                "python": py_score,
                "abs_diff": abs(java_score - py_score),
                "rel_diff_pct": abs(java_score - py_score) / java_score * 100
                if java_score > 0
                else 0.0,
            }
        )

    abs_diffs = [d["abs_diff"] for d in diffs]
    mae = sum(abs_diffs) / len(abs_diffs) if abs_diffs else 0.0
    max_err = max(abs_diffs) if abs_diffs else 0.0

    return {
        "label": label,
        "java_count": len(java_keys),
        "python_count": len(py_keys),
        "common_count": len(common),
        "java_only": [_format_key(k) for k in java_only],
        "python_only": [_format_key(k) for k in py_only],
        "pair_diffs": diffs,
        "mae": mae,
        "max_abs_error": max_err,
    }


def _print_report(comparison: dict) -> None:
    label = comparison["label"]
    print(f"\n{'=' * 70}")
    print(f" {label}")
    print(f"{'=' * 70}")

    print(
        f"\n  Matched pairs — Java: {comparison['java_count']}, "
        f"Python: {comparison['python_count']}, "
        f"Common: {comparison['common_count']}"
    )

    if comparison["java_only"]:
        print(f"\n  Pairs found ONLY by Java ({len(comparison['java_only'])}):")
        for p in comparison["java_only"]:
            print(f"    - {p}")

    if comparison["python_only"]:
        print(f"\n  Pairs found ONLY by Python ({len(comparison['python_only'])}):")
        for p in comparison["python_only"]:
            print(f"    - {p}")

    print(f"\n  {'Pair':<45} {'Java':>8} {'Python':>8} {'Δ':>8} {'Δ%':>7}")
    print(f"  {'-' * 45} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 7}")
    for d in comparison["pair_diffs"]:
        print(
            f"  {d['pair']:<45} {d['java']:>8.6f} {d['python']:>8.6f} "
            f"{d['abs_diff']:>8.6f} {d['rel_diff_pct']:>6.2f}%"
        )

    print(f"\n  Mean Absolute Error:  {comparison['mae']:.6f}")
    print(f"  Max Absolute Error:   {comparison['max_abs_error']:.6f}")


def _print_perf_report(java_ms: float, py_ms: float, label: str) -> None:
    speedup = java_ms / py_ms if py_ms > 0 else float("inf")
    print(f"\n{'=' * 70}")
    print(f" Performance — {label}")
    print(f"{'=' * 70}")
    print(f"  Java avg:   {java_ms:>8.1f} ms / match")
    print(f"  Python avg: {py_ms:>8.1f} ms / match")
    print(f"  Speedup:    {speedup:>8.1f}x {'(Python faster)' if speedup > 1 else '(Java faster)'}")


# ---- Schema-only tests ----


@requires_java
def test_schema_only_same_pairs():
    """Both implementations find the same column pairs (schema-only)."""
    java_results = _run_java(use_instances=False)
    py_results = _run_python(use_instances=False)

    java_keys = set(java_results.keys())
    py_keys = set(py_results.keys())

    missing = java_keys - py_keys
    assert not missing, f"Python missing pairs found by Java: {[_format_key(k) for k in missing]}"


@requires_java
def test_schema_only_scores():
    """Python scores are within 5% absolute of Java (schema-only)."""
    java_results = _run_java(use_instances=False)
    py_results = _run_python(use_instances=False)
    comparison = _compare(java_results, py_results, "Schema-Only Score Comparison")
    _print_report(comparison)

    for d in comparison["pair_diffs"]:
        assert d["abs_diff"] < 0.05, (
            f"Score divergence too large for {d['pair']}: "
            f"Java={d['java']:.6f}, Python={d['python']:.6f}, Δ={d['abs_diff']:.6f}"
        )


@requires_java
def test_schema_only_detailed_report(capsys):
    """Print detailed accuracy report for schema-only matching."""
    java_results = _run_java(use_instances=False)
    py_results = _run_python(use_instances=False)
    comparison = _compare(java_results, py_results, "Schema-Only (COMA_OPT)")
    _print_report(comparison)

    # Soft assertion — this test is primarily for reporting
    assert comparison["mae"] < 0.05, f"MAE too high: {comparison['mae']:.6f}"


# ---- Schema + Instance tests ----


@requires_java
def test_schema_instance_same_pairs():
    """Both implementations find the same column pairs (schema+instance)."""
    java_results = _run_java(use_instances=True)
    py_results = _run_python(use_instances=True)

    java_keys = set(java_results.keys())
    py_keys = set(py_results.keys())

    missing = java_keys - py_keys
    assert not missing, f"Python missing pairs found by Java: {[_format_key(k) for k in missing]}"


@requires_java
def test_schema_instance_scores():
    """Python scores are within 25% absolute of Java (schema+instance).

    Larger tolerance because TF-IDF implementations differ between Java and Python.
    """
    java_results = _run_java(use_instances=True)
    py_results = _run_python(use_instances=True)
    comparison = _compare(java_results, py_results, "Schema+Instance Score Comparison")
    _print_report(comparison)

    for d in comparison["pair_diffs"]:
        assert d["abs_diff"] < 0.25, (
            f"Score divergence too large for {d['pair']}: "
            f"Java={d['java']:.6f}, Python={d['python']:.6f}, Δ={d['abs_diff']:.6f}"
        )


@requires_java
def test_schema_instance_detailed_report(capsys):
    """Print detailed accuracy report for schema+instance matching."""
    java_results = _run_java(use_instances=True)
    py_results = _run_python(use_instances=True)
    comparison = _compare(java_results, py_results, "Schema+Instance (COMA_OPT_INST)")
    _print_report(comparison)

    assert comparison["mae"] < 0.25, f"MAE too high: {comparison['mae']:.6f}"


# ---- Performance tests ----


@requires_java
def test_performance_comparison():
    """Compare wall-clock performance of Java vs Python implementations."""
    n_warmup = 2
    n_runs = 5

    # Warm up Java (JVM startup, JIT)
    for _ in range(n_warmup):
        _run_java(use_instances=False)

    # Warm up Python
    for _ in range(n_warmup):
        _run_python(use_instances=False)

    # Benchmark schema-only
    start = time.perf_counter()
    for _ in range(n_runs):
        _run_java(use_instances=False)
    java_schema_ms = (time.perf_counter() - start) / n_runs * 1000

    start = time.perf_counter()
    for _ in range(n_runs):
        _run_python(use_instances=False)
    py_schema_ms = (time.perf_counter() - start) / n_runs * 1000

    _print_perf_report(java_schema_ms, py_schema_ms, "Schema-Only (COMA_OPT)")

    # Benchmark schema+instance
    # Warm up
    for _ in range(n_warmup):
        _run_java(use_instances=True)
    for _ in range(n_warmup):
        _run_python(use_instances=True)

    start = time.perf_counter()
    for _ in range(n_runs):
        _run_java(use_instances=True)
    java_inst_ms = (time.perf_counter() - start) / n_runs * 1000

    start = time.perf_counter()
    for _ in range(n_runs):
        _run_python(use_instances=True)
    py_inst_ms = (time.perf_counter() - start) / n_runs * 1000

    _print_perf_report(java_inst_ms, py_inst_ms, "Schema+Instance (COMA_OPT_INST)")

    # Python should not be dramatically slower than Java
    # (Java includes subprocess + JVM startup overhead, so Python should win)
    assert py_schema_ms < 5000, f"Python schema-only too slow: {py_schema_ms:.0f}ms"
    assert py_inst_ms < 5000, f"Python schema+instance too slow: {py_inst_ms:.0f}ms"


# ---- Combined summary test ----


@requires_java
def test_full_comparison_report():
    """
    Run both strategies through both implementations and produce a
    comprehensive comparison report with accuracy and timing.
    """
    results = {}
    timings = {}

    for use_instances in [False, True]:
        label = "schema+instance" if use_instances else "schema-only"

        start = time.perf_counter()
        java_res = _run_java(use_instances=use_instances)
        timings[f"java_{label}"] = (time.perf_counter() - start) * 1000

        start = time.perf_counter()
        py_res = _run_python(use_instances=use_instances)
        timings[f"python_{label}"] = (time.perf_counter() - start) * 1000

        strategy_label = "COMA_OPT_INST" if use_instances else "COMA_OPT"
        results[label] = _compare(java_res, py_res, strategy_label)

    # Print combined report
    print(f"\n{'#' * 70}")
    print(" COMPREHENSIVE COMA COMPARISON: Java vs Python")
    print(f"{'#' * 70}")

    for label, comparison in results.items():
        _print_report(comparison)
        _print_perf_report(
            timings[f"java_{label}"], timings[f"python_{label}"], comparison["label"]
        )

    # Summary table
    print(f"\n{'=' * 70}")
    print(" Summary")
    print(f"{'=' * 70}")
    print(
        f"  {'Strategy':<25} {'MAE':>10} {'MaxErr':>10} {'Java ms':>10} {'Py ms':>10} {'Speedup':>10}"
    )
    print(f"  {'-' * 25} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}")
    for label, comparison in results.items():
        j_ms = timings[f"java_{label}"]
        p_ms = timings[f"python_{label}"]
        speedup = j_ms / p_ms if p_ms > 0 else float("inf")
        print(
            f"  {comparison['label']:<25} "
            f"{comparison['mae']:>10.6f} "
            f"{comparison['max_abs_error']:>10.6f} "
            f"{j_ms:>10.1f} "
            f"{p_ms:>10.1f} "
            f"{speedup:>9.1f}x"
        )

    # Assertions
    for label, comparison in results.items():
        {d["pair"] for d in comparison["pair_diffs"]}
        assert comparison["common_count"] == comparison["java_count"], (
            f"[{label}] Python is missing pairs that Java found"
        )

    assert results["schema-only"]["mae"] < 0.05, "Schema-only MAE too high"
    assert results["schema+instance"]["mae"] < 0.25, "Schema+instance MAE too high"
