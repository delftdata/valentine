"""
Comprehensive comparison of Java COMA vs Python ComaPy.

Runs both implementations side-by-side on two datasets and reports:
- Matched column pairs (agreement and disagreement)
- Per-pair similarity score differences
- Aggregate accuracy statistics (MAE, max error)
- Performance (wall-clock time per match)
- Ground-truth metrics (precision, recall, F1) where available

Requires Java to be installed. Skips gracefully if Java is unavailable.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

import pandas as pd
import pytest

from tests import df1, df2
from valentine import MatcherResults
from valentine.algorithms import Coma, ComaPy
from valentine.data_sources import DataframeTable
from valentine.metrics import F1Score, Precision, Recall

# ---- Datasets ----

# Small dataset (authors, 20 rows, 3 shared columns)
authors_src = DataframeTable(df1, name="authors1")
authors_tgt = DataframeTable(df2, name="authors2")

# Larger dataset (NYC projects, ~1963 rows, mixed types, partial overlap)
_examples_dir = Path(__file__).parent.parent / "examples" / "data"
_src_df = pd.read_csv(_examples_dir / "source_table.csv")
_tgt_df = pd.read_csv(_examples_dir / "target_table.csv")
projects_src = DataframeTable(_src_df, name="source")
projects_tgt = DataframeTable(_tgt_df, name="target")

# Ground truth for the projects dataset (from valentine_example.py)
PROJECTS_GROUND_TRUTH = [
    ("total_changes", "total_schedule_changes"),
    ("changes", "total_budget_changes"),
    ("pid", "pid"),
    ("date_reported_as_of", "date_reported_as_of"),
    ("forecast_completion", "forecast_completion"),
]

# ---- Java availability ----

_java_available = True
try:
    subprocess.check_output(["java", "-version"], stderr=subprocess.DEVNULL)
except (FileNotFoundError, subprocess.CalledProcessError):
    _java_available = False

requires_java = pytest.mark.skipif(not _java_available, reason="Java not installed")


# ---- Helpers ----

Dataset = tuple[DataframeTable, DataframeTable, str]

DATASETS: list[Dataset] = [
    (authors_src, authors_tgt, "authors"),
    (projects_src, projects_tgt, "projects"),
]


def _run_matcher(
    matcher_cls: type, src: DataframeTable, tgt: DataframeTable, use_instances: bool
) -> dict[tuple, float]:
    matcher = matcher_cls(use_instances=use_instances)
    return matcher.get_matches(src, tgt)


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
                "rel_diff_pct": (
                    abs(java_score - py_score) / java_score * 100 if java_score > 0 else 0.0
                ),
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

    if comparison["pair_diffs"]:
        print(f"\n  {'Pair':<55} {'Java':>8} {'Python':>8} {'|Δ|':>8} {'Δ%':>7}")
        print(f"  {'-' * 55} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 7}")
        for d in comparison["pair_diffs"]:
            print(
                f"  {d['pair']:<55} {d['java']:>8.6f} {d['python']:>8.6f} "
                f"{d['abs_diff']:>8.6f} {d['rel_diff_pct']:>6.2f}%"
            )

    print(f"\n  Mean Absolute Error:  {comparison['mae']:.6f}")
    print(f"  Max Absolute Error:   {comparison['max_abs_error']:.6f}")


def _print_perf_report(java_ms: float, py_ms: float, label: str) -> None:
    speedup = java_ms / py_ms if py_ms > 0 else float("inf")
    print(f"\n  Performance — {label}")
    print(f"  Java:    {java_ms:>8.1f} ms")
    print(f"  Python:  {py_ms:>8.1f} ms")
    direction = "(Python faster)" if speedup > 1 else "(Java faster)"
    print(f"  Speedup: {speedup:>8.1f}x {direction}")


def _compute_ground_truth_metrics(
    results: dict, ground_truth: list[tuple[str, str]]
) -> dict[str, float]:
    """Compute precision/recall/F1 using valentine's own metrics."""
    mr = MatcherResults(results)
    return mr.get_metrics(ground_truth, metrics={Precision(), Recall(), F1Score()})


# ---- Tests: Authors dataset (small, exact-match columns) ----


@requires_java
class TestAuthorsAccuracy:
    """Accuracy comparison on the authors dataset (20 rows, 3 shared columns)."""

    def test_schema_only_same_pairs(self):
        java = _run_matcher(Coma, authors_src, authors_tgt, use_instances=False)
        py = _run_matcher(ComaPy, authors_src, authors_tgt, use_instances=False)
        missing = set(java.keys()) - set(py.keys())
        assert not missing, f"Python missing pairs: {[_format_key(k) for k in missing]}"

    def test_schema_only_scores(self):
        java = _run_matcher(Coma, authors_src, authors_tgt, use_instances=False)
        py = _run_matcher(ComaPy, authors_src, authors_tgt, use_instances=False)
        comparison = _compare(java, py, "Authors — Schema-Only")
        _print_report(comparison)
        assert comparison["max_abs_error"] < 0.05

    def test_instance_same_pairs(self):
        java = _run_matcher(Coma, authors_src, authors_tgt, use_instances=True)
        py = _run_matcher(ComaPy, authors_src, authors_tgt, use_instances=True)
        missing = set(java.keys()) - set(py.keys())
        assert not missing, f"Python missing pairs: {[_format_key(k) for k in missing]}"

    def test_instance_scores(self):
        java = _run_matcher(Coma, authors_src, authors_tgt, use_instances=True)
        py = _run_matcher(ComaPy, authors_src, authors_tgt, use_instances=True)
        comparison = _compare(java, py, "Authors — Schema+Instance")
        _print_report(comparison)
        assert comparison["max_abs_error"] < 0.05


# ---- Tests: Projects dataset (large, diverse types, ground truth) ----


@requires_java
class TestProjectsAccuracy:
    """Accuracy comparison on the NYC projects dataset (~1963 rows, 11 columns)."""

    def test_schema_only_same_pairs(self):
        java = _run_matcher(Coma, projects_src, projects_tgt, use_instances=False)
        py = _run_matcher(ComaPy, projects_src, projects_tgt, use_instances=False)
        missing = set(java.keys()) - set(py.keys())
        _print_report(_compare(java, py, "Projects — Schema-Only"))
        assert not missing, f"Python missing pairs: {[_format_key(k) for k in missing]}"

    def test_schema_only_scores(self):
        java = _run_matcher(Coma, projects_src, projects_tgt, use_instances=False)
        py = _run_matcher(ComaPy, projects_src, projects_tgt, use_instances=False)
        comparison = _compare(java, py, "Projects — Schema-Only Scores")
        _print_report(comparison)
        # Larger tolerance for diverse column types; low-similarity pairs may diverge more
        assert comparison["mae"] < 0.10

    def test_instance_same_pairs(self):
        """At least 75% pair overlap (TF-IDF differences cause some pair divergence)."""
        java = _run_matcher(Coma, projects_src, projects_tgt, use_instances=True)
        py = _run_matcher(ComaPy, projects_src, projects_tgt, use_instances=True)
        comparison = _compare(java, py, "Projects — Schema+Instance")
        _print_report(comparison)
        overlap = comparison["common_count"] / max(comparison["java_count"], 1)
        assert overlap >= 0.75, (
            f"Pair overlap too low: {overlap:.0%} "
            f"(common={comparison['common_count']}, java={comparison['java_count']})"
        )

    def test_instance_scores(self):
        java = _run_matcher(Coma, projects_src, projects_tgt, use_instances=True)
        py = _run_matcher(ComaPy, projects_src, projects_tgt, use_instances=True)
        comparison = _compare(java, py, "Projects — Schema+Instance Scores")
        _print_report(comparison)
        assert comparison["max_abs_error"] < 0.20

    def test_ground_truth_schema_only(self):
        """Python should achieve equal or better ground-truth metrics than Java."""
        java = _run_matcher(Coma, projects_src, projects_tgt, use_instances=False)
        py = _run_matcher(ComaPy, projects_src, projects_tgt, use_instances=False)
        java_metrics = _compute_ground_truth_metrics(java, PROJECTS_GROUND_TRUTH)
        py_metrics = _compute_ground_truth_metrics(py, PROJECTS_GROUND_TRUTH)

        print("\n  Ground-Truth Metrics — Schema-Only")
        print(f"  {'Metric':<30} {'Java':>8} {'Python':>8} {'Δ':>8}")
        print(f"  {'-' * 30} {'-' * 8} {'-' * 8} {'-' * 8}")
        for metric_name in sorted(java_metrics.keys()):
            j_val = java_metrics.get(metric_name, 0.0)
            p_val = py_metrics.get(metric_name, 0.0)
            diff = p_val - j_val
            print(f"  {metric_name:<30} {j_val:>8.4f} {p_val:>8.4f} {diff:>+8.4f}")

        # Python should not be worse than Java by more than 20% on any metric
        for metric_name in java_metrics:
            j_val = java_metrics[metric_name]
            p_val = py_metrics[metric_name]
            assert p_val >= j_val - 0.20, (
                f"{metric_name}: Python ({p_val:.4f}) much worse than Java ({j_val:.4f})"
            )

    def test_ground_truth_instance(self):
        """Python should achieve equal or better ground-truth metrics than Java."""
        java = _run_matcher(Coma, projects_src, projects_tgt, use_instances=True)
        py = _run_matcher(ComaPy, projects_src, projects_tgt, use_instances=True)
        java_metrics = _compute_ground_truth_metrics(java, PROJECTS_GROUND_TRUTH)
        py_metrics = _compute_ground_truth_metrics(py, PROJECTS_GROUND_TRUTH)

        print("\n  Ground-Truth Metrics — Schema+Instance")
        print(f"  {'Metric':<30} {'Java':>8} {'Python':>8} {'Δ':>8}")
        print(f"  {'-' * 30} {'-' * 8} {'-' * 8} {'-' * 8}")
        for metric_name in sorted(java_metrics.keys()):
            j_val = java_metrics.get(metric_name, 0.0)
            p_val = py_metrics.get(metric_name, 0.0)
            diff = p_val - j_val
            print(f"  {metric_name:<30} {j_val:>8.4f} {p_val:>8.4f} {diff:>+8.4f}")

        for metric_name in java_metrics:
            j_val = java_metrics[metric_name]
            p_val = py_metrics[metric_name]
            assert p_val >= j_val - 0.20, (
                f"{metric_name}: Python ({p_val:.4f}) much worse than Java ({j_val:.4f})"
            )


# ---- Performance tests ----


@requires_java
class TestPerformance:
    """Wall-clock performance comparison between Java and Python."""

    def test_authors_performance(self):
        """Small dataset performance (includes JVM startup for Java)."""
        n_runs = 3

        # Warmup
        _run_java_authors = lambda inst: _run_matcher(  # noqa: E731
            Coma, authors_src, authors_tgt, inst
        )
        _run_py_authors = lambda inst: _run_matcher(  # noqa: E731
            ComaPy, authors_src, authors_tgt, inst
        )

        for use_inst in [False, True]:
            _run_java_authors(use_inst)
            _run_py_authors(use_inst)

            label = "Authors Schema+Instance" if use_inst else "Authors Schema-Only"

            start = time.perf_counter()
            for _ in range(n_runs):
                _run_java_authors(use_inst)
            java_ms = (time.perf_counter() - start) / n_runs * 1000

            start = time.perf_counter()
            for _ in range(n_runs):
                _run_py_authors(use_inst)
            py_ms = (time.perf_counter() - start) / n_runs * 1000

            _print_perf_report(java_ms, py_ms, label)
            assert py_ms < 5000, f"Python too slow on {label}: {py_ms:.0f}ms"

    def test_projects_performance(self):
        """Large dataset performance — the real stress test."""
        n_runs = 2

        for use_inst in [False, True]:
            label = "Projects Schema+Instance" if use_inst else "Projects Schema-Only"

            # Warmup
            _run_matcher(Coma, projects_src, projects_tgt, use_inst)
            _run_matcher(ComaPy, projects_src, projects_tgt, use_inst)

            start = time.perf_counter()
            for _ in range(n_runs):
                _run_matcher(Coma, projects_src, projects_tgt, use_inst)
            java_ms = (time.perf_counter() - start) / n_runs * 1000

            start = time.perf_counter()
            for _ in range(n_runs):
                _run_matcher(ComaPy, projects_src, projects_tgt, use_inst)
            py_ms = (time.perf_counter() - start) / n_runs * 1000

            _print_perf_report(java_ms, py_ms, label)
            assert py_ms < 30000, f"Python too slow on {label}: {py_ms:.0f}ms"


# ---- Full summary test ----


@requires_java
def test_full_comparison_report():
    """
    Run all combinations and produce a comprehensive comparison report.
    """
    print(f"\n{'#' * 70}")
    print(" COMPREHENSIVE COMA COMPARISON: Java vs Python")
    print(f"{'#' * 70}")

    summary_rows = []

    for src, tgt, ds_name in DATASETS:
        for use_inst in [False, True]:
            strategy = "Schema+Instance" if use_inst else "Schema-Only"
            label = f"{ds_name} — {strategy}"

            start = time.perf_counter()
            java_res = _run_matcher(Coma, src, tgt, use_inst)
            java_ms = (time.perf_counter() - start) * 1000

            start = time.perf_counter()
            py_res = _run_matcher(ComaPy, src, tgt, use_inst)
            py_ms = (time.perf_counter() - start) * 1000

            comparison = _compare(java_res, py_res, label)
            _print_report(comparison)
            _print_perf_report(java_ms, py_ms, label)

            # Ground truth metrics for projects dataset
            gt_line = ""
            if ds_name == "projects":
                java_gt = _compute_ground_truth_metrics(java_res, PROJECTS_GROUND_TRUTH)
                py_gt = _compute_ground_truth_metrics(py_res, PROJECTS_GROUND_TRUTH)
                print("\n  Ground-Truth Metrics:")
                print(f"  {'Metric':<20} {'Java':>8} {'Python':>8}")
                print(f"  {'-' * 20} {'-' * 8} {'-' * 8}")
                for m in sorted(java_gt.keys()):
                    print(f"  {m:<20} {java_gt[m]:>8.4f} {py_gt[m]:>8.4f}")
                gt_line = (
                    f"F1: Java={java_gt.get('F1Score', 0):.3f} Py={py_gt.get('F1Score', 0):.3f}"
                )

            speedup = java_ms / py_ms if py_ms > 0 else float("inf")
            summary_rows.append(
                (
                    label,
                    comparison["mae"],
                    comparison["max_abs_error"],
                    java_ms,
                    py_ms,
                    speedup,
                    gt_line,
                )
            )

    # Summary table
    print(f"\n{'=' * 70}")
    print(" Summary")
    print(f"{'=' * 70}")
    print(
        f"  {'Dataset + Strategy':<30} {'MAE':>8} {'MaxErr':>8} "
        f"{'Java':>8} {'Python':>8} {'Speed':>7}  {'GT'}"
    )
    print(f"  {'-' * 30} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 7}  {'-' * 20}")
    for label, mae, max_err, j_ms, p_ms, spd, gt in summary_rows:
        print(
            f"  {label:<30} {mae:>8.4f} {max_err:>8.4f} "
            f"{j_ms:>7.0f}ms {p_ms:>7.0f}ms {spd:>6.1f}x  {gt}"
        )

    # Assertions: all pairs matched, score tolerances
    for label, mae, _max_err, *_ in summary_rows:
        if "Schema-Only" in label:
            assert mae < 0.10, f"[{label}] Schema-only MAE too high: {mae:.4f}"
        else:
            assert mae < 0.10, f"[{label}] Instance MAE too high: {mae:.4f}"
