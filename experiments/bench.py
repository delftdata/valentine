"""Benchmark harness for Valentine matchers.

Two modes:

* ``--quick`` (default): runs every matcher against five small synthetic
  dataset pairs generated deterministically in-process. Total wall time fits
  in roughly a minute on a laptop and the run requires no external data, so
  it is suitable for a CI regression gate.
* ``--full``: runs against the NYU experiment suite under
  ``experiments/data``. Use this locally to reproduce the numbers in
  ``PERFORMANCE_PLAN.md``.

Usage::

    python experiments/bench.py --quick --output bench_results.json
    python experiments/bench.py --quick --baseline experiments/bench_baseline.json --accuracy-only
    python experiments/bench.py --quick --baseline experiments/bench_baseline.json --threshold 0.30
    python experiments/bench.py --quick --baseline experiments/bench_baseline.json --update-baseline
    python experiments/bench.py --full
    python experiments/bench.py --quick --profile  # writes pyinstrument html per matcher

Optional dependency: ``pyinstrument`` for ``--profile``. Install with
``pip install pyinstrument``.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import polars as pl
except ImportError:
    pl = None

from valentine import valentine_match
from valentine.algorithms import (
    BaseMatcher,
    Coma,
    Cupid,
    DistributionBased,
    JaccardDistanceMatcher,
    SimilarityFlooding,
)
from valentine.metrics import F1Score, MeanReciprocalRank, RecallAtSizeofGroundTruth

try:
    from pyinstrument import Profiler  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dep
    Profiler = None  # type: ignore[assignment,misc]


# ---------------------------------------------------------------------------
# Matcher registry
# ---------------------------------------------------------------------------

MatcherFactory = Callable[[], BaseMatcher]


def _matcher_builders() -> list[tuple[str, MatcherFactory]]:
    return [
        ("Coma", Coma),
        ("Coma_Inst", lambda: Coma(use_instances=True)),
        ("Cupid", Cupid),
        ("DistributionBased", DistributionBased),
        ("JaccardDistanceMatcher", JaccardDistanceMatcher),
        ("SimilarityFlooding", SimilarityFlooding),
    ]


# ---------------------------------------------------------------------------
# Synthetic dataset generator (quick mode)
# ---------------------------------------------------------------------------


@dataclass
class SyntheticPair:
    name: str
    source: pd.DataFrame
    target: pd.DataFrame
    ground_truth: list[tuple[str, str]] = field(default_factory=list)


def _make_synthetic_pairs(seed: int = 20260407) -> list[SyntheticPair]:
    """Build five small dataset pairs that are realistic enough to exercise
    every matcher (string, numeric, and date columns) without taking long.

    The pairs are deterministic for a given seed so the bench is repeatable.
    """
    rng = np.random.default_rng(seed)
    pairs: list[SyntheticPair] = []

    # ---- Pair 1: customers (string-heavy, schema rename) -----------------
    n = 120
    first_names = rng.choice(
        ["Alice", "Bob", "Carol", "Dan", "Eve", "Frank", "Grace", "Heidi"], size=n
    )
    last_names = rng.choice(
        ["Smith", "Jones", "Brown", "Taylor", "Wilson", "Davis", "Miller"], size=n
    )
    cities = rng.choice(["Delft", "Athens", "Berlin", "Paris", "Rome", "Madrid", "Lisbon"], size=n)
    src1 = pd.DataFrame(
        {
            "customer_id": np.arange(n),
            "first_name": first_names,
            "last_name": last_names,
            "city": cities,
            "age": rng.integers(18, 80, size=n),
        }
    )
    tgt1 = pd.DataFrame(
        {
            "CustID": np.arange(n),
            "FirstName": first_names,
            "LastName": last_names,
            "City": cities,
            "Age": rng.integers(18, 80, size=n),
        }
    )
    pairs.append(
        SyntheticPair(
            name="customers",
            source=src1,
            target=tgt1,
            ground_truth=[
                ("customer_id", "CustID"),
                ("first_name", "FirstName"),
                ("last_name", "LastName"),
                ("city", "City"),
                ("age", "Age"),
            ],
        )
    )

    # ---- Pair 2: orders (numeric-heavy) ----------------------------------
    n = 200
    src2 = pd.DataFrame(
        {
            "order_id": np.arange(n),
            "customer_id": rng.integers(0, 50, size=n),
            "amount": rng.normal(100, 25, size=n).round(2),
            "quantity": rng.integers(1, 10, size=n),
            "discount": rng.uniform(0, 0.4, size=n).round(3),
        }
    )
    tgt2 = pd.DataFrame(
        {
            "OrderID": np.arange(n),
            "CustomerID": rng.integers(0, 50, size=n),
            "Total": rng.normal(100, 25, size=n).round(2),
            "Qty": rng.integers(1, 10, size=n),
            "DiscountPct": rng.uniform(0, 0.4, size=n).round(3),
        }
    )
    pairs.append(
        SyntheticPair(
            name="orders",
            source=src2,
            target=tgt2,
            ground_truth=[
                ("order_id", "OrderID"),
                ("customer_id", "CustomerID"),
                ("amount", "Total"),
                ("quantity", "Qty"),
                ("discount", "DiscountPct"),
            ],
        )
    )

    # ---- Pair 3: products (mixed types, more columns) --------------------
    n = 80
    categories = rng.choice(["electronics", "clothing", "food", "books", "toys"], size=n)
    src3 = pd.DataFrame(
        {
            "sku": [f"SKU{i:05d}" for i in range(n)],
            "product_name": rng.choice(
                ["Widget", "Gadget", "Doohickey", "Thingamajig", "Whatsit"], size=n
            ),
            "category": categories,
            "price": rng.uniform(5, 500, size=n).round(2),
            "stock": rng.integers(0, 1000, size=n),
            "weight_kg": rng.uniform(0.1, 20.0, size=n).round(2),
        }
    )
    tgt3 = pd.DataFrame(
        {
            "ProductSKU": [f"SKU{i:05d}" for i in range(n)],
            "ProductName": rng.choice(
                ["Widget", "Gadget", "Doohickey", "Thingamajig", "Whatsit"], size=n
            ),
            "Category": categories,
            "UnitPrice": rng.uniform(5, 500, size=n).round(2),
            "InStock": rng.integers(0, 1000, size=n),
            "WeightKg": rng.uniform(0.1, 20.0, size=n).round(2),
        }
    )
    pairs.append(
        SyntheticPair(
            name="products",
            source=src3,
            target=tgt3,
            ground_truth=[
                ("sku", "ProductSKU"),
                ("product_name", "ProductName"),
                ("category", "Category"),
                ("price", "UnitPrice"),
                ("stock", "InStock"),
                ("weight_kg", "WeightKg"),
            ],
        )
    )

    # ---- Pair 4: events (dates + identifiers) ----------------------------
    n = 150
    base = pd.Timestamp("2026-01-01")
    deltas = rng.integers(0, 365, size=n)
    timestamps = [base + pd.Timedelta(days=int(d)) for d in deltas]
    src4 = pd.DataFrame(
        {
            "event_id": np.arange(n),
            "event_date": [t.strftime("%Y-%m-%d") for t in timestamps],
            "user_id": rng.integers(0, 30, size=n),
            "event_type": rng.choice(["click", "view", "purchase", "share"], size=n),
        }
    )
    tgt4 = pd.DataFrame(
        {
            "EventID": np.arange(n),
            "Date": [t.strftime("%Y-%m-%d") for t in timestamps],
            "UserID": rng.integers(0, 30, size=n),
            "Type": rng.choice(["click", "view", "purchase", "share"], size=n),
        }
    )
    pairs.append(
        SyntheticPair(
            name="events",
            source=src4,
            target=tgt4,
            ground_truth=[
                ("event_id", "EventID"),
                ("event_date", "Date"),
                ("user_id", "UserID"),
                ("event_type", "Type"),
            ],
        )
    )

    # ---- Pair 5: addresses (longer strings, partial overlap) -------------
    n = 100
    streets = rng.choice(
        ["Main St", "Oak Ave", "Pine Rd", "Maple Ln", "Cedar Blvd", "Elm Ct"], size=n
    )
    src5 = pd.DataFrame(
        {
            "address_id": np.arange(n),
            "street_name": streets,
            "house_number": rng.integers(1, 9999, size=n),
            "postal_code": [f"{rng.integers(1000, 9999):04d}AB" for _ in range(n)],
            "country": rng.choice(["NL", "DE", "FR", "GR", "IT"], size=n),
        }
    )
    tgt5 = pd.DataFrame(
        {
            "AddrID": np.arange(n),
            "Street": streets,
            "HouseNo": rng.integers(1, 9999, size=n),
            "Postcode": [f"{rng.integers(1000, 9999):04d}AB" for _ in range(n)],
            "Country": rng.choice(["NL", "DE", "FR", "GR", "IT"], size=n),
        }
    )
    pairs.append(
        SyntheticPair(
            name="addresses",
            source=src5,
            target=tgt5,
            ground_truth=[
                ("address_id", "AddrID"),
                ("street_name", "Street"),
                ("house_number", "HouseNo"),
                ("postal_code", "Postcode"),
                ("country", "Country"),
            ],
        )
    )

    return pairs


# ---------------------------------------------------------------------------
# NYU full-suite loader (full mode)
# ---------------------------------------------------------------------------


def _load_nyu_pairs(data_root: Path) -> list[SyntheticPair]:
    pairs: list[SyntheticPair] = []
    for entry in sorted(data_root.iterdir()):
        if not entry.is_dir():
            continue
        source_path = entry / "source_table.csv"
        target_path = entry / "target_table.csv"
        if not (source_path.exists() and target_path.exists()):
            continue
        source = pd.read_csv(source_path)
        target = pd.read_csv(target_path)
        ground_truth: list[tuple[str, str]] = []
        gt_path = entry / "ground_truth.json"
        if gt_path.exists():
            data = json.loads(gt_path.read_text(encoding="utf-8"))
            for match in data.get("matches", []):
                src_col = match.get("source_column")
                tgt_col = match.get("target_column")
                if src_col is not None and tgt_col is not None:
                    ground_truth.append((src_col, tgt_col))
        pairs.append(
            SyntheticPair(
                name=entry.name,
                source=source,
                target=target,
                ground_truth=ground_truth,
            )
        )
    if not pairs:
        raise SystemExit(f"No datasets found under {data_root}")
    return pairs


def _convert_pairs_to_polars(pairs: list[SyntheticPair]) -> list[SyntheticPair]:
    """Convert all pandas DataFrames in *pairs* to Polars DataFrames."""
    if pl is None:
        raise SystemExit("polars is not installed; cannot use --polars")
    return [
        SyntheticPair(
            name=p.name,
            source=pl.from_pandas(p.source),
            target=pl.from_pandas(p.target),
            ground_truth=p.ground_truth,
        )
        for p in pairs
    ]


# ---------------------------------------------------------------------------
# Bench runner
# ---------------------------------------------------------------------------


_BENCH_METRICS = {F1Score(), RecallAtSizeofGroundTruth(), MeanReciprocalRank()}


def _compute_metrics(matches, ground_truth: list[tuple[str, str]]) -> dict[str, float | None]:
    if not ground_truth:
        return {"f1": None, "recall_at_gt": None, "mrr": None}
    raw = matches.get_metrics(ground_truth, metrics=_BENCH_METRICS)
    return {
        "f1": round(raw.get("F1Score", 0.0), 4),
        "recall_at_gt": round(raw.get("RecallAtSizeofGroundTruth", 0.0), 4),
        "mrr": round(raw.get("MeanReciprocalRank", 0.0), 4),
    }


def _run_bench(
    pairs: list[SyntheticPair],
    *,
    profile_dir: Path | None = None,
) -> dict:
    builders = _matcher_builders()
    per_matcher: dict[str, dict] = {name: {"pairs": {}} for name, _ in builders}

    for pair in pairs:
        print(
            f"\nDataset: {pair.name} "
            f"({len(pair.source.columns)} src cols, {len(pair.target.columns)} tgt cols)"
        )
        for matcher_name, builder in builders:
            matcher = builder()

            profiler = None
            if profile_dir is not None:
                if Profiler is None:
                    print("  (pyinstrument not installed; skipping profile)")
                else:
                    profiler = Profiler()
                    profiler.start()

            start = time.perf_counter()
            matches = valentine_match([pair.source, pair.target], matcher)
            elapsed = time.perf_counter() - start

            if profiler is not None:
                profiler.stop()
                profile_dir.mkdir(parents=True, exist_ok=True)
                out = profile_dir / f"{pair.name}__{matcher_name}.html"
                out.write_text(profiler.output_html(), encoding="utf-8")

            metrics = _compute_metrics(matches, pair.ground_truth)
            per_matcher[matcher_name]["pairs"][pair.name] = {
                "seconds": round(elapsed, 4),
                "n_matches": len(matches),
                **metrics,
            }
            f1_str = "n/a" if metrics["f1"] is None else f"{metrics['f1']:.3f}"
            mrr_str = "n/a" if metrics["mrr"] is None else f"{metrics['mrr']:.3f}"
            print(f"  {matcher_name:25s} {elapsed:7.2f}s  F1={f1_str}  MRR={mrr_str}")

    # Aggregate
    for entry in per_matcher.values():
        seconds = [p["seconds"] for p in entry["pairs"].values()]
        f1s = [p["f1"] for p in entry["pairs"].values() if p["f1"] is not None]
        mrrs = [p["mrr"] for p in entry["pairs"].values() if p["mrr"] is not None]
        entry["total_seconds"] = round(sum(seconds), 4)
        entry["worst_seconds"] = round(max(seconds), 4)
        entry["mean_f1"] = round(statistics.fmean(f1s), 4) if f1s else None
        entry["mean_mrr"] = round(statistics.fmean(mrrs), 4) if mrrs else None

    return {"matchers": per_matcher}


# ---------------------------------------------------------------------------
# Baseline comparison
# ---------------------------------------------------------------------------


def _compare_accuracy(
    results: dict, baseline: dict, *, f1_tolerance: float = 0.0001
) -> tuple[bool, list[str]]:
    """Check F1 scores and match counts against the baseline.

    These are deterministic for a given synthetic seed, so any change
    (improvement or regression) indicates a behavioural change that
    should be acknowledged by updating the baseline.

    Returns ``(changed, lines)``.
    """
    lines: list[str] = []
    changed = False
    for matcher_name, current in results["matchers"].items():
        base = baseline.get("matchers", {}).get(matcher_name)
        if base is None:
            lines.append(f"  {matcher_name}: no baseline entry, skipping")
            continue

        # Per-pair accuracy checks
        for pair_name, cur_pair in current["pairs"].items():
            base_pair = base.get("pairs", {}).get(pair_name)
            if base_pair is None:
                continue

            for metric_key in ("f1", "recall_at_gt", "mrr"):
                cur_val = cur_pair.get(metric_key)
                base_val = base_pair.get(metric_key)
                if (
                    cur_val is not None
                    and base_val is not None
                    and abs(cur_val - base_val) > f1_tolerance
                ):
                    delta = cur_val - base_val
                    tag = "IMPROVED" if delta > 0 else "REGRESSION"
                    lines.append(
                        f"  {matcher_name}/{pair_name}: "
                        f"{metric_key} {base_val:.4f} -> {cur_val:.4f} "
                        f"({delta:+.4f})  [{tag}]"
                    )
                    changed = True

            cur_n = cur_pair.get("n_matches")
            base_n = base_pair.get("n_matches")
            if cur_n is not None and base_n is not None and cur_n != base_n:
                lines.append(f"  {matcher_name}/{pair_name}: match count {base_n} -> {cur_n}")
                changed = True

        # Aggregate F1 summary
        cur_f1 = current.get("mean_f1")
        base_f1 = base.get("mean_f1")
        if cur_f1 is not None and base_f1 is not None:
            status = "OK" if abs(cur_f1 - base_f1) <= f1_tolerance else "CHANGED"
            lines.append(f"  {matcher_name:25s} mean F1: {base_f1:.4f} -> {cur_f1:.4f}  [{status}]")

    return changed, lines


def _compare_timing(results: dict, baseline: dict, threshold: float) -> tuple[bool, list[str]]:
    """Compare timing against the baseline (informational).

    Returns ``(regressed, lines)``.  Timing regressions are flagged but
    are inherently noisy on CI runners, so callers may choose to treat
    this as advisory rather than blocking.
    """
    lines: list[str] = []
    regressed = False
    for matcher_name, current in results["matchers"].items():
        base = baseline.get("matchers", {}).get(matcher_name)
        if base is None:
            continue
        cur_t = current["total_seconds"]
        base_t = base["total_seconds"]
        delta = (cur_t - base_t) / base_t if base_t > 0 else 0.0
        marker = ""
        if delta > threshold:
            marker = "  REGRESSION"
            regressed = True
        elif delta < -0.05:
            marker = "  (faster)"
        lines.append(
            f"  {matcher_name:25s} {cur_t:7.2f}s vs baseline {base_t:7.2f}s ({delta:+.1%}){marker}"
        )
    return regressed, lines


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--quick",
        action="store_true",
        help="Run the small synthetic suite (default).",
    )
    mode.add_argument(
        "--full",
        action="store_true",
        help="Run the NYU experiment suite under experiments/data.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON results to this path.",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        help="Compare against a previously written baseline JSON file.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.30,
        help="Flag timing regressions beyond this fraction. Default 0.30 (30%%).",
    )
    parser.add_argument(
        "--accuracy-only",
        action="store_true",
        help="Only check F1 and match-count accuracy against the baseline "
        "(ignore timing). Suitable for CI where runner speed varies.",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Overwrite the baseline file with the current results. "
        "Use after intentional accuracy changes (improvements or refactors).",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Write a pyinstrument HTML profile per matcher to experiments/profiles/.",
    )
    parser.add_argument(
        "--polars",
        action="store_true",
        help="Convert all input DataFrames to Polars before matching.",
    )
    args = parser.parse_args()

    if args.full:
        data_root = Path(__file__).resolve().parent / "data"
        pairs = _load_nyu_pairs(data_root)
    else:
        pairs = _make_synthetic_pairs()

    if args.polars:
        pairs = _convert_pairs_to_polars(pairs)
        print("Using Polars DataFrames\n")

    profile_dir = None
    if args.profile:
        profile_dir = Path(__file__).resolve().parent / "profiles"

    results = _run_bench(pairs, profile_dir=profile_dir)

    print("\nTotals:")
    for matcher_name, data in results["matchers"].items():
        f1 = data["mean_f1"]
        mrr = data.get("mean_mrr")
        f1_str = "n/a" if f1 is None else f"{f1:.3f}"
        mrr_str = "n/a" if mrr is None else f"{mrr:.3f}"
        print(
            f"  {matcher_name:25s} total {data['total_seconds']:7.2f}s "
            f"worst {data['worst_seconds']:6.2f}s  mean F1={f1_str}  mean MRR={mrr_str}"
        )

    if args.output:
        args.output.write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"\nWrote {args.output}")

    if args.update_baseline:
        if not args.baseline:
            print("\n--update-baseline requires --baseline <path>.")
            return 1
        args.baseline.write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"\nBaseline updated: {args.baseline}")
        return 0

    if args.baseline:
        return _check_baseline(results, args)

    return 0


def _check_baseline(results: dict, args: argparse.Namespace) -> int:
    if not args.baseline.exists():
        print(f"\nBaseline {args.baseline} does not exist; skipping comparison.")
        return 0
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))

    # Accuracy check (deterministic — always strict)
    print(f"\nAccuracy comparison against {args.baseline}:")
    acc_changed, acc_lines = _compare_accuracy(results, baseline)
    for line in acc_lines:
        print(line)
    if acc_changed:
        print(
            "\nFAIL: accuracy changed vs baseline. If this is intentional, run:\n"
            "  python experiments/bench.py --quick "
            "--baseline experiments/bench_baseline.json --update-baseline"
        )
        return 1
    print("OK: accuracy matches baseline.")

    # Timing check (noisy — informational unless --accuracy-only)
    if not args.accuracy_only:
        print(f"\nTiming comparison (threshold {args.threshold:.0%}):")
        time_regressed, time_lines = _compare_timing(results, baseline, args.threshold)
        for line in time_lines:
            print(line)
        if time_regressed:
            print("\nWARN: timing regression detected (may be CI noise).")
            return 1
        print("OK: no timing regressions beyond threshold.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
