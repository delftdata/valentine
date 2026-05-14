"""NYU experiment runner for valentine v1.0.0 — wall-clock timing + accuracy.

Per-dataset timeout of 120s prevents hanging on large datasets.

Usage:
    python -u experiments/nyu_v100.py
"""
from __future__ import annotations

import importlib.util
import json
import statistics
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)

from valentine import valentine_match
from valentine.algorithms import (
    Coma,
    Cupid,
    DistributionBased,
    JaccardDistanceMatcher,
    SimilarityFlooding,
)
from valentine.metrics import F1Score, RecallAtSizeofGroundTruth

DATASET_TIMEOUT = 120  # seconds per dataset per matcher

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_datasets(data_root: Path):
    datasets = []
    for entry in sorted(data_root.iterdir()):
        if not entry.is_dir():
            continue
        src_path = entry / "source_table.csv"
        tgt_path = entry / "target_table.csv"
        if not (src_path.exists() and tgt_path.exists()):
            continue
        src = pd.read_csv(src_path)
        tgt = pd.read_csv(tgt_path)
        gt = []
        gt_path = entry / "ground_truth.json"
        if gt_path.exists():
            data = json.loads(gt_path.read_text(encoding="utf-8"))
            for match in data.get("matches", []):
                sc = match.get("source_column")
                tc = match.get("target_column")
                if sc and tc:
                    gt.append((sc, tc))
        datasets.append((entry.name, src, tgt, gt))
    return datasets


# ---------------------------------------------------------------------------
# MRR
# ---------------------------------------------------------------------------

def _mrr(matches, ground_truth):
    rank_map: dict[tuple[str, str], int] = {}
    for rank, key in enumerate(matches.keys(), start=1):
        src_col, tgt_col = key.source_column, key.target_column
        for pair in [(src_col, tgt_col), (tgt_col, src_col)]:
            rank_map.setdefault(pair, rank)
    rr = [1.0 / rank_map[(sc, tc)] if (sc, tc) in rank_map
          else (1.0 / rank_map[(tc, sc)] if (tc, sc) in rank_map else 0.0)
          for sc, tc in ground_truth]
    return round(statistics.mean(rr), 4) if rr else 0.0


_METRICS = {F1Score(), RecallAtSizeofGroundTruth()}


def _run_one(src, tgt, matcher):
    """Runs a single match — called inside a thread so it can be timed out."""
    return valentine_match([src, tgt], matcher)


# ---------------------------------------------------------------------------
# Matchers
# ---------------------------------------------------------------------------

def _build_matchers():
    matchers = [
        ("Coma",                 lambda: Coma(use_instances=False)),
        ("Coma_Inst",            lambda: Coma(use_instances=True)),
        ("Cupid",                Cupid),
        ("DistributionBased",    DistributionBased),
        ("JaccardDistanceMatcher", JaccardDistanceMatcher),
        ("SimilarityFlooding",   SimilarityFlooding),
    ]
    if importlib.util.find_spec("sentence_transformers") is not None:
        from valentine.algorithms.jaccard_distance import StringDistanceFunction
        matchers.append((
            "JaccardDistanceMatcher_emb",
            lambda: JaccardDistanceMatcher(
                distance_fun=StringDistanceFunction.Embedding,
                threshold_dist=0.7,
                embedding_device=None,
            ),
        ))
    return matchers


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    data_root = Path(__file__).resolve().parent / "data"
    if len(sys.argv) > 1:
        data_root = Path(sys.argv[1])
    datasets = load_datasets(data_root)
    print(f"Found {len(datasets)} datasets", flush=True)

    all_results: dict = {}

    for matcher_name, factory in _build_matchers():
        print(f"\n{'='*60}", flush=True)
        print(f"  {matcher_name}", flush=True)
        print(f"{'='*60}", flush=True)
        per_dataset: list[dict] = []
        matcher_total = 0.0

        for ds_name, src, tgt, gt in datasets:
            matcher = factory()
            t0 = time.perf_counter()
            try:
                ex = ThreadPoolExecutor(max_workers=1)
                future = ex.submit(_run_one, src, tgt, matcher)
                try:
                    matches = future.result(timeout=DATASET_TIMEOUT)
                finally:
                    ex.shutdown(wait=False)

                elapsed = time.perf_counter() - t0
                matcher_total += elapsed

                if gt:
                    raw = matches.get_metrics(gt, metrics=_METRICS)
                    f1           = round(raw.get("F1Score", 0.0), 4)
                    recall_at_gt = round(raw.get("RecallAtSizeofGroundTruth", 0.0), 4)
                    mrr          = _mrr(matches, gt)
                else:
                    f1 = recall_at_gt = mrr = None

                print(f"  {ds_name[:48]:48s}  {elapsed:6.2f}s  "
                      f"F1={str(f1):6}  recall@gt={str(recall_at_gt):6}  MRR={mrr}",
                      flush=True)
                per_dataset.append({
                    "dataset": ds_name, "seconds": round(elapsed, 4),
                    "n_src_cols": len(src.columns), "n_tgt_cols": len(tgt.columns),
                    "n_matches": len(matches),
                    "f1": f1, "recall_at_gt": recall_at_gt, "mrr": mrr,
                })

            except FuturesTimeout:
                elapsed = time.perf_counter() - t0
                matcher_total += elapsed
                print(f"  {ds_name[:48]:48s}  TIMEOUT (>{DATASET_TIMEOUT}s)", flush=True)
                per_dataset.append({"dataset": ds_name, "seconds": round(elapsed, 4), "error": "TIMEOUT"})

            except Exception as exc:
                elapsed = time.perf_counter() - t0
                matcher_total += elapsed
                print(f"  {ds_name[:48]:48s}  {elapsed:6.2f}s  ERROR: {exc}", flush=True)
                per_dataset.append({"dataset": ds_name, "seconds": round(elapsed, 4), "error": str(exc)})

        f1s  = [r["f1"]           for r in per_dataset if isinstance(r.get("f1"),           float)]
        recs = [r["recall_at_gt"] for r in per_dataset if isinstance(r.get("recall_at_gt"), float)]
        mrrs = [r["mrr"]          for r in per_dataset if isinstance(r.get("mrr"),          float)]
        mean_f1  = round(statistics.mean(f1s),  4) if f1s  else None
        mean_rec = round(statistics.mean(recs),  4) if recs else None
        mean_mrr = round(statistics.mean(mrrs),  4) if mrrs else None
        print(f"\n  Total: {matcher_total:.2f}s  mean F1={mean_f1}  mean recall@gt={mean_rec}  mean MRR={mean_mrr}", flush=True)

        all_results[matcher_name] = {
            "datasets": per_dataset,
            "total_seconds": round(matcher_total, 4),
            "mean_f1": mean_f1,
            "mean_recall_at_gt": mean_rec,
            "mean_mrr": mean_mrr,
        }

    # Summary
    print(f"\n{'='*60}", flush=True)
    print("SUMMARY", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  {'Matcher':<28} {'Total(s)':>9} {'mean F1':>9} {'recall@gt':>10} {'mean MRR':>10}", flush=True)
    print(f"  {'-'*68}", flush=True)
    for name, r in all_results.items():
        print(f"  {name:<28} {r['total_seconds']:>9.2f} {str(r['mean_f1']):>9} {str(r['mean_recall_at_gt']):>10} {str(r['mean_mrr']):>10}", flush=True)

    out = Path("nyu_v100_results.json")
    out.write_text(json.dumps(all_results, indent=2), encoding="utf-8")
    print(f"\nWrote {out}", flush=True)


if __name__ == "__main__":
    main()
