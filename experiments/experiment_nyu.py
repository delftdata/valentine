import json
import time
from pathlib import Path

import pandas as pd

from valentine import valentine_match
from valentine.algorithms import (
    Coma,
    Cupid,
    DistributionBased,
    JaccardDistanceMatcher,
    SimilarityFlooding,
)


def _iter_datasets(data_root: Path) -> list[Path]:
    datasets = []
    for entry in sorted(data_root.iterdir()):
        if not entry.is_dir():
            continue
        source_path = entry / "source_table.csv"
        target_path = entry / "target_table.csv"
        if source_path.exists() and target_path.exists():
            datasets.append(entry)
    return datasets


def _load_ground_truth(path: Path) -> list[tuple[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    matches = data.get("matches", [])
    ground_truth = []
    for match in matches:
        source_col = match.get("source_column")
        target_col = match.get("target_column")
        if source_col is None or target_col is None:
            continue
        ground_truth.append((source_col, target_col))
    return ground_truth


def _matcher_builders():
    return [
        ("Coma", lambda: Coma(use_instances=True)),
        ("Cupid", Cupid),
        ("DistributionBased", DistributionBased),
        ("JaccardDistanceMatcher", JaccardDistanceMatcher),
        ("SimilarityFlooding", SimilarityFlooding),
    ]


def main():
    data_root = Path(__file__).resolve().parent / "data"
    datasets = _iter_datasets(data_root)
    if not datasets:
        raise SystemExit(f"No datasets found under {data_root}")

    results = []
    failures = []

    for dataset_dir in datasets:
        dataset_name = dataset_dir.name
        source_path = dataset_dir / "source_table.csv"
        target_path = dataset_dir / "target_table.csv"
        ground_truth_path = dataset_dir / "ground_truth.json"

        df_source = pd.read_csv(source_path)
        df_target = pd.read_csv(target_path)
        ground_truth = _load_ground_truth(ground_truth_path)

        print(f"\nDataset: {dataset_name}")
        print(
            f"  Source columns: {len(df_source.columns)} | Target columns: {len(df_target.columns)}"
        )
        print(f"  Ground truth pairs: {len(ground_truth)}")

        for matcher_name, builder in _matcher_builders():
            matcher = builder()
            start = time.perf_counter()
            try:
                matches = valentine_match([df_source, df_target], matcher)
                metrics = matches.get_metrics(ground_truth) if ground_truth else {}
                elapsed = time.perf_counter() - start
                results.append(
                    {
                        "dataset": dataset_name,
                        "matcher": matcher_name,
                        "seconds": round(elapsed, 4),
                        **metrics,
                    }
                )
                print(f"  {matcher_name}: {metrics} (time: {elapsed:.2f}s)")
            except Exception as exc:  # Keep moving to finish all datasets/algorithms
                elapsed = time.perf_counter() - start
                failures.append(
                    {
                        "dataset": dataset_name,
                        "matcher": matcher_name,
                        "seconds": round(elapsed, 4),
                        "error": str(exc),
                    }
                )
                print(f"  {matcher_name}: failed after {elapsed:.2f}s ({exc})")

    if results:
        print("\nSummary (metrics per dataset/algorithm):")
        summary = pd.DataFrame(results)
        print(summary.to_string(index=False))

    if failures:
        print("\nFailures:")
        for failure in failures:
            print(
                f"  {failure['dataset']} | {failure['matcher']} | {failure['seconds']}s | {failure['error']}"
            )


if __name__ == "__main__":
    main()
