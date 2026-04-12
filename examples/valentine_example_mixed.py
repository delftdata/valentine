"""Example: matching a pandas DataFrame against a Polars DataFrame.

Valentine auto-detects the frame type and wraps each one in the
appropriate data source (DataframeTable or PolarsTable). You can freely
mix pandas and Polars frames in the same valentine_match call.
"""

import pprint
from pathlib import Path

import pandas as pd
import polars as pl

from valentine import valentine_match
from valentine.algorithms import Coma
from valentine.metrics import F1Score, PrecisionTopNPercent

pp = pprint.PrettyPrinter(indent=4, sort_dicts=False)


def main():
    # Load the source with pandas, the target with Polars
    d1_path = Path("data") / "source_candidates.csv"
    d2_path = Path("data") / "target_candidates.csv"
    df_pandas = pd.read_csv(d1_path)
    df_polars = pl.read_csv(d2_path)

    print(f"Source: pandas DataFrame  {df_pandas.shape}")
    print(f"Target: Polars DataFrame  {df_polars.shape}")

    # valentine_match handles the mixed input transparently
    matcher = Coma(use_instances=True)
    matches = valentine_match([df_pandas, df_polars], matcher)

    print("\nFound the following matches:")
    for pair, score in matches.items():
        print(f"  {pair.source_column:>20s} <-> {pair.target_column:<20s}  {score:.4f}")

    print("\nOne-to-one matches:")
    for pair, score in matches.one_to_one().items():
        print(f"  {pair.source_column:>20s} <-> {pair.target_column:<20s}  {score:.4f}")

    # Evaluate against ground truth
    ground_truth = [
        ("emp_id", "employee_number"),
        ("fname", "first_name"),
        ("lname", "last_name"),
        ("dept", "department"),
        ("annual_salary", "compensation"),
        ("hire_date", "start_date"),
        ("office_loc", "work_location"),
    ]

    metrics = matches.get_metrics(ground_truth, metrics={F1Score(), PrecisionTopNPercent(n=80)})
    print("\nMetrics:")
    pp.pprint(metrics)


if __name__ == "__main__":
    main()
