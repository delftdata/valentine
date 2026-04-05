import pprint
from pathlib import Path

import pandas as pd

from valentine import valentine_match
from valentine.algorithms import Coma
from valentine.metrics import F1Score, PrecisionTopNPercent

pp = pprint.PrettyPrinter(indent=4, sort_dicts=False)


def main():
    # Load data using pandas
    d1_path = Path("data") / "source_candidates.csv"
    d2_path = Path("data") / "target_candidates.csv"
    df1 = pd.read_csv(d1_path)
    df2 = pd.read_csv(d2_path)

    # Instantiate matcher and run
    matcher = Coma(use_instances=True)
    matches = valentine_match([df1, df2], matcher)

    # MatcherResults is an immutable mapping with convenience methods.
    # Keys are ColumnPair namedtuples with .source_table, .source_column,
    # .target_table, .target_column fields.
    print("Found the following matches:")
    for pair, score in matches.items():
        print(f"  {pair.source_column:>20s} <-> {pair.target_column:<20s}  {score:.4f}")

        # Coma provides per-matcher score breakdowns via .details
        details = matches.get_details(pair)
        if details:
            breakdown = ", ".join(f"{k}={v:.3f}" for k, v in details.items())
            print(f"  {'':>20s}      [{breakdown}]")

    print("\nGetting the one-to-one matches:")
    pp.pprint(matches.one_to_one())

    # If ground truth available valentine could calculate the metrics
    ground_truth = [
        ("emp_id", "employee_number"),
        ("fname", "first_name"),
        ("lname", "last_name"),
        ("dept", "department"),
        ("annual_salary", "compensation"),
        ("hire_date", "start_date"),
        ("office_loc", "work_location"),
    ]

    metrics = matches.get_metrics(ground_truth)

    print("\nAccording to the ground truth:")
    pp.pprint(ground_truth)

    print("\nThese are the scores of the default metrics for the matcher:")
    pp.pprint(metrics)

    print("\nYou can also get specific metric scores:")
    pp.pprint(matches.get_metrics(ground_truth, metrics={PrecisionTopNPercent(n=80), F1Score()}))

    print("\nThe MatcherResults object is a mapping and can be iterated:")
    for pair in matches:
        print(f"{pair.source_column:>20s} <-> {pair.target_column:<20s}  {matches[pair]:.4f}")


if __name__ == "__main__":
    main()
