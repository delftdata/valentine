---
icon: lucide/code
---

# Full example

Below is an end-to-end walkthrough of matching two DataFrames with
[`Coma`](api.md#coma), inspecting the per-sub-matcher score breakdown,
filtering to one-to-one matches, and evaluating the result against a
ground truth. Every API touched here is documented in the
[API reference](api.md).

!!! note

    The same script lives in the repo at
    [`examples/valentine_example.py`][source].

  [source]: https://github.com/delftdata/valentine/blob/master/examples/valentine_example.py

```python title="valentine_example.py"
import pprint
from pathlib import Path

import pandas as pd

from valentine import valentine_match
from valentine.algorithms import Coma
from valentine.metrics import F1Score, PrecisionTopNPercent

pp = pprint.PrettyPrinter(indent=4, sort_dicts=False)


def main():
    # 1. Load two DataFrames you'd like to match.
    d1_path = Path("data") / "source_candidates.csv"
    d2_path = Path("data") / "target_candidates.csv"
    df1 = pd.read_csv(d1_path)
    df2 = pd.read_csv(d2_path)

    # 2. Pick a matcher and run it. `valentine_match` accepts any iterable
    #    of DataFrames and returns a `MatcherResults` mapping.
    matcher = Coma(use_instances=True)
    matches = valentine_match([df1, df2], matcher)

    # 3. Iterate results using ColumnPair named fields.
    print("Found the following matches:")
    for pair, score in matches.items():
        print(f"  {pair.source_column:>20s} <-> {pair.target_column:<20s}  {score:.4f}")

        # Coma provides per-sub-matcher score breakdowns via .details
        details = matches.get_details(pair)
        if details:
            breakdown = ", ".join(f"{k}={v:.3f}" for k, v in details.items())
            print(f"  {'':>20s}      [{breakdown}]")

    # 4. Reduce to one-to-one matches (greedy, highest-first).
    print("\nGetting the one-to-one matches:")
    pp.pprint(matches.one_to_one())

    # 5. If you have a ground truth, compute evaluation metrics.
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

    print("\nDefault metric scores for the matcher:")
    pp.pprint(metrics)

    # 6. You can also pick exactly which metrics to compute.
    print("\nA custom subset of metrics:")
    pp.pprint(
        matches.get_metrics(
            ground_truth,
            metrics={PrecisionTopNPercent(n=80), F1Score()},
        )
    )


if __name__ == "__main__":
    main()
```

## What the output looks like

Running the script on the example data produces something like:

```text
Found the following matches:
          annual_salary <-> compensation          0.8321
                          [NameCM=0.21, PathCM=0.21, LeavesCM=0.19, ParentsCM=0.42, InstancesCM=0.94]
                  dept <-> department            0.7984
                          [NameCM=0.45, PathCM=0.45, LeavesCM=0.31, ParentsCM=0.42, InstancesCM=0.88]
                 fname <-> first_name            0.7551
                          [NameCM=0.22, PathCM=0.22, LeavesCM=0.19, ParentsCM=0.42, InstancesCM=0.93]
                    ...

Default metric scores for the matcher:
{   'Precision': 0.8571,
    'Recall':    0.8571,
    'F1Score':   0.8571,
    'PrecisionTop10Percent':     1.0,
    'RecallAtSizeofGroundTruth': 0.8571}
```

Exact numbers depend on the input data and the random state of any
sampling step, but the shape is the same.
