"""
One-time script to generate golden files from the Java COMA implementation.

Run this while Java is still available to capture reference results:
    python tests/generate_coma_golden.py
"""

import json
from pathlib import Path

import pandas as pd

from valentine.algorithms import Coma
from valentine.data_sources import DataframeTable

script_dir = Path(__file__).parent
d1_path = script_dir / "data" / "authors1.csv"
d2_path = script_dir / "data" / "authors2.csv"
df1 = pd.read_csv(d1_path)
df2 = pd.read_csv(d2_path)

d1 = DataframeTable(df1, name="authors1")
d2 = DataframeTable(df2, name="authors2")


def serialize_matches(matches):
    """Convert match dict to JSON-serializable format."""
    result = {}
    for ((t1, c1), (t2, c2)), score in matches.items():
        key = f"{t1}|{c1}||{t2}|{c2}"
        result[key] = score
    return result


def main():
    output_dir = script_dir / "data"

    # Schema-only (COMA_OPT)
    print("Running Java COMA schema-only (COMA_OPT)...")
    coma_schema = Coma(use_instances=False)
    matches_schema = coma_schema.get_matches(d1, d2)
    golden_schema = serialize_matches(matches_schema)
    with open(output_dir / "coma_golden_schema_only.json", "w") as f:
        json.dump(golden_schema, f, indent=2, sort_keys=True)
    print(f"  Wrote {len(golden_schema)} matches to coma_golden_schema_only.json")

    # Schema + Instance (COMA_OPT_INST)
    print("Running Java COMA schema+instance (COMA_OPT_INST)...")
    coma_inst = Coma(use_instances=True)
    matches_inst = coma_inst.get_matches(d1, d2)
    golden_inst = serialize_matches(matches_inst)
    with open(output_dir / "coma_golden_schema_instance.json", "w") as f:
        json.dump(golden_inst, f, indent=2, sort_keys=True)
    print(f"  Wrote {len(golden_inst)} matches to coma_golden_schema_instance.json")

    print("Done! Golden files generated.")


if __name__ == "__main__":
    main()
