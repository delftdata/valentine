from pathlib import Path

import pandas as pd

# Load the data for the tests
script_dir = Path(__file__).parent
d1_path = script_dir / "data" / "source_candidates.csv"
d2_path = script_dir / "data" / "target_candidates.csv"
df1 = pd.read_csv(d1_path)
df2 = pd.read_csv(d2_path)
