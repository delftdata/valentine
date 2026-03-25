import os
from pathlib import Path

import pandas as pd

# Load the data for the tests
script_dir = Path(__file__).parent
d1_path = Path(script_dir) / "data" / "authors1.csv"
d2_path = Path(script_dir) / "data" / "authors2.csv"
df1 = pd.read_csv(d1_path)
df2 = pd.read_csv(d2_path)
