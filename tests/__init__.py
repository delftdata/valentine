import os
import pandas as pd

# Load the data for the tests
script_dir = os.path.dirname(__file__)
d1_path = os.path.join('data', 'tiny_authors_1_joinable_vertical_co30_target.csv')
d2_path = os.path.join('data', 'tiny_authors_1_joinable_vertical_co30_source.csv')
df1 = pd.read_csv(d1_path)
df2 = pd.read_csv(d2_path)
