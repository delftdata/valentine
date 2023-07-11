import os
import pandas as pd
from valentine import valentine_match
from valentine.algorithms import SimilarityFlooding
import pprint

from valentine.metrics.metrics import get_top_n_columns, get_top_n_columns_for_column


def main():
    # Load data using pandas
    d1_path = os.path.join('data', 'authors1.csv')
    d2_path = os.path.join('data', 'authors2.csv')
    df1 = pd.read_csv(d1_path)
    df2 = pd.read_csv(d2_path)

    # Instantiate matcher and run
    matcher = SimilarityFlooding()
    matches = valentine_match(df1, df2, matcher)

    # Find the top-n columns for all columns in dataframe1 (authors1.csv)
    all_top_2_columns = get_top_n_columns(matches, 2)

    # Find the top-n columns for the column 'Authors' in dataframe1
    authors_top_2_columns = get_top_n_columns_for_column(matches, 2, 'Authors')

    pp = pprint.PrettyPrinter(indent=4)
    print("Found the following matches:")
    pp.pprint(matches)

    print("Top 2 columns for each column:")
    pp.pprint(all_top_2_columns)

    print("Top 2 columns for 'Authors' column in table 1:")
    pp.pprint(authors_top_2_columns)


if __name__ == '__main__':
    main()
