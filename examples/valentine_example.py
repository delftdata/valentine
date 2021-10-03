import os
import pandas as pd
from valentine import valentine_match, valentine_metrics
from valentine.algorithms import Coma


def main():
    # Load data using pandas
    d1_path = os.path.join('data', 'tiny_authors_1_joinable_vertical_co30_target.csv')
    d2_path = os.path.join('data', 'tiny_authors_1_joinable_vertical_co30_source.csv')
    df1 = pd.read_csv(d1_path)
    df2 = pd.read_csv(d2_path)

    # Instantiate matcher and run
    matcher = Coma(strategy="COMA_OPT")
    matches = valentine_match(df1, df2, matcher)

    print(matches)

    # If golden standard available valentine could calculate the metrics
    golden_standard = [('Cited by', 'Cited by'),
                       ('Authors', 'Authors'),
                       ('EID', 'EID')]

    metrics = valentine_metrics.all_metrics(matches, golden_standard)

    print(metrics)


if __name__ == '__main__':
    main()
