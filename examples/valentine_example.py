import os
import pandas as pd
from valentine.metrics import F1Score, PrecisionTopNPercent
from valentine import valentine_match
from valentine.algorithms import JaccardDistanceMatcher
import pprint
pp = pprint.PrettyPrinter(indent=4, sort_dicts=False)


def main():
    # Load data using pandas
    d1_path = os.path.join('data', 'authors1.csv')
    d2_path = os.path.join('data', 'authors2.csv')
    df1 = pd.read_csv(d1_path)
    df2 = pd.read_csv(d2_path)

    # Instantiate matcher and run
    matcher = JaccardDistanceMatcher()
    matches = valentine_match(df1, df2, matcher)

    # MatcherResults is a wrapper object that has several useful
    # utility/transformation functions
    print("Found the following matches:")
    pp.pprint(matches)

    print("\nGetting the one-to-one matches:")
    pp.pprint(matches.one_to_one())

    # If ground truth available valentine could calculate the metrics
    ground_truth = [('Cited by', 'Cited by'),
                    ('Authors', 'Authors'),
                    ('EID', 'EID')]

    metrics = matches.get_metrics(ground_truth)

    print("\nAccording to the ground truth:")
    pp.pprint(ground_truth)

    print("\nThese are the scores of the default metrics for the matcher:")
    pp.pprint(metrics)

    print("\nYou can also get specific metric scores:")
    pp.pprint(matches.get_metrics(ground_truth, metrics={
        PrecisionTopNPercent(n=80),
        F1Score()
    }))

    print("\nThe MatcherResults object is a dict and can be treated such:")
    for match in matches:
        print(f"{str(match): <60} {matches[match]}")


if __name__ == '__main__':
    main()
