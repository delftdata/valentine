import os
import pandas as pd
from valentine import valentine_match, valentine_metrics
from valentine.algorithms import Coma
import pprint


def main():
    # Load data using pandas
    d1_path = os.path.join('data', 'authors1.csv')
    d2_path = os.path.join('data', 'authors2.csv')
    df1 = pd.read_csv(d1_path)
    df2 = pd.read_csv(d2_path)

    # Instantiate matcher and run
    # Coma requires java to be installed on your machine
    # If java is not an option, all the other algorithms are in Python (e.g., Cupid)
    matcher = Coma(use_instances=False)
    matches = valentine_match(df1, df2, matcher)

    # If ground truth available valentine could calculate the metrics
    ground_truth = [('Cited by', 'Cited by'),
                       ('Authors', 'Authors'),
                       ('EID', 'EID')]

    metrics = valentine_metrics.all_metrics(matches, ground_truth)

    pp = pprint.PrettyPrinter(indent=4)
    print("Found the following matches:")
    pp.pprint(matches)

    print("\nAccording to the ground truth:")
    pp.pprint(ground_truth)
    
    print("\nThese are the scores of the matcher:")
    pp.pprint(metrics)



if __name__ == '__main__':
    main()
