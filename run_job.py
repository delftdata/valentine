import argparse
import json
import timeit
import sys
import os
import re

from algorithms.base_matcher import BaseMatcher
from algorithms.coma.coma import Coma
from algorithms.sem_prop.sem_prop_main import SemProp
from data_loader.golden_standard_loader import GoldenStandardLoader
from utils.parse_config import ConfigParser

import data_loader.data_loaders as module_data
import algorithms.algorithms as module_algorithms
import metrics.metrics as module_metric
from utils.utils import get_project_root, create_folder


def write_output(name: str, algorithm_name: str, matches: dict, metrics: dict, run_times: dict):
    """
    Function that writes the output of a schema matching job

    Parameters
    ----------
    name : str
        The experiment unique name
    algorithm_name : str
        The name of the algorithm
    matches : dict
        Dictionary containing the ranked list of matches based on their similarity sorted in descending order
    metrics : dict
        Dictionary containing the metrics calculated in the schema matching job
    run_times : dict
        Dictionary containing the metrics measured in the schema matching job
    """
    create_folder(get_project_root() + "/output")
    create_folder(get_project_root() + "/output/" + algorithm_name)
    with open(get_project_root() + "/output/" + algorithm_name + "/" +
              re.sub('\\W+', '_', str(name)) + ".json", 'w') as fp:
        matches = {str(k): v for k, v in matches.items()}
        output = {"name": name, "matches": matches, "metrics": metrics, "run_times": run_times}
        json.dump(output, fp, indent=2)


def main(config):
    """
    Creates a schema matching job and runs it

    Parameters
    ---------
    config : ConfigParser
        A class containing all of the job's configuration parameters look into parse_config.py for more information.
    """
    sys.path.append(os.getcwd())

    time_start_load = timeit.default_timer()
    # data loader (Schema, Instance, Combined)
    data_loader_source = config.initialize('source', module_data)

    data_loader_target = config.initialize('target', module_data)

    time_start_algorithm = timeit.default_timer()
    # algorithms (Abstracted from BaseMatcher)
    algorithm: BaseMatcher = config.initialize('algorithm', module_algorithms)

    # the result of the algorithm (ranked list of matches based on a similarity metric)
    matches = algorithm.get_matches(data_loader_source, data_loader_target, config['dataset_name'])

    time_end = timeit.default_timer()

    run_times = {"total_time": time_end - time_start_load, "algorithm_time": time_end - time_start_algorithm}

    # Uncomment if you want to see the matches
    # print(matches)

    # the golden standard
    golden_standard = GoldenStandardLoader(config['golden_standard'])

    # load and print the specified metrics
    metric_fns = [getattr(module_metric, met) for met in config['metrics']['names']]

    final_metrics = dict()

    for metric in metric_fns:
        if metric.__name__ != "precision_at_n_percent":
            if metric.__name__ in ['precision', 'recall', 'f1_score'] \
                    and type(algorithm) not in [Coma, SemProp]:  # Do not use the 1-1 match filter on Coma and SemProp
                final_metrics[metric.__name__] = metric(matches, golden_standard, True)
            else:
                final_metrics[metric.__name__] = metric(matches, golden_standard)
        else:
            for n in config['metrics']['args']['n']:
                final_metrics[metric.__name__.replace('_n_', '_' + str(n) + '_')] = metric(matches, golden_standard, n)

    print("Metrics: ", final_metrics)

    write_output(config['name'], config['algorithm']['type'], matches, final_metrics, run_times)


if __name__ == '__main__':
    print("Running job")
    args = argparse.ArgumentParser(description='Schema matching job')
    args.add_argument('-c', '--config', default=None, type=str,
                      help='config file path (default: None)')

    configuration = ConfigParser(args)
    main(configuration)
