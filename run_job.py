import argparse
import numpy as np

from algorithms.base_matcher import BaseMatcher
from data_loader.golden_standard_loader import GoldenStandardLoader
from parse_config import ConfigParser

import data_loader.data_loaders as module_data
import algorithms.algorithms as module_algorithms
import algorithms.metrics.metrics as module_metric


def main(config):
    """
        Args:
            config (ConfigParser): A class containing all of the job's configuration parameters look into
            parse_config.py for more information.
        Returns: Creates a schema matching job and runs it
    """

    # data loader (Schema, Instance, Combined)
    data_loader_source = config.initialize('source', module_data)

    data_loader_target = config.initialize('target', module_data)

    # algorithms (Abstracted from BaseMatcher)
    algorithm: BaseMatcher = config.initialize('algorithm', module_algorithms, data_loader_source, data_loader_target)

    # the result of the algorithm (ranked list of matches based on a similarity metric)
    matches = algorithm.get_matches(data_loader_source, data_loader_target)

    # the golden standard
    golden_standard = GoldenStandardLoader(config['golden_standard'])

    # load and print the specified metrics
    metric_fns = [getattr(module_metric, met) for met in config['metrics']]

    total_metrics = np.zeros(len(metric_fns))

    for i, metric in enumerate(metric_fns):
        total_metrics[i] += metric(matches, golden_standard)

    final_metrics = {met.__name__: total_metrics[i].item() for i, met in enumerate(metric_fns)}

    print("Metrics: ", final_metrics)


if __name__ == '__main__':
    args = argparse.ArgumentParser(description='Schema matching job')
    args.add_argument('-c', '--config', default=None, type=str,
                      help='config file path (default: None)')

    configuration = ConfigParser(args)
    main(configuration)
