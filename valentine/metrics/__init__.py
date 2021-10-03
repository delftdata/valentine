from valentine.metrics import metrics as metrics_module
from typing import List, Dict, Tuple

metrics = {"names": ["precision", "recall", "f1_score", "precision_at_n_percent", "recall_at_sizeof_ground_truth"],
           "args": {
               "n": [10, 30, 50, 70, 90]
           }}


def all_metrics(matches: List[Dict[Tuple[Tuple[str, str], Tuple[str, str]], float]],
                golden_standard):
    # load and print the specified metrics
    metric_fns = [getattr(metrics_module, met) for met in metrics['names']]

    final_metrics = dict()

    for metric in metric_fns:
        if metric.__name__ != "precision_at_n_percent":
            final_metrics[metric.__name__] = metric(matches, golden_standard)
        else:
            for n in metrics['args']['n']:
                final_metrics[metric.__name__.replace('_n_', '_' + str(n) + '_')] = metric(matches, golden_standard, n)
    return final_metrics
