import os
import csv


class params: pass
class metrics: pass
class mem_results: pass

def log_params():
    path = 'pipeline/' + params.par_dict['output_file'] + '.params'
    metrics_dict = {k: v for k, v in metrics.__dict__.items() if not k.startswith('__')}

    if not os.path.exists(path):
        with open(path, 'w') as fp:
            writer = csv.writer(fp, delimiter=',')
            header = list(params.par_dict.keys()) + list(metrics_dict.keys()) + list(mem_results.res_dict.keys())
            writer.writerow(header)
            writer.writerow(list(params.par_dict.values()) + list(metrics_dict.values()) + list(mem_results.res_dict.values()))
    else:
        with open('pipeline/' + params.par_dict['output_file'] + '.results', 'a') as fp:
            writer = csv.writer(fp, delimiter=',')
            # writer.writerow(list(configuration.keys()))
            writer.writerow(list(params.par_dict.values()) + list(metrics_dict.values()) + list(mem_results.res_dict.values()))
