import os
import json
from itertools import product

from algorithms.clustering_scale.scale_utils import create_cache_dirs, generate_global_ranks
from data_loader.instance_loader import InstanceLoader
from utils.utils import get_project_root, create_folder

algorithms = ["CorrelationClustering", "Cupid", "SimilarityFlooding", "JaccardLevenMatcher"]

metrics = {"names": ["precision", "recall", "precision_at_n_percent", "recall_at_sizeof_ground_truth"],
           "args": {
               "n": 50
           }}


def get_file_paths(path: str):
    configuration_dictionaries = {}
    for (root, dirs, files) in os.walk(os.path.join(path), topdown=True):
        if not dirs:  # Get only leaf nodes
            configuration_dictionary = {"name": root.split('/')[-1], "source": {"args": {}}, "target": {"args": {}}}
            for file in files:
                if file.endswith("json"):
                    if file.split(".")[0].endswith("mapping"):
                        configuration_dictionary["golden_standard"] = root + '/' + file
                    elif file.split(".")[0].endswith("source"):
                        configuration_dictionary["source"]["args"]["schema"] = root + '/' + file
                    elif file.split(".")[0].endswith("target"):
                        configuration_dictionary["target"]["args"]["schema"] = root + '/' + file
                elif file.endswith("csv"):
                    if file.split(".")[0].endswith("source"):
                        configuration_dictionary["source"]["args"]["data"] = root + '/' + file
                    elif file.split(".")[0].endswith("target"):
                        configuration_dictionary["target"]["args"]["data"] = root + '/' + file
            configuration_dictionaries[root.split('/')[-1]] = configuration_dictionary
    return configuration_dictionaries


def get_algorithm_configurations(path: str):
    configuration_dict = {}
    with open(path, 'r') as fp:
        configs = json.load(fp)
        for algorithm in configs.keys():
            args: dict = configs[algorithm]["args"]
            combinations = get_all_parameter_combinations(args)
            param_names = args.keys()
            for combination in combinations:
                algorithm_configuration = {"algorithm": {"type": algorithm, "args": {}},
                                           "data_loader": configs[algorithm]["data_loader_type"]}
                algorithm_args = dict(zip(param_names, combination))
                name = algorithm + str(algorithm_args)
                algorithm_configuration["algorithm"]["args"] = algorithm_args
                configuration_dict[name] = algorithm_configuration.copy()
    return configuration_dict


def get_list_from_range(min_val, max_val, step):
    if min_val > max_val or step <= 0:
        return None
    i = min_val
    output = [i]
    while round(i, 10) < max_val:
        i = i + step
        output.append(round(i, 10))
    return output


def get_all_parameter_combinations(args):
    all_params = []
    params = []
    for arg, values in args.items():
        if values['type'] == 'range':
            params = get_list_from_range(values['min'], values['max'], values['step'])
        elif values['type'] == 'values':
            params = values['data']
        all_params.append(params)
    return list(product(*all_params))


def combine_data_algorithms(config_data: dict, config_algo: dict):
    create_folder(str(get_project_root())+"/configuration_files")
    for cfd_key, cfd_value in config_data.items():
        create_folder(str(get_project_root()) + "/configuration_files/" + cfd_key)
        for cfa_key, cfa_value in config_algo.items():
            with open(str(get_project_root())+"/configuration_files/" + cfd_key + '/' + cfa_key + ".json",
                      'w') as fp:
                configuration = {"name": cfd_key + cfa_key,
                                 "dataset_name": cfd_key,
                                 "source": {"type": cfa_value["data_loader"], "args": cfd_value["source"]["args"]},
                                 "target": {"type": cfa_value["data_loader"], "args": cfd_value["target"]["args"]},
                                 "algorithm": cfa_value["algorithm"],
                                 "metrics": metrics,
                                 "golden_standard": cfd_value["golden_standard"]}
                json.dump(configuration, fp, indent=2)


def create_sorted_ranks(path: str):
    create_cache_dirs()
    for (root, dirs, files) in os.walk(os.path.join(path), topdown=True):
        if not dirs:  # Get only leaf nodes
            dataset_name = root.split('/')[-1]
            if not os.path.isfile('cache/global_ranks/' + dataset_name + '.pkl'):
                data = []
                for file in files:
                    if file.endswith("csv"):
                        data.extend(InstanceLoader(data=root + '/' + file).table.get_data())
                generate_global_ranks(data, dataset_name)


if __name__ == "__main__":
    d_path = str(get_project_root()) + "/data/prospect"
    if "CorrelationClustering" in algorithms:
        create_sorted_ranks(d_path)
        print("Correlation Clustering's sorted ranks created")
    dtc = get_file_paths(d_path)
    alc = get_algorithm_configurations(str(get_project_root()) + "/algorithm_configurations.json")
    combine_data_algorithms(dtc, alc)