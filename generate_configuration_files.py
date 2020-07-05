import os
import json
import shutil
import re
from itertools import product

from algorithms.distribution_based.clustering_utils import create_cache_dirs, generate_global_ranks
from data_loader.instance_loader import InstanceLoader
from utils.utils import get_project_root, create_folder, get_relative_path

algorithms = ["CorrelationClustering", "Cupid", "SimilarityFlooding", "JaccardLevenMatcher", "Coma", "SemProp", "EmbDI"]

metrics = {"names": ["precision", "recall", "f1_score", "precision_at_n_percent", "recall_at_sizeof_ground_truth"],
           "args": {
               "n": [10, 20, 30, 40, 50, 60, 70, 80, 90]
           }}


def get_file_paths(path: str):
    configuration_dictionaries = {}
    for (root, dirs, files) in os.walk(os.path.join(path), topdown=True):
        if not dirs:  # Get only leaf nodes
            configuration_dictionary = {"name": root.split('/')[-1], "source": {"args": {}}, "target": {"args": {}}}
            for file in files:
                if file.endswith("json"):
                    if file.split(".")[0].endswith("mapping"):
                        configuration_dictionary["golden_standard"] = get_relative_path(root + '/' + file)
                    elif file.split(".")[0].endswith("source"):
                        configuration_dictionary["source"]["args"]["schema"] = get_relative_path(root + '/' + file)
                    elif file.split(".")[0].endswith("target"):
                        configuration_dictionary["target"]["args"]["schema"] = get_relative_path(root + '/' + file)
                elif file.endswith("csv"):
                    if file.split(".")[0].endswith("source"):
                        configuration_dictionary["source"]["args"]["data"] = get_relative_path(root + '/' + file)
                    elif file.split(".")[0].endswith("target"):
                        configuration_dictionary["target"]["args"]["data"] = get_relative_path(root + '/' + file)
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


def combine_data_algorithms(config_data: dict, config_algo: dict, completed_jobs: dict):
    create_folder(get_project_root()+"/configuration_files")
    for cfd_key, cfd_value in config_data.items():
        for cfa_key, cfa_value in config_algo.items():
            if (cfa_value["algorithm"]["type"] == "SemProp" and "assays" in cfd_key and "SemProp" in algorithms) \
                    or (cfa_value["algorithm"]["type"] != "SemProp" and cfa_value["algorithm"]["type"] in algorithms):
                name = cfd_key + '__' + cfa_key
                if name not in completed_jobs[cfa_value["algorithm"]["type"]]:
                    create_folder(str(get_project_root()) + "/configuration_files/" + cfa_value["algorithm"]["type"])
                    create_folder(str(get_project_root()) + "/configuration_files/" + cfa_value["algorithm"]["type"]
                                  + '/' + cfd_key)
                    cfa_key = re.sub('\\W+', '_', cfa_key)
                    file_name = str(get_project_root())+"/configuration_files/" + cfa_value["algorithm"]["type"] + \
                                                        '/' + cfd_key + '/' + cfa_key + ".json"
                    with open(file_name, 'w') as fp:
                        configuration = {"name": name,
                                         "dataset_name": cfd_key,
                                         "source": {"type": cfa_value["data_loader"],
                                                    "args": cfd_value["source"]["args"]},
                                         "target": {"type": cfa_value["data_loader"],
                                                    "args": cfd_value["target"]["args"]},
                                         "algorithm": cfa_value["algorithm"],
                                         "metrics": metrics,
                                         "golden_standard": cfd_value["golden_standard"]}
                        if cfa_value["algorithm"]["type"] == "SemProp":
                            configuration["source"]["args"]["schema"] = configuration["source"]["args"]["schema"]\
                                .replace(get_project_root(), "/code")
                            configuration["source"]["args"]["data"] = configuration["source"]["args"]["data"]\
                                .replace(get_project_root(), "/code")
                            configuration["target"]["args"]["schema"] = configuration["target"]["args"]["schema"] \
                                .replace(get_project_root(), "/code")
                            configuration["target"]["args"]["data"] = configuration["target"]["args"]["data"] \
                                .replace(get_project_root(), "/code")
                            configuration["golden_standard"] = configuration["golden_standard"] \
                                .replace(get_project_root(), "/code")
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


def get_completed_jobs_of_algorithm(algorithm_name: str):
    completed = []
    output_path = get_project_root() + "/output/" + algorithm_name
    for (root, dirs, files) in os.walk(os.path.join(output_path)):
        if not dirs:  # Get only leaf nodes
            for file in files:
                with open(root + '/' + file) as f:
                    data = json.load(f)
                    completed.append(data["name"])
    return completed


if __name__ == "__main__":
    if os.path.exists(get_project_root()+"/configuration_files"):
        shutil.rmtree(get_project_root()+"/configuration_files")
    cmpl_jobs = {}
    for algo in algorithms:
        cmpl_jobs[algo] = get_completed_jobs_of_algorithm(algo)
    d_path = get_project_root() + "/data"
    if "CorrelationClustering" in algorithms:
        create_sorted_ranks(d_path)
        print("Correlation Clustering's sorted ranks created")
    dtc = get_file_paths(d_path)
    alc = get_algorithm_configurations(get_project_root() + "/algorithm_configurations.json")
    combine_data_algorithms(dtc, alc, cmpl_jobs)
