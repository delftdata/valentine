import subprocess
from collections import defaultdict
from itertools import combinations

import yaml
import os
import time
import sys

from algorithms.sem_prop.inputoutput import utils as io
from algorithms.sem_prop.ontomatch import matcher_lib as matcherlib

from algorithms.base_matcher import BaseMatcher
from algorithms.sem_prop.knowledgerepr import fieldnetwork
from algorithms.sem_prop.modelstore.elasticstore import StoreHandler
from algorithms.sem_prop.ontomatch import glove_api
from algorithms.sem_prop.ontomatch.matcher_lib import MatchingType
from algorithms.sem_prop.ontomatch.ss_api import SSAPI

CONFIG_FILE = os.path.abspath("access-db.yml")
PATH = os.path.dirname(os.path.abspath(__file__))
PATH_DATA = PATH + "/models/"
SEMANTIC_MODEL = "{}{}".format(PATH, "/glove/glove.6B.100d.txt")
ONTOLOGY_NAME = "efo"
PATH_ONTOLOGY = "{}{}".format(PATH, "/cache_onto/")


def make_config_file(source_data_loader, target_data_loader):
    source_path, source_name = make_path(source_data_loader)
    # target_path, target_name = make_path(target_data_loader)

    data = dict()
    data['api_version'] = 0
    data['sources'] = list()
    data['sources'].append(make_config_yml(".".join(source_name.split('.')[:-1]), source_path))
    # data['sources'].append(make_config_yml(".".join(target_name.split('.')[:-1]), target_path))

    with open("access-db.yml", 'w+') as fp:
        yaml.dump(data, fp)


def make_config_yml(dataset_name, path):
    obj = dict()
    obj['config'] = dict()
    obj['config']['path'] = path
    obj['config']['separator'] = ','
    obj['name'] = dataset_name
    obj['type'] = 'csv'
    return obj


def make_path(source_data_loader):
    parts = source_data_loader.split("/")
    file_name = parts[len(parts) - 1]
    relative = os.path.realpath('.')
    parts = source_data_loader.split(relative)[1]
    path = "../../.." + parts.split(file_name)[0]
    return path, file_name


def init_config(dataset_name):
    ping = subprocess.Popen(PATH + '/pipeline.sh ' + CONFIG_FILE + ' ' + dataset_name, stdout=subprocess.PIPE,
                            shell=True)
    time.sleep(1)
    ping.terminate()
    ping.communicate()
    return ping


def generate_matchings(network, store_client, om, minhash_sim_threshold, semantic_sim_threshold, coh_sem_sim_threshold,
                       class_name_relation=False):
    print("Compute fuzzy content matching...")
    l7 = matcherlib.find_hierarchy_content_fuzzy(om.kr_handlers, store_client)

    l4 = None
    if class_name_relation:
        print("Compute syntactic relation-clss matching ...")
        l4 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                           minhash_sim_threshold=minhash_sim_threshold)

    print("Compute syntactic attribute-class matching ...")
    l5 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                           minhash_sim_threshold=minhash_sim_threshold)

    l42 = None
    neg_l42 = None
    if class_name_relation:
        print("Compute semantic relation-clss matching ...")
        l42, neg_l42 = matcherlib.find_relation_class_name_sem_matchings(network, om.kr_handlers,
                                                                         sem_sim_threshold=semantic_sim_threshold,
                                                                         negative_signal_threshold=0.1,
                                                                         add_exact_matches=False,
                                                                         penalize_unknown_word=True)

    print("Compute semantic attribute-class matching ...")
    l52, neg_l52 = matcherlib.find_relation_class_attr_name_sem_matchings(network, om.kr_handlers,
                                                                          semantic_sim_threshold=semantic_sim_threshold,
                                                                          negative_signal_threshold=0.1,
                                                                          add_exact_matches=False,
                                                                          penalize_unknown_word=True)
    print("Compute coherent groups ...")
    l6, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                         sem_sim_threshold=coh_sem_sim_threshold,
                                                         group_size_cutoff=1)
    return l4, l5, l42, l52, neg_l42, neg_l52, l6, l7


def combine_matchings(l4, l5, l42, l52, nl42, nl52, l6, l7, om, cutting_ratio=0.8, summary_threshold=1):

    def list_from_dict(combined):
        l = []
        for k, v in combined.items():
            matchings = v.get_matchings()
            for el in matchings:
                l.append(el)
        return l

    if l4 is not None:
        l4_dict = dict()
        for matching in l4:
            l4_dict[matching] = 1
        total_cancelled = 0
        for m in nl42:
            if m in l4_dict:
                total_cancelled += 1
                l4.remove(m)

    l5_dict = dict()
    for matching in l5:
        l5_dict[matching] = 1
    total_cancelled = 0
    for m in nl52:
        if m in l5_dict:
            total_cancelled += 1
            l5.remove(m)

    l6_dict = dict()
    if l6 is not None:
        for matching in l6:
            l6_dict[matching] = 1

    # curate l42 with l6
    if l6 is not None and l4 is not None:
        removed_l42 = 0
        for m in l42:
            if m not in l6_dict:
                removed_l42 += 1
                l42.remove(m)

    # curate l52 with l6
    # (('chemical', 'activity_stds_lookup', 'std_act_id'), ('efo', 'Metabolomic Profiling'))
    # (('chemical', 'activity_stds_lookup', '_'), ('efo', 'Experimental Factor'))
    removed_l52 = 0
    for m in l52:
        db, relation, attr = m[0]
        el = ((db, relation, '_'), m[1])
        if el not in l6_dict:
            removed_l52 += 1
            l52.remove(m)

    all_matchings = defaultdict(list)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52
    all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7

    combined = matcherlib.combine_matchings(all_matchings)
    combined_list = list_from_dict(combined)

    print("StructS ... ")
    combined_sum = matcherlib.summarize_matchings_to_ancestor(om, combined_list,
                                                              threshold_to_summarize=summary_threshold,
                                                              summary_ratio=cutting_ratio)
    return combined_sum


class SemProp(BaseMatcher):

    def __init__(self, minhash_sim_threshold, semantic_sim_threshold, coh_sem_sim_threshold):
        self.matchings = dict()
        self.minhash_sim_threshold = minhash_sim_threshold
        self.semantic_sim_threshold = semantic_sim_threshold
        self.coh_sem_sim_threshold = coh_sem_sim_threshold

    def get_matches(self, source_data_loader, target_data_loader, dataset_name):

        sys.path.append(os.getcwd())

        make_config_file(source_data_loader.data_path, target_data_loader.data_path)

        status = init_config(dataset_name)
        if status == 0:
            print("Init successful")
        else:
            assert RuntimeError("Error in init")

        path_to_serialized_model = "{}{}/".format(PATH_DATA, dataset_name)
        network = fieldnetwork.deserialize_network(path_to_serialized_model)
        # Create client
        store_client = StoreHandler()

        # Load glove model
        print("Loading language model...")
        glove_api.load_model(SEMANTIC_MODEL)
        print("Loading language model...OK")

        print("Retrieve indexes")
        schema_sim_index = io.deserialize_object(path_to_serialized_model + 'schema_sim_index.pkl')
        content_sim_index = io.deserialize_object(path_to_serialized_model + 'content_sim_index.pkl')

        print("Create ontomatch api")
        om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
        print("Load parsed ontology")
        om.add_krs([(ONTOLOGY_NAME, PATH_ONTOLOGY)], parsed=True)

        print("Build content sim")
        om.priv_build_content_sim(0.6)

        print("Generate matchings")
        l4, l5, l42, l52, neg_l42, neg_l52, l6, l7 = generate_matchings(network, store_client, om,
                                                                        self.minhash_sim_threshold,
                                                                        self.semantic_sim_threshold,
                                                                        self.coh_sem_sim_threshold)

        print("Combine matchings")
        matchings = combine_matchings(l4, l5, l42, l52, neg_l42, neg_l52, l6, l7, om)
        self.process_matchings(matchings)

        return self.matchings

    def process_matchings(self, matchings):
        same_class = dict()
        for source, target in matchings:
            if target not in same_class:
                same_class[target] = list()
            same_class[target].append((source[1], source[2]))

        for key, matches in same_class.items():
            if len(matches) > 1:
                for combination in combinations(matches, 2):
                    combination = ((combination[0][0].replace(".csv", ""), combination[0][1]),
                                   (combination[1][0].replace(".csv", ""), combination[1][1]))
                    if combination[0][0] != combination[1][0]:
                        self.matchings[(combination[0], combination[1])] = 1
