import time
from collections import defaultdict

import numpy as np
from tqdm import tqdm

from algorithms.sem_prop.inputoutput import utils as io
from algorithms.sem_prop.knowledgerepr import fieldnetwork
from algorithms.sem_prop.modelstore.elasticstore import StoreHandler
from algorithms.sem_prop.ontomatch import glove_api
from algorithms.sem_prop.ontomatch import matcher_lib as matcherlib
from algorithms.sem_prop.ontomatch.matcher_lib import MatchingType
from algorithms.sem_prop.ontomatch.ss_api import SSAPI


# INDIVIDUAL FUNCTIONS


# Only L4 and L5
def matchings_syntactic_only(network, kr_handlers, sim_threshold_attr=0.5, sim_threshold_rel=0.5):
    # sim_threshold = 0.5 (original)
    st = time.time()
    l4_matchings = matcherlib.find_relation_class_name_matchings(network, kr_handlers,
                                                                 minhash_sim_threshold=sim_threshold_rel)
    l5_matchings = matcherlib.find_relation_class_attr_name_matching(network, kr_handlers,
                                                                     minhash_sim_threshold=sim_threshold_attr)
    et = time.time()
    print("l4 total: " + str(len(l4_matchings)))
    print("l5 total: " + str(len(l5_matchings)))
    print("Took: " + str(et - st))

    return l4_matchings, l5_matchings


# L4, L5, L42 and L52
def matchings_syntactic_and_naive_wordembeddings(network, kr_handlers, sim_threshold_attr=0.5, sim_threshold_rel=0.5,
                                                 sem_threshold_attr=0.5, sem_threshold_rel=0.5):
    # sim_threshold = 0.5 (original)
    st = time.time()
    l4_matchings = matcherlib.find_relation_class_name_matchings(network, kr_handlers,
                                                                minhash_sim_threshold=sim_threshold_rel)
    l5_matchings = matcherlib.find_relation_class_attr_name_matching(network, kr_handlers,
                                                                minhash_sim_threshold=sim_threshold_attr)
    l42_matchings, neg_l42_matchings =matcherlib.find_relation_class_name_sem_matchings(network, kr_handlers,
                                                        sem_sim_threshold=sem_threshold_rel,
                                                        sensitivity_neg_signal=0.4,
                                                        penalize_unknown_word=False,
                                                        add_exact_matches=True)
    l52_matchings, neg_l52_matchings = matcherlib.find_relation_class_attr_name_sem_matchings(network, kr_handlers,
                                                        semantic_sim_threshold=sem_threshold_attr,
                                                        sensitivity_neg_signal=0.4,
                                                        penalize_unknown_word = False,
                                                        add_exact_matches = True)
    et = time.time()
    print("l4 total: " + str(len(l4_matchings)))
    print("l42 total: " + str(len(l42_matchings)))
    print("l5 total: " + str(len(l5_matchings)))
    print("l52 total: " + str(len(l52_matchings)))
    print("Took: " + str(et - st))

    return l4_matchings, l5_matchings, l42_matchings, l52_matchings


# L4, L5, L42, L52 with cancellation
def matchings_basic_pipeline_attr_rel_cancellation(network, kr_handlers, sim_threshold_attr=0.5,
                                                    sim_threshold_rel=0.5,
                                                    sem_threshold_attr=0.5,
                                                    sem_threshold_rel=0.5,
                                                    sensitivity_cancellation_signal=0.4):
        # sim_threshold = 0.5 (original)
        st = time.time()
        l4_matchings = matcherlib.find_relation_class_name_matchings(network, kr_handlers,
                                                        minhash_sim_threshold=sim_threshold_rel)
        l5_matchings = matcherlib.find_relation_class_attr_name_matching(network, kr_handlers,
                                                        minhash_sim_threshold=sim_threshold_attr)
        l42_matchings, neg_l42_matchings = matcherlib.find_relation_class_name_sem_matchings(network, kr_handlers,
                                                        sem_sim_threshold=sem_threshold_rel,
                                                        sensitivity_neg_signal=sensitivity_cancellation_signal,
                                                        penalize_unknown_word = True,
                                                        add_exact_matches = False)
        l52_matchings, neg_l52_matchings = matcherlib.find_relation_class_attr_name_sem_matchings(network, kr_handlers,
                                                        semantic_sim_threshold=sem_threshold_attr,
                                                        sensitivity_neg_signal=sensitivity_cancellation_signal,
                                                        penalize_unknown_word=True,
                                                        add_exact_matches=False)
        # relation cancellation
        st = time.time()
        cancelled_l4_matchings = []
        l4_dict = dict()
        l4_matchings_set = set(l4_matchings)
        for matching in l4_matchings_set:
            l4_dict[matching] = 1
        total_cancelled = 0
        for m in neg_l42_matchings:
            if m in l4_dict:
                total_cancelled += 1
                l4_matchings_set.remove(m)
                cancelled_l4_matchings.append(m)
        l4_matchings = list(l4_matchings_set)
        et = time.time()
        print("l4 cancel time: " + str((et - st)))

        # attribute cancellation
        st = time.time()
        cancelled_l5_matchings = []
        l5_dict = dict()
        l5_matchings_set = set(l5_matchings)
        for matching in l5_matchings:
            l5_dict[matching] = 1
        total_cancelled = 0
        for m in neg_l52_matchings:
            if m in l5_dict:
                total_cancelled += 1
                l5_matchings_set.remove(m)
                cancelled_l5_matchings.append(m)
        l5_matchings = list(l5_matchings_set)
        et = time.time()
        print("l5 cancel time: " + str((et - st)))

        et = time.time()
        print("l4 total: " + str(len(l4_matchings)))
        print("l42 total: " + str(len(l42_matchings)))
        print("l5 total: " + str(len(l5_matchings)))
        print("l52 total: " + str(len(l52_matchings)))
        print("Took: " + str(et - st))

        return l4_matchings, l5_matchings, l42_matchings, l52_matchings


# L4, L5, L42, L52 with cancellation + L6
def matchings_basic_pipeline_coh_group_cancellation(network, kr_handlers, sim_threshold_attr=0.5,
                                    sim_threshold_rel=0.5,
                                    sem_threshold_attr=0.5,
                                    sem_threshold_rel=0.5,
                                    coh_group_threshold=0.5,
                                    coh_group_size_cutoff=1,
                                    sensitivity_cancellation_signal=0.4):
    # sim_threshold = 0.5 (original)
    st = time.time()
    l4_matchings = matcherlib.find_relation_class_name_matchings(network, kr_handlers,
                                                                minhash_sim_threshold=sim_threshold_rel)
    l5_matchings = matcherlib.find_relation_class_attr_name_matching(network, kr_handlers,
                                                                minhash_sim_threshold=sim_threshold_attr)
    l42_matchings, neg_l42_matchings = matcherlib.find_relation_class_name_sem_matchings(network, kr_handlers,
                                                                sem_sim_threshold=sem_threshold_rel,
                                                                sensitivity_neg_signal=sensitivity_cancellation_signal,
                                                                penalize_unknown_word=True,
                                                                add_exact_matches=False)
    l52_matchings, neg_l52_matchings = matcherlib.find_relation_class_attr_name_sem_matchings(network, kr_handlers,
                                                                semantic_sim_threshold=sem_threshold_attr,
                                                                sensitivity_neg_signal=sensitivity_cancellation_signal,
                                                                penalize_unknown_word=True,
                                                                add_exact_matches=False)
    l6_matchings, table_groups = matcherlib.find_sem_coh_matchings(network, kr_handlers,
                                                                sem_sim_threshold=coh_group_threshold,
                                                                group_size_cutoff=coh_group_size_cutoff)

    # relation cancellation
    st = time.time()
    cancelled_l4_matchings = []
    l4_dict = dict()
    l4_matchings_set = set(l4_matchings)
    for matching in l4_matchings_set:
        l4_dict[matching] = 1
    total_cancelled = 0
    for m in neg_l42_matchings:
        if m in l4_dict:
            total_cancelled += 1
            l4_matchings_set.remove(m)
            cancelled_l4_matchings.append(m)
    l4_matchings = list(l4_matchings_set)
    et = time.time()
    print("l4 cancel time: " + str((et-st)))

    # attribute cancellation
    st = time.time()
    cancelled_l5_matchings = []
    l5_dict = dict()
    l5_matchings_set = set(l5_matchings)
    for matching in l5_matchings:
        l5_dict[matching] = 1
    total_cancelled = 0
    for m in neg_l52_matchings:
        if m in l5_dict:
            total_cancelled += 1
            l5_matchings_set.remove(m)
            cancelled_l5_matchings.append(m)
    l5_matchings = list(l5_matchings_set)
    et = time.time()
    print("l5 cancel time: " + str((et - st)))

    # coh group cancellation relation
    st = time.time()
    l6_dict = dict()
    l42_matchings_set = set(l42_matchings)
    for matching in l6_matchings:
        l6_dict[matching] = 1
    for m in l42_matchings:
        if m not in l6_dict:
            l42_matchings_set.remove(m)
    l42_matchings = list(l42_matchings_set)
    et = time.time()
    print("l42 cancel time: " + str((et-st)))

    # coh group cancellation attribute
    st = time.time()
    l52_dict = defaultdict(list)
    for matching in l52_matchings:
        # adapt matching to be compared to L6
        sch, cla = matching
        sch0, sch1, sch2 = sch
        idx = ((sch0, sch1, '_'), cla)
        l52_dict[idx].append(matching)

    # coh group cancellation attributes
    idx_to_remove = []
    # collect idx to remove
    for k, v in l52_dict.items():
        if k not in l6_dict:
            idx_to_remove.append(k)
    # remove the indexes and take the values as matching list
    for el in idx_to_remove:
        del l52_dict[el]
    l52_matchings = []
    for k, v in l52_dict.items():
        for el in v:
            l52_matchings.append(el)

    """
    for m in l52_dict.keys():
        if m not in l6_dict:
            db_to_remove, rel_to_remove, _ = m[0]
            for el in l52_matchings:
                sch, cla = el
                db, relation, attr = sch
                if db == db_to_remove and relation == rel_to_remove:
                    if el in l52_matchings:
                        l52_matchings.remove(el)
    """
    et = time.time()
    print("l52 cancel time: " + str(et-st))

    et = time.time()
    print("l4 total: " + str(len(l4_matchings)))
    print("l42 total: " + str(len(l42_matchings)))
    print("l5 total: " + str(len(l5_matchings)))
    print("l52 total: " + str(len(l52_matchings)))
    print("Took: " + str(et - st))

    return l4_matchings, l5_matchings, l42_matchings, l52_matchings


def matchings_basic_pipeline_coh_group_cancellation_and_content(om, network, kr_handlers, store_client,
                                                                sim_threshold_attr=0.5,
                                                                sim_threshold_rel=0.5,
                                                                sem_threshold_attr=0.5,
                                                                sem_threshold_rel=0.5,
                                                                coh_group_threshold=0.5,
                                                                coh_group_size_cutoff=1,
                                                                sensitivity_cancellation_signal=0.4):
    # sim_threshold = 0.5 (original)
    st = time.time()
    l4_matchings = matcherlib.find_relation_class_name_matchings(network, kr_handlers,
                                                                minhash_sim_threshold=sim_threshold_rel)
    l5_matchings = matcherlib.find_relation_class_attr_name_matching(network, kr_handlers,
                                                                minhash_sim_threshold=sim_threshold_attr)
    l42_matchings, neg_l42_matchings = matcherlib.find_relation_class_name_sem_matchings(network, kr_handlers,
                                                                sem_sim_threshold=sem_threshold_rel,
                                                                sensitivity_neg_signal=sensitivity_cancellation_signal)
    l52_matchings, neg_l52_matchings = matcherlib.find_relation_class_attr_name_sem_matchings(network, kr_handlers,
                                                                semantic_sim_threshold=sem_threshold_attr,
                                                                sensitivity_neg_signal=sensitivity_cancellation_signal)
    l6_matchings, table_groups = matcherlib.find_sem_coh_matchings(network, kr_handlers,
                                                                sem_sim_threshold=coh_group_threshold,
                                                                group_size_cutoff=coh_group_size_cutoff)

    # relation cancellation
    st = time.time()
    cancelled_l4_matchings = []
    l4_dict = dict()
    l4_matchings_set = set(l4_matchings)
    for matching in l4_matchings_set:
        l4_dict[matching] = 1
    total_cancelled = 0
    for m in neg_l42_matchings:
        if m in l4_dict:
            total_cancelled += 1
            l4_matchings_set.remove(m)
            cancelled_l4_matchings.append(m)
    l4_matchings = list(l4_matchings_set)
    et = time.time()
    print("l4 cancel time: " + str((et - st)))

    # attribute cancellation
    st = time.time()
    cancelled_l5_matchings = []
    l5_dict = dict()
    l5_matchings_set = set(l5_matchings)
    for matching in l5_matchings:
        l5_dict[matching] = 1
    total_cancelled = 0
    for m in neg_l52_matchings:
        if m in l5_dict:
            total_cancelled += 1
            l5_matchings_set.remove(m)
            cancelled_l5_matchings.append(m)
    l5_matchings = list(l5_matchings_set)
    et = time.time()
    print("l5 cancel time: " + str((et - st)))

    # coh group cancellation relation
    st = time.time()
    l6_dict = dict()
    l42_matchings_set = set(l42_matchings)
    for matching in l6_matchings:
        l6_dict[matching] = 1
    for m in l42_matchings:
        if m not in l6_dict:
            l42_matchings_set.remove(m)
    l42_matchings = list(l42_matchings_set)
    et = time.time()
    print("l42 cancel time: " + str((et - st)))

    # coh group cancellation attribute
    st = time.time()
    l52_dict = defaultdict(list)
    for matching in l52_matchings:
        # adapt matching to be compared to L6
        sch, cla = matching
        sch0, sch1, sch2 = sch
        idx = ((sch0, sch1, '_'), cla)
        l52_dict[idx].append(matching)

    # coh group cancellation attributes
    idx_to_remove = []
    # collect idx to remove
    for k, v in l52_dict.items():
        if k not in l6_dict:
            idx_to_remove.append(k)
    # remove the indexes and take the values as matching list
    for el in idx_to_remove:
        del l52_dict[el]
    l52_matchings = []
    for k, v in l52_dict.items():
        for el in v:
            l52_matchings.append(el)

    # Build content sim
    om.priv_build_content_sim(0.6)

    l1_matchings = []
    for kr_name, kr_handler in kr_handlers.items():
        kr_class_signatures = kr_handler.get_classes_signatures()
        l1_matchings += om.compare_content_signatures(kr_name, kr_class_signatures)

    l7_matchings = matcherlib.find_hierarchy_content_fuzzy(kr_handlers, store_client)

    et = time.time()
    print("l1 total: " + str(len(l1_matchings)))
    print("l4 total: " + str(len(l4_matchings)))
    print("l42 total: " + str(len(l42_matchings)))
    print("l5 total: " + str(len(l5_matchings)))
    print("l52 total: " + str(len(l52_matchings)))
    print("l7 total: " + str(len(l7_matchings)))
    print("Took: " + str(et - st))

    return l4_matchings, l5_matchings, l42_matchings, l52_matchings, l1_matchings, l7_matchings


def matchings_full_approach():
    return


def store_results(path, name, matchings):
    with open(path + name, 'w+') as f:
        for k, v in matchings.items():
            lines = v.print_serial()
            for l in lines:
                f.write(l + '\n')


def compute_pr_matchings(gt_matchings, matchings):
    gtm = set(gt_matchings)
    total_results = len(matchings)
    # true_positives = 0
    # for el in matchings:
    #     if el in gtm:
    #         true_positives += 1
    true_positives = sum(map(lambda x: x in gtm, matchings))

    if total_results == 0:
        precision = 0
    else:
        precision = float(true_positives / total_results)
    recall = float(true_positives / len(gt_matchings))

    return precision, recall


def compute_pr(gt_file, r_file):

    def parse_strings(list_of_strings):
        # format is: db %%% table %%% attr ==>> onto %%% class_name %%% list_of_matchers
        matchings = []
        for l in list_of_strings:
            tokens = l.split("==>>")
            sch = tokens[0]
            cla = tokens[1]
            sch_tokens = sch.split("%%%")
            sch_tokens = [t.strip() for t in sch_tokens]
            cla_tokens = cla.split("%%%")
            cla_tokens = [t.strip() for t in cla_tokens]
            #matching_format = (((sch_tokens[0], sch_tokens[1], sch_tokens[2]), (cla_tokens[0], cla_tokens[1])), cla_tokens[2])
            matching_format = (((sch_tokens[0], sch_tokens[1], sch_tokens[2]), (cla_tokens[0], cla_tokens[1])))
            matchings.append(matching_format)
        return matchings

    with open(gt_file, 'r') as gt:
        ground_truth_matchings_strings = gt.readlines()
    with open(r_file, 'r') as r:
        output_matchings_strings = r.readlines()

    ground_truth_matchings = parse_strings(ground_truth_matchings_strings)
    output_matchings = parse_strings(output_matchings_strings)

    gtm = set(ground_truth_matchings)
    total_results = len(output_matchings)
    true_positives = 0
    for el in output_matchings:
        if el in gtm:
            true_positives += 1
    if total_results == 0:
        precision = 0
    else:
        precision = float(true_positives / total_results)
    recall = float(true_positives / len(ground_truth_matchings))

    return precision, recall


# SCRIPT FUNCTIONS
def best_config(path_to_serialized_model, onto_name, path_to_ontology, path_to_sem_model, path_to_results):
    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    # Create client
    store_client = StoreHandler()

    # Load glove model
    print("Loading language model...")
    glove_api.load_model(path_to_sem_model)
    print("Loading language model...OK")

    # Retrieve indexes
    schema_sim_index = io.deserialize_object(path_to_serialized_model + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object(path_to_serialized_model + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    # Load parsed ontology
    om.add_krs([(onto_name, path_to_ontology)], parsed=True)

    print("best config...")
    all_matchings = defaultdict(list)
    l4_01, l5_01, l42_01, l52_01, l1, l7 = matchings_basic_pipeline_coh_group_cancellation_and_content(
        om,
        om.network,
        om.kr_handlers,
        store_client,
        sim_threshold_attr=0.2,
        sim_threshold_rel=0.2,
        sem_threshold_attr=0.6,
        sem_threshold_rel=0.7,
        coh_group_threshold=0.5,
        coh_group_size_cutoff=2,
        sensitivity_cancellation_signal=0.3)
    all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_01
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_01
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_01
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_01
    all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7
    combined_01 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "best_config", combined_01)
    print("best_config...OK")


def relative_merit_one_onto(path_to_serialized_model, onto_name, path_to_ontology, path_to_sem_model, path_to_results):
    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    # Create client
    store_client = StoreHandler()

    # Load glove model
    print("Loading language model...")
    glove_api.load_model(path_to_sem_model)
    print("Loading language model...OK")

    # Retrieve indexes
    schema_sim_index = io.deserialize_object(path_to_serialized_model + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object(path_to_serialized_model + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    # Load parsed ontology
    om.add_krs([(onto_name, path_to_ontology)], parsed=True)

    # Experiments

    # syntactic only
    print("syn_only_01...")
    all_matchings = defaultdict(list)
    l4_01, l5_01 = matchings_syntactic_only(om.network, om.kr_handlers, sim_threshold_attr=0.1, sim_threshold_rel=0.1)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_01
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_01
    combined_01 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_only_01", combined_01)
    print("syn_only_01...OK")

    print("syn_only_03...")
    all_matchings = defaultdict(list)
    l4_03, l5_03 = matchings_syntactic_only(om.network, om.kr_handlers, sim_threshold_attr=0.3, sim_threshold_rel=0.3)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_03
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_03
    combined_03 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_only_03", combined_03)
    print("syn_only_03...OK")

    print("syn_only_05...")
    all_matchings = defaultdict(list)
    l4_05, l5_05 = matchings_syntactic_only(om.network, om.kr_handlers, sim_threshold_attr=0.5, sim_threshold_rel=0.5)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_05
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_05
    combined_05 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_only_05", combined_05)
    print("syn_only_05...OK")

    print("syn_only_07...")
    all_matchings = defaultdict(list)
    l4_07, l5_07 = matchings_syntactic_only(om.network, om.kr_handlers, sim_threshold_attr=0.7, sim_threshold_rel=0.7)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_07
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_07
    combined_07 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_only_07", combined_07)
    print("syn_only_07...OK")

    print("syn_only_09...")
    all_matchings = defaultdict(list)
    l4_09, l5_09 = matchings_syntactic_only(om.network, om.kr_handlers, sim_threshold_attr=0.9, sim_threshold_rel=0.9)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_09
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_09
    combined_09 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_only_09", combined_09)
    print("syn_only_09...OK")

    # syntactic and naive embeddings
    print("syn_naive_we_01...")
    all_matchings = defaultdict(list)
    l4_01, l5_01, l42_01, l52_01 = matchings_syntactic_and_naive_wordembeddings(om.network, om.kr_handlers,
                                                                                sim_threshold_attr=0.1,
                                                                                sim_threshold_rel=0.1,
                                                                                sem_threshold_attr=0.1,
                                                                                sem_threshold_rel=0.1)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_01
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_01
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_01
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_01
    combined_01 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_naive_we_01", combined_01)
    print("syn_naive_we_01...OK")

    print("syn_naive_we_03...")
    all_matchings = defaultdict(list)
    l4_03, l5_03, l42_03, l52_03 = matchings_syntactic_and_naive_wordembeddings(om.network, om.kr_handlers,
                                                                                sim_threshold_attr=0.3,
                                                                                sim_threshold_rel=0.3,
                                                                                sem_threshold_attr=0.3,
                                                                                sem_threshold_rel=0.3)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_03
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_03
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_03
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_03
    combined_03 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_naive_we_03", combined_03)
    print("syn_naive_we_03...OK")

    print("syn_naive_we_05...")
    all_matchings = defaultdict(list)
    l4_05, l5_05, l42_05, l52_05 = matchings_syntactic_and_naive_wordembeddings(om.network, om.kr_handlers,
                                                                                sim_threshold_attr=0.5,
                                                                                sim_threshold_rel=0.5,
                                                                                sem_threshold_attr=0.5,
                                                                                sem_threshold_rel=0.5)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_05
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_05
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_05
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_05
    combined_05 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_naive_we_05", combined_05)
    print("syn_naive_we_05...OK")

    print("syn_naive_we_07...OK")
    all_matchings = defaultdict(list)
    l4_07, l5_07, l42_07, l52_07 = matchings_syntactic_and_naive_wordembeddings(om.network, om.kr_handlers,
                                                                                sim_threshold_attr=0.7,
                                                                                sim_threshold_rel=0.7,
                                                                                sem_threshold_attr=0.7,
                                                                                sem_threshold_rel=0.7)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_07
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_07
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_07
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_07
    combined_07 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_naive_we_07", combined_07)
    print("syn_naive_we_07...OK")

    print("syn_naive_we_09...")
    all_matchings = defaultdict(list)
    l4_09, l5_09, l42_09, l52_09 = matchings_syntactic_and_naive_wordembeddings(om.network, om.kr_handlers,
                                                                                sim_threshold_attr=0.9,
                                                                                sim_threshold_rel=0.9,
                                                                                sem_threshold_attr=0.9,
                                                                                sem_threshold_rel=0.9)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_09
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_09
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_09
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_09
    combined_09 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "syn_naive_we_09", combined_09)
    print("syn_naive_we_09...OK")

    # Basic pipeline
    print("basic_01...")
    all_matchings = defaultdict(list)
    l4_01, l5_01, l42_01, l52_01 = matchings_basic_pipeline_attr_rel_cancellation(om.network, om.kr_handlers,
                                                                                  sim_threshold_attr=0.1,
                                                                                  sim_threshold_rel=0.1,
                                                                                  sem_threshold_attr=0.1,
                                                                                  sem_threshold_rel=0.1,
                                                                                  sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_01
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_01
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_01
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_01
    combined_01 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_01", combined_01)
    print("basic_01...OK")

    print("basic_03...")
    all_matchings = defaultdict(list)
    l4_03, l5_03, l42_03, l52_03 = matchings_basic_pipeline_attr_rel_cancellation(om.network, om.kr_handlers,
                                                                                  sim_threshold_attr=0.3,
                                                                                  sim_threshold_rel=0.3,
                                                                                  sem_threshold_attr=0.3,
                                                                                  sem_threshold_rel=0.3,
                                                                                  sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_03
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_03
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_03
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_03
    combined_03 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_03", combined_03)
    print("basic_03...OK")

    print("basic_05...")
    all_matchings = defaultdict(list)
    l4_05, l5_05, l42_05, l52_05 = matchings_basic_pipeline_attr_rel_cancellation(om.network, om.kr_handlers,
                                                                                  sim_threshold_attr=0.5,
                                                                                  sim_threshold_rel=0.5,
                                                                                  sem_threshold_attr=0.5,
                                                                                  sem_threshold_rel=0.5,
                                                                                  sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_05
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_05
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_05
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_05
    combined_05 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_05", combined_05)
    print("basic_05...OK")

    print("basic_07...")
    all_matchings = defaultdict(list)
    l4_07, l5_07, l42_07, l52_07 = matchings_basic_pipeline_attr_rel_cancellation(om.network, om.kr_handlers,
                                                                                  sim_threshold_attr=0.7,
                                                                                  sim_threshold_rel=0.7,
                                                                                  sem_threshold_attr=0.7,
                                                                                  sem_threshold_rel=0.7,
                                                                                  sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_07
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_07
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_07
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_07
    combined_07 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_07", combined_07)
    print("basic_07...OK")

    print("basic_09...")
    all_matchings = defaultdict(list)
    l4_09, l5_09, l42_09, l52_09 = matchings_basic_pipeline_attr_rel_cancellation(om.network, om.kr_handlers,
                                                                                  sim_threshold_attr=0.9,
                                                                                  sim_threshold_rel=0.9,
                                                                                  sem_threshold_attr=0.9,
                                                                                  sem_threshold_rel=0.9,
                                                                                  sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_09
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_09
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_09
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_09
    combined_09 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_09", combined_09)
    print("basic_09...OK")



    # Basic pipeline with l6 cancellation
    print("basic_l6cancel_01...")
    all_matchings = defaultdict(list)
    l4_01, l5_01, l42_01, l52_01 = matchings_basic_pipeline_coh_group_cancellation(om.network, om.kr_handlers,
                                                                                   sim_threshold_attr=0.1,
                                                                                   sim_threshold_rel=0.1,
                                                                                   sem_threshold_attr=0.1,
                                                                                   sem_threshold_rel=0.1,
                                                                                   coh_group_threshold=0.5,
                                                                                   coh_group_size_cutoff=1,
                                                                                   sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_01
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_01
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_01
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_01
    combined_01 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_l6cancel_01", combined_01)
    print("basic_l6cancel_01...OK")

    print("basic_l6cancel_03...")
    all_matchings = defaultdict(list)
    l4_03, l5_03, l42_03, l52_03 = matchings_basic_pipeline_coh_group_cancellation(om.network, om.kr_handlers,
                                                                                   sim_threshold_attr=0.3,
                                                                                   sim_threshold_rel=0.3,
                                                                                   sem_threshold_attr=0.3,
                                                                                   sem_threshold_rel=0.3,
                                                                                   coh_group_threshold=0.5,
                                                                                   coh_group_size_cutoff=1,
                                                                                   sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_03
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_03
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_03
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_03
    combined_03 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_l6cancel_03", combined_03)
    print("basic_l6cancel_03...OK")

    print("basic_l6cancel_05...")
    all_matchings = defaultdict(list)
    l4_05, l5_05, l42_05, l52_05 = matchings_basic_pipeline_coh_group_cancellation(om.network, om.kr_handlers,
                                                                                   sim_threshold_attr=0.5,
                                                                                   sim_threshold_rel=0.5,
                                                                                   sem_threshold_attr=0.5,
                                                                                   sem_threshold_rel=0.5,
                                                                                   coh_group_threshold=0.5,
                                                                                   coh_group_size_cutoff=1,
                                                                                   sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_05
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_05
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_05
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_05
    combined_05 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_l6cancel_05", combined_05)
    print("basic_l6cancel_05...OK")

    print("basic_l6cancel_07...")
    all_matchings = defaultdict(list)
    l4_07, l5_07, l42_07, l52_07 = matchings_basic_pipeline_coh_group_cancellation(om.network, om.kr_handlers,
                                                                                   sim_threshold_attr=0.7,
                                                                                   sim_threshold_rel=0.7,
                                                                                   sem_threshold_attr=0.7,
                                                                                   sem_threshold_rel=0.7,
                                                                                   coh_group_threshold=0.5,
                                                                                   coh_group_size_cutoff=1,
                                                                                   sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_07
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_07
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_07
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_07
    combined_07 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_l6cancel_07", combined_07)
    print("basic_l6cancel_07...OK")

    print("basic_l6cancel_09...")
    all_matchings = defaultdict(list)
    l4_09, l5_09, l42_09, l52_09 = matchings_basic_pipeline_coh_group_cancellation(om.network, om.kr_handlers,
                                                                                   sim_threshold_attr=0.9,
                                                                                   sim_threshold_rel=0.9,
                                                                                   sem_threshold_attr=0.9,
                                                                                   sem_threshold_rel=0.9,
                                                                                   coh_group_threshold=0.5,
                                                                                   coh_group_size_cutoff=1,
                                                                                   sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_09
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_09
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_09
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_09
    combined_09 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_l6cancel_09", combined_09)
    print("basic_l6cancel_09...OK")

    # Basic pipeline with content
    print("basic_content_01...")
    all_matchings = defaultdict(list)
    l4_01, l5_01, l42_01, l52_01, l1, l7 = matchings_basic_pipeline_coh_group_cancellation_and_content(
        om,
        om.network,
        om.kr_handlers,
        store_client,
        sim_threshold_attr=0.1,
        sim_threshold_rel=0.1,
        sem_threshold_attr=0.1,
        sem_threshold_rel=0.1,
        coh_group_threshold=0.5,
        coh_group_size_cutoff=1,
        sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_01
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_01
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_01
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_01
    all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7
    combined_01 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_content_01", combined_01)
    print("basic_content_01...OK")

    print("basic_content_03...")
    all_matchings = defaultdict(list)
    l4_03, l5_03, l42_03, l52_03, l1, l7 = matchings_basic_pipeline_coh_group_cancellation_and_content(
        om,
        om.network,
        om.kr_handlers,
        store_client,
        sim_threshold_attr=0.3,
        sim_threshold_rel=0.3,
        sem_threshold_attr=0.3,
        sem_threshold_rel=0.3,
        coh_group_threshold=0.5,
        coh_group_size_cutoff=1,
        sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_03
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_03
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_03
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_03
    all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7
    combined_03 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_content_03", combined_03)
    print("basic_content_03...OK")

    print("basic_content_05...")
    all_matchings = defaultdict(list)
    l4_05, l5_05, l42_05, l52_05, l1, l7 = matchings_basic_pipeline_coh_group_cancellation_and_content(
        om,
        om.network,
        om.kr_handlers,
        store_client,
        sim_threshold_attr=0.5,
        sim_threshold_rel=0.5,
        sem_threshold_attr=0.5,
        sem_threshold_rel=0.5,
        coh_group_threshold=0.5,
        coh_group_size_cutoff=1,
        sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_05
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_05
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_05
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_05
    all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7
    combined_05 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_content_05", combined_05)
    print("basic_content_05...OK")

    print("basic_content_07...")
    all_matchings = defaultdict(list)
    l4_07, l5_07, l42_07, l52_07, l1, l7 = matchings_basic_pipeline_coh_group_cancellation_and_content(
        om,
        om.network,
        om.kr_handlers,
        store_client,
        sim_threshold_attr=0.7,
        sim_threshold_rel=0.7,
        sem_threshold_attr=0.7,
        sem_threshold_rel=0.7,
        coh_group_threshold=0.5,
        coh_group_size_cutoff=1,
        sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_07
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_07
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_07
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_07
    all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7
    combined_07 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_content_07", combined_07)
    print("basic_content_07...OK")

    print("basic_content_09...")
    all_matchings = defaultdict(list)
    l4_09, l5_09, l42_09, l52_09, l1, l7 = matchings_basic_pipeline_coh_group_cancellation_and_content(
        om,
        om.network,
        om.kr_handlers,
        store_client,
        sim_threshold_attr=0.7,
        sim_threshold_rel=0.7,
        sem_threshold_attr=0.7,
        sem_threshold_rel=0.7,
        coh_group_threshold=0.5,
        coh_group_size_cutoff=1,
        sensitivity_cancellation_signal=0.4)
    all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1
    all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_09
    all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_09
    all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_09
    all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_09
    all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7
    combined_09 = matcherlib.combine_matchings(all_matchings)
    store_results(path_to_results, "basic_content_09", combined_09)
    print("basic_content_09...OK")


def check_quality_results(path_to_ground_truth_file, path_to_results):
    # Check quality of results
    results = []
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_only_01"))
    results.append(("syn_only_01", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_only_03"))
    results.append(("syn_only_03", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_only_05"))
    results.append(("syn_only_05", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_only_07"))
    results.append(("syn_only_07", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_only_09"))
    results.append(("syn_only_09", precision, recall))

    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_naive_we_01"))
    results.append(("syn_naive_we_01", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_naive_we_03"))
    results.append(("syn_naive_we_03", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_naive_we_05"))
    results.append(("syn_naive_we_05", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_naive_we_07"))
    results.append(("syn_naive_we_07", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "syn_naive_we_09"))
    results.append(("syn_naive_we_09", precision, recall))

    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_01"))
    results.append(("basic_01", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_03"))
    results.append(("basic_03", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_05"))
    results.append(("basic_05", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_07"))
    results.append(("basic_07", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_09"))
    results.append(("basic_09", precision, recall))

    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_l6cancel_01"))
    results.append(("basic_l6cancel_01", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_l6cancel_03"))
    results.append(("basic_l6cancel_03", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_l6cancel_05"))
    results.append(("basic_l6cancel_05", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_l6cancel_07"))
    results.append(("basic_l6cancel_07", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_l6cancel_09"))
    results.append(("basic_l6cancel_09", precision, recall))

    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_content_01"))
    results.append(("basic_content_01", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_content_03"))
    results.append(("basic_content_03", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_content_05"))
    results.append(("basic_content_05", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_content_07"))
    results.append(("basic_content_07", precision, recall))
    precision, recall = compute_pr(path_to_ground_truth_file, (path_to_results + "basic_content_09"))
    results.append(("basic_content_09", precision, recall))

    with open(path_to_results + "RESULTS", 'w') as f:
        for r in results:
            f.write(str(r) + '\n')


def write_matchings_to(path, matchings):
    with open(path, 'w') as f:
        for m in matchings:
            sch, cla = m
            db, relation, attr = sch
            ont, class_name = cla
            matching_str = db + " %%% " + relation + " %%% " + attr + " ==>> " + ont + " %%% " + class_name + " %%% []"
            f.write(matching_str + '\n')


def generate_results_battery_parameters(path_to_serialized_model, onto_name,
                                        path_to_ontology, path_to_sem_model,
                                        path_to_results):

    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    # Create client
    store_client = StoreHandler()

    # Load glove model
    print("Loading language model...")
    glove_api.load_model(path_to_sem_model)
    print("Loading language model...OK")

    # Retrieve indexes
    schema_sim_index = io.deserialize_object(path_to_serialized_model + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object(path_to_serialized_model + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    # Load parsed ontology
    om.add_krs([(onto_name, path_to_ontology)], parsed=True)

    # # Build content sim
    om.priv_build_content_sim(0.6)
    #
    # l1_matchings = []
    # for kr_name, kr_handler in om.kr_handlers.items():
    #     kr_class_signatures = kr_handler.get_classes_signatures()
    #     l1_matchings += om.compare_content_signatures(kr_name, kr_class_signatures)
    #
    # write_matchings_to(path_to_results + 'l1', l1_matchings)

    l7_matchings = matcherlib.find_hierarchy_content_fuzzy(om.kr_handlers, store_client)
    write_matchings_to(path_to_results + 'l7', l7_matchings)

    l4_matchings_01 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.1)
    write_matchings_to(path_to_results + 'l4_01', l4_matchings_01)
    l4_matchings_02 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.2)
    write_matchings_to(path_to_results + 'l4_02', l4_matchings_02)
    l4_matchings_03 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.3)
    write_matchings_to(path_to_results + 'l4_03', l4_matchings_03)


    l5_matchings_01 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                        minhash_sim_threshold=0.1)
    write_matchings_to(path_to_results + 'l5_01', l5_matchings_01)

    l5_matchings_02 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                        minhash_sim_threshold=0.2)
    write_matchings_to(path_to_results + 'l5_02', l5_matchings_02)

    l5_matchings_03 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                        minhash_sim_threshold=0.3)
    write_matchings_to(path_to_results + 'l5_03', l5_matchings_03)

    l42_matchings_04, neg_l42_matchings_02 = matcherlib.find_relation_class_name_sem_matchings(network, om.kr_handlers,
                                                                                         sem_sim_threshold=0.4,
                                                                                         negative_signal_threshold=0.2,
                                                                                         add_exact_matches=False,
                                                                                         penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l42_04', l42_matchings_04)
    write_matchings_to(path_to_results + 'neg_l42_02', neg_l42_matchings_02)
    l42_matchings_04 = None
    neg_l42_matchings_02 = None

    l42_matchings_05, neg_l42_matchings_03 = matcherlib.find_relation_class_name_sem_matchings(network, om.kr_handlers,
                                                                                         sem_sim_threshold=0.5,
                                                                                         negative_signal_threshold=0.3,
                                                                                         add_exact_matches=False,
                                                                                         penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l42_05', l42_matchings_05)
    write_matchings_to(path_to_results + 'neg_l42_03', neg_l42_matchings_03)

    l42_matchings_06, neg_l42_matchings_04 = matcherlib.find_relation_class_name_sem_matchings(network, om.kr_handlers,
                                                                                         sem_sim_threshold=0.6,
                                                                                         negative_signal_threshold=0.4,
                                                                                         add_exact_matches=False,
                                                                                         penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l42_06', l42_matchings_06)
    write_matchings_to(path_to_results + 'neg_l42_04', neg_l42_matchings_04)
    l42_matchings_06 = None
    neg_l42_matchings_04 = None

    l42_matchings_07, neg_l42_matchings_05 = matcherlib.find_relation_class_name_sem_matchings(network, om.kr_handlers,
                                                                                         sem_sim_threshold=0.7,
                                                                                         negative_signal_threshold=0.5,
                                                                                         add_exact_matches=False,
                                                                                         penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l42_07', l42_matchings_07)
    write_matchings_to(path_to_results + 'neg_l42_05', neg_l42_matchings_05)

    l52_matchings_04, neg_l52_matchings_02 = matcherlib.find_relation_class_attr_name_sem_matchings(network, om.kr_handlers,
                                                                                          semantic_sim_threshold=0.4,
                                                                                          negative_signal_threshold=0.2,
                                                                                          add_exact_matches=False,
                                                                                          penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l52_04', l52_matchings_04)
    write_matchings_to(path_to_results + 'neg_l52_02', neg_l52_matchings_02)
    l52_matchings_04 = None
    neg_l52_matchings_02 = None

    l52_matchings_05, neg_l52_matchings_03 = matcherlib.find_relation_class_attr_name_sem_matchings(network, om.kr_handlers,
                                                                                          semantic_sim_threshold=0.5,
                                                                                          negative_signal_threshold=0.3,
                                                                                          penalize_unknown_word=True,
                                                                                          add_exact_matches=False)
    write_matchings_to(path_to_results + 'l52_05', l52_matchings_05)
    write_matchings_to(path_to_results + 'neg_l52_03', neg_l52_matchings_03)

    l52_matchings_06, neg_l52_matchings_04 = matcherlib.find_relation_class_attr_name_sem_matchings(network, om.kr_handlers,
                                                                                          semantic_sim_threshold=0.6,
                                                                                          negative_signal_threshold=0.4,
                                                                                            add_exact_matches=False,
                                                                                            penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l52_06', l52_matchings_06)
    write_matchings_to(path_to_results + 'neg_l52_04', neg_l52_matchings_04)
    l52_matchings_06 = None
    neg_l52_matchings_04 = None

    l52_matchings_07, neg_l52_matchings_05 = matcherlib.find_relation_class_attr_name_sem_matchings(network, om.kr_handlers,
                                                                                          semantic_sim_threshold=0.7,
                                                                                          negative_signal_threshold=0.5,
                                                                                          add_exact_matches=False,
                                                                                          penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l52_07', l52_matchings_07)
    write_matchings_to(path_to_results + 'neg_l52_05', neg_l52_matchings_05)

    l6_matchings_03_1, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.3,
                                                                   group_size_cutoff=1)
    write_matchings_to(path_to_results + 'l6_03_1', l6_matchings_03_1)

    l6_matchings_03_2, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.3,
                                                                   group_size_cutoff=2)
    write_matchings_to(path_to_results + 'l6_03_2', l6_matchings_03_2)

    l6_matchings_03_3, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.3,
                                                                   group_size_cutoff=3)
    write_matchings_to(path_to_results + 'l6_03_3', l6_matchings_03_3)

    l6_matchings_04_1, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.4,
                                                                   group_size_cutoff=1)
    write_matchings_to(path_to_results + 'l6_04_1', l6_matchings_04_1)

    l6_matchings_04_2, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.4,
                                                                   group_size_cutoff=2)
    write_matchings_to(path_to_results + 'l6_04_2', l6_matchings_04_2)

    l6_matchings_04_3, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.4,
                                                                   group_size_cutoff=3)
    write_matchings_to(path_to_results + 'l6_04_3', l6_matchings_04_3)

    l6_matchings_05_1, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.5,
                                                                   group_size_cutoff=1)
    write_matchings_to(path_to_results + 'l6_05_1', l6_matchings_05_1)

    l6_matchings_05_2, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.5,
                                                                   group_size_cutoff=2)
    write_matchings_to(path_to_results + 'l6_05_2', l6_matchings_05_2)
    l6_matchings_05_3, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                   sem_sim_threshold=0.5,
                                                                   group_size_cutoff=3)
    write_matchings_to(path_to_results + 'l6_05_3', l6_matchings_05_3)


def generate_results_battery_parameters2(path_to_serialized_model, onto_name,
                                            path_to_ontology, path_to_sem_model,
                                            path_to_results):
    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    # Create client
    store_client = StoreHandler()

    # Load glove model
    print("Loading language model...")
    glove_api.load_model(path_to_sem_model)
    print("Loading language model...OK")

    # Retrieve indexes
    schema_sim_index = io.deserialize_object(path_to_serialized_model + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object(path_to_serialized_model + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    # Load parsed ontology
    om.add_krs([(onto_name, path_to_ontology)], parsed=True)

    # Build content sim

    l4_matchings_04 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.4)
    write_matchings_to(path_to_results + 'l4_04', l4_matchings_04)
    l4_matchings_05 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.5)
    write_matchings_to(path_to_results + 'l4_05', l4_matchings_05)

    l4_matchings_06 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.6)
    write_matchings_to(path_to_results + 'l4_06', l4_matchings_06)

    l4_matchings_07 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.7)
    write_matchings_to(path_to_results + 'l4_07', l4_matchings_07)

    l4_matchings_08 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                minhash_sim_threshold=0.8)
    write_matchings_to(path_to_results + 'l4_08', l4_matchings_08)

    l4_matchings_09 = matcherlib.find_relation_class_name_matchings(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.9)
    write_matchings_to(path_to_results + 'l4_09', l4_matchings_09)

    l5_matchings_04 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                        minhash_sim_threshold=0.4)
    write_matchings_to(path_to_results + 'l5_04', l5_matchings_04)

    l5_matchings_05 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                        minhash_sim_threshold=0.5)
    write_matchings_to(path_to_results + 'l5_05', l5_matchings_05)

    l5_matchings_06 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                        minhash_sim_threshold=0.6)
    write_matchings_to(path_to_results + 'l5_06', l5_matchings_06)

    l5_matchings_07 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                        minhash_sim_threshold=0.7)
    write_matchings_to(path_to_results + 'l5_07', l5_matchings_07)

    l5_matchings_08 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.8)
    write_matchings_to(path_to_results + 'l5_08', l5_matchings_08)

    l5_matchings_09 = matcherlib.find_relation_class_attr_name_matching(network, om.kr_handlers,
                                                                    minhash_sim_threshold=0.9)
    write_matchings_to(path_to_results + 'l5_09', l5_matchings_09)

    l42_matchings_08, neg_l42_matchings_01 = matcherlib.find_relation_class_name_sem_matchings(network, om.kr_handlers,
                                                                                               sem_sim_threshold=0.8,
                                                                                               negative_signal_threshold=0.1,
                                                                                               add_exact_matches=False,
                                                                                               penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l42_08', l42_matchings_08)
    write_matchings_to(path_to_results + 'neg_l42_01', neg_l42_matchings_01)
    l42_matchings_08 = None
    neg_l42_matchings_01 = None

    l42_matchings_09, neg_l42_matchings_06 = matcherlib.find_relation_class_name_sem_matchings(network, om.kr_handlers,
                                                                                               sem_sim_threshold=0.9,
                                                                                               negative_signal_threshold=0.6,
                                                                                               add_exact_matches=False,
                                                                                               penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l42_09', l42_matchings_09)
    write_matchings_to(path_to_results + 'neg_l42_06', neg_l42_matchings_06)
    l42_matchings_09 = None
    neg_l42_matchings_06 = None

    l42_matchings_03, neg_l42_matchings_07 = matcherlib.find_relation_class_name_sem_matchings(network, om.kr_handlers,
                                                                                               sem_sim_threshold=0.3,
                                                                                               negative_signal_threshold=0.7,
                                                                                               add_exact_matches=False,
                                                                                               penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l42_03', l42_matchings_03)
    write_matchings_to(path_to_results + 'neg_l42_07', neg_l42_matchings_07)
    l42_matchings_03 = None
    neg_l42_matchings_07 = None

    l52_matchings_08, neg_l52_matchings_01 = matcherlib.find_relation_class_attr_name_sem_matchings(network, om.kr_handlers,
                                                                                                    semantic_sim_threshold=0.8,
                                                                                                    negative_signal_threshold=0.1,
                                                                                                    add_exact_matches=False,
                                                                                                    penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l52_08', l52_matchings_08)
    write_matchings_to(path_to_results + 'neg_l52_01', neg_l52_matchings_01)
    l52_matchings_08 = None
    neg_l52_matchings_01 = None

    l52_matchings_09, neg_l52_matchings_06 = matcherlib.find_relation_class_attr_name_sem_matchings(network, om.kr_handlers,
                                                                                                    semantic_sim_threshold=0.9,
                                                                                                    negative_signal_threshold=0.6,
                                                                                                    add_exact_matches=False,
                                                                                                    penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l52_09', l52_matchings_09)
    write_matchings_to(path_to_results + 'neg_l52_06', neg_l52_matchings_06)
    l52_matchings_09 = None
    neg_l52_matchings_06 = None

    l52_matchings_03, neg_l52_matchings_07 = matcherlib.find_relation_class_attr_name_sem_matchings(network, om.kr_handlers,
                                                                                                    semantic_sim_threshold=0.3,
                                                                                                    negative_signal_threshold=0.7,
                                                                                                    add_exact_matches=False,
                                                                                                    penalize_unknown_word=True)
    write_matchings_to(path_to_results + 'l52_03', l52_matchings_03)
    write_matchings_to(path_to_results + 'neg_l52_07', neg_l52_matchings_07)
    l52_matchings_03 = None
    neg_l52_matchings_07 = None

    l6_matchings_02_1, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.2,
                                                                        group_size_cutoff=1)
    write_matchings_to(path_to_results + 'l6_02_1', l6_matchings_02_1)

    l6_matchings_02_2, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.2,
                                                                        group_size_cutoff=2)
    write_matchings_to(path_to_results + 'l6_02_2', l6_matchings_02_2)

    l6_matchings_02_3, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.2,
                                                                        group_size_cutoff=3)
    write_matchings_to(path_to_results + 'l6_02_3', l6_matchings_02_3)

    l6_matchings_06_1, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.6,
                                                                        group_size_cutoff=1)
    write_matchings_to(path_to_results + 'l6_06_1', l6_matchings_06_1)

    l6_matchings_06_2, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.6,
                                                                        group_size_cutoff=2)
    write_matchings_to(path_to_results + 'l6_06_2', l6_matchings_06_2)

    l6_matchings_06_3, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.6,
                                                                        group_size_cutoff=3)
    write_matchings_to(path_to_results + 'l6_06_3', l6_matchings_06_3)

    l6_matchings_07_1, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.7,
                                                                        group_size_cutoff=1)
    write_matchings_to(path_to_results + 'l6_07_1', l6_matchings_07_1)

    l6_matchings_07_2, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.7,
                                                                        group_size_cutoff=2)
    write_matchings_to(path_to_results + 'l6_07_2', l6_matchings_07_2)

    l6_matchings_07_3, table_groups = matcherlib.find_sem_coh_matchings(network, om.kr_handlers,
                                                                        sem_sim_threshold=0.7,
                                                                        group_size_cutoff=3)
    write_matchings_to(path_to_results + 'l6_07_3', l6_matchings_07_3)


def read(path):
    matchings = []
    with open(path, 'r') as f:
        #lines = f.readlines()
        for l in f:
            tokens = l.split("==>>")
            sch = tokens[0]
            cla = tokens[1]
            sch_tokens = sch.split("%%%")
            sch_tokens = [t.strip() for t in sch_tokens]
            cla_tokens = cla.split("%%%")
            cla_tokens = [t.strip() for t in cla_tokens]
            matching_format = ((sch_tokens[0], sch_tokens[1], sch_tokens[2]), (cla_tokens[0], cla_tokens[1]))
            matchings.append(matching_format)
    return matchings


def local_test(path_to_raw_data, path_to_ground_truth_file):
    # Getting ground truth
    with open(path_to_ground_truth_file, 'r') as gt:
        ground_truth_matchings_strings = gt.readlines()

    def parse_strings(list_of_strings):
        # format is: db %%% table %%% attr ==>> onto %%% class_name %%% list_of_matchers
        matchings = []
        for l in list_of_strings:
            tokens = l.split("==>>")
            sch = tokens[0]
            cla = tokens[1]
            sch_tokens = sch.split("%%%")
            sch_tokens = [t.strip() for t in sch_tokens]
            cla_tokens = cla.split("%%%")
            cla_tokens = [t.strip() for t in cla_tokens]
            matching_format = (((sch_tokens[0], sch_tokens[1], sch_tokens[2]), (cla_tokens[0], cla_tokens[1])))
            matchings.append(matching_format)
        return matchings

    ground_truth_matchings = parse_strings(ground_truth_matchings_strings)

    # Deserialize model
    network = fieldnetwork.deserialize_network("../models/chembl22/")
    # Create client
    store_client = StoreHandler()

    # Retrieve indexes
    schema_sim_index = io.deserialize_object("../models/chembl22/" + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object("../models/chembl22/" + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    om.add_krs([("efo", "cache_onto/efo.pkl")], parsed=True)

    def list_from_dict(combined):
        l = []
        for k, v in combined.items():
            matchings = v.get_matchings()
            for el in matchings:
                l.append(el)
        return l

    def syn_and_we(l4, l5, l42, l52, name):
        all_matchings = defaultdict(list)
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
        all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42
        all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52
        combined = matcherlib.combine_matchings(all_matchings)
        combined_list = list_from_dict(combined)

        print(str(len(combined_list)))
        precision, recall = compute_pr_matchings(ground_truth_matchings, combined_list)

        combined_sum = matcherlib.summarize_matchings_to_ancestor(om.kr_handlers["efo"], combined_list)
        precision_sum, recall_sum = compute_pr_matchings(ground_truth_matchings, combined_sum)
        print(name + ", " + str(precision) + ", " + str(recall))
        print(name + "_sum, " + str(precision_sum) + ", " + str(recall_sum))
        return combined_list, combined_sum

    def test1(name, l4=[], l5=[], nl42=[], nl52=[], l42=[], l52=[], l1=[], l7=[], l6=[]):

        # 1-. l4 - l42
        l4_dict = dict()
        l4_cancelled = []
        for matching in l4:
            l4_dict[matching] = 1
        total_cancelled = 0
        for m in nl42:
            if m in l4_dict:
                total_cancelled += 1
                l4_cancelled.append(m)
                l4.remove(m)
        print("total l4 cancelled: " + str(total_cancelled))

        print("#######")
        print("cancelled matching in gts: ")
        gt_set = set(ground_truth_matchings)
        for m in l4_cancelled:
            if m in gt_set:
                continue
                print(m)
        print("#######")

        # 2-. l5 - l52
        l5_dict = dict()
        l5_cancelled = []
        for matching in l5:
            l5_dict[matching] = 1
        total_cancelled = 0
        for m in nl52:
            if m in l5_dict:
                total_cancelled += 1
                l5_cancelled.append(m)
                l5.remove(m)
        print("total l5 cancelled: " + str(total_cancelled))

        print("#######")
        print("cancelled matching in gts: ")
        gt_set = set(ground_truth_matchings)
        for m in l5_cancelled:
            if m in gt_set:
                continue
                print(m)
        print("#######")

        # print("l42 orig: " + str(len(l42)))
        # print("l52 orig: " + str(len(l52)))
        # l6_set = set(l6)
        # l42_set = set(l42)
        # l52_set = set(l52)
        # for m in l42:
        #     if m not in l6:
        #         l42_set.remove(m)
        # for m in l52:
        #     if m not in l6:
        #         l52_set.remove(m)
        # l42 = list(l42_set)
        # l52 = list(l52_set)
        #
        # print("l42 orig: " + str(len(l42)))
        # print("l52 orig: " + str(len(l52)))

        # 3- structural summary l42 and l52
        if len(l42) > 0:
            l42 = matcherlib.summarize_matchings_to_ancestor(om, l42, summarize_or_remove=True)
        if len(l52) > 0:
            l52 = matcherlib.summarize_matchings_to_ancestor(om, l52, summarize_or_remove=True)

        print("SUM L42")
        for m in l42:
            continue
            print(str(m))

        print("SUM L52")
        for m in l52:
            continue
            print(str(m))

        #l4 = matcherlib.summarize_matchings_to_ancestor(om.kr_handlers["efo"], l4, summarize_or_remove=True)
        #l5 = matcherlib.summarize_matchings_to_ancestor(om.kr_handlers["efo"], l5, summarize_or_remove=True)

        # 4-. combine l1, l7, l4, l5
        all_matchings = defaultdict(list)
        all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1
        all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
        all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42
        all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52

        combined = matcherlib.combine_matchings(all_matchings)
        combined_list = list_from_dict(combined)

        # 5-. summarize l1, l7, l4, l5
        print(str(len(combined_list)))
        precision, recall = compute_pr_matchings(ground_truth_matchings, combined_list)

        combined_sum = matcherlib.summarize_matchings_to_ancestor(om, combined_list, summarize_or_remove=True)
        precision_sum, recall_sum = compute_pr_matchings(ground_truth_matchings, combined_sum)
        print(name + ", " + str(precision) + ", " + str(recall))
        print(name + "_sum, " + str(precision_sum) + ", " + str(recall_sum))

        return combined_list, combined_sum

        # def all_matchings_from_combined_list(combined_matchings):
        #     l4 = []
        #     l5 = []
        #     l1 = []
        #     l7 = []
        #     for m, type in combined_matchings:
        #         if MatchingType.L1_CLASSNAME_ATTRVALUE in type:
        #             l1.append(m)
        #         if MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY in type:
        #             l7.append(m)
        #         if MatchingType.L4_CLASSNAME_RELATIONNAME_SYN in type:
        #             l4.append(m)
        #         if MatchingType.L5_CLASSNAME_ATTRNAME_SYN in type:
        #             l5.append(m)
        #
        #     return l1, l7, l4, l5
        #
        # # 6-. add curated l42 and l52
        # l1, l7, l4, l5 = all_matchings_from_combined_list(combined_sum)
        #
        # all_matchings = defaultdict(list)
        # all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1
        # all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7
        # all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
        # all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
        # all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42
        # all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52
        #
        # combined = matcherlib.combine_matchings(all_matchings)
        # combined_list = list_from_dict(combined)
        #
        # print(str(len(combined_list)))
        # precision, recall = compute_pr_matchings(ground_truth_matchings, combined_list)
        #
        # combined_sum = matcherlib.summarize_matchings_to_ancestor(om.kr_handlers["efo"], combined_list, summarize_or_remove=False)
        # precision_sum, recall_sum = compute_pr_matchings(ground_truth_matchings, combined_sum)
        # print(name + ", " + str(precision) + ", " + str(recall))
        # print(name + "_sum, " + str(precision_sum) + ", " + str(recall_sum))
        # return combined_list, combined_sum

    l1 = read(path_to_raw_data + "l1")
    l7 = read(path_to_raw_data + "l7")
    l4 = read(path_to_raw_data + "l4_02")  #l4_02 0.015/0.46 - 0.33/0.38 (summary=2)
    l5 = read(path_to_raw_data + "l5_02")  #l5_02
    l42 = read(path_to_raw_data + "l42_07")  #l42_05
    l52 = read(path_to_raw_data + "l52_06")  #l52_05
    nl42 = read(path_to_raw_data + "neg_l42_05")  #neg_l42_05
    nl52 = read(path_to_raw_data + "neg_l52_05")  #neg_l42_05
    l6 = read(path_to_raw_data + "l6_05_2")  #neg_l42_05

    #combined_list, combined_sum = syn_and_we(l4, l5, l42, l52, "syn_and_we_02_03")
    # combined_list, combined_sum = test1("test1", l4, l5, nl42=nl42, nl52=nl52, l42=l42, l52=l52, l1=l1, l7=l7)

    # combined_list, combined_sum = test1("test1", l4=l4, l5=l5, nl42=[], nl52=[], l42=[], l52=[], l1=[], l7=[])
    # combined_list, combined_sum = test1("test1", l4=l4, l5=l5, nl42=nl42, nl52=nl52, l42=[], l52=[], l1=[], l7=[])
    # combined_list, combined_sum = test1("test1", l4=l4, l5=l5, nl42=nl42, nl52=nl52, l42=l42, l52=l52, l1=[], l7=[])
    combined_list, combined_sum = test1("test1", l4=l4, l5=l5, nl42=nl42, nl52=nl52,
                                        l42=l42, l52=l52, l1=l1, l7=l7, l6=l6)

    print("num combined_list: " + str(len(combined_list)))
    for m in combined_list:
        continue
        print(str(m))

    print("SUMMARY")

    print("num combined_sum: " + str(len(combined_sum)))
    for m in combined_sum:
        continue
        print(str(m))

    print("the matchings that we could not find - list")
    print("#############")
    gt_set = set(ground_truth_matchings)
    missed_list = []
    for m in gt_set:
        if m not in combined_list:
            continue
            print(m)
            missed_list.append(m)
    print("#############")

    print("the matchings that we could not find - sum")
    print("#############")
    missed_sum = []
    gt_set = set(ground_truth_matchings)
    for m in gt_set:
        if m not in combined_sum:
            continue
            print(m)
            missed_sum.append(m)
    print("#############")

    print("so the matchings removed by summary are: ")
    for m in missed_sum:
        if m not in missed_list:
            continue
            print(m)

    return combined_list, combined_sum, om


def combine_and_report_results(path_to_raw_data, path_to_ground_truth_file):

    # Getting ground truth
    with open(path_to_ground_truth_file, 'r') as gt:
        ground_truth_matchings_strings = gt.readlines()

    def parse_strings(list_of_strings):
        # format is: db %%% table %%% attr ==>> onto %%% class_name %%% list_of_matchers
        matchings = []
        for l in list_of_strings:
            tokens = l.split("==>>")
            sch = tokens[0]
            cla = tokens[1]
            sch_tokens = sch.split("%%%")
            sch_tokens = [t.strip() for t in sch_tokens]
            cla_tokens = cla.split("%%%")
            cla_tokens = [t.strip() for t in cla_tokens]
            matching_format = (((sch_tokens[0], sch_tokens[1], sch_tokens[2]), (cla_tokens[0], cla_tokens[1])))
            matchings.append(matching_format)
        return matchings

    ground_truth_matchings = parse_strings(ground_truth_matchings_strings)

    # Deserialize model
    network = fieldnetwork.deserialize_network("../models/chembl22/")
    # Create client
    store_client = StoreHandler()

    # Retrieve indexes
    schema_sim_index = io.deserialize_object("../models/chembl22/" + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object("../models/chembl22/" + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    om.add_krs([("efo", "cache_onto/efo.pkl")], parsed=True)

    def list_from_dict(combined):
        l = []
        for k, v in combined.items():
            matchings = v.get_matchings()
            for el in matchings:
                l.append(el)
        return l

    def syn_only(l4, l5, name):
        # l4 and l5 01
        all_matchings = defaultdict(list)
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
        combined = matcherlib.combine_matchings(all_matchings)
        combined_list = list_from_dict(combined)

        print(str(len(combined_list)))
        precision, recall = compute_pr_matchings(ground_truth_matchings, combined_list)

        combined_sum = matcherlib.summarize_matchings_to_ancestor(om.kr_handlers["efo"], combined_list)
        precision_sum, recall_sum = compute_pr_matchings(ground_truth_matchings, combined_sum)
        print(name + ", " + str(precision) + ", " + str(recall))
        print(name + "_sum, " + str(precision_sum) + ", " + str(recall_sum))

    def syn_and_we(l4, l5, l42, l52, name):
        all_matchings = defaultdict(list)
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
        all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42
        all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52
        combined = matcherlib.combine_matchings(all_matchings)
        combined_list = list_from_dict(combined)

        print(str(len(combined_list)))
        precision, recall = compute_pr_matchings(ground_truth_matchings, combined_list)

        combined_sum = matcherlib.summarize_matchings_to_ancestor(om.kr_handlers["efo"], combined_list)
        precision_sum, recall_sum = compute_pr_matchings(ground_truth_matchings, combined_sum)
        print(name + ", " + str(precision) + ", " + str(recall))
        print(name + "_sum, " + str(precision_sum) + ", " + str(recall_sum))

    def syn_we_c4(l4, l5, l42, l52, nl42, name):
        l4_dict = dict()
        for matching in l4:
            l4_dict[matching] = 1
        total_cancelled = 0
        for m in nl42:
            if m in l4_dict:
                total_cancelled += 1
                l4.remove(m)

        all_matchings = defaultdict(list)
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
        all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42
        all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52
        combined = matcherlib.combine_matchings(all_matchings)
        combined_list = list_from_dict(combined)

        print(str(len(combined_list)))
        precision, recall = compute_pr_matchings(ground_truth_matchings, combined_list)

        combined_sum = matcherlib.summarize_matchings_to_ancestor(om.kr_handlers["efo"], combined_list)
        precision_sum, recall_sum = compute_pr_matchings(ground_truth_matchings, combined_sum)
        print(name + ", " + str(precision) + ", " + str(recall))
        print(name + "_sum, " + str(precision_sum) + ", " + str(recall_sum))

    def syn_we_c4_c5(l4, l5, l42, l52, nl42, nl52, name):
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

        all_matchings = defaultdict(list)
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
        all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42
        all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52
        combined = matcherlib.combine_matchings(all_matchings)
        combined_list = list_from_dict(combined)

        print(str(len(combined_list)))
        precision, recall = compute_pr_matchings(ground_truth_matchings, combined_list)

        combined_sum = matcherlib.summarize_matchings_to_ancestor(om.kr_handlers["efo"], combined_list)
        precision_sum, recall_sum = compute_pr_matchings(ground_truth_matchings, combined_sum)
        print(name + ", " + str(precision) + ", " + str(recall))
        print(name + "_sum, " + str(precision_sum) + ", " + str(recall_sum))

    def syn_we_c4_c5_c6(l4, l5, l6, l42, l52, nl42, nl52, name, cuttingRatio, summaryT):
        print("Started computation ... ")
        l4_dict = dict()
        for matching in l4:
            l4_dict[matching] = 1
        total_cancelled = 0
        for m in nl42:
            if m in l4_dict:
                total_cancelled += 1
                l4.remove(m)
        l4_dict = None

        l5_dict = dict()
        for matching in l5:
            l5_dict[matching] = 1
        total_cancelled = 0
        for m in nl52:
            if m in l5_dict:
                total_cancelled += 1
                l5.remove(m)
        l5_dict = None

        l6_dict = dict()
        for matching in l6:
            l6_dict[matching] = 1

        # curate l42 with l6
        removed_l42 = 0
        for m in l42:
            if m not in l6_dict:
                removed_l42 += 1
                l42.remove(m)
        print("rem-l42: " + str(removed_l42))

        # curate l52 with l6
        # (('chemical', 'activity_stds_lookup', 'std_act_id'), ('efo', 'Metabolomic Profiling'))
        # (('chemical', 'activity_stds_lookup', '_'), ('efo', 'Experimental Factor'))
        removed_l52 = 0
        for m in l52:
            db, relation, attr = m[0]
            el = ((db, relation, '_'), m[1])
            if el not in l6_dict:
                # db_to_remove, rel_to_remove, _ = m[0]
                # for el in l52:
                #     sch, cla = el
                #     db, relation, attr = sch
                #     if db == db_to_remove and relation == rel_to_remove:
                #         if el in l52:
                removed_l52 += 1
                l52.remove(m)
        print("rem-l52: " + str(removed_l52))

        all_matchings = defaultdict(list)
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5
        all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42
        all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52
        combined = matcherlib.combine_matchings(all_matchings)
        combined_list = list_from_dict(combined)

        print("Compute precision/recall: {}".format(str(len(combined_list))))
        precision, recall = compute_pr_matchings(ground_truth_matchings, combined_list)
        print("StructS ... ")
        combined_sum = matcherlib.summarize_matchings_to_ancestor(om, combined_list,
                                                                  threshold_to_summarize=summaryT,
                                                                  summary_ratio=cuttingRatio)
        precision_sum, recall_sum = compute_pr_matchings(ground_truth_matchings, combined_sum)
        print(name + ", " + str(precision) + ", " + str(recall))
        print(name + "_sum, " + str(precision_sum) + ", " + str(recall_sum))

        return precision, recall, precision_sum, recall_sum

    print("Syn only")
    # l4 and l5 01


    # syn and we

    # # syn and we c4
    #

    # # syn and we c4 and c5
    #

    neg = 1
    six = 2
    pos = 5
    neg_l42 = read(path_to_raw_data + "neg_l42_0{}".format(neg))
    neg_l52 = read(path_to_raw_data + "neg_l52_0{}".format(neg))

    # for six in tqdm(range(2, 8, 2)):
    l6 = read(path_to_raw_data + "l6_0{}_1".format(six))

    # for pos in tqdm(range(3, 10)):
    l42 = read(path_to_raw_data + "l42_0{}".format(pos))
    l52 = read(path_to_raw_data + "l52_0{}".format(pos))

    results = dict()
    # for neg in tqdm(range(1, 3)):
    for st in tqdm(np.arange(1, 5.5, 0.5)):
        print("\t{}".format(st))
        for cr in tqdm(np.arange(0.5, 0.99, 0.1)):
            print("\t\t{}".format(cr))
            i = 1
            # for i in tqdm(range(2, 10, 2)):
            l4 = read(path_to_raw_data + "l4_0{}".format(i))
            l5 = read(path_to_raw_data + "l5_0{}".format(i))
            name = "st_{}_cr_{}".format(st, cr)
            p, r, ps, rs = syn_we_c4_c5_c6(l4, l5, l6, l42, l52, neg_l42, neg_l52, name, cr, st)
            results[name] = (p, r, ps, rs)

    # f = open('results.txt', 'rw')
    # for r in results.keys():
    #     f.write("{}:{}\n".format(r, str(results[r])))
    # f.close()


    # last we add l1 and l7 to whatever is the best configuration


if __name__ == "__main__":

    # Load glove model
    print("Loading language model...")
    path_to_glove_model = "../glove/glove.6B.100d.txt"
    glove_api.load_model(path_to_glove_model)
    print("Loading language model...OK")

    print("Benchmarking matchers and linkers")
    # relative_merit_one_onto("../models/chembl22/",
    #                         "efo",
    #                         "cache_onto/efo.pkl",
    #                         "../glove/glove.6B.100d.txt",
    #                         "results/")

    generate_results_battery_parameters("../models/chembl22/", "efo", "cache_onto/efo.pkl",
                                        "../glove/glove.6B.100d.txt", "raw/")
    #
    # generate_results_battery_parameters2("../models/chembl22/", "efo", "cache_onto/efo.pkl",
    #                                     "../glove/glove.6B.100d.txt", "raw/")

    # best_config("../models/chembl22/", "efo", "cache_onto/efo.pkl", "../glove/glove.6B.100d.txt", "results/")

    #precision, recall = compute_pr("MATCHINGS_GROUND_TRUTH_CHEMBL", "results/" + "best_config")

    #print("Precision: " + str(precision))
    #print("Recall   : " + str(recall))

    #check_quality_results("MATCHINGS_GROUND_TRUTH_CHEMBL", "results/")

    # combine_and_report_results("raw/", "MATCHINGS_GROUND_TRUTH_CHEMBL")

    # cl, cs, om = local_test("raw/", "MATCHINGS_GROUND_TRUTH_CHEMBL")
    # exit()

    # cs = [(el, "none") for el in cs]
    #
    # links = om.find_links(cs)
    #
    # fanout = defaultdict(int)
    # for sch, prop, cla in links:
    #     fanout[sch] += 1
    #
    # for k, v, in fanout.items():
    #     print(str(k) + " fanout: " + str(v))
    #
    # print("LINKS TOTAL: " + str(len(links)))
    # for l in links:
    #     print(str(l))
