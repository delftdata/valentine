from algorithms.base_matcher import BaseMatcher
import re
from text_unidecode import unidecode
from data_loader.instance_loader import InstanceLoader
from algorithms.embdi.edgelist import EdgeList
from operator import itemgetter

from algorithms.embdi.EmbDI.embeddings import learn_embeddings
from algorithms.embdi.EmbDI.sentence_generation_strategies import generate_walks
from algorithms.embdi.EmbDI.utils import *

from algorithms.embdi.EmbDI.graph import Graph
import warnings


class EmbDI(BaseMatcher):

    def __init__(self, with_cid, with_rid, flatten):
        self.with_cid = with_cid
        self.with_rid = with_rid
        self.flatten = flatten

    def get_matches(self, source: InstanceLoader, target: InstanceLoader, dataset_name):
        src_table = preprocess_relation(source.table.as_df)
        trg_table = preprocess_relation(target.table.as_df)
        merged_table = merge_relations(src_table, trg_table)
        el = EdgeList(merged_table)

        edgelist = el.get_edgelist()
        prefixes = el.get_pref()
        configuration = return_default_values(self.flatten, self.with_cid, self.with_rid)
        emb_model = training_driver(configuration, prefixes, edgelist)
        matches_list = calculate_matches(emb_model, merged_table)

        matches = dict()

        for map_pair in matches_list:
            v1, v2, sim = map_pair
            matches[((source.table.name, v1), (target.table.name, v2))] = sim

        matches = dict(filter(lambda elem: elem[1] > 0.0, matches.items()))  # Remove the pairs with zero similarity

        sorted_matches = {k: float(v) for k, v in sorted(matches.items(), key=lambda item: item[1], reverse=True)}

        return sorted_matches


def training_driver(configuration, prefixes, edgelist):
    """
    This function trains local embeddings according to the parameters specified in the configuration.
    The input dataset is transformed into a graph,
    then random walks are generated and the result is passed to the embeddings training algorithm.
    """
    graph = graph_generation(configuration, edgelist, prefixes)

    configuration['n_sentences'] = graph.compute_n_sentences(int(configuration['sentence_length']))
    walks = random_walks_generation(configuration, None, graph)
    del graph  # Graph is not needed anymore, so it is deleted to reduce memory cost
    model = embeddings_generation(walks, configuration)
    return model


def calculate_matches(embeddings, dataset):
    candidates = []
    for _1 in range(len(dataset.columns)):
        for _2 in range(0, len(dataset.columns)):
            if _1 == _2:
                continue
            c1 = dataset.columns[_1]
            c2 = dataset.columns[_2]
            try:
                rank = embeddings.similarity('cid__' + c1, 'cid__' + c2)
                tup = (c1, c2, rank)
                candidates.append(tup)
            except KeyError:
                continue
    cleaned = []
    for k in candidates:
        prefix = k[0].split('_')[0]
        if not k[1].startswith(prefix):
            cleaned.append(k)

    sorted_cand = sorted(cleaned, key=itemgetter(2), reverse=True)

    cands = []
    flag = True
    for value in sorted_cand:
        if flag:
            v1, v2, rank = value
            cands.append(("_".join(v1.split("_")[1:]), "_".join(v2.split("_")[1:]), rank))
            flag = False
        else:
            flag = True

    return cands


def graph_generation(configuration, edgelist, prefixes, dictionary=None):
    # Read external info file to perform replacement.
    if configuration['walks_strategy'] == 'replacement':
        print('# Reading similarity file {}'.format(configuration['similarity_file']))
        list_sim = read_similarities(configuration['similarity_file'])
    else:
        list_sim = None

    if 'flatten' in configuration:
        if configuration['flatten'].lower() not in  ['all', 'false']:
            flatten = configuration['flatten'].strip().split(',')
        elif configuration['flatten'].lower() == 'false':
            flatten = []
        else:
            flatten = 'all'
    else:
        flatten = []
    if dictionary:
        for __ in edgelist:
            l = []
            for _ in __:
                if _ in dictionary:
                    l.append(dictionary[_])
                else:
                    l.append(_)

        # edgelist_file = [dictionary[_] for __ in edgelist_file for _ in __[:2] if _ in dictionary]
    g = Graph(edgelist=edgelist, prefixes=prefixes, sim_list=list_sim, flatten=flatten)

    return g


def random_walks_generation(configuration, df, graph):
    """
    Traverse the graph using different random walks strategies.
    :param configuration: run parameters to be used during the generation
    :param df: input dataframe
    :param graph: graph generated starting from the input dataframe
    :return: the collection of random walks
    """
    # Find values in common between the datasets.
    if configuration['intersection']:
        print('# Finding overlapping values. ')
        # Expansion works better when all tokens are considered, rather than only the overlapping ones.
        if configuration['flatten']:
            warnings.warn('Executing intersection while flatten = True.')
        # Find the intersection
        intersection = find_intersection_flatten(df, configuration['dataset_info'])
        if len(intersection) == 0:
            warnings.warn('Datasets have no tokens in common. Falling back to no-intersection.')
            intersection = None
        else:
            print('# Number of common values: {}'.format(len(intersection)))
    else:
        print('# Skipping search of overlapping values. ')
        intersection = None
        # configuration['with_rid'] = WITH_RID_FIRST

    # Generating walks.
    walks = generate_walks(configuration, graph, intersection=intersection)

    return walks


def embeddings_generation(walks, configuration):
    model = learn_embeddings(walks, write_walks=configuration['write_walks'],
                             dimensions=int(configuration['n_dimensions']),
                             window_size=int(configuration['window_size']),
                             training_algorithm=configuration['training_algorithm'],
                             learning_method=configuration['learning_method'],
                             sampling_factor=configuration['sampling_factor'])
    return model


def preprocess_relation(rel):

    regex = re.compile('[^a-z0-9]')
    rel = rel.fillna('')
    for col in rel.columns:
        rel[col] = rel[col].apply(lambda x: regex.sub('_', unidecode(str.lower(str(x)))))
        final_tokens = []
        for cv in rel[col]:  # remove multiple consequent '_'s
            final_tokens.append('_'.join((' '.join(cv.split('_'))).split()))
        rel[col] = final_tokens

    return rel


def return_default_values(flatten_choice, with_cid_choice, with_rid_choice):
    config = dict()
    default_values = {
        'ntop': 10,
        'ncand': 1,
        'max_rank': 3,
        'follow_sub': False,
        'smoothing_method': 'no',
        'backtrack': True,
        'training_algorithm': 'word2vec',
        'write_walks': False,
        'indexing': 'basic',
        'epsilon': 0.1,
        'num_trees': 250,
        'flatten': flatten_choice,
        'with_cid': with_cid_choice,
        'with_rid': with_rid_choice,
        'compression': False,
        'n_sentences': 'default',
        'walks_strategy': 'basic',
        'learning_method': 'skipgram',
        'sentence_length': 60,
        'window_size': 3,
        'n_dimensions': 300,
        'numeric': 'no',
        'intersection': False,
        'walks_file': None,
        'refinement_task': 'rotation',
        'mlflow': False,
        'repl_numbers': False,
        'repl_strings': False,
        'sampling_factor': 0.001
    }

    for k in default_values:
        config[k] = default_values[k]
    return config


def merge_relations(rel1, rel2):

    cols_to_change = []
    for col in rel1.columns:
        cols_to_change.append('0_' + col)

    rel1.columns = cols_to_change

    cols_to_change = []

    for col in rel2.columns:
        cols_to_change.append('1_' + col)

    rel2.columns = cols_to_change
    rel1['zeros'] = 0
    rel2['ones'] = 1

    merged_rel = pd.merge(left=rel1, right=rel2, how='outer', left_on='zeros', right_on='ones')

    merged_rel = merged_rel.drop(columns=['zeros', 'ones']).fillna('')

    return merged_rel
