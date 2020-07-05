import argparse
import datetime as dt
import pickle
import csv
from operator import itemgetter

import gensim.models as models
import mlflow
from tqdm import tqdm

from .blocking import execute_blocking
from .utils import *


NGT_NOT_FOUND = ANNOY_NOT_FOUND = FAISS_NOT_FOUND = False

try:
    import ngtpy
except ModuleNotFoundError:
    warnings.warn('ngtpy not found. NGT indexing will not be available.')
    NGT_NOT_FOUND = True

try:
    import faiss
except ModuleNotFoundError:
    warnings.warn('faiss not found. faiss indexing will not be available.')
    FAISS_NOT_FOUND = True

try:
    from gensim.similarities.index import AnnoyIndexer
except ImportError:
    warnings.warn('AnnoyIndexer not found. Annoy indexing will not be available.')
    ANNOY_NOT_FOUND = True



def parse_args():
    '''Argument parser for standalone execution. 
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', action='store', required=True, type=str)
    parser.add_argument('-m', '--matches_file', action='store', required=True, type=str)
    parser.add_argument('--n_top', default=5, type=int, help='Number of neighbors to choose from.')
    parser.add_argument('--n_candidates', default=1, type=int, help='Number of candidates to choose among the n_top '
                                                                    'neighbors.')
    parser.add_argument('--info_file', default='', type=str, required=True)

    return parser.parse_args()


def _check_symmetry(target, most_similar, n_top):
    valid = []
    for cand in most_similar[target]:
        mm = most_similar[cand]
        if target in mm[:n_top]:
            valid.append(cand)
    return valid


def build_similarity_structure(model_file, viable_lines, n_items, strategy,
                               n_top=10, n_candidates=1, num_trees=None, epsilon=None, df=None):
    t_start = dt.datetime.now()
    most_similar = {}
    c = 1

    nodes = [_.split(' ', maxsplit=1)[0] for _ in viable_lines]
    # viable_lines = []

    if strategy == 'annoy' and ANNOY_NOT_FOUND:
        warnings.warn('Chosen strategy = \'annoy\', but the module is not installed. Falling back to basic.')
        strategy = 'basic'
    if strategy == 'ngt' and NGT_NOT_FOUND:
        warnings.warn('Chosen strategy = \'NGT\', but the module is not installed. Falling back to basic.')
        strategy = 'basic'
    if strategy == 'faiss' and FAISS_NOT_FOUND:
        warnings.warn('Chosen strategy = \'faiss\', but the module is not installed. Falling back to basic.')
        strategy = 'basic'

    if strategy == 'basic':
        model = models.KeyedVectors.load_word2vec_format(model_file, unicode_errors='ignore')

        for n in tqdm(nodes):
            ms = model.most_similar(str(n), topn=n_top)
            mm = [item[0] for item in ms]
            idx = int(n.split('__')[1])
            if idx < n_items:
                candidates = [_ for _ in mm if int(_.split('__')[1]) >= n_items]
            else:
                candidates = [_ for _ in mm if int(_.split('__')[1]) < n_items]

            candidates = candidates[:n_candidates]
            most_similar[n] = candidates
            c += 1
        print('')

    elif strategy == 'annoy':
        assert num_trees is not None
        assert type(num_trees) == int
        assert num_trees > 0

        print('Using ANNOY indexing.')
        model = models.KeyedVectors.load_word2vec_format(model_file, unicode_errors='ignore')
        annoy_index = AnnoyIndexer(model, num_trees=num_trees)
        for n in tqdm(nodes):
            ms = model.most_similar(str(n), topn=n_top, indexer=annoy_index)
            mm = [item[0] for item in ms]
            idx = int(n.split('__')[1])
            if idx < n_items:
                candidates = [_ for _ in mm if int(_.split('__')[1]) >= n_items]
            else:
                candidates = [_ for _ in mm if int(_.split('__')[1]) < n_items]

            candidates = candidates[:n_candidates]
            most_similar[n] = candidates
            print('\rBuilding similarity structure: {:0.1f} - {}/{} tuples'.format(c / len(nodes) * 100, c, len(nodes)),
                  end='')
            c += 1
        print('')

    elif strategy == 'lsh':
        print('Using DeepER LSH blocking.')
        blocking_candidates = execute_blocking(model_file)
        model = models.KeyedVectors.load_word2vec_format(model_file, unicode_errors='ignore')
        for n in blocking_candidates:
            ms = []
            bucket = blocking_candidates[n]
            for cand in bucket:
                ms.append((cand, model.similarity(n, cand)))
            ms.sort(key=itemgetter(1), reverse=True)

            mm = [item[0] for item in ms]
            idx = int(n.split('_')[1])
            if idx < n_items:
                candidates = [_ for _ in mm if idx >= n_items]
            else:
                candidates = [_ for _ in mm if idx < n_items]

            candidates = candidates[:n_candidates]
            most_similar[n] = candidates
            print('\rBuilding similarity structure: {:0.1f} - {}/{} tuples'.format(c / len(nodes) * 100, c, len(nodes)),
                  end='')
            c += 1
        print('')

    elif strategy == 'ngt':
        assert epsilon is not None
        assert type(epsilon) == float
        assert 0 <= epsilon <= 1

        print('Using NGT indexing.')
        ngt_index_path = 'pipeline/dump/ngt_index.nn'
        words = []
        with open(model_file, 'r') as fp:
            n, dim = map(int, fp.readline().split())
            ngtpy.create(ngt_index_path, dim, distance_type='Cosine')
            index = ngtpy.Index(ngt_index_path)

            for idx, line in enumerate(fp):
                k, v = line.rstrip().split(' ', maxsplit=1)
                vector = list(map(float, v.split(' ')))
                index.insert(vector)  # insert objects
                words.append(k)

        index.build_index()
        index.save()
        most_similar = {}

        for n in tqdm(nodes):
            query = index.get_object(words.index(n))
            ms = index.search(query, size=n_top, epsilon=epsilon)
            mm = [item[0] for item in ms[1:]]
            mm = list(map(words.__getitem__, mm))
            idx = int(n.split('_')[1])
            if idx < n_items:
                candidates = [_ for _ in mm if idx >= n_items]
            else:
                candidates = [_ for _ in mm if idx < n_items]

            candidates = candidates[:n_candidates]
            most_similar[n] = candidates
            print('\rBuilding similarity structure: {:0.1f} - {}/{} tuples'.format(c / len(nodes) * 100, c, len(nodes)),
                  end='')
            c += 1
        print('')

    elif strategy == 'faiss':
        print('Using faiss indexing.')
        # ngt_index_path = 'pipeline/dump/ngt_index.nn'
        words = []
        with open(model_file, 'r') as fp:
            n, dim = map(int, fp.readline().split())
            mat = []
            index = faiss.IndexFlatL2(dim)
            for idx, line in enumerate(fp):
                k, v = line.rstrip().split(' ', maxsplit=1)
                vector = np.array(list(map(float, v.split(' '))), ndmin=1).astype('float32')
                mat.append(vector)
                words.append(k)

        mat = np.array(mat)
        index.add(mat)

        most_similar = {}

        D, I = index.search(mat, k=n_top+1)
        # D, I = index.search(query, size=n_top, epsilon=epsilon)
        # mm = [item[0] for item in ms[1:]]
        # mm = list(map(words.__getitem__, mm))
        for n in tqdm(nodes):
            idx = int(n.split('__')[1])
            mm = I[idx]
            if idx < n_items:
                candidates = [_ for _ in mm if idx >= n_items]
            else:
                candidates = [_ for _ in mm if idx < n_items]

            candidates = candidates[:n_candidates]
            most_similar[n] = ['idx__{}'.format(_) for _ in candidates]
        # print('\rBuilding similarity structure: {:0.1f} - {}/{} tuples'.format(c / len(nodes) * 100, c, len(nodes)),
        #       end='')
        c += 1
        print('')



    else:
        raise ValueError('Unknown strategy {0}'.format(strategy))

    t_end = dt.datetime.now()
    diff = t_end - t_start
    print('Time required to build sim struct: {}'.format(diff.total_seconds()))
    pickle.dump(most_similar, open('most_similar.pickle', 'wb'))

    return most_similar


def compare_ground_truth_only(most_similar, matches_file, n_items, n_top):
    """
    Test the accuracy of matches by
    :param most_similar:
    :param matches_file:
    :param n_items:
    :param n_top:
    """
    # mlflow.active_run()
    matches = _read_matches(matches_file)

    in_ground_truth = set()
    for tup in matches.items():
        tmp = [tup[0]] + tup[1]
        for _ in tmp:
            in_ground_truth.add(_)

    prefix = list(matches.keys())[0].split('_')[0]

    count_miss = count_hit = 0
    iteration_counter = 0
    total_candidates = no_candidate_found = 0
    false_candidates = 0
    golden_candidates = 0

    csvfile = open('suspicious_matches.csv', 'w')
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(['id1','id2'])

    for n in range(n_items):
        item = prefix + '__' + str(n)
        # Take the n_top closest neighbors of item
        try:
            # Extract only the name of the neighbors
            candidates = _check_symmetry(item, most_similar, n_top)
            if item in matches:
                for val in candidates:
                    if val in matches[item]:
                        count_hit += 1
                    else:
                        count_miss += 1
                if len(candidates) == 0: no_candidate_found += 1
                total_candidates += len(candidates)
                false_candidates += len(candidates)
            else:
                for val in candidates:
                    if val in in_ground_truth:
                        count_miss += 1
                        total_candidates += 1
            golden_candidates+=len(candidates)
        except KeyError:
            if item in matches: count_miss += 1

        iteration_counter += 1
    if total_candidates < 1: precision = 0
    else: precision = count_hit / total_candidates
    recall = count_hit / len(matches)
    try:
        f1_score = 2 * (precision * recall) / (precision + recall)
    except ZeroDivisionError:
        f1_score = 0

    if golden_candidates < 1: golden_precision = 0
    else: golden_precision = count_hit / golden_candidates
    try:
        golden_f1 = 2 * (golden_precision * recall) / (golden_precision + recall)
    except ZeroDivisionError:
        golden_f1 = 0

    print('Total candidates tested: {}'.format(total_candidates))
    print('{} cases where no candidates were found'.format(no_candidate_found))
    result_dict = {
        'P': precision,
        'R': recall,
        'F': f1_score,
        'GP': golden_precision,
        'GR': recall,
        'GF': golden_f1,
    }

    # mlflow.log_metric('ER_p', golden_precision)
    # mlflow.log_metric('ER_r', recall)
    # mlflow.log_metric('ER_f', golden_f1)
    print('P\tR\tF\tGP\tGR\tGF')
    for _ in result_dict.values():
        print('{:.4f}\t'.format(_*100), end='')
    print('\r')
    # print(s.format([_*100 for _ in ]))
    print('Correct: {}\nWrong: {}\nTotal items: {}\nTotal matches: {}'.format(count_hit, count_miss,
                                                                              iteration_counter, len(matches)))
    return result_dict


def perform_matching(most_similar):
    matches = []
    for idx in most_similar:
        for m in most_similar[idx]:
            i1 = idx.split('_')[1]
            i2 = m.split('_')[1]
            t = sorted([i1, i2])
            matches.append(tuple(['idx_{}'.format(_) for _ in t]))
    return matches


def entity_resolution(input_file: str, configuration: dict, df: pd.DataFrame = None,
                                 task: str = 'test', info_file: str = None):
    t_start = dt.datetime.now()

    n_top = configuration['ntop']
    n_candidates = configuration['ncand']
    strategy = configuration['indexing']
    matches_file = configuration['match_file']

    model_file, viable_lines = _prepare_tests(input_file)
    with open(info_file, 'r', encoding='utf-8') as fp:
        line = fp.readline()
        n_items = int(line.split(',')[1])

    most_similar = build_similarity_structure(model_file, viable_lines, n_items, strategy, n_top, n_candidates,
                                              epsilon=configuration['epsilon'], num_trees=configuration['num_trees'],
                                              df=df)
    if task == 'test':
        dict_result = compare_ground_truth_only(most_similar, matches_file, n_items, n_top)
    elif task == 'match':
        matches = perform_matching(most_similar)
    else:
        raise ValueError('Unknown task {}'.format(task))

    t1 = dt.datetime.now()
    str_start_time = t1.strftime(TIME_FORMAT)
    t_end = dt.datetime.now()
    diff = t_end - t_start
    print('Time required to execute the ER task: {}'.format(diff.total_seconds()))

    if configuration['mlflow']:
        mlflow.active_run()
        mlflow.log_param('embeddings_file', input_file)
        mlflow.log_param('ntop', n_top)
        mlflow.log_param('indexing', strategy)

    if task == 'test':
        return dict_result
    elif task == 'match':
        return matches
    else:
        return None


def _read_matches(matches_file):
    matches = {}
    n_lines = 0
    with open(matches_file, 'r', encoding='utf-8') as fp:
        for n, line in enumerate(fp.readlines()):
            if len(line.strip()) > 0:
                item, match = line.replace('_', '__').split(',')
                if item not in matches:
                    matches[item] = [match.strip()]
                else:
                    matches[item].append(match.strip())
                n_lines = n
        if n_lines == 0:
            raise IOError('Matches file is empty. ')
    return matches


def _prepare_tests(model_file):
    with open(model_file, 'r', encoding='utf-8') as fp:
        s = fp.readline()
        _, dimensions = s.strip().split(' ')
        viable_idx = []
        for i, row in enumerate(fp):
            if i >= 0:
                idx, vec = row.split(' ', maxsplit=1)
                if idx.startswith('idx__'):
                    try:
                        prefix, n = idx.split('__')
                        n = int(n)
                    except ValueError:
                        continue
                    viable_idx.append(row)
        # viable_idx = [row for idx, row in enumerate(fp) if idx > 0 and row.startswith('idx_')]

    f = 'pipeline/dump/indices.emb'
    with open(f, 'w', encoding='utf-8') as fp:
        fp.write('{} {}\n'.format(len(viable_idx), dimensions))
        for _ in viable_idx:
            fp.write(_)

    return f, viable_idx


if __name__ == '__main__':
    args = parse_args()

    experiment_name = 'ER'
    id_exp = mlflow.set_experiment(experiment_name)

    for val in [1]:
        with mlflow.start_run():
            entity_resolution(args.input_file, args.matches_file, val, args.n_candidates, args.info_file)
