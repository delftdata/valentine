import argparse
import datetime

import mlflow
import warnings
import os
import pickle

import sys

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    from algorithms.embdi.EmbDI.embeddings import learn_embeddings
    from algorithms.embdi.EmbDI.graph_generation_strategies import generate_graph_cell_class
    from algorithms.embdi.EmbDI.sentence_generation_strategies import generate_walks_driver
    from algorithms.embdi.EmbDI.utils import *

    from algorithms.embdi.EmbDI.testing_functions import test_driver_old


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--experiment-name', action='store')
    parser.add_argument('--config-file', action='store', type=str)
    args = parser.parse_args()
    return args


def single_run(configuration, df, ):
    with mlflow.start_run():
        # TODO CHECK THIS CHECK THIS CHECK THIS CHECK THIS
        if 'test_run' not in configuration:
            run_parameters = {
                'n_sentences': configuration['n_sentences'],
                'sentence_length': configuration['sentence_length'],
                'walks_strategy': configuration['walks_strategy']
            }
            mlflow.set_tag('input_file', configuration['input_file'])
            mlflow.log_param('input_file', configuration['input_file'])
            mlflow.log_param('numeric', configuration['numeric'])
            mlflow.log_param('strategy', configuration['walks_strategy'])
            mlflow.log_param('learning_method', configuration['learning_method'])

            start_time = datetime.datetime.now()
            str_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        if not configuration['embeddings_file']:
            # mlflow.log_param('input_file', configuration['input_file'])
            mlflow.log_param('n_sentences', configuration['n_sentences'])
            mlflow.log_param('max_sentence_length', configuration['sentence_length'])
            mlflow.log_param('numeric', configuration['numeric'])

            if configuration['walks_file']:
                print('Reading walks from file {}'.format(configuration['walks_file']))
                walks = []
                with open(configuration['walks_file'], 'r', encoding='utf-8') as fp:
                    for n, line in enumerate(fp.readlines()):
                        walk = line.strip().split(',')
                        # walk = ast.literal_eval(line)
                        walks.append(['{}'.format(_) for _ in walk])
            else:

                if 'with_rid' in configuration:
                    with_rid = configuration['with_rid']
                    # print('Using rid flag {}'.format(with_rid))
                else:
                    with_rid = 'first'
                if 'with_cid' in configuration:
                    with_cid = configuration['with_cid']
                    # print('Using cid flag {}'.format(with_cid))
                else:
                    with_cid = 'all'

                if configuration['experiment_type'] != 'EQ':
                    intersection = find_intersection(df, configuration['dataset_info'])
                    print('# Finding intersection. Number of common values: {}'.format(len(intersection)))
                else:
                    intersection = None
                    with_rid = 'first'

                mlflow.log_param('with_rid', with_rid)
                mlflow.log_param('with_cid', with_cid)

                t1 = datetime.datetime.now()
                if configuration['walks_strategy'] == 'replacement':
                    print('# Reading similarity file {}'.format(configuration['similarity_file']))
                    list_sim = read_similarities(configuration['similarity_file'])
                else:
                    list_sim = None
                if not configuration['graph_file']:
                    print('# {} Starting graph construction'.format(str_start_time))
                    graph, cell_list, rid_list, tokens = generate_graph_cell_class(df, sim_list=list_sim)
                    t2 = datetime.datetime.now()
                    dt = t2 - t1
                    mlflow.log_metric('time_graph', dt.microseconds)
                    str_start_time = t2.strftime('%Y-%m-%d %H:%M:%S')
                    print('# {} Graph construction complete. Generating embeddings'.format(str_start_time))
                    graph_struct = (graph, cell_list, rid_list, tokens)
                    graph_file = 'pipeline/graphs/' + configuration['output_file'] + '.graph'
                    with open(graph_file, 'wb') as gfp:
                        pickle.dump(graph_struct, gfp)
                else:
                    t2 = datetime.datetime.now()
                    str_start_time = t2.strftime('%Y-%m-%d %H:%M:%S')

                    print('# {} Reading graph from file {}'.format(str_start_time, configuration['graph_file']))
                    with open(configuration['graph_file'], 'rb') as fp:
                        graph, cell_list, rid_list, tokens = pickle.load(fp)
                # if False :

                t1 = datetime.datetime.now()

                walks = generate_walks_driver(run_parameters, graph, cell_list, tokens=tokens,
                                              follow_sub=configuration['follow_sub'], df=df, intersection=intersection,
                                              with_rid=with_rid, with_cid=with_cid, numeric=configuration['numeric'])
                t2 = datetime.datetime.now()
                dt = t2 - t1
                mlflow.log_metric('time_walks', dt.microseconds)

                walks_file = 'pipeline/walks/' + configuration['output_file'] + '.walks'
                print('# Writing walks on file {}.'.format(walks_file))
                with open(walks_file, 'w', encoding='utf-8') as fp:
                    for walk in walks:
                        fp.write(','.join(walk))
                        fp.write('\n')

            start_time = datetime.datetime.now()
            str_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            print('# {} Walks complete. Generating embeddings'.format(str_start_time))

            mlflow.log_param('n_dimensions', configuration['n_dimensions'])
            mlflow.log_param('window_size', configuration['window_size'])

            mlflow.log_metric('n_walks', len(walks))

            name_file = configuration['output_file']

            output_file = 'pipeline/embeddings/' + name_file + '.emb'

            t1 = datetime.datetime.now()
            learn_embeddings(output_file, walks, int(configuration['n_dimensions']),
                             int(configuration['window_size']), training_algorithm='word2vec',
                             learning_method=configuration['learning_method'])
            t2 = datetime.datetime.now()
            dt = t2 - t1
            mlflow.log_metric('time_embeddings', dt.microseconds)

            start_time = datetime.datetime.now()
            str_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            print('# {} Embeddings generation complete.'.format(str_start_time))

        else:
            print('Using precomputed embeddings from file {}'.format(configuration['embeddings_file']))
            output_file = configuration['embeddings_file']
        mlflow.log_param('output_file', output_file)
        mlflow.log_artifact(output_file)

        test_driver_old(output_file, df, configuration['experiment_type'],
                        configuration['match_file'],
                        configuration['test_dir'],
                        configuration['dataset_info'],
                        ntop=configuration['ntop'],
                        ncand=configuration['ncand'])

        end_time = datetime.datetime.now()
        str_end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
        print('# {} Execution complete.'.format(str_end_time))
        print('')


def read_configuration(config_file):
    config = {}

    with open(config_file, 'r', encoding='utf-8') as fp:
        for idx, line in enumerate(fp):
            if line[0] == '#':
                continue
            key, value = line.split(':')
            value = value.strip()
            config[key] = value

    return config


if __name__ == '__main__':
    os.makedirs('pipeline/dump', exist_ok=True)
    os.makedirs('pipeline/walks', exist_ok=True)
    os.makedirs('pipeline/embeddings', exist_ok=True)
    os.makedirs('pipeline/graphs', exist_ok=True)

    config_dir = sys.argv[1]
    n_files = len(os.listdir(config_dir)) + 1
    valid_files = [_ for _ in os.listdir(config_dir) if not _.startswith('default')
                   and not os.path.isdir(config_dir + '/' + _)]
    print('Found {} files'.format(valid_files))
    c = 1
    for file in sorted(valid_files):
        if file.startswith('default'):
            continue
        if os.path.isdir(config_dir + '/' + file):
            continue
        print('#' * 80)
        print('# File {} out of {}'.format(c, len(valid_files)))
        print('# Configuration file: {}'.format(file))

        configuration = read_configuration(config_dir + '/' + file)

        if 'test_run' not in configuration:
            configuration = check_config_validity(configuration)

        id_exp = mlflow.set_experiment(configuration['experiment_type'])
        df = pd.read_csv(configuration['input_file'], dtype=str)
        df.fillna('', inplace=True)
        single_run(configuration, df)
        c += 1
