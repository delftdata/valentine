import argparse
import datetime

import mlflow
import mlflow.tracking as tracking
import mlflow.exceptions as mlexceptions
import warnings

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    from algorithms.embdi.EmbDI.embeddings import learn_embeddings
    from algorithms.embdi.EmbDI.sentence_generation_strategies import generate_walks
    from algorithms.embdi.EmbDI.utils import *

    from algorithms.embdi.EmbDI.testing_functions import test_driver, match_driver
    from algorithms.embdi.EmbDI.graph import Graph
    from algorithms.embdi.EmbDI.logging import *


def parse_args():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-f','--config_file', action='store', default=None)
    group.add_argument('-d','--config_dir', action='store', default=None)
    parser.add_argument('--mlflow', action='store_true', default=False)
    args = parser.parse_args()
    return args


def graph_generation(configuration, edgelist, prefixes, dictionary=None):
    """
    Generate the graph for the given dataframe following the specifications in configuration.
    :param df: dataframe to transform in graph.
    :param configuration: dictionary with all the run parameters
    :return: the generated graph
    """
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
    t_start = datetime.datetime.now()
    print(OUTPUT_FORMAT.format('Starting graph construction', t_start.strftime(TIME_FORMAT)))
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
    t_end = datetime.datetime.now()
    dt = t_end - t_start
    print(OUTPUT_FORMAT.format('Graph construction complete', t_end.strftime(TIME_FORMAT)))
    print(OUTPUT_FORMAT.format('Time required to build graph:', dt.total_seconds()))
    metrics.time_graph = dt.total_seconds()
    return g


def random_walks_generation(configuration, df, graph):
    """
    Traverse the graph using different random walks strategies.
    :param configuration: run parameters to be used during the generation
    :param df: input dataframe
    :param graph: graph generated starting from the input dataframe
    :return: the collection of random walks
    """
    t1 = datetime.datetime.now()
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
    t2 = datetime.datetime.now()
    dt = t2 - t1

    if configuration['mlflow']:
        with mlflow.start_run(run_id=configuration['run_id']):
            # Reporting the intersection flag.
            if intersection is None:
                mlflow.log_param('intersection', False)
            else:
                mlflow.log_param('intersection', True)
            mlflow.log_metric('generated_walks', len(walks))
            mlflow.log_metric('time_walks', dt.total_seconds())
    metrics.time_walks = dt.total_seconds()
    metrics.generated_walks = len(walks)
    return walks


def embeddings_generation(walks, configuration, dictionary):
    """
    Take the generated walks and train embeddings using the walks as training corpus.
    :param walks:
    :param configuration:
    :param dictionary:
    :return:
    """
    t1 = datetime.datetime.now()
    output_file = configuration['run-tag']

    print(OUTPUT_FORMAT.format('Training embeddings', t1))
    t = 'pipeline/embeddings/' + output_file + '.emb'
    print('File: {}'.format(t))
    learn_embeddings(t, walks, write_walks=configuration['write_walks'],
                     dimensions=int(configuration['n_dimensions']),
                     window_size=int(configuration['window_size']),
                     training_algorithm=configuration['training_algorithm'],
                     learning_method=configuration['learning_method'],
                     sampling_factor=configuration['sampling_factor'])
    if configuration['compression']: newf = clean_embeddings_file(t, dictionary)
    else: newf = t
    t2 = datetime.datetime.now()
    dt = t2 - t1
    if configuration['mlflow']:
        with mlflow.start_run(run_id=configuration['run_id']):
            mlflow.log_metric('time_embeddings', dt.total_seconds())
    str_ttime= t2.strftime(TIME_FORMAT)
    print(OUTPUT_FORMAT.format('Embeddings generation complete', str_ttime))

    configuration['embeddings_file'] = newf

    metrics.time_embeddings = dt.total_seconds()
    return configuration




def training_driver(configuration):
    '''This function trains local embeddings according to the parameters specified in the configuration. The input dataset is transformed into a graph,
    then random walks are generated and the result is passed to the embeddings training algorithm. 

    '''

    df = pd.read_csv(configuration['input_file'], dtype=str, index_col=False)
    df = df[df.columns[:2]]
    df.dropna(inplace=True)
    run_tag = configuration['output_file']
    configuration['run-tag'] = run_tag
    # with mlflow.start_run(run_id=configuration['run_id']):
    # If task requires training, execute all the steps needed to generate the embeddings.
    if configuration['task'] in ['train', 'train-test', 'train-match']:
        # Check if walks have been provided. If not, graph and walks will be generated.
        if configuration['walks_file'] is None:
            prefixes, edgelist = read_edgelist(configuration['input_file'])

            if configuration['compression']:  # Execute compression if required.
                df, dictionary = dict_compression_edgelist(df, prefixes=prefixes)
                el = df.values.tolist()
            else:
                dictionary = None
                el = edgelist
            # dictionary=None

            graph = graph_generation(configuration, el, prefixes, dictionary)
            if configuration['n_sentences'] == 'default':
                #  Compute the number of sentences according to the rule of thumb.
                configuration['n_sentences'] = graph.compute_n_sentences(int(configuration['sentence_length']))
            walks = random_walks_generation(configuration, df, graph)
            del graph # Graph is not needed anymore, so it is deleted to reduce memory cost
        else:
            if configuration['compression']:  # Execute compression if required.
                prefixes, edgelist = read_edgelist(configuration['input_file'])
                df, dictionary = dict_compression_edgelist(df, prefixes=prefixes)
            else:
                dictionary = None
            configuration['write_walks'] = True
            walks = configuration['walks_file']
        configuration = embeddings_generation(walks, configuration, dictionary)
    return configuration


def testing_driver(configuration):
    '''Simple caller function for the testing functions.'''
    embeddings_file = configuration['embeddings_file']
    df = pd.read_csv(configuration['input_file'])
    # if configuration['mlflow']:
    #     with mlflow.start_run(configuration['run_id']):
    #         mlflow.log_param('run_name', configuration['embeddings_file'])
    test_driver(embeddings_file, df, configuration)


def matching_driver(configuration):
    # mlflow.active_run()
    embeddings_file = configuration['embeddings_file']
    df = pd.read_csv(configuration['input_file'])

    matches_tuples, matches_columns = match_driver(embeddings_file, df, configuration)

    root_matches = 'pipeline/generated-matches/'
    if 'run-tag' in configuration:
        matches_file = root_matches + configuration['run-tag']
    else:
        matches_file = root_matches + configuration['output_file']
    file_col = matches_file + '_col' + '.matches'
    file_row = matches_file + '_tup' + '.matches'

    with open(file_col, 'w') as fp:
        for m in matches_columns:
            s = '{} {}\n'.format(*m)
            fp.write(s)

    with open(file_row, 'w') as fp:
        for m in matches_tuples:
            s = '{} {}\n'.format(*m)
            fp.write(s)

    # mlflow.log_artifact(file_col)
    # mlflow.log_artifact(file_row)

    return file_row


def refinement_driver(configuration):
    '''Function used to drive refinement tasks. At the moment, only rotation is implemented. 
    '''
    ref_task = configuration['refinement_task']
    if ref_task == 'rotation':
        match_file = matching_driver(configuration)
        (src_emb, tgt_emb), syn_file = split_embeddings(configuration['embeddings_file'],
                                            configuration['dataset_info'],
                                            configuration['n_dimensions'],
                                            configuration)
        complete_syn = 'pipeline/dump/complete_syn'
        with open(complete_syn, 'w') as fp:
            with open(match_file, 'r') as match_fp:
                for idx, row in enumerate(match_fp):
                    fp.write(row)

        src_rotated, tgt_rotated = apply_rotation(src_emb, tgt_emb, emb_dim=configuration['n_dimensions'],
                                                  synonym_file=complete_syn,
                                                  eval_file=configuration['match_file'])

        merged_file = configuration['embeddings_file'].rsplit('.', maxsplit=1)[0]
        merged_file += '_rotated.emb'

        merge_files([src_rotated, tgt_rotated], merged_file)
    else:
        raise ValueError('Unknown refinement task {}'.format(ref_task))


def read_configuration(config_file):
    config = {}

    with open(config_file, 'r') as fp:
        for idx, line in enumerate(fp):
            line = line.strip()
            if len(line) == 0 or line[0] == '#': continue
            split_line = line.split(':')
            if len(split_line) < 2: continue
            else:
                key, value = split_line
                value = value.strip()
                config[key] = value
    return config


def main(file_path=None, dir_path=None, args=None):
    results = None
    configuration = None

    # Building dir tree required to run the code.
    os.makedirs('pipeline/dump', exist_ok=True)
    os.makedirs('pipeline/walks', exist_ok=True)
    os.makedirs('pipeline/embeddings', exist_ok=True)
    os.makedirs('pipeline/generated-matches', exist_ok=True)

    # TODO turn this into configuration parameter
    # Finding the configuration file paths.
    if args:
        if args.config_dir:
            config_dir = args.config_dir
            config_file = None
        else:
            config_dir = None
            config_file = args.config_file
    else:
        config_dir = dir_path
        config_file = file_path

    # Extracting valid files
    if config_dir:
        valid_files = [_ for _ in os.listdir(config_dir) if not _.startswith('default')
                       and not os.path.isdir(config_dir + '/' + _)]
        n_files = len(valid_files)
        print('Found {} files'.format(n_files))
    elif config_file:
        if args:
            valid_files = [os.path.basename(args.config_file)]
            config_dir = os.path.dirname(args.config_file)
        else:
            valid_files = [os.path.basename(config_file)]
            config_dir = os.path.dirname(config_file)

    else:
        raise ValueError('Missing file_path or config_path')


    for idx, file in enumerate(sorted(valid_files)):
        print('#' * 80)
        print('# File {} out of {}'.format(idx+1, len(valid_files)))
        print('# Configuration file: {}'.format(file))
        t_start = datetime.datetime.now()
        print(OUTPUT_FORMAT.format('Starting run.', t_start))
        # Parsing the configuration file.
        configuration = read_configuration(config_dir + '/' + file)
        # Checking the correctness of the configuration, setting default values for missing values.
        configuration = check_config_validity(configuration)

        if configuration['mlflow']:
            mlclient = tracking.MlflowClient()
            # Preparing the mlflow experiment.
            try:
                exp_id = mlclient.create_experiment(configuration['experiment_type'])
            except mlexceptions.MlflowException:
                experiment = mlclient.get_experiment_by_name(configuration['experiment_type'])
                exp_id = experiment.experiment_id
            run = mlclient.create_run(experiment_id=exp_id)
            configuration['run_id'] = run.info.run_id

        # Running the task specified in the configuration file.
        params.par_dict = configuration

        if configuration['task'] == 'train':
            configuration = training_driver(configuration)
        elif configuration['task'] == 'test':
            results = testing_driver(configuration)
            log_params()
        elif configuration['task'] == 'match':
            matching_driver(configuration)
        elif configuration['task'] == 'refinement':
            refinement_driver(configuration)
        elif configuration['task'] == 'train-test':
            configuration = training_driver(configuration)
            results = testing_driver(configuration)
            log_params()
        elif configuration['task'] == 'train-match':
            configuration = training_driver(configuration)
            matching_driver(configuration)
        t_end = datetime.datetime.now()
        print(OUTPUT_FORMAT.format('Ending run.', t_end))
        dt = t_end-t_start
        print('# Time required: {}'.format(dt.total_seconds()))
        if configuration['mlflow']:
            mlflow.log_params(configuration)
            if mem_results.res_dict  is not None:
                mlflow.log_metrics(mem_results.res_dict)
            mlflow.log_metric('time_overall', dt.total_seconds())
            mlflow.end_run()
        # clean_dump()

if __name__ == '__main__':
    args = parse_args()
    main(args=args)
