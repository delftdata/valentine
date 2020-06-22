import argparse

import gensim.models as models

from EmbDI.embeddings import learn_embeddings, generate_concatenated_file
from EmbDI.graph_generation_strategies import generate_graph_cell_class, generate_graph_cell_class_tuple_id, \
    add_rid_connections
from EmbDI.sentence_generation_strategies import generate_walks_driver
from EmbDI.utils import *
from EmbDI.entity_resolution import entity_resolution, _prepare_tests, _read_matches

from timeit import default_timer as timer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', action='store', required=True, type=str, help='Input dataset.')
    parser.add_argument('-o', '--output_file', action='store', required=True, type=str, help='Name of the experiment. ')
    parser.add_argument('--key_as_rid', action='store_true', default=False, help='Use this to use the primary key '
                                                                                 'of a tuple instead of the RID.'
                                                                                 'The input dataset must contain a '
                                                                                 'column labeled \"key\".')
    parser.add_argument('--walks_strategy', action='store', default='basic', type=str,
                        choices=['basic', 'replacement', 'backtracking', 'permutation'],
                        help='Choose the random walk strategy among '
                             'the given possibilities.')
    parser.add_argument('--n_dimensions', action='store', default=100, type=int, help='Number of dimensions to be used'
                                                                                      'when generating embeddings.')
    parser.add_argument('--concatenate', action='store_true', default=False, help='Flag for generating tuple embeddings'
                                                                                  'by concatenating cell embeddings.')
    parser.add_argument('--weights_file', action='store', type=str, default=None, help='Path to weights file for node2vec walks.')
    parser.add_argument('--n_sentences', action='store', default=1e6, type=int, help='Number of random walks to be '
                                                                                     'generated.')
    parser.add_argument('--sentence_length', action='store', default=60, type=int, help='Length of the random walks.')
    parser.add_argument('--window_size', action='store', default=5, type=int, help='Size of word2vec windows.')
    parser.add_argument('--dataset_info', action='store', type=str, required=True, help='Path for file containing '
                                                                                        'dataset path and size.')
    parser.add_argument('--follow_sub', action='store_true', default=False, help='When using replacement, whether to '
                                                                                 'follow the old node or the '
                                                                                 'replacement')
    parser.add_argument('--similarity_file', action='store', type=str, help='Similarity file required when using '
                                                                            'replacement.')

    parser.add_argument('--pca', action='store_true', default=False, help='Flag for reducing the dimensionality of '
                                                                          'concatenated embeddings through PCA.')
    parser.add_argument('--n_components', action='store', type=int, default=300, help='Number of PCA components to be '
                                                                                      'used. It should be the same '
                                                                                      'number as n_dimensions for '
                                                                                      'better results.')
    parser.add_argument('--rotation', action='store_true', default=False, help='Generated and apply the W matrix. ')

    parser.add_argument('--measuring', action='store_true', help='Flag for measuring ER accuracy.')
    parser.add_argument('--n_top', action='store', type=int, default=10, help='Number of neighbors to take candidates'
                                                                             'from. More neighbors may increase '
                                                                             'performance, but evaluation will be '
                                                                             'slower and precision may decrease.')
    parser.add_argument('--n_candidates', action='store', type=int, default=1, help='Number of candidates out of the '
                                                                                    'neighbors to be considered when '
                                                                                    'looking for the match. ')
    parser.add_argument('--matches_file', action='store', type=str, default='', help='Path of the matches file, with '
                                                                                     'format \"idx1,match_idx1\"')

    parser.add_argument('--walks_file', action='store', type=str, default='', help='Path to walks file, for bypassing'
                                                                                   'generation of random walks. ')

    parser.add_argument('--embeddings_file', action='store', type=str, help='Path to embeddings file, for bypassing'
                                                                            'generation of walks and embeddings.')

    parser.add_argument('--synonym_file', action='store', type=str, default='identical_char',
                        help='Path to synonym file, used to provide anchors for the rotation matrix W.')

    parser.add_argument('--eval_file', action='store', type=str, default=None, help='Test file required by the rotation'
                                                                                    'matrix W. ')

    parser.add_argument('--rid_connections', action='store', type=str, default=None, help='Path to candidates for '
                                                                                          'connections between RIDs. ')
    parser.add_argument('--skipgram', action='store_true', default=False)

    parser.add_argument('--save_walks', action='store_true', default=False, help='Use this flag to write walks on disk.')

    return parser.parse_args()


if __name__ == '__main__':
    start_time = timer()
    args = parse_args()
    print_run_info(args)
    check_args_validity(args)

    print('Reading input dataset.')
    df = pd.read_csv(args.input_file)
    for col in df.columns:
        df[col] = df[col].astype('object')
    run_parameters = {
        'n_sentences': args.n_sentences,
        'max_sentence_length': args.sentence_length
    }
    out_temp = args.output_file

    intersection = find_intersection(df, args.dataset_info)

    if not args.embeddings_file:
        print('No embeddings file supplied.')
        if not args.walks_file:
            print('No walks file supplied. Generating graph and random walks according to the required strategy.')
            weights = None
            if args.walks_strategy == 'replacement':
                print('Reading similarity file {}'.format(args.similarity_file))
                list_sim = read_similarities(args.similarity_file)
                run_parameters['strategies'] = 'replacement'
            elif args.walks_strategy == 'backtracking':
                run_parameters['strategies'] = 'backtracking'
                if not args.weights_file:
                    print('Using backtracking with default weights.')
                list_sim = None
            elif args.walks_strategy == 'permutation':
                print('Reading similarity file {}'.format(args.similarity_file))
                list_sim = read_similarities(args.similarity_file)
                run_parameters['strategies'] = 'permutation'
            else:
                list_sim = None
                run_parameters['strategies'] = 'basic'
            # run
            if args.concatenate and args.key_as_rid:
                print('Generating graph, keys are used as rids.')
                graph, cell_list, rid_list, tokens = generate_graph_cell_class_tuple_id(df, weights=weights,
                                                                                        sim_list=list_sim)
                with_rid = True
            else:
                if args.concatenate:
                    print('WARNING: Using concatenation without employing key as rid will produce worse results.')
                    with_rid = False
                else:
                    with_rid = True
                print('Generating basic graph. ')

                graph, cell_list, rid_list, tokens = generate_graph_cell_class(df, weights=weights, sim_list=list_sim)
                if args.rid_connections:
                    graph = add_rid_connections(graph, df, args.rid_connections)

            walks = generate_walks_driver(run_parameters, graph, cell_list, tokens=tokens,
                                          follow_sub=args.follow_sub, with_rid=with_rid, df=df, intersection=intersection)
            walks_file = 'pipeline/walks/' + out_temp + '.walks'

            if args.save_walks:
                print('Writing walks on file {}.'.format(walks_file))
                with open(walks_file, 'w') as fp:
                    for walk in walks:
                        fp.write(','.join(walk))
                        fp.write('\n')
        else:
            print('Reading walks from file {}'.format(args.walks_file))
            walks = []
            with open(args.walks_file, 'r') as fp:
                for n, line in enumerate(fp.readlines()):
                    walk = line.split(',')
                    # walk = ast.literal_eval(line)
                    walks.append(['{}'.format(_) for _ in walk])

        embeddings_file = 'pipeline/embeddings/' + out_temp + '.emb'

        print('Learning embeddings.')
        learn_embeddings(embeddings_file, walks, args.n_dimensions, args.window_size, training_algorithm=args.skipgram)
        print('Embeddings written on file {}.'.format(embeddings_file))
    else:
        embeddings_file = args.embeddings_file
    if args.rotation:
        src_emb, tgt_emb, syn_file = split_embeddings(embeddings_file, args.dataset_info, args.n_dimensions)

        # src_emb = '/home/spoutnik23/PycharmProjects/EmbDI/pipeline/dump/corleone-dblp.emb'
        # tgt_emb = '/home/spoutnik23/PycharmProjects/EmbDI/pipeline/dump/corleone-scholar.emb'

        src_rotated, tgt_rotated = apply_rotation(src_emb, tgt_emb, emb_dim=args.n_dimensions,
                                                  synonym_file=args.synonym_file, eval_file=args.eval_file)

        merged_file = embeddings_file.rsplit('.', maxsplit=1)[0]
        merged_file += '_merged.emb'
        merge_files([src_rotated, tgt_rotated], merged_file)
        # clean_dump()
    else:
        merged_file = embeddings_file
    if args.concatenate:
        emb_file = merged_file
        concatenated_file = generate_concatenated_file(df, emb_file, prefix='idx_', n_dimensions=args.n_dimensions)

        if args.pca:
            reduced_file = concatenated_file.rsplit('.', maxsplit=1)[0] + '_reduced.emb'
            apply_PCA(concatenated_file, reduced_file, n_components=args.n_components)
        else:
            reduced_file = concatenated_file
        test_file = reduced_file
    else:
        test_file = merged_file

    if args.measuring:
        if not args.matches_file:
            raise ValueError('Empty matches file.')

        test_file, viable = _prepare_tests(test_file)
        matches = _read_matches(matches_file=args.matches_file)

        print('Testing with n_top={}, n_candidates={}'.format(args.n_top, args.n_candidates))
        print('Reading model from file {}. '.format(test_file))
        model = models.KeyedVectors.load_word2vec_format(test_file, unicode_errors='ignore')

        entity_resolution(model, matches, args.n_top, args.n_candidates, info_file=args.dataset_info)
