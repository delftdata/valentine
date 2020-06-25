import gensim.models as models
import numpy as np
import pandas as pd
from gensim.models import Word2Vec, FastText
import multiprocessing as mp


def learn_embeddings(walks, write_walks, dimensions, window_size, training_algorithm='word2vec',
                     learning_method='skipgram', workers=mp.cpu_count(), sampling_factor=0.001):
    """Function used to train the embeddings based on the given walks corpus. Multiple parameters are available to
    tweak the training procedure. The resulting embedding file will be saved in the given path to be used later in the
    experimental phase.

    :param walks: path to the walks file (if write_walks == True), list of walks otherwise.
    :param write_walks: flag used to read walks from a file rather than taking them from memory.
    :param dimensions: number of dimensions to be used when training the model
    :param window_size: size of the context window
    :param training_algorithm: either fasttext or word2vec.
    :param learning_method: skipgram or CBOW
    :param workers: number of CPU workers to be used in during the training. Default = mp.cpu_count().
    """
    if training_algorithm == 'word2vec':
        if learning_method == 'skipgram':
            sg = 1
        elif learning_method == 'CBOW':
            sg = 0
        else:
            raise ValueError('Unknown learning method {}'.format(learning_method))
        if write_walks:
            model = Word2Vec(corpus_file=walks, size=dimensions, window=window_size, min_count=2, sg=sg,
                             workers=workers,
                             sample=sampling_factor)
            # model.wv.save_word2vec_format(output_embeddings_file, binary=False)
        else:
            model = Word2Vec(sentences=walks, size=dimensions, window=window_size, min_count=2, sg=sg, workers=workers,
                             sample=sampling_factor)
            # model.wv.save_word2vec_format(output_embeddings_file, binary=False)
    elif training_algorithm == 'fasttext':
        print('Using Fasttext')
        if write_walks:
            model = FastText(corpus_file=walks, window=window_size, min_count=2, workers=workers, size=dimensions)
            # model.wv.save(output_embeddings_file)
        else:
            model = FastText(sentences=walks, size=dimensions, workers=workers, min_count=2, window=window_size)
            # model.wv.save(output_embeddings_file)
    return model

def return_combined(row, wv, n_dimensions):
    vector = []
    for word in row[:]:
        try:
            v = wv.get_vector(word)
        except KeyError:
            v = np.zeros(n_dimensions)
        vector += list(v)
    return vector


def generate_concatenated_file(df: pd.DataFrame, old_emb_file, prefix, n_dimensions=100):
    wv = models.KeyedVectors.load_word2vec_format(old_emb_file, unicode_errors='ignore')
    print('Model built from file {}'.format(old_emb_file))

    concatenated_wv = {}

    for idx, row in df.iterrows():
        concatenated_wv[idx] = return_combined(row, wv, n_dimensions)

    new_emb_file = old_emb_file.split('.')[0] + '_tuples.emb'

    with open(new_emb_file, 'w') as fp:
        fp.write('{} {}\n'.format(str(len(concatenated_wv)), len(concatenated_wv[0])))
        for key in concatenated_wv:
            s = '{}{} '.format(prefix, key)
            for value in concatenated_wv[key]:
                s += '{} '.format(value)
            s = s.strip(' ') + '\n'
            fp.write(s)

    print('Model saved on file {}'.format(new_emb_file))

    return new_emb_file