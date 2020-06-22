from algorithms.embdi.EmbDI.embeddings import learn_embeddings
import os.path
import datetime

ds = ''  # dataset name

# lightweight configuration to run the embeddings training algorithm
configuration = {
    'write_walks': True,
    'n_dimensions': 300,
    'window_size': 3,
    'training_algorithm': 'word2vec',
    'learning_method': 'skipgram'
}

walks_file = '../pipeline/walks/{}-permutations.walks'.format(ds)
print('Reading walks file {}'.format(walks_file))
t_start = datetime.datetime.now()
t = '../pipeline/embeddings/' + ds + '-permutations.emb'
learn_embeddings(t, walks_file, write_walks=configuration['write_walks'],
                 dimensions=int(configuration['n_dimensions']),
                 window_size=int(configuration['window_size']),
                 training_algorithm=configuration['training_algorithm'],
                 learning_method=configuration['learning_method'])
t_end = datetime.datetime.now()
d = t_end - t_start

print('Time required: {}'.format(d.total_seconds()))
