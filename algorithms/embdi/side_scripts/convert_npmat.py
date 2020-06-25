import numpy as np
from algorithms.embdi.EmbDI.graph import Graph
import pandas as pd

dataset_name = 'walmart_amazon'
case = 'master'
dataset_variant = dataset_name + '-{}'.format(case)

dirpath = 'pipeline/datasets/tograph/convert/'
fi = 'pipeline/embeddings/harp/convert/{}-harp.npy'.format(dataset_variant)
mat = np.load('pipeline/embeddings/harp/convert/{}-harp.npy'.format(dataset_variant))
# dataset_name = fi.split('.')[0]
# dataset_name = dataset_name.rsplit('_', maxsplit=1)[0]
print(dataset_name)
df = pd.read_csv(dirpath + dataset_variant + '.csv')
for c in df.columns:
    df[c] = df[c].astype('str')

if case == 'heuristic':
    flatten = True
else:
    flatten = False

print(case, flatten)
configuration = {
    'smoothing_method': 'no',
    'flatten': flatten,
}

list_sim = None

g = Graph(df, sim_list=list_sim, smoothing_method=configuration['smoothing_method'], flatten=configuration['flatten'])

d = dict()
for idx, k in enumerate(g.nodes):
    d[idx] = k

emb_file = 'pipeline/embeddings/harp/emb/' + dataset_variant + '-harp.emb'

# mat = np.load('pipeline/embeddings/harp/' + dataset_name + '_harp.npy')
print(emb_file)

with open(emb_file, 'w') as fp:
    fp.write('{} {}\n'.format(*mat.shape))
    for idx, line in enumerate(mat):
        vec = [str(_) for _ in line.tolist()]
        try:
            s = '{} {}\n'.format(d[idx], ' '.join(vec))
            fp.write(s)
        except KeyError:
            continue
