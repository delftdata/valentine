import pandas as pd
import numpy as np
import hashlib
import re
import random as rd
import datetime
from datasketch import MinHash, MinHashLSH, MinHashLSHForest
from similarity.levenshtein import Levenshtein
from similarity.normalized_levenshtein import NormalizedLevenshtein

import sys
# sys.path.append('../')

from collections import Counter
import matplotlib.pyplot as plt
import EmbDI.data_preprocessing as dp

dataset_name = 'walmart_amazon'

test = 'EQ'
if test == 'SM':
    wr = 'sm'
    concatenate='horizon'
elif test == 'ER':
    wr = 'er'
    concatenate='outer'
else:
    wr = 'eq'
    concatenate = 'outer'

f1 = 'pipeline/datasets/imdb_movielens/imdb.csv'
# f1 = 'pipeline/datasets/dblp_acm/acm.csv'
# f1 = 'pipeline/datasets/dblp_scholar/dblp.csv'
f1 = 'pipeline/experiments/{}/exp_data/tableA.csv'.format(dataset_name)
df1 = pd.read_csv(f1, encoding='utf-8')

f2 = 'pipeline/datasets/imdb_movielens/movielens.csv'
# f2 = 'pipeline/datasets/dblp_scholar/google_scholar.csv'
# f2 = 'pipeline/datasets/dblp_acm/dblp.csv'
f2 = 'pipeline/experiments/{}/exp_data/tableB.csv'.format(dataset_name)
df2 = pd.read_csv(f2, encoding='utf-8')

for case in ['tokenize']:
# for case in ['flatten', 'heuristic', 'tokenize']:
    if case == 'heuristic':
        t_shared = True
    else:
        t_shared = False
    parameters = {
        'output_file': '{}-{}'.format(dataset_name, case),
        'concatenate': concatenate,
        'missing_value': 'nan,ukn,none,unknown,-,',
        'missing_value_strategy': '',
        'round_number': 1,
        'round_columns': '',
        'auto_merge': False,
        'split_columns': 'genre',
        'split_delimiter': ',',
        'tokenize_shared': t_shared,
        'expand_columns': ','.join([_.lower() for _ in df1.columns])
    }

    df_c = dp.data_preprocessing([df1.drop('id', axis=1), df2.drop('id', axis=1)], parameters)
    # df_c = dp.data_preprocessing([df1, df2], parameters)
    # df_c = df_c.drop('id', axis=1)
    dfile = '{}-{}-{}.csv'.format(dataset_name,case,wr)
    df_c.to_csv('pipeline/datasets/{}/'.format(dataset_name) + dfile, index=False)

    pars = '''smoothing_method:no
window_size:3
n_dimensions:300
sentence_length:60
walks_strategy:basic
ntop:10
ncand:1
max_rank:3
learning_method:skipgram
training_algorithm:word2vec
n_sentences:default
experiment_type:{}
task:train-test
with_cid:all
with_rid:first
numeric:no
backtrack:True
match_file:
write_walks:True
output_file:
input_file:
test_dir:
flatten:
embeddings_file:
intersection:true'''.format(test).split('\n')

    parameters = {_.split(':')[0]: _.split(':')[1] for _ in pars}

    parameters['input_file'] = 'pipeline/datasets/{}/{}'.format(dataset_name, dfile)
    parameters['match_file'] = 'pipeline/matches/matches-{}.txt'.format(dataset_name)
    parameters['dataset_info'] = 'pipeline/info/info-{}.txt'.format(dataset_name)
    parameters['output_file'] = '{}-{}-{}'.format(dataset_name, wr, case)
    parameters['embeddings_file'] = 'pipeline/embeddings/{}.embs'.format(parameters['output_file'])
    if case == 'tokenize':
        parameters['flatten'] = 'false'
        parameters['intersection'] = 'true'
    else:
        parameters['flatten'] = 'true'
        parameters['intersection'] = 'false'

    if test == 'EQ':
        parameters['intersection'] = 'false'

    with open('pipeline/config_files/{}/{}-{}-{}'.format(dataset_name, dataset_name, wr, case), 'w') as fp:
        for k, v in parameters.items():
            s = '{}:{}\n'.format(k, v)
            fp.write(s)
if test == 'SM':
    matches_name = '{}-matches-{}.txt'.format(wr,dataset_name)
    with open('pipeline/matches/' + matches_name, 'w') as fp:
        lcol = len(df_c.columns)//2
        for _ in range(lcol):
            fp.write('{},{}\n'.format(df_c.columns[_], df_c.columns[_+lcol]))

