import datetime as dt

import numpy as np

from ..side_scripts import lsh


def create_lsh(K, L):
    #create L projects of size K each
    lsh_engine = lsh.index(float('inf'), K, L)
    return lsh_engine


def index_data(lsh_engine, data, dimension=300):
    for i in data:
        vector = data[i]
        # vector = data[i]['vec']
        vector.shape = (dimension, 1)
        lsh_engine.InsertIntoTable(i, vector)


def query_data_non_binary(lsh_engine, query_vec, data, topK=None, multi_probe=False):
    multi_probe_radius = 0
    if multi_probe:
        multi_probe_radius = 2
    # matches = lsh_engine.Find(query_vec, multi_probe_radius, topK)
    matches = lsh_engine.FindExact(query_vec, data.__getitem__, multi_probe_radius)
    if topK is None:
        tuple_ids = [elem[0] for elem in matches]
    else:
        # matches = sorted(matches, key=lambda x: x[1])
        tuple_ids = [elem[0] for elem in matches[:topK]]
    return tuple_ids, len(set(tuple_ids))


def fetch_similar(lsh_engine, data, topK=None, multi_probe=False):
    c = 0
    candidates = {}
    for k in data:
        vector = data[k]
        result, block_size = query_data_non_binary(lsh_engine, vector, data, topK, multi_probe)
        # result = [_ for _ in result if _ in keys]
        if result[0] == k:
            result.pop(0)
        if len(result) > 0:
            candidates[k] = result
        print('\rBLOCKING: generating bins: {:0.1f} - {}/{} tuples'.format(c / len(data) * 100, c, len(data)),
              end='')
        c += 1
    print('')
    return candidates


def execute_blocking(emb_path, K=10, L=10):
    lsh_engine = create_lsh(K, L)

    torch_id_to_vec_hash = {}

    key_to_value_mapping = {}

    print('\n# {} Starting to read index file.'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('BLOCKING: Reading and indexing file.')
    with open(emb_path) as fp:
        n_items, n_dim = fp.readline().split(' ')
        for idx, line in enumerate(fp):
            if len(line) == 0:
                continue
            key, values = line.strip().split(' ', maxsplit=1)
            arr = np.array(values.split(' ')).astype('float64')
            torch_id_to_vec_hash[key] = arr
            arr.shape = (int(n_dim), 1)

            key_to_value_mapping[key] = idx - 1
            # lsh_engine.InsertIntoTable(idx - 1, arr)
            lsh_engine.InsertIntoTable(key, arr)
            print('\rProgress: {:0.1f} - {}/{} tuples'.format(idx / int(n_items) * 100, idx, n_items),
                  end='')

    # print('\nBLOCKING: Indexing data.')
    # index_data(lsh_engine, torch_id_to_vec_hash)

    print('\n# {} Reading complete.'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('BLOCKING: Applying LSH.')
    # the_dict = lsh_engine.CreateDictionary()
    tl = {}
    for pt in torch_id_to_vec_hash:
        the_list = lsh_engine.GetBucketsSum(torch_id_to_vec_hash[pt])
        tl[pt] = the_list

    return fetch_similar(lsh_engine, torch_id_to_vec_hash, topK=10, multi_probe=False)
    # print()
