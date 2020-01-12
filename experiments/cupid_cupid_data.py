import itertools
import os
import re

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy import argmax
from tqdm import tqdm

from os import listdir
from os.path import isfile, join

from cupid.cupid_model import Cupid
from cupid.tree_match import tree_match, recompute_wsim, mapping_generation_leaves, mapping_generation_non_leaves


def make_model(file_path1, file_path2):
    cupid_model = Cupid()

    df1 = pd.read_csv(file_path1)
    table_list1 = df1['table'].unique()

    for table in table_list1:
        column_data1 = zip(df1[df1['table'] == table]['column'], itertools.repeat('string'))
        cupid_model.add_data(file_path1, table, column_data1)

    df2 = pd.read_csv(file_path2)
    table_list2 = df2['table'].unique()

    for table in table_list2:
        column_data2 = zip(df2[df2['table'] == table]['column'], itertools.repeat('string'))
        cupid_model.add_data(file_path2, table, column_data2)

    return cupid_model


def write_mappings(mappings, filename):
    f = open(filename, 'w+')
    for m in mappings:
        f.write(str(m))
        f.write("\n")
    f.close()


def run_experiments():
    file1 = '../data/cupid/rdb_schema.csv'
    file2 = '../data/cupid/star_schema.csv'
    cupid_model = make_model(file1, file2)

    print('Computing matchings ... ')
    source_tree = cupid_model.get_schema_by_index(0)
    target_tree = cupid_model.get_schema_by_index(1)

    # i = 0.37
    # j = 0.5
    factor = 0.01
    for j in tqdm(np.arange(0.3, 1.0, 0.1)):
        dirname = 'cupid-output/out/j-' + str(j)
        os.mkdir(dirname)
        for i in tqdm(np.arange(0.05, 0.5, 0.02)):
            sims = tree_match(source_tree, target_tree, cupid_model.get_categories(), th_accept=i, th_low=i - factor, th_high=i + factor,
                              leaf_w_struct=j, w_struct=j + 0.1, th_ns=0.45)
            # new_sims = recompute_wsim(source_tree, target_tree, sims, th_accept=i)
            map1 = mapping_generation_leaves(source_tree, target_tree, sims, th_accept=i)
            # map2 = mapping_generation_non_leaves(source_tree, target_tree, new_sims, th_accept=i)
            print("Leaf matchings:\n {}".format(map1))
            # print("Non-leaf matchings:\n {}".format(map2))

            write_mappings(map1, '{}/test_{}.txt'.format(dirname, i))
    # write_mappings(map2, 'cupid-output/non-leaf_{}.txt'.format(i))


def read_tuple_file(filepath):
    list_tuples = list()
    f = open(filepath, 'r')
    lines = f.readlines()
    for line in lines:
        y = re.search("('*\w+[\(FK\)]*__\w+[\(FK\)]*'*), *('*\w+[\(FK\)]*__\w+[\(FK\)]*'*)", line)
        if y and len(y.groups()) == 2:
            list_tuples.append((re.search("(\w+__\w+)", y.group(1)).group(), re.search("(\w+__\w+)", y.group(2)).group()))
    f.close()
    return list_tuples


def compute_precision(golden_standard, mappings):
    if len(mappings) == 0:
        return 0

    matches = [item for item in golden_standard if item in mappings]
    return len(matches) / len(mappings)


def compute_recall(golden_standard, mappings):
    if len(mappings) == 0:
        return 0

    matches = [item for item in golden_standard if item in mappings]
    return len(matches) / len(golden_standard)


def compute_f1_score(precision, recall):
    f1 = 2 * (precision * recall) / (precision + recall)
    return f1


def make_plot(x, precision_list, recall_list, f1_list, name):
    plt.figure()
    plt.plot(x, precision_list, color='skyblue', linewidth=2, label='Precision')
    plt.plot(x, recall_list, color='green', linewidth=2, label='Recall')
    plt.plot(x, f1_list, color='red', linewidth=2, label="F1-score")
    plt.plot(x[argmax(f1_list)], max(f1_list), color='red', linewidth=2, marker="o")
    plt.legend()
    # plt.xticks([i for j in (np.arange(0.05, x[argmax(f1_list)] - 0.05, 0.05),
    #                         np.arange(x[argmax(f1_list)], 0.5, 0.05)) for i in j])
    plt.xlabel('Threshold')
    plt.ylabel('Value')
    plt.title('Precision/Recall/F1-score given threshold')
    # plt.show()
    plt.savefig('cupid_{}.pdf'.format(name), dpi=300)


def make_output_size(x, sizes):
    plt.plot(x, sizes, color='black', linewidth=2, label='Precision')
    plt.xlabel('Threshold')
    plt.ylabel('# matches')
    plt.title('Number of matches given threshold')
    plt.show()
    # plt.savefig('big_data.pdf', dpi=300)


def compute_statistics():
    golden_standard_file = 'cupid-output/golden_standard.txt'
    golden_standard = read_tuple_file(golden_standard_file)
    path = 'cupid-output/out/'
    x = np.arange(0.05, 0.5, 0.02)

    dirs = [join(path, f) for f in listdir(path) if not isfile(join(path, f))]
    dirs.sort()

    count = 0
    for dir in dirs:
        files = [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]
        files.sort()

    # f = open('cupid-output/stats.txt', 'w+')
    # x = np.arange(0.05, 0.5, 0.01)
        precision_list = list()
        recall_list = list()
        f1_list = list()
        data_set_size = list()

        for file in files:
            tuples = read_tuple_file(file)

            precision = compute_precision(golden_standard, tuples)
            recall = compute_recall(golden_standard, tuples)
            f1 = compute_f1_score(precision, recall)

            data_set_size.append(len(tuples))
            precision_list.append(precision)
            recall_list.append(recall)
            f1_list.append(f1)

            # f.write('Filename: {}\n\tPrecision: {}\n\tRecall: {}\n\tF1-score: {}\n'.format(file, precision, recall, f1))
        # f.close()

        make_plot(x, precision_list, recall_list, f1_list, count)
        count = count + 1
    # make_output_size(x[np.where(np.array(data_set_size) < 500)[0][5]:],
    #                  data_set_size[np.where(np.array(data_set_size) < 500)[0][5]:])
    # make_output_size(x, data_set_size)


if __name__ == '__main__':
    run_experiments()
    # compute_statistics()
