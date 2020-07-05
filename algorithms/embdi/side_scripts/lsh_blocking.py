#GiG

import csv
import torchfile
from algorithms.embdi.side_scripts import lsh
import numpy

#To ensure reproducibility - feel free to change them
# Use 20 random seeds as LSH is very probabilistic
# the values are got by sorted([random.randint(0, 100000) for gig in range(20)])
#random_seeds = [778, 3527, 3647, 6917, 12539, 12684, 16778, 19300, 27564, 42453, 47877, 50564, 55767, 60546, 60885, 66032, 71986, 90605, 95046, 97852]
random_seeds = [778, 3527, 3647, 6917, 12539, 12684, 16778, 19300, 27564, 42453]



def get_csv_reader(csv_file_name):
    f = open(csv_file_name)
    reader = csv.reader(f)
    next(reader)
    return reader

torch_id_to_vec_hash = {}

def id_to_vec(id_val):
    return torch_id_to_vec_hash[id_val]

def get_torch_data_as_dicts(torch_file_name):
    global torch_id_to_vec_hash

    data = torchfile.load(torch_file_name)
    dict_id_to_index = {}
    dict_vec_to_id = {}
    for i in range(len(data)):
        id_val = data[i]['id']
        dict_id_to_index[id_val] = i
        torch_id_to_vec_hash[id_val] = data[i]['vec']
    return data, dict_id_to_index

#No need to index both datasets
#Index the smaller one and query the bigger one
def index_data(lsh_engine, data, dataset1_size, dimension=300):
    for i in range(dataset1_size):
        #Sara: the following doesnt seem to work after some numpy update
        #vector = data[i]['vec'].reshape((dimension, 1))
        vector = data[i]['vec']
        # vector = data[i]['vec']
        vector.shape = (dimension, 1)
        id_val = data[i]['id']
        # id_val = data[i]['id']
        lsh_engine.InsertIntoTable(id_val, vector)


def create_lsh(K, L):
    #create L projects of size K each
    lsh_engine = lsh.index(float('inf'), K, L)
    return lsh_engine


def query_data_binary(lsh_engine, query_vec, match_id):
    matches = lsh_engine.Find(query_vec)
    tuple_ids = [elem[0] for elem in matches]
    return match_id in tuple_ids


def query_data_non_binary(lsh_engine, query_vec, match_id, topK=None, multi_probe=False):
    #old code that gives a distance proxy which is the number of buckets in which the item fell into same bucket as query vector
    #if multi_probe:
    #    matches = lsh_engine.FindMP(query_vec, 2)
    #else:
    #    matches = lsh_engine.Find(query_vec)
    multi_probe_radius = 0
    if multi_probe:
        multi_probe_radius = 2
    matches = lsh_engine.FindExact(query_vec, id_to_vec, multi_probe_radius)
    if topK is None:
        tuple_ids = [elem[0] for elem in matches]
    else:
        #Old code
        #sorted_matches = sorted(matches, key=lambda x: x[1], reverse=True)
        sorted_matches = sorted(matches, key=lambda x: x[1])
        tuple_ids = [elem[0] for elem in matches[:topK]]
    return match_id in tuple_ids, len(set(tuple_ids))

def get_vector(data, dict_id_to_index, index, dimension=300):
    vector_index = dict_id_to_index[index]
    vector = data[vector_index]['vec']
    vector = vector.reshape((dimension, 1))
    return vector


def compute_recall(lsh_engine, data, ground_truth_mapping, dict_id_to_index, topK=None, multi_probe=False):
    total_duplicates = 0
    found_duplicates = 0
    total_comparisons = 0
    for tuple_id1, tuple_id2 in ground_truth_mapping:
        vector2 = get_vector(data, dict_id_to_index, tuple_id2)
        result2, block_size = query_data_non_binary(lsh_engine, vector2, tuple_id1, topK, multi_probe)
        total_duplicates = total_duplicates + 1
        total_comparisons = total_comparisons + block_size
        if result2 is True:
            found_duplicates = found_duplicates + 1
    return float(found_duplicates) / float(total_duplicates), total_comparisons


def print_stats(torch_file_name, dataset1_size, dataset2_size):
    data = torchfile.load(torch_file_name)
    print((len(data), dataset1_size + dataset2_size))
    print((data[0]['id'], data[dataset1_size - 1]['id']))
    print((data[dataset1_size]['id'], data[dataset1_size + dataset2_size - 1]['id'], data[-1]['id']))


def test_abt_buy(K, L):
    lsh_engine = create_lsh(K, L)
    ground_truth_mapping = get_csv_reader ("abt_buy_perfectMapping.csv")
    data, dict_id_to_index  = get_torch_data_as_dicts("Abt-Buy.t7")
    dataset1_size = 1081
    dataset2_size = 1092
    index_data(lsh_engine, data, dataset1_size)
    recall, block_size  = compute_recall(lsh_engine, data, ground_truth_mapping, dict_id_to_index)
    return recall, block_size

def test_amzn_google(K,L, topK=None, multi_probe=False):
    lsh_engine = create_lsh(K, L)
    ground_truth_mapping = get_csv_reader ("Amzon_GoogleProducts_perfectMapping.csv")
    data, dict_id_to_index  = get_torch_data_as_dicts("Amazon-GoogleProducts.t7")
    dataset1_size = 1363
    dataset2_size = 3226
    index_data(lsh_engine, data, dataset1_size)
    recall, block_size  = compute_recall(lsh_engine, data, ground_truth_mapping, dict_id_to_index, topK, multi_probe)
    return recall, block_size

def test_dblp_acm(K,L):
    lsh_engine = create_lsh(K, L)
    ground_truth_mapping = get_csv_reader ("DBLP-ACM_perfectMapping.csv")
    data, dict_id_to_index  = get_torch_data_as_dicts("DBLP-ACM.t7")
    dataset1_size = 2616
    dataset2_size = 2294
    index_data(lsh_engine, data, dataset1_size)
    recall, block_size  = compute_recall(lsh_engine, data, ground_truth_mapping, dict_id_to_index, multi_probe)
    return recall, block_size

def test_dblp_scholar(K,L, topK=None, multi_probe=False):
    lsh_engine = create_lsh(K, L)
    ground_truth_mapping = get_csv_reader ("DBLP-Scholar_perfectMapping.csv")
    data, dict_id_to_index  = get_torch_data_as_dicts("DBLP-Scholar.t7")
    dataset1_size = 2616
    dataset2_size = 64263
    index_data(lsh_engine, data, dataset1_size)
    recall, block_size  = compute_recall(lsh_engine, data, ground_truth_mapping, dict_id_to_index, topK)
    return recall, block_size

def test_dataset(test_fn_name, K, L, topK=None, multi_probe=False):
    total_recall = 0
    total_comparisons = 0
    for random_seed in random_seeds:
        #Lsh module uses numpy for random numbers
        numpy.random.seed(random_seed)
        recall, block_size = test_fn_name(K, L, topK, multi_probe)
        #print K, L, random_seed, "Recall: ", recall
        total_recall = total_recall + recall
        total_comparisons = total_comparisons + block_size
    avg_recall = float(total_recall) / len(random_seeds)
    avg_comparisons = float(total_comparisons) / len(random_seeds)
    #print K, L, avg_recall
    return avg_recall, avg_comparisons

def test_dataset_diff_K_L(test_fn_name, op_file_name, dataset_name, range_K=10, range_L=10):
    with open(op_file_name, 'a') as csv_file:
        csv_writer = csv.writer(csv_file)
        for K in range(1, range_K+1):
            for L in range(1, range_L+1):
                avg_recall, avg_comparisons = test_dataset(test_fn_name, K, L)
                csv_writer.writerow([dataset_name, K, L, avg_recall, avg_comparisons])
                csv_file.flush()

def compute_K_L(n, P_1, P_2):
    inv_P1 = 1.0 / P_1
    inv_P2 = 1.0 / P_2

    K = int( numpy.ceil( numpy.log(n) /  numpy.log(inv_P2)))

    rho = numpy.log(inv_P1) / numpy.log(inv_P2)
    L = int( numpy.ceil( numpy.power(n, rho) ))
    return K, L


def test_dataset_vary_P2(dataset_name, test_fn_name, n, P_1 = 0.95):
    with open("vary_P2_fixed_P1.csv", "a") as csv_file:
        csv_writer = csv.writer(csv_file)

        for P_2 in [0.5, 0.6, 0.7, 0.8, 0.9]:
            K, L = compute_K_L(n, P_1, P_2)

            avg_recall, avg_comparisons = test_dataset(test_fn_name, K, L) #run LSH for 10 runs
            csv_writer.writerow([dataset_name, K, L, P_1, P_2, avg_recall, avg_comparisons])
            csv_file.flush()


def test_dataset_vary_P2_wrapper():
    amazn_google_size = 1363
    test_dataset_vary_P2("Amazon-Google", test_amzn_google, amazn_google_size)
    dblp_scholar_size = 2616
    test_dataset_vary_P2("DBLP-Scholar", test_dblp_scholar, dblp_scholar_size)


def test_topK_vs_recall(test_fn_name, dataset_name, op_file_name, K, L, multi_probe=False):
    with open(op_file_name, "a") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Dataset name", "K", "L", "TopK", "Avg Recall", "Avg Comparisons", "MultiProbe"])
        for topK in [10, 20, 30, 40, 50, 100]:
            avg_recall, avg_comparisons = test_dataset(test_fn_name, K, L, topK, multi_probe)
            csv_writer.writerow([dataset_name, K, L, topK, avg_recall, avg_comparisons, multi_probe])
            csv_file.flush()

def test_topK_vs_recall_wrapper():
    K = 10
    L = 1
    #test_topK_vs_recall(test_amzn_google, "Amzn-Google", "op_topK_vs_Recall.csv", K, L, multi_probe=False)
    #test_topK_vs_recall(test_dblp_scholar, "DBLP-Scholar", "op_topK_vs_Recall.csv", K, L, multi_probe=False)
    test_topK_vs_recall(test_amzn_google, "Amzn-Google", "op_topK_vs_Recall.csv", K, L, multi_probe=True)
    #test_topK_vs_recall(test_dblp_scholar, "DBLP-Scholar", "op_topK_vs_Recall.csv", K, L, multi_probe=True)

def test_multi_probe_vs_recall_wrapper():
    test_multi_probe_vs_recall(test_amzn_google, "Amzn-Google", "op_multiprobe_vs_recall.csv")
    test_multi_probe_vs_recall(test_dblp_scholar, "DBLP-Scholar", "op_multiprobe_vs_recall.csv")


#print time.ctime()
#test_dataset_diff_K_L(test_abt_buy, "op_all_K_vs_L.csv", "Abt_Buy", 10, 10)
#print time.ctime()
#test_dataset_diff_K_L(test_amzn_google, "op_all_K_vs_L.csv", "Amzn_Google", 10, 10)
#print time.ctime()
#test_dataset_diff_K_L(test_dblp_acm, "op_all_K_vs_L.csv", "DBLP_ACM", 10, 10)
#print time.ctime()
#test_dataset_diff_K_L(test_dblp_scholar, "op_all_K_vs_L.csv", "DBLP_Scholar", 10, 10)
#print time.ctime()

#test_dataset_vary_P2_wrapper()

#test_topK_vs_recall_wrapper()

#test_multi_probe_vs_recall_wrapper()