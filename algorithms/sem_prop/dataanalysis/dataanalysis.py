from sklearn.neighbors import KernelDensity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn import svm
from scipy.stats import ks_2samp
from collections import Counter
import numpy as np
#from nltk.corpus import stopwords
import nltk
import string
import time

import algorithms.sem_prop.config as C


def build_dict_values(values):
    d = dict()
    for v in values:
        if v not in d:
            d[v] = 0
        d[v] = d[v] + 1
    return d


def compute_overlap(values1, values2, th_overlap, th_cutoff):
    overlap = 0
    non_overlap = 0
    longest = None
    shortest = None
    if len(values1) > len(values2):
        longest = values1
        shortest = values2
    else:
        longest = values2
        shortest = values1

    for k, v in shortest.items():
        if k in longest.keys():
            overlap = overlap + 1
        else:
            non_overlap = non_overlap + 1
        if overlap > th_overlap:
            return True
        if non_overlap > th_cutoff:
            return False


def compute_overlap_of_columns(col1, col2):
    vals1 = build_dict_values(col1)
    vals2 = build_dict_values(col2)
    total_size = len(vals1) + len(vals2)
    th_overlap = C.join_overlap_th * total_size
    th_cutoff = total_size - th_overlap
    overlap = compute_overlap(vals1, vals2, th_overlap, th_cutoff)
    return overlap


def get_numerical_signature(values, S):
    '''
    Learns a distribution of the values
    Then generates a sample of size S
    '''
    # Transform data to numpy array
    Xnumpy = np.asarray(values)
    X = Xnumpy.reshape(-1, 1)
    # Learn kernel
    kde = KernelDensity(
        kernel=C.kd["kernel"],
        bandwidth=C.kd["bandwidth"]
    ).fit(X)
    sig_v = [kde.sample()[0][0] for x in range(S)]
    return sig_v


def get_textual_signature(values, S):
    '''
    Creates a representative vector of the values of at
    most size S
    '''
    raw = ' '.join(values)
    vectorizer = CountVectorizer(min_df=1,
                                 stop_words="english",
                                 max_features=5)
    analyze = vectorizer.build_analyzer()
    sig_v = analyze(raw)
    return sig_v[:S]


def compare_pair_num_columns(col1, col2):
    '''
    Returns true if columns are similar
    '''
    sim_value = compare_num_columns_dist_ks(col1, col2)
    dvalue = sim_value[0]
    pvalue = sim_value[1]
    if dvalue < C.ks["dvalue"] and pvalue > C.ks["pvalue"]:
        return True
    return False


def compare_pair_text_columns(col1, col2):
    '''
    Returns true if columns are similar
    '''
    docs = [col1, col2]
    sim_value = None
    try:
        sim_value = compare_text_columns_dist(docs)
        #sim_value = 0.01
    except ValueError:
        sim_value = -1
    if sim_value > C.cosine["threshold"]:  # right threshold?
        return True
    return False


def compare_num_columns_dist(columnA, columnB, method):
    if method == "ks":
        return compare_num_columns_dist_ks(columnA, columnB)
    if method == "odsvm":
        return compare_num_columns_dist_odsvm(columnA, columnB)


def compare_num_columns_dist_ks(columnA, columnB):
    ''' 
        Kolmogorov-Smirnov test
    '''
    return ks_2samp(columnA, columnB)


def compare_num_columns_dist_odsvm(svm, columnBdata):
    Xnumpy = np.asarray(columnBdata)
    X = Xnumpy.reshape(-1, 1)
    prediction_vector = svm.predict(X)
    return prediction_vector


def get_sim_items_ks(key, ncol_dist):
    sim_columns = []
    sim_vector = get_sim_vector_numerical(
        key,
        ncol_dist,
        "ks")
    for filekey, sim in sim_vector.items():
        dvalue = sim[0]
        pvalue = sim[1]
        if dvalue < C.ks["dvalue"] \
                and \
                pvalue > C.ks["pvalue"]:
            sim_columns.append(filekey)
    return sim_columns


def get_num_dist(data, method):
    '''
    Get signature for numerical data following the
    provided method
    '''
    return get_dist(data, method)


def get_dist(data_list, method):
    Xnumpy = np.asarray(data_list)
    X = Xnumpy.reshape(-1, 1)
    dist = None
    if method == "raw":
        dist = data_list  # raw column data
    if method == "kd":
        kde = KernelDensity(
            kernel=C.kd["kernel"],
            bandwidth=C.kd["bandwidth"]
        ).fit(X)
        dist = kde.score_samples(X)
    elif method == "odsvm":
        svmachine = svm.OneClassSVM(
            nu=C.odsvm["nu"],
            kernel=C.odsvm["kernel"],
            gamma=C.odsvm["gamma"]
        )
        dist = svmachine.fit(X)
    return dist


def get_textual_dist(data, method):
    '''
    Get signature for textual data following
    the provided method
    '''
    sig = None
    if method == "vector":
        try:
            sig = ' '.join(data)
        except TypeError:
            sig = ' '.join(str(data))  # forcing to string here
    return sig

tt = 0
it = 1


def tokenize(text):
    lower = text.lower()
    npunc = lower.translate(string.punctuation)
    tokens = nltk.word_tokenize(npunc)
    #ft = [w for w in tokens if not w in stopwords.words('english')]
    return tokens[:20]


def _compare_text_columns_dist(doc1, doc2):
    # tokenize and filter stop words
    doc1 = doc1[:4000]
    doc2 = doc2[:4000]
    doc1 = doc1.lower()
    doc2 = doc2.lower()
    #f1doc1 = doc1.translate(None, [string.punctuation])
    #f1doc2 = doc2.translate(None, [string.punctuation])
    tokens1 = nltk.word_tokenize(doc1)
    tokens2 = nltk.word_tokenize(doc2)
    f2t1 = [t for t in tokens1 if len(t) > 3]
    f2t2 = [t for t in tokens2 if len(t) > 3]

    # order by term frequency
    c1 = Counter(f2t1)
    c2 = Counter(f2t2)
    t1 = c1.most_common(50)
    t2 = c2.most_common(50)
    print(str(t1))

    # compute tf

    # compute idf

    # compute tf*idf

    # compute cosine similarity (custom function, faster)

    # return similarity


vect = TfidfVectorizer(min_df=1, sublinear_tf=True, use_idf=True)


def get_tfidf_docs(docs):
    st = time.time()
    tfidf = vect.fit_transform(docs)
    et = time.time()
    print("Time to TFIDF: " + str(et - st))
    return tfidf


def compare_text_columns_dist(docs):
    ''' cosine distance between two vector of hash(words)'''
    docs = [docs[0][:4000], docs[1][:4000]]
    #docs = [docs[0], docs[1]]
    st = time.time()
    #vect = TfidfVectorizer(tokenizer = tokenize, min_df=1)
    global vect
    tfidf = vect.fit_transform(docs)
    sim = ((tfidf * tfidf.T).A)[0, 1]
    #sim = 0.01
    et = time.time()
    global tt
    tt = tt + (et - st)
    global it
    it = it + 1
    total_time = et - st
    #if total_time == 0:
    #    length = len(docs[0]) + len(docs[1])
    #    print(" length = " + str(length))
    if total_time > 0.1:
        print("* " + str(et - st))
    #    #length = len(docs[0]) + len(docs[1])
    #    #print(" length = " + str(length))
    #    print("d0: " + str(docs[0][:70]))
    #    print("d1: " + str(docs[1][:70]))
    #    #print(str(tfidf * tfidf.T))
    #pairwise_similarity = tfidf * tfidf.T
    avg = tt / it
    #print(str(avg))
    return sim


def get_sim_vector_numerical(column, ncol_dist, method):
    value_to_compare = ncol_dist[column]
    vn = dict()
    for key, value in ncol_dist.items():
        test = compare_num_columns_dist(
            value_to_compare,
            value,
            method)
        vn[key] = test
    return vn


def get_sim_matrix_numerical(ncol_dist, method):
    '''
         Pairwise comparison of all numerical column dist. 
         keep them in matrix
    '''
    mn = dict()
    for key, value in ncol_dist.items():
        vn = get_sim_vector_numerical(key, ncol_dist, method)
        mn[key] = vn
    return mn


def get_sim_vector_text(column, tcol_dist):
    value_to_compare = tcol_dist[column]
    vt = dict()
    for key, value in tcol_dist.items():
        if value_to_compare != "" and value != "":
            try:
                sim = compare_text_columns_dist(
                    [value_to_compare, value]
                )
                vt[key] = sim
            except ValueError:
                print("No sim for (" + str(column) +
                      "" + str(key) + ")")
                vt[key] = -1
    return vt


def get_sim_matrix_text(tcol_dist):
    ''' 
    Pairwise comparison of all textual column dist. 
    keep them in matrix
    '''
    mt = dict()
    for key, value in tcol_dist.items():
        mt[key] = dict()
        vt = get_sim_vector_text(key, tcol_dist)
        mt[key] = vt
    return mt


def get_column_signature(column, method):
    dist = None
    tcols = 0
    ncols = 0
    if U.is_column_num(column):
        #print('TYPE: num')
        # Get dist only for numerical columns
        dist = get_dist(column, method)
        ncols = ncols + 1
    else:  # only numerical and text columns supported so far
        #print('TYPE: text')
        dist = ' '.join(column)
        tcols = tcols + 1
    print("Num. columns: " + str(ncols))
    print("Text columns: " + str(tcols))
    return dist


def get_columns_signature(columns, method):
    ncol_dist = dict()
    tcol_dist = dict()
    ncols = 0
    tcols = 0
    # Get distribution for numerical columns
    for key, value in columns.items():
        # the case of non-numerical columns
        if U.is_column_num(value):
            # Get dist only for numerical columns
            dist = get_dist(value, method)
            ncol_dist[key] = dist
            ncols = ncols + 1
        else:  # only numerical and text columns supported so far
            column_repr = ' '.join(value)
            tcol_dist[key] = column_repr
            tcols = tcols + 1
    print("Num. columns: " + str(ncols))
    print("Text columns: " + str(tcols))
    return (ncol_dist, tcol_dist)

if __name__ == "__main__":
    print("Data analysis")
