import time
from collections import defaultdict
from enum import Enum
from math import isinf

import numpy as np
from datasketch import MinHash, MinHashLSH
from sklearn.cluster import DBSCAN
from sklearn.decomposition import TruncatedSVD

from algorithms.sem_prop.dataanalysis import dataanalysis as da
from algorithms.sem_prop.knowledgerepr.fieldnetwork import Relation
from algorithms.sem_prop.nearpy import Engine
from algorithms.sem_prop.nearpy.distances import CosineDistance
from algorithms.sem_prop.nearpy.hashes import RandomBinaryProjections
from algorithms.sem_prop.nearpy.hashes import RandomDiscretizedProjections

rbp = RandomBinaryProjections('default', 30)


def create_sim_graph_text(nid_gen, network, text_engine, tfidf, relation, tfidf_is_dense=False):
    st = time.time()
    row_idx = 0
    for nid in nid_gen:
        if tfidf_is_dense:
            dense_row = tfidf[row_idx]
            array = dense_row
        else:
            sparse_row = tfidf.getrow(row_idx)
            dense_row = sparse_row.todense()
            array = dense_row.A[0]
        row_idx += 1
        N = text_engine.neighbours(array)
        if len(N) > 1:
            for n in N:
                (data, key, value) = n
                if nid != key:
                    #print("tsim: {0} <-> {1}".format(nid, key))
                    network.add_relation(nid, key, relation, value)
    et = time.time()
    print("Create graph schema: {0}".format(str(et - st)))


def index_in_text_engine(nid_gen, tfidf, lsh_projections, tfidf_is_dense=False):
    num_features = tfidf.shape[1]
    print("TF-IDF shape: " + str(tfidf.shape))
    text_engine = Engine(num_features,
                         lshashes=[lsh_projections],
                         distance=CosineDistance())

    st = time.time()
    row_idx = 0
    for key in nid_gen:
        if tfidf_is_dense:
            dense_row = tfidf[row_idx]
            array = dense_row
        else:
            sparse_row = tfidf.getrow(row_idx)
            dense_row = sparse_row.todense()
            array = dense_row.A[0]
        row_idx += 1
        text_engine.store_vector(array, key)
    et = time.time()
    print("Total index text: " + str((et - st)))
    return text_engine


def lsa_dimensionality_reduction(tfidf):
    svd = TruncatedSVD(n_components=1000, random_state=42)
    svd.fit(tfidf)
    new_tfidf_vectors = svd.transform(tfidf)
    return new_tfidf_vectors


class LSHRandomProjectionsIndex:

    def __init__(self, num_features, projection_count=30):
        self.num_features = num_features
        #self.rbp = RandomDiscretizedProjections('default', projection_count, bin_width=100)
        self.rbp = RandomBinaryProjections('default', projection_count)
        #self.rbp = RandomBinaryProjectionTree('default', projection_count, 1)
        self.text_engine = Engine(num_features, lshashes=[self.rbp], distance=CosineDistance())

    def index(self, vector, key):
        if len(vector) != self.num_features:
            print("ERROR received vector.dim: " + str(len(vector)) + " on engine.dim: " + str(self.num_features))
            raise Exception
        self.text_engine.store_vector(vector, key)

    def query(self, vector):
        res = self.text_engine.neighbours(vector)
        return res


def build_schema_sim_relation(network):

    def connect(nid1, nid2, score):
        network.add_relation(nid1, nid2, Relation.SCHEMA_SIM, score)

    st = time.time()
    docs = []
    for (_, _, field_name, _) in network.iterate_values():
        docs.append(field_name)

    tfidf = da.get_tfidf_docs(docs)
    et = time.time()
    print("Time to create docs and TF-IDF: ")
    print("Create docs and TF-IDF: {0}".format(str(et - st)))

    nid_gen = network.iterate_ids()
    num_features = tfidf.shape[1]
    new_index_engine = LSHRandomProjectionsIndex(num_features)

    # Index vectors in engine
    st = time.time()
    row_idx = 0
    for key in nid_gen:
        sparse_row = tfidf.getrow(row_idx)
        dense_row = sparse_row.todense()
        array = dense_row.A[0]
        row_idx += 1
        new_index_engine.index(array, key)
    et = time.time()
    print("Total index text: " + str((et - st)))

    # Create schema_sim links
    nid_gen = network.iterate_ids()
    st = time.time()
    row_idx = 0
    for nid in nid_gen:

        sparse_row = tfidf.getrow(row_idx)
        dense_row = sparse_row.todense()
        array = dense_row.A[0]
        row_idx += 1
        N = new_index_engine.query(array)
        if len(N) > 1:
            for n in N:
                (data, key, value) = n
                if nid != key:
                    connect(nid, key, value)
    et = time.time()
    print("Create graph schema: {0}".format(str(et - st)))

    return new_index_engine


def build_schema_sim_relation_lsa(network, fields):
    docs = []
    for (nid, sn, fn, _, _) in fields:
        docs.append(fn)

    tfidf = da.get_tfidf_docs(docs)

    print("tfidf shape before LSA: " + str(tfidf.shape))
    tfidf = lsa_dimensionality_reduction(tfidf)
    print("tfidf shape after LSA: " + str(tfidf.shape))

    text_engine = index_in_text_engine(
        fields, tfidf, rbp, tfidf_is_dense=True)  # rbp the global variable
    create_sim_graph_text(network, text_engine, fields,
                          tfidf, Relation.SCHEMA_SIM, tfidf_is_dense=True)


def build_entity_sim_relation(network, fields, entities):
    docs = []
    for e in entities:
        if e != "":  # Append only non-empty documents
            docs.append(e)
    print(str(docs))

    if len(docs) > 0:  # If documents are empty, then skip this step; not entity similarity will be found
        tfidf = da.get_tfidf_docs(docs)
        text_engine = index_in_text_engine(
            fields, tfidf, rbp)  # rbp the global variable
        create_sim_graph_text(network, text_engine, fields,
                              tfidf, Relation.ENTITY_SIM)


def build_content_sim_relation_text_lsa(network, signatures):

    def get_nid_gen(signatures):
        for nid, sig in signatures:
            yield nid

    docs = []
    for nid, e in signatures:
        docs.append(' '.join(e))

    # this may become redundant if we exploit the store characteristics
    tfidf = da.get_tfidf_docs(docs)

    print("TF-IDF shape before LSA: " + str(tfidf.shape))
    st = time.time()
    tfidf = lsa_dimensionality_reduction(tfidf)
    et = time.time()
    print("TF-IDF shape after LSA: " + str(tfidf.shape))
    print("Time to compute LSA: {0}".format(str(et - st)))
    lsh_projections = RandomBinaryProjections('default', 10000)
    #lsh_projections = RandomDiscretizedProjections('rnddiscretized', 1000, 2)
    nid_gen = get_nid_gen(signatures)  # to preserve the order nid -> signature
    text_engine = index_in_text_engine(nid_gen, tfidf, lsh_projections, tfidf_is_dense=True)
    nid_gen = get_nid_gen(signatures)  # to preserve the order nid -> signature
    create_sim_graph_text(nid_gen, network, text_engine, tfidf, Relation.CONTENT_SIM, tfidf_is_dense=True)


def build_content_sim_relation_text(network, signatures):

    def get_nid_gen(signatures):
        for nid, sig in signatures:
            yield nid

    docs = []
    for nid, e in signatures:
        docs.append(' '.join(e))

    # this may become redundant if we exploit the store characteristics
    tfidf = da.get_tfidf_docs(docs)
    # rbp = RandomBinaryProjections('default', 1000)
    lsh_projections = RandomDiscretizedProjections('rnddiscretized', 1000, 2)
    nid_gen = get_nid_gen(signatures)
    text_engine = index_in_text_engine(nid_gen, tfidf, lsh_projections)
    nid_gen = get_nid_gen(signatures)
    create_sim_graph_text(nid_gen, network, text_engine, tfidf, Relation.CONTENT_SIM)


def build_content_sim_mh_text(network, mh_signatures):

    def connect(nid1, nid2, score):
        network.add_relation(nid1, nid2, Relation.CONTENT_SIM, score)

    # Materialize signatures for convenience
    mh_sig_obj = []

    content_index = MinHashLSH(threshold=0.7, num_perm=512)

    # Create minhash objects and index
    for nid, mh_sig in mh_signatures:
        mh_obj = MinHash(num_perm=512)
        mh_array = np.asarray(mh_sig, dtype=int)
        mh_obj.hashvalues = mh_array
        content_index.insert(nid, mh_obj)
        mh_sig_obj.append((nid, mh_obj))

    # Query objects
    for nid, mh_obj in mh_sig_obj:
        res = content_index.query(mh_obj)
        for r_nid in res:
            if r_nid != nid:
                connect(nid, r_nid, 1)

    return content_index


def build_content_sim_relation_num_overlap_distr_indexed(network, id_sig):

    def compute_overlap(value1, value2):
        ov = 0
        if value1 == value2:
            ov = 1
        if value1 > value2:
            if value1 > 0:
                ov = value2 / value1
        else:
            if value2 > 0:
                ov = value1 / value2
        return ov

    def check_overlap_maybe_connect(overlap):
        return

    class Event(Enum):
        OPEN = 0
        CLOSE = 1
        FINISHED = 2

    overlap = 0.7

    # Materialize data
    nids = []
    # events are one of (value, open_event) or (value, close_event)
    events = []
    for c_k, (c_median, c_iqr, c_min_v, c_max_v) in id_sig:
        nids.append(c_k)
        open_event_value = c_median - c_iqr
        events.append((open_event_value, Event.OPEN))
        nids.append(c_k)
        close_event_value = c_median + c_iqr
        events.append((close_event_value, Event.CLOSE))

    # Sort data
    sorted_data = sorted(zip(events, nids), key=lambda x: x[0][0])
    events = [x for x, y in sorted_data]
    nids = [y for x, y in sorted_data]
    active_set = dict()
    start_events = dict()

    # Iterate data
    for idx in range(len(events)):
        event_value, event_type = events[idx]
        current_nid = nids[idx]
        # Open or Close event?
        if event_type == Event.OPEN:
            for active_element_nid, active_element_values in active_set.items():
                active_element_values[current_nid] = (event_value, event_type)
            active_set[current_nid] = dict()
            start_events[current_nid] = event_value
        elif event_type == Event.CLOSE:
            for key in list(active_set.keys()):
                active_element_nid = key
                active_element_values = active_set[active_element_nid]
                if current_nid != active_element_nid:
                    if current_nid in active_element_values.keys():
                        open_event_value = active_element_values[current_nid][0]  # 1 is event_type
                        overlap = event_value - open_event_value
                        active_element_values[current_nid] = (overlap, Event.FINISHED)
                elif current_nid == active_element_nid:
                    for key in list(active_element_values.keys()):
                        ae_nid = key
                        ae_event = active_element_values[ae_nid]
                        ae_value, ae_type = ae_event
                        if ae_type == Event.FINISHED:
                            closing_event_start_time = start_events[current_nid]
                            overlap = compute_overlap(closing_event_start_time, ae_value)
                            check_overlap_maybe_connect(overlap)
                        elif ae_type == Event.OPEN:
                            overlap = compute_overlap(event_value, ae_value)
                            # store value in the active element
                            active_set[active_element_nid][ae_nid] = (overlap, Event.FINISHED)
                        # remove entry from values
                        #del active_element_values[current_nid]
                    # remove entry from active set
                    del active_set[current_nid]


def build_content_sim_relation_num_overlap_distr(network, id_sig):

    def compute_overlap(ref_left, ref_right, left, right):
        ov = 0
        if left >= ref_left and right <= ref_right:
            ov = float((right - left) / (ref_right - ref_left))
        elif left >= ref_left and left <= ref_right:
            domain_ov = ref_right - left
            ov = float(domain_ov / (ref_right - ref_left))
        elif right <= ref_right and right >= ref_left:
            domain_ov = right - ref_left
            ov = float(domain_ov / (ref_right - ref_left))
        return float(ov)

    def connect(nid1, nid2, score, inddep=False):
        if inddep is False:
            network.add_relation(nid1, nid2, Relation.CONTENT_SIM, score)
        else:
            network.add_relation(nid1, nid2, Relation.INCLUSION_DEPENDENCY, score)

    overlap = 0.85

    fields = []
    domains = []
    stats = []
    for c_k, (c_median, c_iqr, c_min_v, c_max_v) in id_sig:
        fields.append(c_k)
        domain = (c_median + c_iqr) - (c_median - c_iqr)
        domains.append(domain)
        extreme_left = c_median - c_iqr
        min = c_min_v
        extreme_right = c_median + c_iqr
        max = c_max_v
        #print(str(extreme_left) + " - " + str(domain) + " - " + str(extreme_right))
        stats.append((min, extreme_left, extreme_right, max))

    zipped_and_sorted = sorted(zip(domains, fields, stats), reverse=True)
    candidate_entries = [(y, x, z[0], z[1], z[2], z[3]) for (x, y, z) in zipped_and_sorted]

    single_points = []

    for ref in candidate_entries:
        ref_nid, ref_domain, ref_x_min, ref_x_left, ref_x_right, ref_x_max = ref

        if ref_nid == '2314808454':
            debug = True

        if ref_domain == 0:
            single_points.append(ref)

        info1 = network.get_info_for([ref_nid])

        (nid, db_name, source_name, field_name) = info1[0]
        #print("")
        #print("")
        #print("Checking: " + source_name + " - " + field_name)
        #print("")
        #print("")

        for entry in candidate_entries:
            candidate_nid, candidate_domain, candidate_x_min, candidate_x_left, candidate_x_right, candidate_x_max = entry

            if candidate_nid == '1504465753':
                debug = True

            if candidate_nid == ref_nid:
                continue

            if ref_domain == 0:
                continue

            # Check for filtered inclusion dependencies first
            if not isinstance(candidate_domain, float):  # Filter these out
                # Check ind. dep.
                info2 = network.get_info_for([candidate_nid])
                (_, _, sn1, fn1) = info1[0]
                if len(info2) == 0:
                    continue
                (_, _, sn2, fn2) = info2[0]
                if isinf(float(ref_x_min)) or isinf(float(ref_x_max)) or isinf(float(candidate_x_max)) or isinf(float(candidate_x_min)):
                    continue
                if candidate_x_min >= ref_x_min and candidate_x_max <= ref_x_max:
                    # inclusion relation
                    if candidate_x_min >= 0:

                        # min overlap for precision
                        actual_overlap = compute_overlap(ref_x_left, ref_x_right, candidate_x_left, candidate_x_right)
                        if actual_overlap >= 0.3:
                            connect(candidate_nid, ref_nid, 1, inddep=True)
                    """
                    if candidate_x_left >= ref_x_left and candidate_x_right <= ref_x_right:
                        # TODO: probably want to apply some filter here, division of medians or similar
                        # TODO: or maybe try max min instead of median-+iqr
                        #candidate_median = int((candidate_x_left + candidate_x_right)/2)
                        #ref_median = float((ref_x_left + ref_x_right)/2)
                        #heuristic = 0  # uninitialized
                        #if ref_median > 0:
                        #    heuristic = float(candidate_median / ref_median)
                        #elif candidate_median > 0:
                        #    heuristic = float(ref_median / candidate_median)
                        #else:
                        #    continue
                        #if heuristic > 0.2 and heuristic < 5:

                        if candidate_x_min >= 0:  # Only consider positive numbers as IDs

                            info2 = network.get_info_for([candidate_nid])
                            #(nid, db_name, source_name, field_name) = info2[0]
                            #print(str(source_name) + " - " + str(field_name) + " ov: " + str(actual_overlap))
                            connect(candidate_nid, ref_nid, 1, inddep=True)
                    """

            #if float(candidate_domain / ref_domain) <= overlap:
            #    # There won't be a content sim relation -> not even the entire domain would overlap more than the th.
            #    break
            actual_overlap = compute_overlap(ref_x_left, ref_x_right, candidate_x_left, candidate_x_right)
            if actual_overlap >= overlap:
                connect(candidate_nid, ref_nid, actual_overlap)

            """
            if candidate_x_left >= ref_x_left and candidate_x_right <= ref_x_right:
                if float(candidate_domain / ref_domain) >= overlap:  # has to be as per the break condition above
                    actual_overlap = float(candidate_domain / ref_domain)
                    #info2 = network.get_info_for([candidate_nid])
                    #(nid, db_name, source_name, field_name) = info2[0]
                    #print(str(source_name) + " - " + str(field_name) + " ov: " + str(actual_overlap))
                    connect(candidate_nid, ref_nid, actual_overlap)
            elif candidate_x_left >= ref_x_left and candidate_x_left <= ref_x_right:  # make sure there's overlap
                domain_overlap = ref_x_right - candidate_x_left
                if float(domain_overlap / ref_domain) >= overlap:
                    actual_overlap = float(domain_overlap / ref_domain)
                    #info2 = network.get_info_for([candidate_nid])
                    #(nid, db_name, source_name, field_name) = info2[0]
                    #print(str(source_name) + " - " + str(field_name) + " ov: " + str(actual_overlap))
                    connect(candidate_nid, ref_nid, actual_overlap)
            elif candidate_x_right <= ref_x_right and candidate_x_right >= ref_x_left:  # make sure there's overlap
                domain_overlap = candidate_x_right - ref_x_left
                if float(domain_overlap / ref_domain) >= overlap:
                    actual_overlap = float(domain_overlap / ref_domain)
                    #info2 = network.get_info_for([candidate_nid])
                    #(nid, db_name, source_name, field_name) = info2[0]
                    #print(str(source_name) + " - " + str(field_name) + " ov: " + str(actual_overlap))
                    connect(candidate_nid, ref_nid, actual_overlap)
            """

    # Final clustering for single points

    fields = []
    medians = []

    for (nid, domain, x_min, x_left, x_right, x_max) in single_points:
        median = x_right - float(x_right / 2)
        fields.append(nid)
        medians.append(median)

    x_median = np.asarray(medians)
    x_median = x_median.reshape(-1, 1)

    # At this point, we may have not found any points at all, in which case we can
    # safely exit
    if len(x_median) == 0:
        return

    db_median = DBSCAN(eps=0.1, min_samples=2).fit(x_median)
    labels_median = db_median.labels_
    n_clusters = len(set(labels_median)) - (1 if -1 in labels_median else 0)
    #print("#clusters: " + str(n_clusters))

    clusters_median = defaultdict(list)
    for i in range(len(labels_median)):
        clusters_median[labels_median[i]].append(i)

    for k, v in clusters_median.items():
        if k == -1:
            continue
        #print("Cluster: " + str(k))
        for el in v:
            nid = fields[el]
            info = network.get_info_for([nid])
            (nid, db_name, source_name, field_name) = info[0]
            #print(source_name + " - " + field_name + " median: " + str(medians[el]))
            for el2 in v:
                if el != el2:
                    nid1 = fields[el]
                    nid2 = fields[el2]
                    connect(nid1, nid2, overlap)


def build_content_sim_relation_num_double_clustering(network, id_sig):

    fields = []
    median_vector = []
    iqr_vector = []

    # populate vectors
    total = 0
    for k, (c_median, c_iqr, c_min_v, c_max_v) in id_sig:
        total += 1
        fields.append(k)
        median_vector.append(c_median)
        iqr_vector.append(c_iqr)

    print("Total samples: " + str(total))

    #median_vector = median_vector.reshape(-1, 1)
    #iqr_vector = iqr_vector.reshape(-1, 1)

    x_median = np.asarray(median_vector)
    x_iqr = np.asarray(iqr_vector)
    x_median = x_median.reshape(-1, 1)
    x_iqr = x_iqr.reshape(-1, 1)

    db_median = DBSCAN(eps=0.1, min_samples=3).fit(x_median)
    db_iqr = DBSCAN(eps=0.1, min_samples=3).fit(x_iqr)
    labels_median = db_median.labels_
    labels_iqr = db_iqr.labels_
    n_clusters_median = len(set(labels_median)) - (1 if -1 in labels_median else 0)
    n_clusters_iqr = len(set(labels_iqr)) - (1 if -1 in labels_iqr else 0)
    print("Num clusters median: " + str(n_clusters_median))
    print("Num clusters iqr: " + str(n_clusters_iqr))

    clusters_median = defaultdict(list)
    for i in range(len(labels_median)):
        clusters_median[labels_median[i]].append(i)

    clusters_iqr = defaultdict(list)
    for i in range(len(labels_iqr)):
        clusters_iqr[labels_iqr[i]].append(i)

    print("Clusters median")
    print("")
    for k, v in clusters_median.items():
        if k == -1:
            continue
        print("cluster: " + str(k))
        for el in v:
            nid = fields[el]
            info = network.get_info_for([nid])
            (nid, db_name, source_name, field_name) = info[0]
            print(source_name + " - " + field_name + " median: " + str(median_vector[el]))
        print("")
        print("")

    print("Clusters IQR")
    print("")
    for k, v in clusters_iqr.items():
        if k == -1:
            continue
        print("cluster: " + str(k))
        for el in v:
            nid = fields[el]
            info = network.get_info_for([nid])
            (nid, db_name, source_name, field_name) = info[0]
            print(source_name + " - " + field_name + " iqr: " + str(iqr_vector[el]))
        print("")
        print("")


def build_content_sim_relation_num(network, id_sig):

    def get_sig_gen(id_sig):
        for nid, sig in id_sig:
            yield sig

    features_gen = get_sig_gen(id_sig)
    fields = [x[0] for x in id_sig]

    X = np.asarray([x for x in features_gen])
    db = DBSCAN(eps=0.3, min_samples=2).fit(X)
    labels = db.labels_
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    print("Total num clusters found: " + str(n_clusters))
    # group indices by label
    clusters = defaultdict(list)
    for i in range(len(labels)):
        clusters[labels[i]].append(i)
    # create relations
    for k, v in clusters.items():
        if k == -1:
            continue
        for el1 in v:
            for el2 in v:
                if el1 != el2:
                    nid1 = fields[el1]
                    nid2 = fields[el2]
                    network.add_relation(nid1, nid2, Relation.CONTENT_SIM, 1)


def build_pkfk_relation(network):

    def get_neighborhood(n):
        neighbors = []
        data_type = network.get_data_type_of(n)
        if data_type == "N":
            neighbors = network.neighbors_id(n, Relation.INCLUSION_DEPENDENCY)
        if data_type == "T":
            neighbors = network.neighbors_id(n, Relation.CONTENT_SIM)
        return neighbors

    total_pkfk_relations = 0
    for n in network.iterate_ids():
        n_card = network.get_cardinality_of(n)
        if n == '2314808454' or n == '1504465753':
            debug = True
        if n_card > 0.7:  # Early check if this is a candidate
            neighborhood = get_neighborhood(n)
            for ne in neighborhood:
                if ne is not n:
                    if ne.nid == '1504465753' or ne.nid == '2314808454':
                        debug = True
                    ne_card = network.get_cardinality_of(ne.nid)
                    if n_card > ne_card:
                        highest_card = n_card
                    else:
                        highest_card = ne_card
                    #if ne_card < 0.5:
                    network.add_relation(n, ne.nid, Relation.PKFK, highest_card)
                    total_pkfk_relations += 1
                    #print(str(n) + " -> " + str(ne))
    print("Total number PKFK: {0}".format(str(total_pkfk_relations)))


if __name__ == "__main__":
    print("TODO")

    # test
    from scipy import spatial
    import numpy

    d = [1, 2, 3]
    d2 = [3, 4, 5]

    result = 1 - spatial.distance.cosine(d, d2)
    print("result: " + str(result))
    result2 = 1.0 - numpy.dot(d, d2)
    print("result2: " + str(result))
