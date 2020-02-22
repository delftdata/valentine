import time

from nltk.corpus import stopwords

from algorithms.sem_prop.dataanalysis import nlp_utils as nlp
from algorithms.sem_prop.knowledgerepr import fieldnetwork
from algorithms.sem_prop.modelstore.elasticstore import StoreHandler
from algorithms.sem_prop.ontomatch import glove_api
from algorithms.sem_prop.ontomatch import ss_utils as SS
from algorithms.sem_prop.ontomatch.matcher_lib import get_ban_indexes, remove_banned_vectors


def find_matching_to_text(network,
                          semantic_sim_threshold=0.5,
                          sensitivity_neg_signal=0.5,
                          negative_signal_threshold=0.4,
                          penalize_unknown_word=False,
                          add_exact_matches=True,
                          reference_name="",
                          reference_gen=None):
    # Retrieve relation names
    st = time.time()
    names = []
    seen_fields = set()
    for (db_name, source_name, field_name, _) in network.iterate_values():
        orig_field_name = field_name
        key_seen = source_name + field_name
        if key_seen not in seen_fields:
            seen_fields.add(key_seen)  # seen already
            field_name = nlp.camelcase_to_snakecase(field_name)
            field_name = field_name.replace('-', ' ')
            field_name = field_name.replace('_', ' ')
            field_name = field_name.lower()
            svs = []
            for token in field_name.split():
                if token not in stopwords.words('english'):
                    sv = glove_api.get_embedding_for_word(token)
                    if sv is not None:
                        svs.append(sv)
            names.append(('attribute', (db_name, source_name, orig_field_name), svs))

    num_attributes_inserted = len(names)

    # Retrieve class names

    for cl in reference_gen:
        original_cl_name = cl
        cl = cl.replace('-', ' ')
        cl = cl.replace('_', ' ')
        cl = cl.lower()
        svs = []
        for token in cl.split():
            if token not in stopwords.words('english'):
                sv = glove_api.get_embedding_for_word(token)
                if sv is not None:
                    svs.append(sv)
        names.append(('class', (reference_name, original_cl_name), svs))

    print("N equals: " + str(len(names)))

    pos_matchings = []
    neg_matchings = []
    for idx_class in range(num_attributes_inserted, len(names)):
        for idx_rel in range(0, num_attributes_inserted):  # Compare only with classes
            ban_index1, ban_index2 = get_ban_indexes(names[idx_rel][1][2], names[idx_class][1][1])
            svs_rel = remove_banned_vectors(ban_index1, names[idx_rel][2])
            svs_cla = remove_banned_vectors(ban_index2, names[idx_class][2])
            semantic_sim, strong_signal = SS.compute_semantic_similarity(svs_rel, svs_cla,
                                    penalize_unknown_word=penalize_unknown_word,
                                    add_exact_matches=add_exact_matches,
                                    signal_strength_threshold=sensitivity_neg_signal)
            if strong_signal and semantic_sim > semantic_sim_threshold:
                # match.format db_name, source_name, field_name -> class_name
                match = ((names[idx_rel][1][0], names[idx_rel][1][1], names[idx_rel][1][2]), names[idx_class][1])
                pos_matchings.append(match)
                continue  # FIXME: one matching per entity
            elif strong_signal and semantic_sim < negative_signal_threshold:
                match = ((names[idx_rel][1][0], names[idx_rel][1][1], names[idx_rel][1][2]), names[idx_class][1])
                neg_matchings.append(match)
    et = time.time()
    print("l52: " + str(et - st))
    return pos_matchings, neg_matchings

if __name__ == "__main__":

    print("No matcher")

    # Deserialize model
    path_to_serialized_model = "../models/testwithserialdwh/"
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    # Create client
    store_client = StoreHandler()

    # Load glove model
    print("Loading language model...")
    path_to_glove_model = "../glove/glove.6B.100d.txt"
    glove_api.load_model(path_to_glove_model)
    print("Loading language model...OK")

    ref_path = "/Users/ra-mit/development/discovery_proto/ontomatch/enwiki-latest-all-titles-in-ns0"

    def gen(ref_path):
        i = 0
        with open(ref_path, 'r') as f:
            for l in f:
                i += 1
                if i % 50000 == 0:
                    print(i)
                yield l

    pos, neg = find_matching_to_text(network, reference_name="wikipedia", reference_gen=gen(ref_path))
    for p in pos:
        print(str(p))
