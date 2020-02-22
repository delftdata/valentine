import math
import operator
import string
from itertools import product

import nltk
import snakecase as snakecase
from anytree import LevelOrderIter
from nltk.corpus import stopwords
from nltk.corpus import wordnet as wn
from similarity.ngram import NGram

from algorithms.cupid.elements import SchemaElement, Token, TokenTypes


def normalization(element, schema_element=None):
    if schema_element is None:
        schema_element = SchemaElement(element)

    tokens = nltk.word_tokenize(element)

    for token in tokens:
        token_obj = Token()

        if token in string.punctuation:
            token_obj.ignore = True
            token_obj.data = token
            token_obj.token_type = TokenTypes.SYMBOLS
            schema_element.add_token(token_obj)
        else:
            try:
                float(token)
                token_obj.data = token
                token_obj.token_type = TokenTypes.NUMBER
                schema_element.add_token(token_obj)
            except ValueError:
                token_snake = snakecase.convert(token)

                if '_' in token_snake:
                    token_snake = token_snake.replace('_', ' ')
                    schema_element = normalization(token_snake, schema_element)
                elif token.lower() in stopwords.words('english'):
                    token_obj.data = token.lower()
                    token_obj.ignore = True
                    token_obj.token_type = TokenTypes.COMMON_WORDS
                    schema_element.add_token(token_obj)
                else:
                    token_obj.data = token.lower()
                    token_obj.token_type = TokenTypes.CONTENT
                    schema_element.add_token(token_obj)

    return schema_element


def compute_compatibility(categories1, categories2):
    compatibility_table = dict()

    for cat1 in categories1:
        compatibility_table[cat1] = dict()
        tokens1 = list(map(lambda t: Token().add_data(t),
                           list(filter(lambda x: x.isalnum(), nltk.word_tokenize(cat1)))))

        for cat2 in categories2:
            tokens2 = list(map(lambda t: Token().add_data(t),
                               list(filter(lambda x: x.isalnum(), nltk.word_tokenize(cat2)))))

            if cat2 not in compatibility_table:
                compatibility_table[cat2] = dict()

            compatibility = name_similarity_tokens(tokens1, tokens2)
            compatibility_table[cat1][cat2] = compatibility
            compatibility_table[cat2][cat1] = compatibility

    return compatibility_table


def comparison(source_tree, target_tree, categories_source, categories_target, compatibility_table, th_ns):
    all_nodes_s = [node.name for node in LevelOrderIter(source_tree)]
    all_nodes_t = [node.name for node in LevelOrderIter(target_tree)]
    all_nodes = list(product(all_nodes_s, all_nodes_t))

    keys_source = categories_source.keys()
    keys_target = categories_target.keys()

    name_sim = dict()
    lsim = dict()

    for cat_s in keys_source:
        for cat_t in keys_target:
            if compatibility_table[cat_s][cat_t] > th_ns:
                all_compatible_nodes = list(product(categories_source[cat_s], categories_target[cat_t]))
                name_sim.update({pair: name_similarity_elements(pair[0], pair[1])
                                 for pair in all_compatible_nodes if pair in all_nodes})
    for s, t in name_sim.keys():
        s_cat = s.categories
        t_cat = t.categories
        maxs = [max(dict(filter(lambda x: x[0] in t_cat, compatibility_table[c].items())).items(),
                    key=operator.itemgetter(1))[1] for c in s_cat]
        lsim[(s, t)] = name_sim[(s, t)] * max(maxs)

    return lsim


# max = 1
def name_similarity_tokens(token_set1, token_set2):
    sum1 = get_partial_similarity(token_set1, token_set2)
    sum2 = get_partial_similarity(token_set2, token_set1)
    return (sum1 + sum2) / (len(token_set1) + len(token_set2))


def get_partial_similarity(token_set1, token_set2, n=2):
    total_sum = 0
    for t1 in token_set1:
        max_sim = -math.inf
        for t2 in token_set2:
            sim = compute_similarity_wordnet(t1.data, t2.data)
            if math.isnan(sim):
                sim = 1 - compute_similarity_ngram(t1.data, t2.data, n)

            if sim > max_sim:
                max_sim = sim

        total_sum = total_sum + max_sim

    return total_sum


# the higher, the better
def compute_similarity_wordnet(word1, word2):
    allsyns1 = set(ss for ss in wn.synsets(word1))
    allsyns2 = set(ss for ss in wn.synsets(word2))

    if len(allsyns1) == 0 or len(allsyns2) == 0:
        return math.nan

    # best = max((wn.wup_similarity(s1, s2) or 0, s1, s2) for s1, s2 in product(allsyns1, allsyns2))
    best = max(wn.wup_similarity(s1, s2) or math.nan for s1, s2 in product(allsyns1, allsyns2))

    return best


# the lower, the better
def compute_similarity_ngram(word1, word2, n):
    ngram = NGram(n)
    sim = ngram.distance(word1, word2)
    # print(sim)
    return sim


# max is 0.5
def name_similarity_elements(element1, element2):
    sum1 = 0
    sum2 = 0

    for tt in TokenTypes:
        if tt == TokenTypes.SYMBOLS:
            continue
        t1 = element1.get_tokens_by_token_type(tt)
        t2 = element2.get_tokens_by_token_type(tt)
        if len(t1) == 0 or len(t2) == 0:
            continue
        sim = name_similarity_tokens(t1, t2)
        sum1 = sum1 + tt.weight * sim
        sum2 = sum2 + tt.weight  # * (len(t1) + len(t2))

    if sum1 == 0 or sum2 == 0:
        return 0

    return sum1 / sum2


def compute_lsim(element1, element2):
    name_similarity = name_similarity_elements(element1, element2)
    max_category = get_max_ns_category(element1.categories, element2.categories)

    return name_similarity * max_category


def get_max_ns_category(categories_e1, categories_e2):
    max_category = -math.inf

    for c1 in categories_e1:
        c1_tokens = list(map(lambda t: Token().add_data(t), nltk.word_tokenize(c1)))

        for c2 in categories_e2:
            c2_tokens = list(map(lambda t: Token().add_data(t), nltk.word_tokenize(c2)))
            name_similarity_categories = name_similarity_tokens(c1_tokens, c2_tokens)

            if name_similarity_categories > max_category:
                max_category = name_similarity_categories

    return max_category
