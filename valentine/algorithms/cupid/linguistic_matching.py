import math
import operator
import string
from itertools import product, repeat, combinations_with_replacement
from multiprocessing import get_context
import nltk
import snakecase as snakecase
from anytree import LevelOrderIter
from nltk.corpus import stopwords
from nltk.corpus import wordnet as wn
import Levenshtein as Lv
from similarity.ngram import NGram

from .schema_element import SchemaElement, Token, TokenTypes

DATATYPE_COMPATIBILITY_TABLE = {
    "text": {"keyword": 1.0, "varchar": 1.0, "nvarchar": 0.9, "nchar": 0.8, "char": 0.6},
    "keyword": {"text": 1.0, "varchar": 1.0, "nvarchar": 0.9, "nchar": 0.8, "char": 0.6},
    "varchar": {"text": 1.0, "keyword": 1.0, "nvarchar": 0.9, "nchar": 0.8, "char": 0.6},
    "nvarchar": {"text": 0.9, "keyword":  0.9, "varchar": 0.9, "nchar": 0.8, "char": 0.6},
    "nchar": {"text": 0.7, "keyword":  0.7, "varchar": 0.7, "nvarchar": 1.0, "char": 0.7},
    "char": {"text": 0.7, "keyword":  0.7, "varchar": 0.7, "nchar": 0.8, "nvarchar": 0.6},
    "date": {"double": 0.1, "int": 0.1, "decimal": 0.1, "bit": 0.1},
    "double": {"date": 0.1, "float": 1.0, "decimal": 1.0},
    "decimal": {"date": 0.1, "float": 1.0, "double": 1.0},
    "int": {"date": 0.1, "long": 0.8, "short": 0.7, "smallint": 0.7, "integer": 1.0},
    "integer": {"date": 0.1, "long": 0.8, "short": 0.7, "smallint": 0.7, "int": 1.0},
    "bit": {"time": 0.1, "date": 0.1},
    "time": {"bit": 0.1},
    "float": {"double": 0.9},
    "long": {"short": 0.6, "int": 0.8, "bigint": 1.0, "smallint": 0.6, "integer": 0.8},
    "bigint": {"short": 0.6, "int": 0.8, "long": 1.0, "smallint": 0.6, "integer": 0.8},
    "short": {"long": 0.6, "int": 0.8, "bigint": 0.6, "smallint": 1.0, "integer": 0.8},
    "smallint": {"long": 0.6, "int": 0.8, "bigint": 0.6, "short": 1.0, "integer": 0.8}
}


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


def add_token_type(token: Token):
    try:
        float(token.data)
        return TokenTypes.NUMBER
    except ValueError:
        return TokenTypes.CONTENT


def compute_compatibility(categories):
    compatibility_table = dict()
    for cat1, cat2 in combinations_with_replacement(categories, 2):
        if cat1 not in compatibility_table:
            compatibility_table[cat1] = dict()
        if cat2 not in compatibility_table:
            compatibility_table[cat2] = dict()
        if cat1 in DATATYPE_COMPATIBILITY_TABLE and cat2 in DATATYPE_COMPATIBILITY_TABLE[cat1]:
            compatibility_table[cat1][cat2] = DATATYPE_COMPATIBILITY_TABLE[cat1][cat2]
            compatibility_table[cat2][cat1] = DATATYPE_COMPATIBILITY_TABLE[cat1][cat2]
        else:
            tokens1 = list(map(lambda t: Token().add_data(t),
                               list(filter(lambda x: x.isalnum(), nltk.word_tokenize(cat1)))))
            for token in tokens1:
                token.token_type = add_token_type(token)

            tokens2 = list(map(lambda t: Token().add_data(t),
                               list(filter(lambda x: x.isalnum(), nltk.word_tokenize(cat2)))))
            for token in tokens2:
                token.token_type = add_token_type(token)
            compatibility = data_type_similarity(tokens1, tokens2)
            compatibility_table[cat1][cat2] = compatibility
            compatibility_table[cat2][cat1] = compatibility
    return compatibility_table


def comparison(source_tree, target_tree, compatibility_table, th_ns, parallelism):
    elements_to_compare = generate_parallel_l_sim_input(source_tree, target_tree, compatibility_table, th_ns)
    if parallelism == 1:
        l_sim = {k: v for k, v in [l_sim_proc(pair, compatibility_table) for pair in elements_to_compare]}
    else:
        with get_context("spawn").Pool(parallelism) as process_pool:
            l_sim = dict(process_pool.starmap(l_sim_proc, zip(list(elements_to_compare), repeat(compatibility_table))))
    return l_sim


def generate_parallel_l_sim_input(source_tree, target_tree, compatibility_table, th_ns):
    all_nodes_s = [node for node in LevelOrderIter(source_tree.root)]
    all_nodes_t = [node for node in LevelOrderIter(target_tree.root)]
    all_nodes = product(all_nodes_s, all_nodes_t)
    for pair in all_nodes:
        if pair[0].categories[0] in compatibility_table and \
            pair[1].categories[0] in compatibility_table[pair[0].categories[0]] and \
                compatibility_table[pair[0].categories[0]][pair[1].categories[0]] > th_ns:
            yield pair


def l_sim_proc(pair: tuple, compatibility_table: dict):
    s, t = pair
    s_cat = s.categories
    t_cat = t.categories
    max_s = [max(dict(filter(lambda x: x[0] in t_cat, compatibility_table[c].items())).items(),
                 key=operator.itemgetter(1))[1] for c in s_cat]
    return (s.long_name, t.long_name), name_similarity_elements(s, t) * max(max_s)


def data_type_similarity(token_set1, token_set2):
    sum1 = 0
    sum2 = 0
    for tt in TokenTypes:
        if tt == TokenTypes.SYMBOLS:
            continue
        t1 = list(filter(lambda t: t.token_type == tt, token_set1))
        t2 = list(filter(lambda t: t.token_type == tt, token_set2))
        if len(t1) == 0 or len(t2) == 0:
            continue
        sim = name_similarity_tokens(t1, t2)
        sum1 = sum1 + tt.weight * sim
        sum2 = sum2 + tt.weight
    if sum1 == 0 or sum2 == 0:
        return 0
    return sum1 / sum2


# max = 1
def name_similarity_tokens(token_set1, token_set2):
    sum1 = get_partial_similarity(token_set1, token_set2)
    sum2 = get_partial_similarity(token_set2, token_set1)
    return (sum1 + sum2) / (len(token_set1) + len(token_set2))


def get_partial_similarity(token_set1, token_set2):
    total_sum = 0
    for t1 in token_set1:
        max_sim = -math.inf
        for t2 in token_set2:
            sim = compute_similarity_wordnet(t1.data, t2.data)
            if math.isnan(sim):
                sim = compute_similarity_leven(t1.data, t2.data)

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
    best = max(wn.wup_similarity(s1, s2) or math.nan for s1, s2 in product(allsyns1, allsyns2))
    return best


# the lower, the better
def compute_similarity_ngram(word1, word2, n):
    ngram = NGram(n)
    sim = ngram.distance(word1, word2)
    return sim


# Higher the better
def compute_similarity_leven(word1, word2):
    return Lv.ratio(word1, word2)


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
        sum2 = sum2 + tt.weight

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
