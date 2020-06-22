import random as rd
import re
import warnings
from itertools import chain

import nltk
import numpy as np
import pandas as pd
from datasketch import MinHash, MinHashLSH
from similarity.levenshtein import Levenshtein
from similarity.normalized_levenshtein import NormalizedLevenshtein

nltk.download('stopwords')
from nltk.corpus import stopwords

REPLACE_BY_SPACE_RE = re.compile('[-._/(){}\[\]\|@,;\\\/]')
BAD_SYMBOLS_NUMBERS = re.compile('[^\d*(\.\d+)?]')
BAD_SYMBOLS_RE = re.compile('[^0-9a-z A-Z]')
STOPWORDS = set(stopwords.words('english'))


def check_info(df, columns=[]):
    null = df.isnull().sum()
    if len(columns) == 0:
        columns = df.columns
    for c in columns:
        print("column: ", c)
        print("total not null values: ", len(df[df[c].notnull()]))
        print("total null values: ", null[c])
        print("total unique values: ", len(df[c].unique()))
        gr_title_count = df.groupby(c).count()
        print("Duplicated values: ", gr_title_count[gr_title_count['id'] > 1].shape[0])
        print("Duplicated instances: ",
              gr_title_count[gr_title_count['id'] > 1]['id'].sum() - gr_title_count[gr_title_count['id'] > 1].shape[0])
        print("===================")


def data_preprocessing(dfs, params):
    """
    Preprocess dataframes according to the parameters passed in the params dict. Default parameters are provided.
    :param dfs: list of data frames
    :param params: processing parameters
    :return: the concatenated dataset
    """
    parameters = {
        'missing_value': 'nan,ukn,none,unknown,', # values to be interpreted as nulls
        'missing_value_strategy': '', # strategy to be used when handling null values
        'round_number': -1, # number of rounding digits for float numbers
        'round_columns': '', # columns to be rounded
        'split_columns': '', # columns that contain lists to be split
        'split_delimiter': ',', # delimiter used in the lists
        'expand_columns': '', # columns that should be expanded in the graph generation phase
        'tokenize_shared': True, # flag to use heuristic
        'concatenate': '', # one of {'outer', 'inner', 'horizon'}
        'auto_merge': False, # flag used to trigger automatic LSH merge of candidates
        'mh_k_shingles': 3, # size of lsh shingles
        'mh_threshold': .5, # minhash threshold
        'mh_perm': 128, # minhash permutations
        'distance': 'normalized_edit_distance', # string distance to be used
        'distance_threshold': .20, # distance threshold to decide similar values
        'merge_columns': '', # columns to study when performing the lsh merge
        'remove_stop_word': False, # flag to use to remove stop words
        'case_sensitive': False, # flag to use to specify if the content of the dataset is case sensitive
    }
    for _ in params.keys():
        if _ in parameters:
            parameters[_] = params[_]

    # Check if parameters are valid
    try:
        parameters['round_number'] = int(parameters['round_number'])
    except ValueError:
        parameters['round_number'] = -1
        warnings.warn('Round number must be an integer. Run without rounding number.')
    if parameters['round_number'] > -1 and parameters['round_columns'] == '':
        warnings.warn('No attributes chosen to round.')
    if parameters['auto_merge'] and parameters['merge_columns'] == '':
        parameters['auto_merge'] = False
        warnings.warn('No attributes chosen to merge.')

    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]
        parameters['concatenate'] = ''

    all_merge_columns = []

    df_words = {}

    # Iterate through all data frames.
    for i, df in enumerate(dfs):
        # Set all values to lowercase if words are not case sensitive.
        if not parameters['case_sensitive']:
            df.columns = [_.lower() for _ in df.columns]
        df.columns = [re.sub(r'\s+', '_', str(x)) for x in df.columns]
        # df[c] = df[c].apply(lambda x: )
        #
        # normalize missing values
        if parameters['missing_value'] != '':
            missing_value = parameters['missing_value'].split(',')
            missing_value = [_.strip() for _ in missing_value]
            missing_value = "|".join(missing_value)
            pattern = re.compile('^\s*(' + missing_value + ')\s*$', re.IGNORECASE)
            df = df.replace(pattern, np.nan)

        # Extract the columns needed to perform different operations on
        num_columns = [_ for _ in parameters['round_columns'].split(',') if _ in df.columns]
        split_columns = [_ for _ in parameters['split_columns'].split(',') if _ in df.columns]
        if parameters['tokenize_shared']:
            expand_columns = df.columns
        else:
            expand_columns = [_ for _ in parameters['expand_columns'].split(',') if _ in df.columns]

        # normalize text: lower/trim/replace by space/remove bad chars/concatenate string
        for c in df.columns:

            # Apply string normalization only on string fields.
            if df[c].dtype == 'object':

                if c in num_columns:
                    warnings.warn(
                        'Column {} is marked to be rounded, but it contains non-numeric characters.'.format(c))
                    df[c] = df[c].apply(lambda x: re.sub(BAD_SYMBOLS_NUMBERS, '', str(x)))
                    try:
                        df[c] = df[c].replace('', np.nan).astype(float)
                    except ValueError():
                        print('Something went wrong. Wrong type found. Skipping column {}'.format(c))
                else:
                    if not parameters['case_sensitive']:
                        df[c] = df[c].str.lower()
                    # If the column under observation is a list, it will be expanded
                    if c in split_columns:
                        df[c] = df[c].apply(_split_lists, delimiter=parameters['split_delimiter'])
                    else:
                        df[c] = df[c].apply(lambda x: re.sub(BAD_SYMBOLS_RE, '', str(x)))
                        df[c] = df[c].apply(lambda x: re.sub(REPLACE_BY_SPACE_RE, ' ', str(x).strip().lower()))
                        # If the column should be expanded,
                        if c in expand_columns:
                            df[c] = df[c].apply(lambda x: re.sub(r'\s+', '_', str(x)))
                        else:
                            df[c] = df[c].apply(lambda x: re.sub(r'\s+', '|', str(x)))
                if parameters['remove_stop_word']:
                    df[c] = df[c].apply(_remove_stop_words, '_')

        for c in num_columns:
            df[c] = df[c].apply(lambda x: re.sub(r'[^\d.]', '', str(x)))
            df[c] = df[c].apply(_round_number, ndigits=parameters['round_number'])

        # change the columns name in case of horizon concatenation
        if parameters['concatenate'] == 'horizon' and len(dfs) > 1:
            for idx, c in enumerate(parameters['merge_columns'].split(',')):
                if c in df.columns:
                    all_merge_columns.append(str(i) + '_' + c)
            df.columns = str(i) + '_' + df.columns

        dfs[i] = df
        df_words[i] = get_unique_string_values(df, df.columns)

    # concatenate datasets
    if len(dfs) > 1:
        if parameters['concatenate'] == 'inner':
            concat_df = pd.concat(dfs, join="inner")
        elif parameters['concatenate'] == 'horizon':
            concat_df = pd.concat(dfs, axis=0, ignore_index=True, sort=True)
        else:
            concat_df = pd.concat(dfs, sort=True)
    else:
        concat_df = dfs[0]

    def retokenize(line, words):
        if line in words:
            return line.replace('_', '|')
        else:
            return line

    if len(dfs) > 1:
        if parameters['tokenize_shared']:
            intersection = df_words[0].intersection(df_words[1])
            if len(dfs) > 2:
                for i, df in enumerate(dfs[2:]):
                    intersection = intersection.intersection(df_words[i])
            for c in concat_df.columns:
                concat_df[c] = concat_df[c].apply(retokenize, words=intersection)

    concat_df.reset_index(inplace=True, drop=True)

    # merge
    if parameters['auto_merge']:
        if len(all_merge_columns) == 0:
            all_merge_columns = [_ for _ in parameters['merge_columns'].split(',') if _ in concat_df.columns]
        if len(all_merge_columns) > 0:
            # get unique values
            uniq_values = set(concat_df[all_merge_columns].astype(str).values.ravel())
            lsh = LSHMerge(uniq_values, parameters['mh_k_shingles'], parameters['mh_threshold'], parameters['mh_perm'])
            replacement = lsh.get_replacement(parameters['distance'], parameters['distance_threshold'])

            # rep_path = '../pipeline/replacements/'
            # with open(rep_path + parameters['output_file'] + '.txt', 'wb') as fp:
            #     fp.write(pickle.dumps(replacement))

            concat_df = concat_df.replace(replacement)
        else:
            pass

    # empty value
    # concat_df = concat_df.fillna('')
    if parameters['missing_value_strategy'] == 'separated_null':
        for c in concat_df.columns:
            c_idx = concat_df.columns.get_loc(c)
            concat_df.loc[concat_df[c] == '', c] = c_idx + '_null_' + concat_df.loc[concat_df[c] == '', c].index.astype(
                str)
    elif parameters['missing_value_strategy'] == 'one_null':
        concat_df = concat_df.replace('', 'null')

    return concat_df


def _remove_stop_words(text, delimiter=" "):
    text = delimiter.join([word for word in text.split(delimiter) if word not in STOPWORDS])
    return text


def _split_lists(line, delimiter=','):
    split = []
    if line is np.nan:
        return ''
    for word in [_.strip() for _ in line.split(delimiter)]:
        x = re.sub(BAD_SYMBOLS_RE, '', str(word))
        x = re.sub(REPLACE_BY_SPACE_RE, ' ', str(x).strip().lower())
        x = re.sub(' ', '|', str(x).strip().lower())
        split.append(x)
    s = '_'.join(split)
    return s


def _split_sticked_words(text):
    try:
        f_text = float(text)
        return f_text
    except ValueError:
        s = ""
        i = 0
        while i < (len(text) - 1):
            if text[i].isdigit():
                if text[i - 1].isalpha() and text[i + 1].isdigit():
                    s = s + " " + text[i]
                elif text[i - 1].isalpha() and text[i + 1].isalpha():
                    s = s + text[i] + " "
                else:
                    s += text[i]
            elif text[i].islower() and text[i + 1].isupper():
                s = s + text[i] + " "
            else:
                s += text[i]
            i += 1
        return s + text[-1]


def expand_columns_old(df, columns, drop=True):
    """
    Expand a column into word-based columns
    :param df:
    :param column:
    :param drop:
    :return:
    """
    if isinstance(columns, str):
        columns = [columns]
    for c in columns:
        old_columns = df.columns
        new_columns = []
        expanded_columns = df[c].str.split("_", expand=True)
        for i in range(expanded_columns.shape[1]):
            df[c + "_" + str(i)] = expanded_columns[i]
            new_columns.append(c)
        if drop:
            df.drop(columns=[c], inplace=True)
            old_columns.remove(c)
        df.columns = old_columns + new_columns

    # if drop:
    #     df.drop(columns=columns, inplace=True)
    return df


def _round_number(value, ndigits):
    try:
        return round(float(value), ndigits)
    except ValueError:
        return value


def get_unique_string_values(df, columns, level='token'):
    """
    Get unique values in token level or word level
    :param df:
    :param columns:
    :param level:
    :return:
    """
    values = df[columns].astype(str).values.ravel()
    if level == 'word':
        words = [v.split('_') for v in values]
        words = set(chain.from_iterable(words))
    else:
        words = set(values)
    w_f = []
    for w in words:
        try:
            if float(w):
                w_f.append(w)
        except ValueError:
            pass
    for w in w_f:
        words.remove(w)
    return words


def merge(df, replacement_dict, columns, level='token'):
    """
    Merge values based on a dictionary of replacement in level token / word
    :param df:
    :param replacement_dict:
    :param columns:
    :param level:
    :return:
    """
    if isinstance(columns, str):
        columns = [columns]
    if level == 'token':
        df = df.replace(replacement_dict)
    elif level == 'word':
        for f, r in replacement_dict.items():
            f_pattern = re.compile('(^|_)' + f + '($|_)', re.IGNORECASE)
            r_repl = lambda m: m.group(1) + r + m.group(2)
            for c in columns:
                df[c] = df[c].str.replace(f_pattern, r_repl)
    return df


def write_info_file(dfs, output, f=[]):
    """
    Write the info file
    :param f: list of file paths
    :param dfs: list of dataframes in the same order as the concatenation in the dp phase
    :param output: output file name
    :return:
    """
    # info_path = '../pipeline/info/'
    with open(output, 'w') as fp:
        if len(f) != len(dfs):
            f = list(range(len(dfs)))
        for idx, df in enumerate(dfs):
            fp.write('{},{}\n'.format(f[idx], df.shape[0]))


class LSHMerge:
    def __init__(self, uniq_values, k_shingles=3, mh_threshold=.5, mh_num_perm=128, delimiter='_'):
        self.k_shingles = k_shingles
        self.mh_threshold = mh_threshold
        self.mh_num_perm = mh_num_perm
        self.uniq_values = uniq_values
        self.delimiter = delimiter
        self.lsh = MinHashLSH(threshold=self.mh_threshold, num_perm=self.mh_num_perm)
        self._generate_hashes()

    def _generate_hashes(self):
        for token in self.uniq_values:
            if isinstance(token, str):
                m = self._generate_hash(token)
                self.lsh.insert(token, m)

    def _generate_hash(self, value):
        if isinstance(self.k_shingles, int):
            shingles = set([value[max(0, i - self.k_shingles):i] for i in range(self.k_shingles, len(value) + 1)])
        elif self.k_shingles == 'word':
            shingles = set(value.split(self.delimiter))

        m = MinHash(num_perm=self.mh_num_perm)
        for shingle in shingles:
            m.update(shingle.encode('utf8'))
        return m

    def get_similarities(self, value):
        m = self._generate_hash(value)
        return self.lsh.query(m)

    def get_sample_blocks(self, n=5):
        count = 0
        n_tries = 20
        results = []
        tried = set()
        while len(results) < n and count < n_tries:
            samples = rd.sample(self.uniq_values, min(n, len(self.uniq_values)))
            for _ in samples:
                if len(results) >= n:
                    break
                if _ not in tried:
                    m = self._generate_hash(_)
                    similarities = self.lsh.query(m)
                    if len(similarities) > 1:
                        results.append(similarities)
                    tried.add(_)
            count += 1
        return results

    def get_replacement(self, distance='lsh', threshold=.8):
        if distance == 'edit_distance':
            distance = Levenshtein()
        elif distance == 'normalized_edit_distance':
            distance = NormalizedLevenshtein()

        # for each token, get its bin
        # for each bin, iterate each element and get the groups of satisfied tokens such as
        # [white] = [whit, whie, whit]
        # [whie] = [whine,white]

        replacement = {}
        s = self.uniq_values

        while len(s) > 0:
            token = rd.sample(s, 1)[0]
            s.remove(token)
            m = self._generate_hash(token)
            similarities = self.lsh.query(m)
            similarities = [_ for _ in similarities if _ not in replacement.values() and _ not in replacement.keys()]
            if len(similarities) > 1:
                scores = {}
                bin_replacement = {}
                if distance != 'lsh':
                    for idx, item in enumerate(similarities):
                        count = 0
                        candidates = []
                        for idx_compared in range(idx + 1, len(similarities)):
                            candidate = similarities[idx_compared]
                            if item != candidate and distance.distance(item, candidate) < threshold:
                                if idx not in bin_replacement:
                                    bin_replacement[idx] = [idx_compared]
                                else:
                                    bin_replacement[idx].append(idx_compared)
                                if idx_compared not in bin_replacement:
                                    bin_replacement[idx_compared] = [idx]
                                else:
                                    bin_replacement[idx_compared].append(idx)

                    for idx_item, candidates in sorted(bin_replacement.items(), key=lambda x: -len(x[1])):
                        item = similarities[idx_item]
                        if item in replacement.keys():
                            item = replacement[item]
                        for idx_candidate in candidates:
                            candidate = similarities[idx_candidate]
                            if candidate != item and candidate not in replacement.keys():
                                if item not in replacement.keys():
                                    replacement[candidate] = item
                                elif replacement[item] != candidate:
                                    replacement[candidate] = replacement[item]
                else:
                    for candidate in similarities:
                        if candidate != token:
                            replacement[candidate] = token

        return replacement


if __name__ == '__main__':
    parameters = {
        'output_file': '',
        'concatenate': 'outer',
        'missing_value': 'nan,ukn,none,unknown,-',
        'missing_value_strategy': '',
        'round_number': 1,
        'round_columns': '',
        'auto_merge': False,
        'expand_columns': '',
        'tokenize_shared': False
    }
    f1 = '../pipeline/experiments/programming_challenge/www.shopbot.com.au.csv'
    df1 = pd.read_csv(f1, encoding='ISO-8859-1')
    for c in df1.columns:
        if df1[c].dtype == 'object':
            df1[c] = df1[c].str.replace('_', ' ')

    f2 = '../pipeline/datasets/programming_challenge/www.pricedekho.com.csv'
    df2 = pd.read_csv(f2, encoding='ISO-8859-1')

    for c in df2.columns:
        if df2[c].dtype == 'object':
            df2[c] = df2[c].str.replace('_', ' ')

    # print("Datasets exploration:", f1)
    # check_info(df1, ['title', 'manufacturer', 'description'])
    # print("Datasets exploration:", f2)
    # check_info(df2, ['title', 'manufacturer', 'description'])

    df_c = data_preprocessing([df1, df2], parameters)
    write_info_file([df1, df2], 'cameras_testing', [f1, f2])

    # Merge title in word level
    # words = get_unique_string_values(df_c, 'title', 'word')
    #
    # lsh = LSHMerge(words, 2, .5, 128)
    # replacement = lsh.get_replacement('normalized_edit_distance', .35)
    #
    # df_c = merge(df_c, replacement, ['title'], 'word')

    # Manually handle the merge for each column using different params
    # print("Start merging title")
    # uniq_values = set(df_c[['title']].values.ravel())
    #
    # start_time = datetime.datetime.now()
    # lsh = LSHMerge(uniq_values, 2, .3, 128)
    # replacement = lsh.get_replacement('normalized_edit_distance', .7)
    # end_time = datetime.datetime.now()
    #
    # print('Generate replacement list: ', end_time - start_time)
    # print('Length of replacement list: ', len(replacement))
    #
    # df_c = df_c.replace(replacement)

    # print("Start merging manufacturer")
    # uniq_values = set(df_c[['manufacturer']].values.ravel())
    #
    # start_time = datetime.datetime.now()
    # lsh = LSHMerge(uniq_values, 3, .5, 128)
    # replacement = lsh.get_replacement('normalized_edit_distance', .2)
    # end_time = datetime.datetime.now()
    #
    # print('Generate replacement list: ', end_time - start_time)
    # print('Length of replacement list: ', len(replacement))
    #
    # print("Concatenated dataset exploration:")
    # check_info(df_c, ['title', 'manufacturer', 'description'])

    # Export the file
    df_c.to_csv('../pipeline/datasets/' + parameters['output_file'] + '.csv', index=False)
