import pandas as pd
import numpy as np


def generate_noisy_dataframe(df, frac_nulls):
    df_mat = df.to_numpy()
    tot_values = df_mat.size
    null_values = int(tot_values * frac_nulls)
    #     print(null_values)
    arr = np.array([1] * null_values + [0] * (tot_values - null_values))
    np.random.shuffle(arr)

    mask = arr.reshape(df_mat.shape)

    df_mat_noisy = np.ma.masked_array(df_mat, mask, fill_value=np.nan)
    df_noisy = pd.DataFrame(df_mat_noisy.filled(np.nan))
    df_noisy.columns = df.columns

    return df_noisy

def generic_replacer(line, fdep, substitutes):
    key = fdep[0]
    value = fdep[1]
    tt = tuple(line[key].values.tolist())
    if tt in substitutes:
        line[value] = substitutes[tt]
    return line


def produce_refined_dataset(df, fdep):
    key = tuple(fdep[0])
    value = tuple(fdep[1])
    sub_col = list(key + value)
    sub_df = df[sub_col]

    groups = sub_df.groupby(key)
    matches = {}
    print(value)
    for idx, grp in groups:
        s = set(grp[value[0]].values.tolist())
        matches[idx] = s

    substitutes = {}
    for tup in matches:
        this = matches[tup]
        str_tup = '{}_{}'.format(*tup)
        if len(this) == 2:
            substitutes[tup] = '#' + str_tup + '#'
        elif len(this) > 2:
            substitutes[tup] = '#' * 5 + str_tup + '#' * 5
    result_df = df.copy()

    result_df = result_df.apply(generic_replacer, fdep=fdep, substitutes=substitutes, axis=1)
    return result_df, substitutes


df1 = pd.read_csv('pipeline/datasets/corleone-dblp.csv')
df2 = pd.read_csv('pipeline/datasets/corleone-scholar.csv')
if True:

    aligned_master = pd.concat([df1, df2], ignore_index=True)

    print(aligned_master.columns)

    # aligned_master = aligned_master[['title', 'director', 'actor_1', 'actor_2', 'actor_3', 'year', 'original_language', 'production_countries', 'vote_average']]

    noisy_aligned_master = generate_noisy_dataframe(aligned_master, 0.25)
    noisy_aligned_master.to_csv('corleone-fd-clear.csv', index=False)

    fd = [
        [['title', 'year'], ['venue']],
        [['title', 'author_1'], ['venue']],
        [['title', 'year'], ['author_1']],
    ]

    subs = []

    for funcdep in fd:
        resdf: pd.DataFrame
        resdf, temp_subs = produce_refined_dataset(noisy_aligned_master, funcdep)
        subs = subs + list(temp_subs.values())

    resdf.to_csv('corleone-fd.csv', index=False)



resdf = pd.read_csv('corleone-fd.csv')
values = resdf.values.ravel()

values_str = [str(_) for _ in values]

uniq, counts = np.unique(np.array(values_str),return_counts=True)

counts_dict = dict(zip(uniq, counts))

tolookfor = subs

sub_occurrences = {k: counts_dict.get(k, 0) for k in tolookfor}

n_substituted = sum(sub_occurrences.values())

n_nulls = sum(pd.Series(values).isnull())

n_values = len(values)

print(n_nulls)

print(n_nulls/n_values)

print(n_substituted/n_values)