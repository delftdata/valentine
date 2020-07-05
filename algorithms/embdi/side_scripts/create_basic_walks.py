import random
import pandas as pd
import re

dataset_name = ''  # name of the input dataset
n_sentences = 0  # number of sentences to generate
sentence_length = 60  # chosen sentence length
ntokens = n_sentences*sentence_length

# read input dataset
df = pd.read_csv('../pipeline/datasets/{}/{}-master.csv'.format(dataset_name, dataset_name), engine='python')

# remove bad symbols from the input datasets
BAD_SYMBOLS_RE = re.compile('[^0-9a-z A-Z]')
for c in df.columns:
    df[c] = df[c].apply(lambda x: re.sub(BAD_SYMBOLS_RE, '', str(x)))
df = df.fillna('')

# decide size of budget for rows and columns
n_row = ntokens*0.5
n_col = ntokens*0.5

# compute number of samples for rows and columns
ntoken_row = n_row/len(df.columns)
nperm = ntoken_row/len(df)
ntoken_col = n_col/60
nsamples = ntoken_col/len(df.columns)

walks = []
# create the walks file
with open('../pipeline/walks/{}-permutations.walks'.format(dataset_name), 'w') as fp:
    for idx, row in df.iterrows():
        for count in range(int(nperm)):
            l = row.tolist()
            l = [str(_) if type(_) != float else str(int(_)) for _ in l ]
            random.shuffle(l)
            fp.write(','.join(l))
            fp.write('\n')
    for col in df.columns:
        for count in range(int(nsamples)):
            l = random.sample(df[col].tolist(), k=60)
            l = [str(_) if type(_) != float else str(int(_)) for _ in l]

            fp.write(' '.join(l))
            fp.write('\n')


