import pandas as pd

matches_file = 'labeled_matches.csv'
matches = pd.read_csv(matches_file)

df = pd.read_csv('pipeline/datasets/beer-master.csv')

print('File {} has {} lines.'.format(matches_file, len(matches)))

last_flagged = max(matches.loc[matches['flag']>0].index)

print('Last row with label 1 or 2: {}'.format(last_flagged))

from_idx = int(input('First row: '))
to_idx = int(input('Last row: '))

labels = {_:0 for _ in range(len(matches))}
idx = from_idx
while 0 <= idx < to_idx:
    line = matches.iloc[idx]
    idx1, idx2, _ = line
    print('Line # {}'.format(line))
    print('Previously set label: {}'.format(_+1))
    rid1 = int(idx1.split('_')[1])
    rid2 = int(idx2.split('_')[1])

    print(df.iloc[[rid1, rid2]].to_string())

    label = input('\nMatch? 1 = NO, 2 = YES, 3 = NOT SURE, B = BACK, Q = QUIT:\t\t')
    if label.lower() == 'b':
        idx -= 1
    elif label.lower() == 'q':
        break
    else:
        idx += 1
        matches.loc[idx, 'flag'] = int(label) - 1

    print('#'*20)

matches.to_csv('labeled_matches.csv', index=False)
