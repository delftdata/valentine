import argparse
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', action='store', required=True, type=str)
    parser.add_argument('-o', '--output_file', action='store', required=True, type=str)
    parser.add_argument('-d', '--dataset_file', action='store', required=False, type=str, default=None)

    args = parser.parse_args()
    return args


args = parse_args()

in_filename = args.input_file

out_filename = args.output_file

flag_dataset = False
if args.dataset_file is not None:
    col_uniques = {}
    flag_dataset = True
    df = pd.read_csv(args.dataset_file)
    f_classes = open(out_filename + '_classes.txt', 'w')
    for col in df.columns:
        col_uniques[col] = df[col].unique()


fin = open(in_filename, 'r') 
f_labels = open(out_filename + '_labels.txt', 'w')
f_vectors = open(out_filename + '_vectors.tsv', 'w')

lines = fin.readlines()
labels = []
n_tokens, n_dimensions = lines[0].split(' ')
for l in lines[1:]:
    split = l.strip().split(' ')
    if len(split) == (int(n_dimensions) + 1):
        f_labels.write('{}\n'.format(split[0]))
        s = ''
        for _ in split[1:]:
            s+='{}\t'.format(_)
        s = s.strip() + '\n'
        f_vectors.write(s)
        if flag_dataset:
            for col in df.columns:
                if split[0] in col_uniques[col]:
                    s = '{}\n'.format(col)
                    f_classes.write(s)

    
f_vectors.close()
f_labels.close()

if flag_dataset:
    f_classes.close()

fin.close()
