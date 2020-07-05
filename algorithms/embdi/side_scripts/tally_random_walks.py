from collections import Counter
import ast

walks_file = 'pipeline/walks/movies-EQ-newgraph-smooth-nobacktrack-s0.05-colbias05-nocid.walks'

walks = []
with open(walks_file, 'r') as fp:
    for i, line in enumerate(fp):
        # walks = walks + line.strip().split(',')
        walks.append(line.strip().split(','))
    walks = [_ for walk in walks for _ in walk]

counts = Counter(walks)
for tup in counts.most_common(10):
    s = '{}, {}'.format(*tup)
    print(s)

with open(walks_file + '.counts', 'w') as fp:
    # for tup in counts:
    for tup in counts.most_common():
        s = '{}, {}\n'.format(*tup)
        fp.write(s)
