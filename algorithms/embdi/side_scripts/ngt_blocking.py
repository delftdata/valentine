import ngtpy


def prepare_files(emb_path):
    with open(emb_path, 'r') as fi, \
            open('objects.tsv', 'w') as fov, open('words.tsv', 'w') as fow:
        n, dim = map(int, fi.readline().split())
        fov.write('{0}\t{1}\n'.format(n, dim))
        for line in fi:
            tokens = line.rstrip().split(' ')
            if tokens[0].startswith('idx_'):
                fow.write(tokens[0] + '\n')
                fov.write('{0}\n'.format('\t'.join(tokens[1:])))


if __name__ == '__main__':
    emb_path = 'pipeline/embeddings/movies-ER-golden.emb'
    # prepare_files(emb_path)

    index_path = 'index.anng'

    with open('objects.tsv', 'r') as fin:
        n, dim = map(int, fin.readline().split())
        ngtpy.create(index_path, dim, distance_type='Cosine')  # create an empty index
        index = ngtpy.Index(index_path)  # open the index
        for line in fin:
            object = list(map(float, line.rstrip().split('\t')))
            index.insert(object)  # insert objects
    index.build_index()  # build the index
    index.save()  # save the index

    with open('words.tsv', 'r') as fin:
        words = list(map(lambda x: x.rstrip('\n'), fin.readlines()))

    index = ngtpy.Index('index.anng') # open the index
    query_id = 31
    query_object = index.get_object(query_id) # get the object

    result = index.search(query_object, epsilon = 0.10) # approximate nearest neighbor search
    print('Query={}'.format(words[query_id]))
    for rank, object in enumerate(result):
        print('{}\t{}\t{:.6f}\t{}'.format(rank + 1, object[0], object[1], words[object[0]]))

