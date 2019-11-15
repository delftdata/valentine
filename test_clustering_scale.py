from clustering_scale.correlation_clustering_scale import CorrelationClustering
from read_data_movies import data_imdb, data_rt

if __name__ == '__main__':
    # Should vary depending on the quantile and data
    threshold = 0.5
    # Should vary depending on the size of the data
    quantile = 100
    # name of the db

    cc = CorrelationClustering(quantile, threshold)
    cc.add_data(data_imdb, 'imdb')
    cc.add_data(data_rt, 'rt')
    cc.process_data()

    matchings = cc.find_matchings()

    f = open("clustering-matching.txt", "w+")
    for m in matchings:
        f.write(str(m))
    f.close()