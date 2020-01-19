from clustering.correlation_clustering import CorrelationClustering
from read_data_movies import data_imdb, data_rt


# if __name__ == '__main__':
# Should vary depending on the quantile and data
threshold = 0.03
# Should vary depending on the size of the data
quantile = 256
# name of the db

cc = CorrelationClustering()
cc.set_threshold(threshold)
cc.set_quantiles(quantile)
cc.add_data('imdb', data_imdb)
cc.add_data('rt', data_rt)
cc.process_data()

matchings = cc.find_matchings()

f = open("clustering-matching.txt", "w+")
for m in matchings:
    f.write(str(m))
f.close()

