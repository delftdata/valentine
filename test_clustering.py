from clustering.correlation_clustering import CorrelationClustering
from read_data_movies import data_imdb, data_rt


# Should vary depending on the quantile and data
threshold = 0.05
# Should vary depending on the size of the data
quantile = 100
# name of the db
data_type = 'movies'
# data_type = 'tpch'

all_columns = []

cc = CorrelationClustering(quantile, threshold)
cc.add_data(data_imdb, 'imdb', ['Name', 'YearRange', 'Genre'])
cc.add_data(data_rt, 'rt', ['Name', 'Year', 'Genre'])
cc.process_data()

matchings = cc.find_matchings()

f = open("clustering-matching.txt", "w+")
for m in matchings:
    f.write(str(m))
f.close()

