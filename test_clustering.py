import clustering as cl
from clustering.column_model import Column
# from read_data_tpch import data
from read_data_movies import data_imdb, data_rt

# Should vary depending on the quantile and data
threshold = 0.05
# Should vary depending on the size of the data
quantile = 5
# name of the db
data_type = 'movies'
# data_type = 'tpch'

all_columns = []


def process_data(dataset, column_list, source):
    # tokenize each db column and create the clustering.column_model.Column entity

    columns = []

    for column in column_list:
        print("Process column %s" % column)
        c = Column(column, dataset[column], source)
        print("\tTokenize data...")
        c.process_data()
        columns.append(c)

    return columns


if data_type == 'tpch':
    # selected a small subset from the db

    selected = ['C_Comment', 'AC_Comment', 'EC_Comment']

    all_columns.extend(process_data(data, selected, 'tpch'))
else:
    selected_imdb = ['Name', 'YearRange', 'Genre']
    selected_rt = ['Name', 'Year', 'Genre']

    all_columns.extend(process_data(data_imdb, list(data_imdb.columns), 'imdb'))
    all_columns.extend(process_data(data_rt, list(data_rt.columns), 'rt'))

print("Compute distribution clusters ...\n")
distribution_clusters = cl.discovery.compute_distribution_clusters(all_columns, threshold, quantile)

print("Find connected components ... \n")
connected_components = cl.discovery.bfs(distribution_clusters, list(distribution_clusters.keys())[0])
print(connected_components)

print("Compute attributes ... \n")
edges = cl.discovery.compute_attributes(all_columns, list(connected_components), threshold, quantile)

result = cl.discovery.correlation_clustering_pulp(list(connected_components), edges)

print(cl.discovery.process_correlation_clustering_result(result))

