import clustering as cl
from read_data import data

threshold = 0.1
distribution_clusters = cl.discovery.compute_distribution_clusters(data, data.columns, threshold)