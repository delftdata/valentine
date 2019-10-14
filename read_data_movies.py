import pandas as pd

# REQUIREMENTS: Movies1 db - https://sites.google.com/site/anhaidgroup/useful-stuff/data

data_imdb = pd.read_csv('data/google/movies1/csv_files/imdb.csv', index_col=False)
data_rt = pd.read_csv('data/google/movies1/csv_files/rotten_tomatoes.csv', index_col=False)
# data_imdb.set_index('Unnamed: 0', inplace=True)
# data_rt.set_index('Unnamed: 0', inplace=True)
data_imdb = data_imdb.fillna('')
data_rt = data_rt.fillna('')