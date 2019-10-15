from clustering.column_model import Column
import clustering.discovery as discovery


class CorrelationClustering:
    def __init__(self, quantiles, threshold):
        self.data = list(dict())
        self.quantiles = quantiles
        self.threshold = threshold
        self.columns = list()

    def add_data(self, data, source_name, selected_columns=[]):
        new_data = dict()
        new_data['data'] = data
        new_data['source_name'] = source_name
        new_data['selected_columns'] = selected_columns
        self.data.append(new_data)

    def set_quantiles(self, quantiles):
        self.quantiles = quantiles

    def set_threshold(self, threshold):
        self.threshold = threshold

    def __process_data(self, data_object):
        """
        Tokenize each data column

        :return: clustering.column_model.Column entity
        """
        columns = []

        if len(data_object["selected_columns"]) > 0:
            column_list = data_object["selected_columns"]
        else:
            column_list = list(data_object["data"].columns)

        for column in column_list:
            print("Process column %s" % column)
            c = Column(column, data_object["data"][column], data_object["source_name"])
            print("\tTokenize data...")
            c.process_data()
            columns.append(c)

        return columns

    def find_matchings(self):
        print("Process data ... \n")
        for item in self.data:
            self.columns.extend(self.__process_data(item))

        print("Compute distribution clusters ...\n")
        distribution_clusters = discovery.compute_distribution_clusters(self.columns, self.threshold, self.quantiles)

        print("Find connected components ... \n")
        connected_components = discovery.get_connected_components(distribution_clusters)

        print("Compute attributes ... \n")
        all_attributes = list()
        for components in connected_components:
            edges = discovery.compute_attributes(self.columns, components, self.threshold, self.quantiles)
            all_attributes.append((components, edges))

        print("Solve linear program ... \n")
        results = list()
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        print(results)

        print("Extract clusters ... \n")
        clusters = list()
        for result in results:
            clusters.append(discovery.process_correlation_clustering_result(result))

        return clusters


def test():
    cc = CorrelationClustering(256, 0.05)

    from read_data_movies import data_imdb, data_rt
    cc.add_data(data_imdb, 'imdb', ['Name', 'YearRange', 'Genre'])
    cc.add_data(data_rt, 'rt', ['Name', 'Year', 'Genre'])

    matchings = cc.find_matchings()

    return matchings

