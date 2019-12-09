from clustering.column_model import Column
import clustering.discovery as discovery


class CorrelationClustering:
    def __init__(self):
        self.data = list(dict())
        self.quantiles = None
        self.threshold = None
        self.columns = list()
        self.__processed = False

    def add_data(self, table_name, data, selected_columns=[]):
        """
        Create data objects

        :param data: Dataframe
        :type data: pandas Dataframe
        :param table_name: Name of the table
        :param selected_columns: If only a part of the dataset should be used, specify the columns
        :return: The function adds the new data object to the data param
        """
        new_data = dict()
        new_data['data'] = data
        new_data['source_name'] = table_name
        new_data['selected_columns'] = selected_columns
        self.data.append(new_data)
        self.__processed = False

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

    def process_data(self):
        print("Process data ... \n")
        self.columns = list()
        for item in self.data:
            self.columns.extend(self.__process_data(item))
        self.__processed = True

    def find_matchings(self):
        if self.quantiles is None:
            print("Please set the quantiles before finding matchings (call set_quantiles())")
            return

        if self.threshold is None:
            print("Please set a threshold before finding matchings (call set_threshold())")
            return

        if not self.__processed:
            print("Please process data before finding matchings (call process_data())")
            return

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


