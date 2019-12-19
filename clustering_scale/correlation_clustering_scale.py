import timeit
from pandas import DataFrame
from multiprocessing import Pool

import clustering_scale.discovery_scale as discovery
from clustering_scale.scale_utils import process_columns, ingestion_column_generator


class CorrelationClustering:
    """
    A class that contains the data and methods required for the algorithms proposed in
    "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]

    Attributes
    ----------
    quantiles: int
        the number of quantiles of the histograms
    threshold : float
        the global threshold described in [1]
    columns : list(str)
        a list with all the compound column names (table_name + '_' + column_name)

    Methods
    -------
    add_data(data, source_name, pool)
        Returns the quantile histogram of the column

    find_matches(pool)
        Returns the column name

    """
    def __init__(self, quantiles: int, threshold: float):
        """
        Parameters
        ----------
        quantiles : int
            The number of quantiles of the column's quantile histogram
        threshold : float
            The global threshold described in [1]
        """
        self.quantiles = quantiles
        self.threshold = threshold
        self.columns = list()

    def add_data(self, data: DataFrame, source_name: str, pool: Pool):
        """
        Processes a table into column_model_scale.Column objects and stores them as pickle files

        Parameters
        ---------
        data : pandas.Dataframe
            a table of the database
        source_name : str
            the name of the table
        pool: multiprocessing.Pool
            the process pool that will be used in the pre-processing of the table's columns
        """
        pool.map(process_columns, ingestion_column_generator(data, source_name, self.quantiles))

        self.columns = self.columns + list(map(lambda name: source_name + '_' + name, data.columns))

    def find_matches(self, pool: Pool, chunk_size: int = None):
        """
        "Main" function of [1] that will calculate first the distribution clusters and then the attribute clusters

        Parameters
        ---------
        pool: multiprocessing.Pool
            the process pool that will be used in the algorithms 1, 2 and 3 of [1]
        chunk_size: int, optional
            the number of chunks of each job process (default let the framework decide)
        """
        start = timeit.default_timer()

        print("Compute distribution clusters ...\n")

        connected_components = discovery.compute_distribution_clusters(self.columns, self.threshold, pool,
                                                                       chunk_size, self.quantiles)

        stop = timeit.default_timer()

        print('Time: ', stop - start)

        start = timeit.default_timer()

        print("Compute attributes ... \n")
        all_attributes = list()
        for components in connected_components:
            edges = discovery.compute_attributes(list(components), self.threshold, pool, chunk_size, self.quantiles)
            all_attributes.append((list(components), edges))

        stop = timeit.default_timer()

        print('Time: ', stop - start)

        start = timeit.default_timer()

        print("Solve linear program ... \n")
        results = list()
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        stop = timeit.default_timer()

        print('Time: ', stop - start)

        start = timeit.default_timer()

        print("Extract clusters ... \n")
        clusters = list()
        for result in results:
            clusters.append(discovery.process_correlation_clustering_result(result))

        stop = timeit.default_timer()

        print('Time: ', stop - start)

        print(clusters)

        return clusters
