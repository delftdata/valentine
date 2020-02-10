import json
from multiprocessing import Pool, get_context
from itertools import combinations

from algorithms.base_matcher import BaseMatcher
from data_loader.instance_loader import InstanceLoader

import algorithms.clustering_scale.discovery_scale as discovery
from algorithms.clustering_scale.scale_utils import process_columns, ingestion_column_generator, create_cache_dirs, \
    generate_global_ranks, process_emd
from utils.utils import get_project_root


class CorrelationClustering(BaseMatcher):
    """
    A class that contains the data and methods required for the algorithms proposed in
    "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]

    Attributes
    ----------
    quantiles: int
        the number of quantiles of the histograms

    Methods
    -------
    add_data(data, source_name, pool)
        Returns the quantile histogram of the column

    find_matches(pool)
        Returns the column name

    """

    def __init__(self, threshold1: float = 0.1, threshold2: float = 0.1,
                 quantiles: int = 256, process_num: int = 1, chunk_size: int = None, clear_cache: bool = True):
        """
        Parameters
        ----------
        quantiles : int
            The number of quantiles of the column's quantile histogram
        """
        self.quantiles = quantiles
        self.threshold1 = threshold1
        self.threshold2 = threshold2
        self.process_num = process_num
        self.chunk_size = chunk_size
        self.clear_cache = clear_cache
        self.column_names = list()
        create_cache_dirs()

    def get_matches(self, source: InstanceLoader, target: InstanceLoader, dataset_name: str):

        if self.clear_cache:
            data = list(source.table.get_data()) + list(target.table.get_data())
            generate_global_ranks(data, dataset_name)
            del data

        with get_context("spawn").Pool(self.process_num) as process_pool:
            columns = list(source.table.columns.values()) + list(target.table.columns.values())

            print("Processing columns ...")
            process_pool.map(process_columns, ingestion_column_generator(columns, dataset_name, self.quantiles))

            del columns

            self.column_names.extend(source.column_names)
            self.column_names.extend(target.column_names)

            return self.find_matches(process_pool, self.chunk_size)

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
        print("Computing distribution clusters ...")

        connected_components = discovery.compute_distribution_clusters(self.column_names, self.threshold1, pool,
                                                                       chunk_size, self.quantiles)

        self.write_clusters_to_json(connected_components, 'Distribution_Clusters.json')

        print("Computing attributes ...")
        all_attributes = list()
        i = 1
        for components in connected_components:
            if len(components) > 1:
                print("Distribution cluster: ", i)
                i = i + 1
                edges = discovery.compute_attributes(list(components), self.threshold2, pool, chunk_size,
                                                     self.quantiles)
                all_attributes.append((list(components), edges))

        print("Solving linear program ...")
        results = list()
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        attribute_clusters = discovery.process_correlation_clustering_result(results, self.column_names)

        self.write_clusters_to_json(attribute_clusters, 'Attribute_Clusters(Matches).json')

        return self.rank_output(attribute_clusters)

    @staticmethod
    def write_clusters_to_json(clusters: list, file_name: str):
        """
        Writes the clusters with their attributes and their connections in a json file

        Parameters
        ---------
        clusters : list(list(str))
            a list with the clusters, their attributes and their connections
        file_name : str
            the name of the JSON file to write
        """
        d = {}
        clusters.sort(key=lambda item: -len(item))
        for (cluster, idx) in zip(clusters, range(len(clusters))):
            d["Cluster " + str(idx + 1)] = {}
            table_names = list(map(lambda x: x[0], cluster))
            for table_name in table_names:
                column_names = map(lambda x: x[1], filter(lambda x: x[0] == table_name, cluster))
                d["Cluster " + str(idx + 1)][table_name] = list(column_names)
        with open(str(get_project_root()) + "/" + file_name, 'w') as fp:
            json.dump(d, fp, indent=2)

    def rank_output(self, attribute_clusters: list):
        emd_per_match = dict()
        for cluster in attribute_clusters:
            if len(cluster) > 1:
                for combination in combinations(cluster, 2):
                    table1 = combination[0][0]
                    table2 = combination[1][0]
                    if table1 != table2:
                        k, emd = process_emd(((combination[0], combination[1]), self.quantiles, False))
                        emd_per_match[k] = 1 / (1 + emd)
        emd_per_match = dict(sorted(emd_per_match.items(), key=lambda x: -x[1]))
        return emd_per_match
