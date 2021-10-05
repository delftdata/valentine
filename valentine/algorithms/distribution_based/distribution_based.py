import tempfile
from multiprocessing import Pool, get_context
from itertools import combinations
from typing import List

from . import discovery
from .clustering_utils import generate_global_ranks, process_columns, ingestion_column_generator, process_emd
from ..base_matcher import BaseMatcher
from ..match import Match
from ...data_sources.base_column import BaseColumn
from ...data_sources.base_table import BaseTable


class DistributionBased(BaseMatcher):
    """
    A class that contains the data and methods required for the algorithms proposed in
    "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]

    Attributes
    ----------
    threshold1: float
        The threshold for phase 1
    threshold2: float
        The threshold for phase 2
    quantiles: int
        the number of quantiles of the histograms
    process_num: int
        The number of processes to spawn

    Methods
    -------
    find_matches(pool, chunk_size)
         A dictionary with matches and their similarity

    rank_output(attribute_clusters)
        Take the attribute clusters that the algorithm produces and give a ranked list of matches based on the the EMD
        between each pair inside an attribute cluster

    """

    def __init__(self,
                 threshold1: float = 0.15,
                 threshold2: float = 0.15,
                 quantiles: int = 256,
                 process_num: int = 1):
        """
        Parameters
        ----------
        threshold1: float
            The threshold for phase 1
        threshold2: float
            The threshold for phase 2
        quantiles: int
            the number of quantiles of the histograms
        process_num: int
            The number of processes to spawn
        """
        self.__quantiles: int = int(quantiles)
        self.__threshold1: float = float(threshold1)
        self.__threshold2: float = float(threshold2)
        self.__process_num: int = int(process_num)
        self.__column_names: list = []
        self.__target_name: str = ""

    def get_matches(self,
                    source_input: BaseTable,
                    target_input: BaseTable):
        """
        Overridden function of the BaseMatcher tha gets the source, the target data loaders and the dataset name.
        Next it gives as an output a ranked list of column pair matches.

        Returns
        -------
        dict
            A dictionary with matches and their similarity
        """
        self.__target_name = target_input.name

        all_tables: List[BaseTable] = [source_input, target_input]
        with tempfile.TemporaryDirectory() as tmp_folder_path:
            data = []
            for table in all_tables:
                for column in table.get_columns():
                    data.extend(column.data)
            generate_global_ranks(data, tmp_folder_path)
            del data

            if self.__process_num == 1:
                for table in all_tables:

                    self.__column_names.extend([(table.name, table.unique_identifier,
                                                 x.name, x.unique_identifier) for x in table.get_columns()])

                    columns: List[BaseColumn] = table.get_columns()
                    for tup in ingestion_column_generator(columns,
                                                          table.name,
                                                          table.unique_identifier,
                                                          self.__quantiles,
                                                          tmp_folder_path):
                        process_columns(tup)
                matches = self.__find_matches(tmp_folder_path)
            else:
                with get_context("spawn").Pool(self.__process_num) as process_pool:
                    for table in all_tables:
                        self.__column_names.extend([(table.name, table.unique_identifier,
                                                     x.name, x.unique_identifier) for x in table.get_columns()])
                        columns: List[BaseColumn] = table.get_columns()
                        process_pool.map(process_columns, ingestion_column_generator(columns,
                                                                                     table.name,
                                                                                     table.unique_identifier,
                                                                                     self.__quantiles,
                                                                                     tmp_folder_path), chunksize=1)
                    matches = self.__find_matches_parallel(tmp_folder_path, process_pool)

        return matches

    def __find_matches(self, tmp_folder_path: str):
        connected_components = discovery.compute_distribution_clusters(self.__column_names,
                                                                       self.__threshold1,
                                                                       tmp_folder_path,
                                                                       self.__quantiles)

        all_attributes = list()
        i = 1
        for components in connected_components:
            if len(components) > 1:
                i = i + 1
                edges = discovery.compute_attributes(list(components),
                                                     self.__threshold2,
                                                     tmp_folder_path,
                                                     self.__quantiles)
                all_attributes.append((list(components), edges))

        results = list()
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        attribute_clusters = discovery.process_correlation_clustering_result(results, self.__column_names)

        return self.__rank_output(attribute_clusters, tmp_folder_path)

    def __find_matches_parallel(self,
                                tmp_folder_path: str,
                                pool: Pool):
        """
        "Main" function of [1] that will calculate first the distribution clusters and then the attribute clusters

        Parameters
        ---------
        tmp_folder_path: str
            The path of the temporary folder that will serve as a cache for the run
        pool: multiprocessing.Pool
            the process pool that will be used in the algorithms 1, 2 and 3 of [1]
        """
        connected_components = discovery.compute_distribution_clusters_parallel(self.__column_names,
                                                                                self.__threshold1,
                                                                                pool,
                                                                                tmp_folder_path,
                                                                                self.__quantiles)

        all_attributes = list()
        i = 1
        for components in connected_components:
            if len(components) > 1:
                i = i + 1
                edges = discovery.compute_attributes_parallel(list(components),
                                                              self.__threshold2,
                                                              pool,
                                                              tmp_folder_path,
                                                              self.__quantiles)
                all_attributes.append((list(components), edges))

        results = list()
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        attribute_clusters = discovery.process_correlation_clustering_result(results, self.__column_names)

        return self.__rank_output(attribute_clusters, tmp_folder_path)

    def __rank_output(self,
                      attribute_clusters: iter,
                      tmp_folder_path: str):
        """
        Take the attribute clusters that the algorithm produces and give a ranked list of matches based on the the EMD
        between each pair inside an attribute cluster . The ranked list will look like:
        ((table_name1, column_name1), (table_name2, column_name2)): similarity

        Parameters
        ----------
        attribute_clusters: list
            The attribute clusters
        tmp_folder_path: str
            The path of the temporary folder that will serve as a cache for the run

        Returns
        -------
        dict
            A ranked list that will look like: ((table_name1, column_name1), (table_name2, column_name2)): similarity
        """
        matches = {}
        for cluster in attribute_clusters:
            if len(cluster) > 1:
                for combination in combinations(cluster, 2):
                    table1 = combination[0][0]
                    table2 = combination[1][0]
                    if table1 != table2:
                        k, emd = process_emd(((combination[0], combination[1]),
                                              self.__quantiles,
                                              False,
                                              tmp_folder_path))
                        sim = 1 / (1 + emd)
                        tn_i, tguid_i, cn_i, cguid_i = k[0]
                        tn_j, tguid_j, cn_j, cguid_j = k[1]
                        if self.__target_name == tn_i:
                            matches.update(Match(tn_i, cn_i,
                                                 tn_j, cn_j,
                                                 sim)
                                           .to_dict)
                        else:
                            matches.update(Match(tn_j, cn_j,
                                                 tn_i, cn_i,
                                                 sim)
                                           .to_dict)
        return matches
