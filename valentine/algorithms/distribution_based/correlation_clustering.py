import uuid
from multiprocessing import Pool, get_context
from itertools import combinations
from typing import List, Union

from . import discovery
from .clustering_utils import create_cache_dirs, generate_global_ranks, process_columns, ingestion_column_generator, \
    process_emd, cleanup_files
from ..base_matcher import BaseMatcher
from ..match import Match
from ...data_sources.base_column import BaseColumn
from ...data_sources.base_db import BaseDB
from ...data_sources.base_table import BaseTable


# FIXME (1) cache files need to go
# FIXME (2) make multithreading easy to use


class CorrelationClustering(BaseMatcher):
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
    clear_cache: bool, optional
        Clear cached files or not
    column_names: list
        List containing all the column names

    Methods
    -------
    find_matches(pool, chunk_size)
         A dictionary with matches and their similarity

    rank_output(attribute_clusters)
        Take the attribute clusters that the algorithm produces and give a ranked list of matches based on the the EMD
        between each pair inside an attribute cluster

    """

    def __init__(self, threshold1: float = 0.15, threshold2: float = 0.15,
                 quantiles: int = 256, process_num: int = 1, clear_cache: bool = True):
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
        clear_cache: bool, optional
            Clear cached files or not
        """
        self.uuid: str = str(uuid.uuid4())
        self.quantiles: int = int(quantiles)
        self.threshold1: float = float(threshold1)
        self.threshold2: float = float(threshold2)
        self.process_num: int = int(process_num)
        self.clear_cache: bool = clear_cache
        self.column_names: list = []
        self.target_name: str = ""
        self.target_guid: object = None
        self.source_guid: object = None
        create_cache_dirs(self.uuid)

    def get_matches(self, source_input: Union[BaseDB, BaseTable], target_input: Union[BaseDB, BaseTable]):
        """
        Overridden function of the BaseMatcher tha gets the source, the target data loaders and the dataset name.
        Next it gives as an output a ranked list of column pair matches.

        Returns
        -------
        dict
            A dictionary with matches and their similarity
        """
        self.target_name = target_input.name
        self.source_guid = source_input.db_belongs_uid if isinstance(source_input, BaseTable) \
            else source_input.unique_identifier
        self.target_guid = target_input.db_belongs_uid if isinstance(target_input, BaseTable) \
            else target_input.unique_identifier

        all_tables: List[Union[BaseTable, BaseDB]] = \
            list(source_input.get_tables().values()) + list(target_input.get_tables().values())

        if self.clear_cache:
            data = []
            for table in all_tables:
                for column in table.get_columns():
                    data.extend(column.data)
            generate_global_ranks(data, self.uuid)
            del data

        if self.process_num == 1:
            for table in all_tables:

                self.column_names.extend([(table.name, table.unique_identifier,
                                           x.name, x.unique_identifier)for x in table.get_columns()])

                columns: List[BaseColumn] = table.get_columns()
                for tup in ingestion_column_generator(columns, table.name, table.unique_identifier,
                                                      self.quantiles, self.uuid):
                    process_columns(tup)
            matches = self.find_matches()
        else:
            with get_context("spawn").Pool(self.process_num) as process_pool:
                for table in all_tables:
                    self.column_names.extend([(table.name, table.unique_identifier,
                                               x.name, x.unique_identifier)for x in table.get_columns()])
                    columns: List[BaseColumn] = table.get_columns()
                    process_pool.map(process_columns, ingestion_column_generator(columns, table.name,
                                                                                 table.unique_identifier,
                                                                                 self.quantiles,
                                                                                 self.uuid), chunksize=1)
                matches = self.find_matches_parallel(process_pool)
        cleanup_files(self.uuid)
        return matches

    def find_matches(self):
        connected_components = discovery.compute_distribution_clusters(self.column_names, self.threshold1, self.uuid,
                                                                       self.quantiles)

        all_attributes = list()
        i = 1
        for components in connected_components:
            if len(components) > 1:
                i = i + 1
                edges = discovery.compute_attributes(list(components), self.threshold2, self.uuid, self.quantiles)
                all_attributes.append((list(components), edges))

        results = list()
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        attribute_clusters = discovery.process_correlation_clustering_result(results, self.column_names)

        return self.rank_output(attribute_clusters)

    def find_matches_parallel(self, pool: Pool):
        """
        "Main" function of [1] that will calculate first the distribution clusters and then the attribute clusters

        Parameters
        ---------
        pool: multiprocessing.Pool
            the process pool that will be used in the algorithms 1, 2 and 3 of [1]
        """
        connected_components = discovery.compute_distribution_clusters_parallel(self.column_names, self.threshold1,
                                                                                pool, self.uuid, self.quantiles)

        all_attributes = list()
        i = 1
        for components in connected_components:
            if len(components) > 1:
                i = i + 1
                edges = discovery.compute_attributes_parallel(list(components), self.threshold2, pool, self.uuid,
                                                              self.quantiles)
                all_attributes.append((list(components), edges))

        results = list()
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        attribute_clusters = discovery.process_correlation_clustering_result(results, self.column_names)

        return self.rank_output(attribute_clusters)

    def rank_output(self, attribute_clusters: iter):
        """
        Take the attribute clusters that the algorithm produces and give a ranked list of matches based on the the EMD
        between each pair inside an attribute cluster . The ranked list will look like:
        ((table_name1, column_name1), (table_name2, column_name2)): similarity

        Parameters
        ----------
        attribute_clusters: list
            The attribute clusters
        Returns
        -------
        dict
            A ranked list that will look like: ((table_name1, column_name1), (table_name2, column_name2)): similarity
        """
        matches = []
        for cluster in attribute_clusters:
            if len(cluster) > 1:
                for combination in combinations(cluster, 2):
                    table1 = combination[0][0]
                    table2 = combination[1][0]
                    if table1 != table2:
                        k, emd = process_emd(((combination[0], combination[1]), self.quantiles, False, self.uuid))
                        sim = 1 / (1 + emd)
                        tn_i, tguid_i, cn_i, cguid_i = k[0]
                        tn_j, tguid_j, cn_j, cguid_j = k[1]
                        if self.target_name == tn_i:
                            matches.append(Match(self.target_guid,
                                                 tn_i, tguid_i, cn_i, cguid_i,
                                                 self.source_guid,
                                                 tn_j, tguid_j, cn_j, cguid_j,
                                                 sim)
                                           .to_dict)
                        else:
                            matches.append(Match(self.target_guid,
                                                 tn_j, tguid_j, cn_j, cguid_j,
                                                 self.source_guid,
                                                 tn_i, tguid_i, cn_i, cguid_i,
                                                 sim)
                                           .to_dict)
        return matches
