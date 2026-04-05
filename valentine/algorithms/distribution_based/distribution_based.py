import tempfile
from itertools import combinations
from multiprocessing import Pool, get_context

from ...data_sources.base_column import BaseColumn
from ...data_sources.base_table import BaseTable
from ..base_matcher import BaseMatcher
from ..match import Match
from . import discovery
from .clustering_utils import (
    generate_global_ranks,
    ingestion_column_generator,
    process_columns,
    process_emd,
)


class DistributionBased(BaseMatcher):
    """
    A class that contains the data and methods required for the algorithms proposed in
    "Automatic Discovery of Attributes in Relational Databases" from M. Zhang et al. [1]

    Attributes
    ----------
    __threshold1: float
        The threshold for phase 1
    __threshold2: float
        The threshold for phase 2
    __quantiles: int
        the number of quantiles of the histograms
    __process_num: int
        The number of processes to spawn
    __use_bloom_filters: bool
        Whether to use Bloom filters for approximate intersection in phase 2

    Methods
    -------
    find_matches(pool, chunk_size)
         A dictionary with matches and their similarity

    rank_output(attribute_clusters)
        Take the attribute clusters that the algorithm produces and give a ranked list of matches based on the the EMD
        between each pair inside an attribute cluster

    """

    def __init__(
        self,
        threshold1: float = 0.15,
        threshold2: float = 0.15,
        quantiles: int = 256,
        process_num: int = 1,
        use_bloom_filters: bool = False,
    ):
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
        use_bloom_filters: bool
            Whether to use Bloom filters for approximate intersection in phase 2.
            When True, uses Bloom filters as described in Section 4 of the paper to
            approximate set intersection, trading a small false positive rate for
            reduced computation on large columns. Default is False (exact intersection).
        """
        self.__quantiles: int = int(quantiles)
        self.__threshold1: float = float(threshold1)
        self.__threshold2: float = float(threshold2)
        self.__process_num: int = int(process_num)
        self.__use_bloom_filters: bool = bool(use_bloom_filters)
        self.__column_names: list = []

    def get_matches(self, source_input: BaseTable, target_input: BaseTable):
        """
        Overridden function of the BaseMatcher tha gets the source, the target data loaders and the dataset name.
        Next it gives as an output a ranked list of column pair matches.

        Returns
        -------
        dict
            A dictionary with matches and their similarity
        """
        table_order = {source_input.name: 0, target_input.name: 1}
        return self.__ingest_and_match([source_input, target_input], table_order)

    def get_matches_batch(self, tables: list[BaseTable]):
        """
        Override that computes global ranks from ALL tables at once, so that
        the distribution clustering reflects the full data landscape rather
        than only a single pair.
        """
        table_order = {table.name: i for i, table in enumerate(tables)}
        return self.__ingest_and_match(tables, table_order)

    def __ingest_and_match(self, tables: list[BaseTable], table_order: dict[str, int]):
        self.__column_names = []

        with tempfile.TemporaryDirectory() as tmp_folder_path:
            unique_values: set = set()
            for table in tables:
                for column in table.get_columns():
                    unique_values.update(column.data)
            generate_global_ranks(unique_values, tmp_folder_path)
            del unique_values

            if self.__process_num == 1:
                for table in tables:
                    self.__column_names.extend(
                        [
                            (
                                table.name,
                                table.unique_identifier,
                                x.name,
                                x.unique_identifier,
                            )
                            for x in table.get_columns()
                            if not x.is_empty
                        ]
                    )

                    columns: list[BaseColumn] = table.get_columns()
                    for tup in ingestion_column_generator(
                        columns,
                        table.name,
                        table.unique_identifier,
                        self.__quantiles,
                        tmp_folder_path,
                    ):
                        process_columns(tup)
                matches = self.__find_matches(tmp_folder_path, table_order)
            else:
                with get_context("spawn").Pool(self.__process_num) as process_pool:
                    for table in tables:
                        self.__column_names.extend(
                            [
                                (
                                    table.name,
                                    table.unique_identifier,
                                    x.name,
                                    x.unique_identifier,
                                )
                                for x in table.get_columns()
                                if not x.is_empty
                            ]
                        )
                        columns: list[BaseColumn] = table.get_columns()
                        process_pool.map(
                            process_columns,
                            ingestion_column_generator(
                                columns,
                                table.name,
                                table.unique_identifier,
                                self.__quantiles,
                                tmp_folder_path,
                            ),
                            chunksize=1,
                        )
                    matches = self.__find_matches_parallel(
                        tmp_folder_path, process_pool, table_order
                    )

        return matches

    def __find_matches(self, tmp_folder_path: str, table_order: dict[str, int]):
        connected_components = discovery.compute_distribution_clusters(
            self.__column_names, self.__threshold1, tmp_folder_path, self.__quantiles
        )

        all_attributes = []
        i = 1
        for components in connected_components:
            if len(components) > 1:
                i = i + 1
                edges = discovery.compute_attributes(
                    list(components),
                    self.__threshold2,
                    tmp_folder_path,
                    self.__quantiles,
                    self.__use_bloom_filters,
                )
                all_attributes.append((list(components), edges))

        results = []
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        attribute_clusters = discovery.process_correlation_clustering_result(
            results, self.__column_names
        )

        return self.__rank_output(attribute_clusters, tmp_folder_path, table_order)

    def __find_matches_parallel(
        self, tmp_folder_path: str, pool: Pool, table_order: dict[str, int]
    ):
        """
        "Main" function of [1] that will calculate first the distribution clusters and then the attribute clusters

        Parameters
        ---------
        tmp_folder_path: str
            The path of the temporary folder that will serve as a cache for the run
        pool: multiprocessing.Pool
            the process pool that will be used in the algorithms 1, 2 and 3 of [1]
        table_order: dict[str, int]
            Mapping of table name to position index for consistent match direction
        """
        connected_components = discovery.compute_distribution_clusters_parallel(
            self.__column_names,
            self.__threshold1,
            pool,
            tmp_folder_path,
            self.__quantiles,
        )

        all_attributes = []
        i = 1
        for components in connected_components:
            if len(components) > 1:
                i = i + 1
                edges = discovery.compute_attributes_parallel(
                    list(components),
                    self.__threshold2,
                    pool,
                    tmp_folder_path,
                    self.__quantiles,
                    self.__use_bloom_filters,
                )
                all_attributes.append((list(components), edges))

        results = []
        for components, edges in all_attributes:
            results.append(discovery.correlation_clustering_pulp(components, edges))

        attribute_clusters = discovery.process_correlation_clustering_result(
            results, self.__column_names
        )

        return self.__rank_output(attribute_clusters, tmp_folder_path, table_order)

    def __rank_output(
        self,
        attribute_clusters: iter,
        tmp_folder_path: str,
        table_order: dict[str, int],
    ):
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
        table_order: dict[str, int]
            Mapping of table name to position index for consistent match direction

        Returns
        -------
        dict
            A ranked list that will look like: ((table_name1, column_name1), (table_name2, column_name2)): similarity
        """
        matches = {}
        for cluster in attribute_clusters:
            if len(cluster) < 2:
                continue
            for combination in combinations(cluster, 2):
                table1 = combination[0][0]
                table2 = combination[1][0]
                if table1 != table2:
                    k, emd = process_emd(
                        (
                            (combination[0], combination[1]),
                            self.__quantiles,
                            False,
                            tmp_folder_path,
                            False,
                        )
                    )
                    sim = 1 / (1 + emd)
                    tn_i, _, cn_i, _ = k[0]
                    tn_j, _, cn_j, _ = k[1]
                    if table_order.get(tn_i, 0) > table_order.get(tn_j, 0):
                        matches.update(Match(tn_i, cn_i, tn_j, cn_j, sim).to_dict)
                    else:
                        matches.update(Match(tn_j, cn_j, tn_i, cn_i, sim).to_dict)
        return matches
