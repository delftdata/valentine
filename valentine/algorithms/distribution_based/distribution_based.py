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
    """Distribution-based column matching.

    Implementation of the algorithm from *Automatic Discovery of Attributes
    in Relational Databases* (Zhang et al., SIGMOD 2011). Columns are
    compared by quantile histograms of their value distributions; Earth
    Mover's Distance drives the ranking of matches within each cluster.

    Parameters
    ----------
    threshold1 : float, optional
        Distance threshold used in phase 1 (distribution clustering), in
        ``[0, 1]`` (default: ``0.15``).
    threshold2 : float, optional
        Distance threshold used in phase 2 (attribute clustering), in
        ``[0, 1]`` (default: ``0.15``).
    quantiles : int, optional
        Number of quantiles used for histogram summaries (must be ``>= 1``,
        default: ``256``).
    process_num : int, optional
        Number of worker processes (must be ``>= 1``, default: ``1``).
    use_bloom_filters : bool, optional
        When ``True``, use Bloom filters for approximate set intersection
        in phase 2 (Section 4 of the paper). Trades a small false-positive
        rate for cheaper computation on large columns (default: ``False``).
    """

    def __init__(
        self,
        threshold1: float = 0.15,
        threshold2: float = 0.15,
        quantiles: int = 256,
        process_num: int = 1,
        use_bloom_filters: bool = False,
    ):
        self.__quantiles: int = int(quantiles)
        self.__threshold1: float = float(threshold1)
        self.__threshold2: float = float(threshold2)
        self.__process_num: int = int(process_num)
        self.__use_bloom_filters: bool = bool(use_bloom_filters)
        if self.__quantiles < 1:
            raise ValueError(f"quantiles must be >= 1, got {self.__quantiles}")
        if not 0.0 <= self.__threshold1 <= 1.0:
            raise ValueError(f"threshold1 must be between 0.0 and 1.0, got {self.__threshold1}")
        if not 0.0 <= self.__threshold2 <= 1.0:
            raise ValueError(f"threshold2 must be between 0.0 and 1.0, got {self.__threshold2}")
        if self.__process_num < 1:
            raise ValueError(f"process_num must be >= 1, got {self.__process_num}")
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
                for column in table.get_instances_columns():
                    unique_values.update(column.data)
            generate_global_ranks(unique_values, tmp_folder_path)
            del unique_values

            if self.__process_num == 1:
                for table in tables:
                    columns: list[BaseColumn] = table.get_instances_columns()
                    self.__column_names.extend(
                        [
                            (
                                table.name,
                                table.unique_identifier,
                                x.name,
                                x.unique_identifier,
                            )
                            for x in columns
                            if not x.is_empty
                        ]
                    )

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
                        columns: list[BaseColumn] = table.get_instances_columns()
                        self.__column_names.extend(
                            [
                                (
                                    table.name,
                                    table.unique_identifier,
                                    x.name,
                                    x.unique_identifier,
                                )
                                for x in columns
                                if not x.is_empty
                            ]
                        )
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
