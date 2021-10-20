import unittest

from tests import df1, df2
from valentine.algorithms import Coma, JaccardLevenMatcher, DistributionBased, SimilarityFlooding, Cupid
from valentine.data_sources import DataframeTable

d1 = DataframeTable(df1, name='authors1')
d2 = DataframeTable(df2, name='authors2')


class TestAlgorithms(unittest.TestCase):

    def test_coma(self):
        # Test the schema variant of coma
        coma_matcher_schema = Coma(strategy="COMA_OPT")
        matches_coma_matcher_schema = coma_matcher_schema.get_matches(d1, d2)
        assert len(matches_coma_matcher_schema) > 0  # Check that it actually produced output
        # Test the instance variant of coma
        coma_matcher_instances = Coma(strategy="COMA_OPT_INST")
        matches_coma_matcher_instances = coma_matcher_instances.get_matches(d1, d2)
        assert len(matches_coma_matcher_instances) > 0  # Check that it actually produced output
        # Assume the Schema and instance should provide different results
        assert matches_coma_matcher_schema != matches_coma_matcher_instances

    def test_cupid(self):
        # Test the CUPID matcher
        cu_matcher = Cupid()
        matches_cu_matcher = cu_matcher.get_matches(d1, d2)
        assert len(matches_cu_matcher) > 0  # Check that it actually produced output
        cu_matcher = Cupid(parallelism=2)
        matches_cu_matcher = cu_matcher.get_matches(d1, d2)
        assert len(matches_cu_matcher) > 0  # Check that it actually produced output

    def test_distribution_based(self):
        # Test the Distribution based matcher
        distribution_based_matcher = DistributionBased()
        matches_db_matcher = distribution_based_matcher.get_matches(d1, d2)
        assert len(matches_db_matcher) > 0  # Check that it actually produced output
        distribution_based_matcher = DistributionBased(process_num=2)
        matches_db_matcher = distribution_based_matcher.get_matches(d1, d2)
        assert len(matches_db_matcher) > 0  # Check that it actually produced output

    def test_jaccard_levenshtein(self):
        # Test the Jaccard Levenshtein matcher
        jl_matcher = JaccardLevenMatcher()
        matches_jl_matcher = jl_matcher.get_matches(d1, d2)
        assert len(matches_jl_matcher) > 0  # Check that it actually produced output
        jl_matcher = JaccardLevenMatcher(threshold_leven=0.5, process_num=2)
        matches_jl_matcher = jl_matcher.get_matches(d1, d2)
        assert len(matches_jl_matcher) > 0  # Check that it actually produced output

    def test_similarity_flooding(self):
        # Test the Similarity flooding matcher
        sf_matcher = SimilarityFlooding()
        matches_sf_matcher = sf_matcher.get_matches(d1, d2)
        assert len(matches_sf_matcher) > 0  # Check that it actually produced output
