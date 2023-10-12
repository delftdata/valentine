import unittest

from tests import df1, df2
from valentine.algorithms import Coma, JaccardDistanceMatcher, DistributionBased, SimilarityFlooding, Cupid
from valentine.data_sources import DataframeTable
from valentine.algorithms.jaccard_distance import StringDistanceFunction

d1 = DataframeTable(df1, name='authors1')
d2 = DataframeTable(df2, name='authors2')


class TestAlgorithms(unittest.TestCase):

    def test_coma(self):
        # Test the schema variant of coma
        coma_matcher_schema = Coma(use_instances=False)
        matches_coma_matcher_schema = coma_matcher_schema.get_matches(d1, d2)
        assert len(matches_coma_matcher_schema) > 0  # Check that it actually produced output
        # Test the instance variant of coma
        coma_matcher_instances = Coma(use_instances=True)
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

    def test_jaccard(self):
        # Test the Jaccard matcher with exact string similarity
        jd_matcher = JaccardDistanceMatcher(distance_fun=StringDistanceFunction.Exact)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output

    def test_jaccard_hamming(self):
        # Test the Jaccard matcher with Hamming distance
        jd_matcher = JaccardDistanceMatcher(distance_fun=StringDistanceFunction.Hamming)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output
        jd_matcher = JaccardDistanceMatcher(threshold_dist=0.5, process_num=2, distance_fun=StringDistanceFunction.Hamming)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output
    
    def test_jaccard_levenshtein(self):
        # Test the Jaccard matcher with Levenshtein distance
        jd_matcher = JaccardDistanceMatcher(distance_fun=StringDistanceFunction.Levenshtein)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output
        jd_matcher = JaccardDistanceMatcher(threshold_dist=0.5, process_num=2, distance_fun=StringDistanceFunction.Levenshtein)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output
    
    def test_jaccard_damerau_levenshtein(self):
        # Test the Jaccard matcher with Damerau-Levenshtein distance
        jd_matcher = JaccardDistanceMatcher(distance_fun=StringDistanceFunction.DamerauLevenshtein)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output
        jd_matcher = JaccardDistanceMatcher(threshold_dist=0.5, process_num=2, distance_fun=StringDistanceFunction.DamerauLevenshtein)
        matches_jl_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output

    def test_jaccard_jaro_winkler(self):
        # Test the Jaccard matcher with Jaro-Winkler distance
        jd_matcher = JaccardDistanceMatcher(distance_fun=StringDistanceFunction.JaroWinkler)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output
        jd_matcher = JaccardDistanceMatcher(threshold_dist=0.5, process_num=2, distance_fun=StringDistanceFunction.JaroWinkler)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output

    def test_jaccard_jaro(self):
        # Test the Jaccard matcher with Jaro distance
        jd_matcher = JaccardDistanceMatcher(distance_fun=StringDistanceFunction.Jaro)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output
        jd_matcher = JaccardDistanceMatcher(threshold_dist=0.5, process_num=2, distance_fun=StringDistanceFunction.Jaro)
        matches_jd_matcher = jd_matcher.get_matches(d1, d2)
        assert len(matches_jd_matcher) > 0  # Check that it actually produced output

    def test_similarity_flooding(self):
        # Test the Similarity flooding matcher
        sf_matcher = SimilarityFlooding()
        matches_sf_matcher = sf_matcher.get_matches(d1, d2)
        assert len(matches_sf_matcher) > 0  # Check that it actually produced output
