import pytest

from tests import df1, df2
from valentine.algorithms import Coma, JaccardDistanceMatcher, DistributionBased, SimilarityFlooding, Cupid
from valentine.algorithms.jaccard_distance import StringDistanceFunction
from valentine.data_sources import DataframeTable

d1 = DataframeTable(df1, name='authors1')
d2 = DataframeTable(df2, name='authors2')


def test_coma():
    # Test the schema variant of coma
    coma_matcher_schema = Coma(use_instances=False)
    matches_coma_matcher_schema = coma_matcher_schema.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_coma_matcher_schema) > 0
    # Test the instance variant of coma
    coma_matcher_instances = Coma(use_instances=True)
    matches_coma_matcher_instances = coma_matcher_instances.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_coma_matcher_instances) > 0
    # Assume the Schema and instance should provide different results
    assert matches_coma_matcher_schema != matches_coma_matcher_instances


def test_cupid():
    # Test the CUPID matcher
    cu_matcher = Cupid()
    matches_cu_matcher = cu_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_cu_matcher) > 0
    cu_matcher = Cupid(parallelism=2)
    matches_cu_matcher = cu_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_cu_matcher) > 0


def test_distribution_based():
    # Test the Distribution based matcher
    distribution_based_matcher = DistributionBased()
    matches_db_matcher = distribution_based_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_db_matcher) > 0
    distribution_based_matcher = DistributionBased(process_num=2)
    matches_db_matcher = distribution_based_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_db_matcher) > 0


def test_jaccard():
    # Test the Jaccard matcher with exact string similarity
    jd_matcher = JaccardDistanceMatcher(distance_fun=StringDistanceFunction.Exact)
    matches_jd_matcher = jd_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_jd_matcher) > 0


@pytest.mark.parametrize("distance_function", [StringDistanceFunction.Hamming, StringDistanceFunction.Levenshtein,
                                               StringDistanceFunction.DamerauLevenshtein,
                                               StringDistanceFunction.JaroWinkler, StringDistanceFunction.Jaro])
def test_jaccard_distance_function(distance_function):
    # Test the Jaccard matcher with different distance functions
    jd_matcher = JaccardDistanceMatcher(distance_fun=distance_function)
    matches_jd_matcher = jd_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_jd_matcher) > 0
    jd_matcher = JaccardDistanceMatcher(threshold_dist=0.5, process_num=2, distance_fun=distance_function)
    matches_jd_matcher = jd_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_jd_matcher) > 0


def test_similarity_flooding():
    # Test the Similarity flooding matcher
    sf_matcher = SimilarityFlooding()
    matches_sf_matcher = sf_matcher.get_matches(d1, d2)
    # Check that it actually produced output
    assert len(matches_sf_matcher) > 0
