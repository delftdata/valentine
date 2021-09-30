import unittest
import os

from valentine import Coma, JaccardLevenMatcher, DistributionBased, SimilarityFlooding, Cupid
from valentine.data_sources.local_fs.local_fs_table import LocalFSTable

script_dir = os.path.dirname(__file__)
d1_path = os.path.join('data', 'tiny_authors_1_joinable_vertical_co30_target.csv')
d2_path = os.path.join('data', 'tiny_authors_1_joinable_vertical_co30_source.csv')


class TestAlgorithms(unittest.TestCase):

    def test_coma(self):
        # Load data
        d1 = LocalFSTable(os.path.join(script_dir, d1_path),
                          load_data=True)
        d2 = LocalFSTable(os.path.join(script_dir, d2_path),
                          load_data=True)
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
        # Load data
        d1 = LocalFSTable(os.path.join(script_dir, d1_path),
                          load_data=False)
        d2 = LocalFSTable(os.path.join(script_dir, d2_path),
                          load_data=False)
        # Test the CUPID matcher
        cu_matcher = Cupid()
        matches_cu_matcher = cu_matcher.get_matches(d1, d2)
        print(matches_cu_matcher)
        assert len(matches_cu_matcher) > 0  # Check that it actually produced output

    def test_distribution_based(self):
        # Load data
        d1 = LocalFSTable(os.path.join(script_dir, d1_path),
                          load_data=True)
        d2 = LocalFSTable(os.path.join(script_dir, d2_path),
                          load_data=True)
        # Test the Distribution based matcher
        distribution_based_matcher = DistributionBased()
        matches_jl_matcher = distribution_based_matcher.get_matches(d1, d2)
        assert len(matches_jl_matcher) > 0  # Check that it actually produced output

    def test_jaccard_lenshtein(self):
        # Load data
        d1 = LocalFSTable(os.path.join(script_dir, d1_path),
                          load_data=True)
        d2 = LocalFSTable(os.path.join(script_dir, d2_path),
                          load_data=True)
        # Test the Jaccard Levenshtein matcher
        jl_matcher = JaccardLevenMatcher()
        matches_jl_matcher = jl_matcher.get_matches(d1, d2)
        assert len(matches_jl_matcher) > 0  # Check that it actually produced output

    def test_similarity_flooding(self):
        # Load data
        d1 = LocalFSTable(os.path.join(script_dir, d1_path),
                          load_data=False)
        d2 = LocalFSTable(os.path.join(script_dir, d2_path),
                          load_data=False)
        # Test the Similarity flooding matcher
        sf_matcher = SimilarityFlooding()
        matches_sf_matcher = sf_matcher.get_matches(d1, d2)
        assert len(matches_sf_matcher) > 0  # Check that it actually produced output
