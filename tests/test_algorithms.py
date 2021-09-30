import unittest
import os

from valentine import Coma
from valentine.data_sources.local_fs.local_fs_table import LocalFSTable

script_dir = os.path.dirname(__file__)


class TestAlgorithms(unittest.TestCase):

    def test_coma(self):
        # Load data
        d1 = LocalFSTable(os.path.join(script_dir, 'data/tiny_authors_1_joinable_vertical_co30_target.csv'),
                          load_data=True)
        d2 = LocalFSTable(os.path.join(script_dir, 'data/tiny_authors_1_joinable_vertical_co30_source.csv'),
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
        pass

    def test_distribution_based(self):
        pass

    def test_jaccard_lenshtein(self):
        pass

    def test_similarity_flooding(self):
        pass
