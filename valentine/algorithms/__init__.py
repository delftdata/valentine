from .base_matcher import BaseMatcher
from .coma.coma import Coma
from .cupid.cupid_model import Cupid
from .distribution_based.distribution_based import DistributionBased
from .jaccard_distance.jaccard_distance import JaccardDistanceMatcher
from .similarity_flooding.similarity_flooding import SimilarityFlooding

schema_only_algorithms = [SimilarityFlooding.__name__, Cupid.__name__]
instance_only_algorithms = [DistributionBased.__name__, JaccardDistanceMatcher.__name__]
schema_instance_algorithms = [Coma.__name__]
all_matchers = schema_only_algorithms + instance_only_algorithms + schema_instance_algorithms

__all__ = [
    "schema_only_algorithms",
    "instance_only_algorithms",
    "schema_instance_algorithms",
    "all_matchers",
    "Coma",
    "Cupid",
    "DistributionBased",
    "JaccardDistanceMatcher",
    "SimilarityFlooding",
    "BaseMatcher"
]
