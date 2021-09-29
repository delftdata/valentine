from valentine.algorithms.coma.coma import Coma
from valentine.algorithms.cupid.cupid_model import Cupid
from valentine.algorithms.distribution_based.correlation_clustering import CorrelationClustering
from valentine.algorithms.jaccard_levenshtein.jaccard_leven import JaccardLevenMatcher
from valentine.algorithms.similarity_flooding.similarity_flooding import SimilarityFlooding

schema_only_algorithms = [SimilarityFlooding.__name__, Cupid.__name__]
instance_only_algorithms = [CorrelationClustering.__name__, JaccardLevenMatcher.__name__]
schema_instance_algorithms = [Coma.__name__]
