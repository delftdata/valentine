"""
This script contains all the algorithm classes (that extend BaseMatcher) in one place.
This helps the framework to find them easily
"""
from distribution_based.correlation_clustering import CorrelationClustering
from similarity_flooding.similarity_flooding import SimilarityFlooding
from cupid.cupid_model import Cupid
from jaccard_levenshtein.jaccard_leven import JaccardLevenMatcher
from coma.coma import Coma
