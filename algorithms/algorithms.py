"""
This script contains all the algorithm classes (that extend BaseMatcher) in one place.
This helps the framework to find them easily
"""
from algorithms.distribution_based.correlation_clustering import CorrelationClustering
from algorithms.similarity_flooding.similarity_flooding import SimilarityFlooding
from algorithms.cupid.cupid_model import Cupid
from algorithms.jaccard_levenshtein.jaccard_leven import JaccardLevenMatcher
from algorithms.coma.coma import Coma
from algorithms.sem_prop.sem_prop_main import SemProp
from algorithms.embdi.embdi import EmbDI
