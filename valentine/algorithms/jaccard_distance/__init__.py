from enum import Enum, auto


class StringDistanceFunction(Enum):
    Levenshtein = auto()
    DamerauLevenshtein = auto()
    Jaro = auto()
    JaroWinkler = auto()
    Hamming = auto()
    Exact = auto()
    # Sentence-transformer embedding cosine similarity. Requires the
    # ``sentence-transformers`` extra (``pip install valentine[embeddings]``).
    Embedding = auto()
