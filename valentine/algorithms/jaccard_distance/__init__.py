from enum import Enum, auto


class StringDistanceFunction(Enum):
    """Element-equality functions supported by `JaccardDistanceMatcher`."""

    Levenshtein = auto()
    """Normalized Levenshtein ratio (default)."""
    DamerauLevenshtein = auto()
    """Normalized Damerau-Levenshtein ratio."""
    Jaro = auto()
    """Jaro similarity."""
    JaroWinkler = auto()
    """Jaro-Winkler similarity."""
    Hamming = auto()
    """Normalized Hamming distance (strings of equal length)."""
    Exact = auto()
    """Exact string equality (forces the threshold to 1.0)."""
    Embedding = auto()
    """Cosine similarity of sentence-transformer embeddings. Requires the
    ``sentence-transformers`` extra (``pip install valentine[embeddings]``).
    """
