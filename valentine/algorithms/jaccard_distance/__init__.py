from enum import Enum, auto

class StringDistanceFunction(Enum):
    Levenshtein = auto()
    DamerauLevenshtein = auto()
    Jaro = auto()
    JaroWinkler = auto()
    Hamming = auto()
    Exact = auto()
