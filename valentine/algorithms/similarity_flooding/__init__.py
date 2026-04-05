from enum import Enum, auto

TABLE = "Table"
COLUMN = "Column"
COLUMN_TYPE = "ColumnType"


class Policy(Enum):
    """Coefficient policy for the propagation graph."""

    INVERSE_AVERAGE = auto()
    INVERSE_PRODUCT = auto()


class Formula(Enum):
    """Fixpoint iteration formula."""

    BASIC = auto()
    FORMULA_A = auto()
    FORMULA_B = auto()
    FORMULA_C = auto()


class StringMatcher(Enum):
    """String matching function for the initial similarity mapping."""

    PREFIX_SUFFIX = auto()
    PREFIX_SUFFIX_TFIDF = auto()
    LEVENSHTEIN = auto()
