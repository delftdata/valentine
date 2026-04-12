from enum import Enum, auto

TABLE = "Table"
COLUMN = "Column"
COLUMN_TYPE = "ColumnType"

# Sentinel prefix for structural node IDs in the SF graph.  Uses a null
# byte so it can never collide with real column or table names.
NODE_ID_PREFIX = "\x00NID"


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
