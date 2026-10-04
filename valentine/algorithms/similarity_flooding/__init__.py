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
    """Inverse of the average in-degree (default)."""
    INVERSE_PRODUCT = auto()
    """Inverse of the product of in-degrees."""


class Formula(Enum):
    """Fixpoint iteration formula."""

    BASIC = auto()
    """Basic fixpoint formula."""
    FORMULA_A = auto()
    """Variant A from the Similarity Flooding paper."""
    FORMULA_B = auto()
    """Variant B from the Similarity Flooding paper."""
    FORMULA_C = auto()
    """Variant C (default in Valentine)."""


class StringMatcher(Enum):
    """String matching function for the initial similarity mapping."""

    PREFIX_SUFFIX = auto()
    """Prefix/suffix trigram matcher (default)."""
    PREFIX_SUFFIX_TFIDF = auto()
    """Prefix/suffix matcher weighted by IDF computed from the corpus."""
    LEVENSHTEIN = auto()
    """Normalized Levenshtein similarity on node labels."""
