from functools import lru_cache

# ---------------------------------------------------------------------------
# Generic datatype compatibility
# ---------------------------------------------------------------------------
# Instead of a static table of ~20 SQL type names, classify any type string
# into one of four families and derive compatibility from family distance.
# Same-family pairs score 1.0; adjacent families (text↔numeric) score 0.1;
# all others score 0.0. This handles every type Valentine's get_data_type()
# can produce (varchar, int, float, date) plus arbitrary SQL types.

_TEXT_KEYWORDS = frozenset(
    {
        "text",
        "keyword",
        "varchar",
        "nvarchar",
        "nchar",
        "char",
        "string",
        "str",
        "utf8",
        "categorical",
        "clob",
        "xml",
        "json",
    }
)
_INT_KEYWORDS = frozenset(
    {
        "int",
        "integer",
        "long",
        "bigint",
        "short",
        "smallint",
        "tinyint",
        "uint",
        "bit",
        "boolean",
        "bool",
        "serial",
    }
)
_FLOAT_KEYWORDS = frozenset(
    {
        "float",
        "double",
        "decimal",
        "numeric",
        "real",
        "number",
        "money",
    }
)
_DATE_KEYWORDS = frozenset(
    {
        "date",
        "datetime",
        "time",
        "timestamp",
        "interval",
    }
)

_FAMILY_TEXT = 0
_FAMILY_INT = 1
_FAMILY_FLOAT = 2
_FAMILY_DATE = 3

# Cross-family compatibility: binary — different family = 0.0.
# Same family = 1.0 (handled separately in datatype_compatibility).
_CROSS_FAMILY: dict[tuple[int, int], float] = {}


_FAMILY_GROUPS = (
    (_FAMILY_TEXT, _TEXT_KEYWORDS),
    (_FAMILY_INT, _INT_KEYWORDS),
    (_FAMILY_FLOAT, _FLOAT_KEYWORDS),
    (_FAMILY_DATE, _DATE_KEYWORDS),
)


@lru_cache(maxsize=256)
def _classify_type(dtype: str) -> int | None:
    """Classify a type string into a family, or None if unrecognised."""
    d = dtype.lower().strip()
    # Exact match first, then prefix match for parameterised types like "varchar(255)"
    for family, keywords in _FAMILY_GROUPS:
        if d in keywords:
            return family
    for family, keywords in _FAMILY_GROUPS:
        if any(d.startswith(kw) for kw in keywords):
            return family
    return None


def datatype_compatibility(cat1: str, cat2: str) -> float | None:
    """Return compatibility score for two type strings, or None if unknown.

    Returns 1.0 for same-family pairs, a fractional score for cross-family
    pairs, and None when at least one type cannot be classified (so the
    caller can fall back to token-based similarity).
    """
    f1 = _classify_type(cat1)
    f2 = _classify_type(cat2)
    if f1 is None or f2 is None:
        return None
    if f1 == f2:
        return 1.0
    return _CROSS_FAMILY.get((f1, f2), 0.0)


__all__ = [
    "cupid_model",
]
