from __future__ import annotations

# Compatibility matrix for Valentine's data types.
# Same type -> 1.0; compatible types get fractional scores.
_COMPAT: dict[tuple[str, str], float] = {
    ("varchar", "varchar"): 1.0,
    ("int", "int"): 1.0,
    ("float", "float"): 1.0,
    ("date", "date"): 1.0,
    ("int", "float"): 0.5,
    ("float", "int"): 0.5,
    ("varchar", "date"): 0.3,
    ("date", "varchar"): 0.3,
}


def datatype_similarity(dt1: str, dt2: str) -> float:
    """
    Compute datatype compatibility between two Valentine type strings.

    Known types: "varchar", "int", "float", "date".
    Returns 1.0 for identical types, predefined scores for compatible types, 0.0 otherwise.
    """
    dt1 = dt1.lower().strip() if dt1 else ""
    dt2 = dt2.lower().strip() if dt2 else ""

    if dt1 == dt2:
        return 1.0

    return _COMPAT.get((dt1, dt2), 0.0)
