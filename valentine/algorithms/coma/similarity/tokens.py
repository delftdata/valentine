"""Token-level name similarity for column-name matching.

Complements the trigram matcher by normalising column names into a
set of lowercased word tokens (splitting ``camelCase``, ``PascalCase``,
``snake_case``, digits, and punctuation) before comparing. This catches
matches like ``ApproxDate`` <-> ``date_created_approximation`` where the
raw trigram overlap is weak but the underlying tokens overlap strongly.
"""

from __future__ import annotations

import re
from functools import lru_cache

# Splits a name into word/number runs. ``findall`` with these patterns
# turns both ``ApproxDate`` and ``approx_date`` into ``["approx", "date"]``.
_TOKEN_PATTERNS = re.compile(
    r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|\d+",
)

# Small abbreviation/synonym dictionary. Keys are lowercased short forms
# produced by ``_TOKEN_PATTERNS``; values are the canonical full word
# they expand to. Only entries where the short form is unambiguous in
# typical column-name usage are included, so ``num`` deliberately maps
# to ``number`` (not ``numeric``) and ``addr`` to ``address``.
#
# The mapping is applied as an *expansion*: both the original and the
# expansion are retained in the token set, which lets a column named
# ``BlkNum`` still match ``BlkNum`` exactly while also matching
# ``block`` via the expansion. This gives recall without risking
# regressions on columns whose short names were already meaningful.
_ABBREVIATIONS: dict[str, str] = {
    "addr": "address",
    "amt": "amount",
    "approx": "approximation",
    "arch": "architect",
    "blk": "block",
    "bldg": "building",
    "cat": "category",
    "cnt": "count",
    "cust": "customer",
    "dept": "department",
    "desc": "description",
    "dt": "date",
    "empl": "employee",
    "fname": "firstname",
    "ft": "feet",
    "gen": "general",
    "id": "identifier",
    "ident": "identifier",
    "lname": "lastname",
    "loc": "location",
    "lon": "longitude",
    "lng": "longitude",
    "lat": "latitude",
    "max": "maximum",
    "med": "medium",
    "min": "minimum",
    "mgr": "manager",
    "no": "number",
    "num": "number",
    "org": "organization",
    "pct": "percent",
    "perc": "percent",
    "phn": "phone",
    "pnt": "point",
    "prim": "primary",
    "qty": "quantity",
    "rec": "record",
    "ref": "reference",
    "sec": "secondary",
    "st": "street",
    "str": "string",
    "tel": "telephone",
    "tgt": "target",
    "tot": "total",
    "txt": "text",
    "uid": "identifier",
    "val": "value",
    "ver": "version",
    "zip": "zipcode",
}


@lru_cache(maxsize=4096)
def tokenize_name(name: str) -> tuple[str, ...]:
    """Split a column name into lowercased word tokens, with abbreviations
    expanded alongside their short forms.

    >>> tokenize_name("ApproxDate")
    ('approx', 'approximation', 'date')
    >>> tokenize_name("date_created_approximation")
    ('date', 'created', 'approximation')
    >>> tokenize_name("BlkNum")
    ('blk', 'block', 'num', 'number')
    """
    if not name:
        return ()
    tokens: list[str] = []
    seen: set[str] = set()
    for raw in _TOKEN_PATTERNS.findall(name):
        tok = raw.lower()
        if tok not in seen:
            seen.add(tok)
            tokens.append(tok)
        expansion = _ABBREVIATIONS.get(tok)
        if expansion is not None and expansion not in seen:
            seen.add(expansion)
            tokens.append(expansion)
    return tuple(tokens)


def _token_jaccard(tokens1: tuple[str, ...], tokens2: tuple[str, ...]) -> float:
    if not tokens1 or not tokens2:
        return 0.0
    s1, s2 = set(tokens1), set(tokens2)
    inter = s1 & s2
    if not inter:
        return 0.0
    return len(inter) / len(s1 | s2)


def tokens_similarity(name1: str, name2: str) -> float:
    """Token-level Jaccard similarity between two column names.

    Returns 0.0 when either name tokenises to the empty set, or when
    the token sets are disjoint.
    """
    return _token_jaccard(tokenize_name(name1), tokenize_name(name2))
