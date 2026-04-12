"""Token-level name similarity for column-name matching.

Complements the trigram matcher by normalising column names into a
set of lowercased word tokens (splitting ``camelCase``, ``PascalCase``,
``snake_case``, digits, and punctuation) before comparing.

Abbreviations are detected generically at comparison time using prefix
and ordered-subsequence matching, so there is no static dictionary to
maintain. For example ``approx`` matches ``approximation`` (prefix),
``mgr`` matches ``manager`` (subsequence), and ``fname`` matches
``firstname`` (subsequence).
"""

from __future__ import annotations

import re
from functools import lru_cache

# Splits a name into word/number runs. ``findall`` with these patterns
# turns both ``ApproxDate`` and ``approx_date`` into ``["approx", "date"]``.
_TOKEN_PATTERNS = re.compile(
    r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|\d+",
)

# Minimum length for the short side of an abbreviation pair.
_MIN_ABBREV_LEN = 3
# The short token must cover at least this fraction of the long token
# to be considered an abbreviation (avoids spurious matches like
# "cat" → "concatenation").
_MIN_COVERAGE = 0.3


@lru_cache(maxsize=4096)
def tokenize_name(name: str) -> tuple[str, ...]:
    """Split a column name into unique lowercased word tokens.

    >>> tokenize_name("ApproxDate")
    ('approx', 'date')
    >>> tokenize_name("date_created_approximation")
    ('date', 'created', 'approximation')
    >>> tokenize_name("BlkNum")
    ('blk', 'num')
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
    return tuple(tokens)


def _is_subsequence(short: str, long: str) -> bool:
    """Return True if every character of *short* appears in *long* in order."""
    it = iter(long)
    return all(c in it for c in short)


@lru_cache(maxsize=8192)
def _is_abbreviation_of(a: str, b: str) -> bool:
    """Return True if *a* and *b* are exact matches or one is an
    abbreviation of the other.

    An abbreviation is recognised when:
    - Both tokens start with the same character.
    - The shorter token is at least ``_MIN_ABBREV_LEN`` characters.
    - The shorter token is either a prefix of the longer one, or an
      ordered subsequence covering at least ``_MIN_COVERAGE`` of it.

    >>> _is_abbreviation_of("dept", "department")
    True
    >>> _is_abbreviation_of("mgr", "manager")
    True
    >>> _is_abbreviation_of("fname", "firstname")
    True
    >>> _is_abbreviation_of("qty", "quantity")
    True
    >>> _is_abbreviation_of("cat", "customer")
    False
    """
    if a == b:
        return True
    short, long = (a, b) if len(a) <= len(b) else (b, a)
    if len(short) < _MIN_ABBREV_LEN:
        return False
    if short[0] != long[0]:
        return False
    if len(short) / len(long) < _MIN_COVERAGE:
        return False
    if long.startswith(short):
        return True
    return _is_subsequence(short, long)


def _soft_jaccard(tokens1: tuple[str, ...], tokens2: tuple[str, ...]) -> float:
    """Jaccard-like similarity with generic abbreviation matching.

    Instead of requiring exact token equality for the intersection,
    tokens from one set can match tokens in the other via
    ``_is_abbreviation_of``. A greedy bipartite matching is used so
    each token is matched at most once.
    """
    if not tokens1 or not tokens2:
        return 0.0

    # Always iterate over the smaller set as "queries".
    if len(tokens1) > len(tokens2):
        tokens1, tokens2 = tokens2, tokens1
    s2 = list(tokens2)
    matched = 0
    used: set[int] = set()
    for t1 in tokens1:
        for j, t2 in enumerate(s2):
            if j not in used and _is_abbreviation_of(t1, t2):
                matched += 1
                used.add(j)
                break
    if matched == 0:
        return 0.0
    return matched / (len(tokens1) + len(tokens2) - matched)


def _containment_bonus(tokens1: tuple[str, ...], tokens2: tuple[str, ...]) -> float:
    """Return a bonus when all tokens from one side appear in the other.

    This catches cases like ``curb`` ⊂ ``pap_curb_pri`` or
    ``ArtForm`` ⊂ ``artwork_type1`` where soft Jaccard penalises the
    unmatched tokens on the longer side but the short side is fully
    contained.

    Returns a value in [0, 0.15] that is added to the base Jaccard
    score. The bonus is proportional to how much of the longer side
    is covered, so ``curb`` in a 3-token name gets less than ``art form``
    in a 3-token name.
    """
    if not tokens1 or not tokens2:
        return 0.0
    short, long = (tokens1, tokens2) if len(tokens1) <= len(tokens2) else (tokens2, tokens1)
    # Check if every token in the short side has a match in the long side
    long_list = list(long)
    used: set[int] = set()
    for t in short:
        found = False
        for j, t2 in enumerate(long_list):
            if j not in used and _is_abbreviation_of(t, t2):
                used.add(j)
                found = True
                break
        if not found:
            return 0.0
    # All short tokens matched — bonus scaled by coverage ratio
    coverage = len(short) / len(long)
    return 0.15 * coverage


def tokens_similarity(name1: str, name2: str) -> float:
    """Token-level similarity between two column names.

    Uses soft Jaccard with generic abbreviation detection, plus a
    containment bonus when all tokens from one name appear within the
    other. This catches partial matches like ``curb`` inside
    ``pap_curb_pri``.
    """
    t1 = tokenize_name(name1)
    t2 = tokenize_name(name2)
    base = _soft_jaccard(t1, t2)
    bonus = _containment_bonus(t1, t2)
    return min(base + bonus, 1.0)
