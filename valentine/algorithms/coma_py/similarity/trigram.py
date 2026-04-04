from __future__ import annotations


def trigram_similarity(s1: str, s2: str) -> float:
    """
    Compute trigram (Dice coefficient on character 3-grams) similarity.

    Matches Java COMA's default IFuiceTrigram behavior:
    case-normalize the entire string and compute character 3-grams
    without tokenization.
    """
    if not s1 or not s2:
        return 1.0 if (not s1 and not s2) else 0.0

    s1 = s1.lower()
    s2 = s2.lower()

    if s1 == s2:
        return 1.0

    t1 = _get_trigrams(s1)
    t2 = _get_trigrams(s2)

    if not t1 or not t2:
        return 1.0 if (not t1 and not t2) else 0.0

    total = len(t1) + len(t2)
    return (2.0 * _multiset_intersection_size(t1, t2)) / total if total > 0 else 1.0


def _get_trigrams(s: str) -> list[str]:
    """Generate character trigrams for a string (no padding)."""
    if len(s) < 3:
        return [s]
    return [s[i : i + 3] for i in range(len(s) - 2)]


def _multiset_intersection_size(a: list[str], b: list[str]) -> int:
    """Count shared trigrams (multiset intersection)."""
    counts_b: dict[str, int] = {}
    for t in b:
        counts_b[t] = counts_b.get(t, 0) + 1

    shared = 0
    for t in a:
        if counts_b.get(t, 0) > 0:
            shared += 1
            counts_b[t] -= 1

    return shared
