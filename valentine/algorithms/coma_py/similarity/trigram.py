from __future__ import annotations

PAD_CHAR = "#"
END_CHAR = "$"


def trigram_similarity(s1: str, s2: str) -> float:
    """
    Compute Dice coefficient on character 3-grams (trigrams).

    Strings are padded: "##s", "#st", "str", ..., "ng$", "g$$"
    Similarity = 2 * |intersection| / (|trigrams_s1| + |trigrams_s2|)

    Returns 1.0 if both strings are empty, 0.0 if only one is empty.
    """
    if not s1 and not s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    t1 = _get_trigrams(s1.lower())
    t2 = _get_trigrams(s2.lower())

    if not t1 and not t2:
        return 1.0
    if not t1 or not t2:
        return 0.0

    intersection_size = _multiset_intersection_size(t1, t2)
    total = len(t1) + len(t2)

    if total == 0:
        return 1.0

    return (2.0 * intersection_size) / total


def _get_trigrams(s: str) -> list[str]:
    """Generate padded character trigrams for a string."""
    padded = PAD_CHAR + PAD_CHAR + s + END_CHAR + END_CHAR
    return [padded[i : i + 3] for i in range(len(padded) - 2)]


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
