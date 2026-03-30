from __future__ import annotations

import re


def trigram_similarity(s1: str, s2: str) -> float:
    """
    Compute COMA-style token-level trigram similarity.

    Matches Java ComaTrigram2 behavior:
    1. Tokenize both strings (split on delimiters)
    2. Compute pairwise token-level trigram (Dice on 3-grams)
    3. Combine using max-matching Dice formula (computeSetSimilarity)

    Returns 1.0 if both strings are empty, 0.0 if only one is empty.
    """
    if not s1 and not s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    tokens1 = _tokenize(s1)
    tokens2 = _tokenize(s2)

    if not tokens1 and not tokens2:
        return 1.0
    if not tokens1 or not tokens2:
        return 0.0

    # Compute pairwise token trigram similarities
    m = len(tokens1)
    n = len(tokens2)
    sim_matrix = [[0.0] * n for _ in range(m)]
    for i in range(m):
        for j in range(n):
            sim_matrix[i][j] = _token_trigram(tokens1[i], tokens2[j])

    # Combine using COMA's computeSetSimilarity (max-matching Dice)
    return _compute_set_similarity(sim_matrix, m, n)


def _tokenize(s: str) -> list[str]:
    """Tokenize a string by splitting on common delimiters."""
    return [t for t in re.split(r"[\s._\-]+", s) if t]


def _token_trigram(s1: str, s2: str) -> float:
    """Compute Dice coefficient on character 3-grams between two tokens."""
    s1 = s1.lower()
    s2 = s2.lower()

    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    t1 = _get_trigrams(s1)
    t2 = _get_trigrams(s2)

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


def _compute_set_similarity(sim_matrix: list[list[float]], m: int, n: int) -> float:
    """
    COMA's computeSetSimilarity: max-matching Dice formula.

    For each row: sum the max similarity to any column.
    For each column: sum the max similarity to any row.
    Result = (sum_row_maxes + sum_col_maxes) / (m + n)
    """
    if m == 0 or n == 0:
        return 0.0

    sum_row_max = 0.0
    for i in range(m):
        row_max = 0.0
        for j in range(n):
            row_max = max(row_max, sim_matrix[i][j])
        sum_row_max += row_max

    sum_col_max = 0.0
    for j in range(n):
        col_max = 0.0
        for i in range(m):
            col_max = max(col_max, sim_matrix[i][j])
        sum_col_max += col_max

    return (sum_row_max + sum_col_max) / (m + n)
