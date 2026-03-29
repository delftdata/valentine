"""
String matching functions for computing initial similarity mappings.

The paper (Melnik et al., ICDE 2002) describes StringMatch as:
  "The string matcher we are currently using splits a text string into a set
   of words and compares the word in two sets pairwise. In word comparison,
   we examine only common prefix and suffix."

This module provides both the paper's prefix/suffix token matcher and the
existing Levenshtein-based matcher.
"""

import math
import re

from jellyfish import levenshtein_distance

from ...utils.utils import normalize_distance


def _camel_case_split(s: str) -> list[str]:
    """Split a CamelCase string into tokens.

    Examples:
        "ColumnType" -> ["Column", "Type"]
        "DeptName"   -> ["Dept", "Name"]
        "EmpNo"      -> ["Emp", "No"]
        "Pname"      -> ["Pname"]
        "date"       -> ["date"]
    """
    return re.sub(r"([a-z])([A-Z])", r"\1 \2", s).split()


def _word_prefix_suffix_sim(w1: str, w2: str) -> float:
    """Compute similarity between two words using common prefix and suffix.

    Finds the longest common prefix, then the longest common suffix
    of the remaining (non-prefix) portions. The base similarity (fraction
    of matched characters) is scaled by a length ratio min/max to penalize
    partial matches between words of very different lengths. This preserves
    the correct ranking from the extended report's Table 1 (sf_ext.pdf, p.6),
    e.g. 'date' vs 'Birthdate' ≈ 0.22, 'int' vs 'Department' = 0.06.
    """
    if w1 == w2:
        return 1.0
    if not w1 or not w2:
        return 0.0

    # Longest common prefix
    prefix_len = 0
    for a, b in zip(w1, w2, strict=False):
        if a == b:
            prefix_len += 1
        else:
            break

    # Longest common suffix (non-overlapping with prefix)
    r1, r2 = w1[prefix_len:], w2[prefix_len:]
    suffix_len = 0
    for a, b in zip(reversed(r1), reversed(r2), strict=False):
        if a == b:
            suffix_len += 1
        else:
            break

    total = prefix_len + suffix_len
    base_sim = total / max(len(w1), len(w2))
    length_ratio = min(len(w1), len(w2)) / max(len(w1), len(w2))
    return base_sim * length_ratio


def prefix_suffix_tokenized(s1: str, s2: str) -> float:
    """Paper-style StringMatch using CamelCase tokenization and soft Dice coefficient.

    Algorithm:
      1. Split both strings into tokens via CamelCase boundaries
      2. For each token in W1, find its best prefix/suffix match in W2 (and vice versa)
      3. Aggregate using a soft Dice coefficient:
         sum(best_match scores) / (|W1| + |W2|)

    This matches Table 1 of the extended report (sf_ext.pdf, p.6) for
    structural node pairs (lines 1-5) and preserves the correct ranking
    for literal node pairs (lines 6-10).
    """
    w1 = _camel_case_split(s1)
    w2 = _camel_case_split(s2)
    if not w1 or not w2:
        return 0.0

    # For each token in w1, find its best match in w2
    total_sim = 0.0
    for a in w1:
        best = max(_word_prefix_suffix_sim(a, b) for b in w2)
        total_sim += best
    for b in w2:
        best = max(_word_prefix_suffix_sim(b, a) for a in w1)
        total_sim += best

    return total_sim / (len(w1) + len(w2))


def prefix_suffix_tfidf(s1: str, s2: str, idf_weights: dict[str, float] | None = None) -> float:
    """IDF-weighted variant of prefix/suffix token matcher.

    Same as prefix_suffix_tokenized but each token's contribution is weighted
    by its inverse document frequency. Tokens that appear in many node names
    across the schema contribute less to the similarity score.

    Args:
        s1: First string to compare.
        s2: Second string to compare.
        idf_weights: Mapping from lowercase token to IDF weight.
            If None, falls back to unweighted prefix_suffix_tokenized.
    """
    if idf_weights is None:
        return prefix_suffix_tokenized(s1, s2)

    w1 = _camel_case_split(s1)
    w2 = _camel_case_split(s2)
    if not w1 or not w2:
        return 0.0

    def idf(token: str) -> float:
        return idf_weights.get(token.lower(), 1.0)

    total_sim = 0.0
    for a in w1:
        best = max(_word_prefix_suffix_sim(a, b) for b in w2)
        total_sim += best * idf(a)
    for b in w2:
        best = max(_word_prefix_suffix_sim(b, a) for a in w1)
        total_sim += best * idf(b)

    denom = sum(idf(a) for a in w1) + sum(idf(b) for b in w2)
    return total_sim / denom if denom > 0 else 0.0


def compute_idf_weights(node_names: list[str]) -> dict[str, float]:
    """Compute IDF weights from a list of graph node names.

    Each node name is CamelCase-split into tokens. IDF is computed as
    log(N / df) where N is the total number of node names and df is
    the number of names containing the token.

    Args:
        node_names: List of non-NodeID node names from both graphs.

    Returns:
        Dict mapping lowercase token to its IDF weight.
    """
    n = len(node_names)
    if n == 0:
        return {}

    df: dict[str, int] = {}
    for name in node_names:
        tokens = {t.lower() for t in _camel_case_split(name)}
        for t in tokens:
            df[t] = df.get(t, 0) + 1

    return {token: math.log(n / count) for token, count in df.items()}


def levenshtein_sim(s1: str, s2: str) -> float:
    """Levenshtein-based similarity (valentine's original matcher)."""
    return normalize_distance(levenshtein_distance(s1, s2), s1, s2)
