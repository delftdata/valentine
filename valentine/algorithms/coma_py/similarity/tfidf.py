from __future__ import annotations

import math


def tfidf_similarity(instances1: list[str], instances2: list[str]) -> float:
    """
    Compute TF-IDF cosine similarity between two sets of instance values.

    Each instance list is treated as a single "document" (all values concatenated).
    TF-IDF is computed over a 2-document corpus, then cosine similarity is returned.

    Returns 0.0 if either list is empty or if vectors are zero.
    """
    if not instances1 or not instances2:
        return 0.0

    # Tokenize: split each instance value into words, build per-document token lists
    tokens1 = _tokenize_instances(instances1)
    tokens2 = _tokenize_instances(instances2)

    if not tokens1 and not tokens2:
        return 1.0
    if not tokens1 or not tokens2:
        return 0.0

    # Build vocabulary
    vocab: dict[str, int] = {}
    for t in tokens1:
        if t not in vocab:
            vocab[t] = len(vocab)
    for t in tokens2:
        if t not in vocab:
            vocab[t] = len(vocab)

    if not vocab:
        return 0.0

    n_docs = 2

    # Compute term frequencies
    tf1 = _term_frequencies(tokens1, vocab)
    tf2 = _term_frequencies(tokens2, vocab)

    # Compute document frequencies (how many of the 2 docs contain each term)
    df = [0] * len(vocab)
    for i in range(len(vocab)):
        if tf1[i] > 0:
            df[i] += 1
        if tf2[i] > 0:
            df[i] += 1

    # Compute IDF: log(N / df) with smoothing
    idf = [math.log(n_docs / d) if d > 0 else 0.0 for d in df]

    # Compute TF-IDF vectors
    tfidf1 = [tf1[i] * idf[i] for i in range(len(vocab))]
    tfidf2 = [tf2[i] * idf[i] for i in range(len(vocab))]

    # Cosine similarity
    return _cosine(tfidf1, tfidf2)


def _tokenize_instances(instances: list[str]) -> list[str]:
    """Tokenize instance values into lowercase words."""
    tokens = []
    for val in instances:
        for word in str(val).lower().split():
            stripped = word.strip()
            if stripped:
                tokens.append(stripped)
    return tokens


def _term_frequencies(tokens: list[str], vocab: dict[str, int]) -> list[float]:
    """Compute normalized term frequency vector."""
    tf = [0.0] * len(vocab)
    for t in tokens:
        idx = vocab.get(t)
        if idx is not None:
            tf[idx] += 1.0
    # Normalize by max frequency
    max_tf = max(tf) if tf else 0.0
    if max_tf > 0:
        tf = [f / max_tf for f in tf]
    return tf


def _cosine(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
