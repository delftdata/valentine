from __future__ import annotations

import re

import numpy as np
from scipy.sparse import csr_matrix

# Lucene StandardAnalyzer English stop words (Lucene 3.x, used by COMA's Java implementation)
_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "but",
        "by",
        "for",
        "if",
        "in",
        "into",
        "is",
        "it",
        "no",
        "not",
        "of",
        "on",
        "or",
        "such",
        "that",
        "the",
        "their",
        "then",
        "there",
        "these",
        "they",
        "this",
        "to",
        "was",
        "will",
        "with",
    }
)

_SPLIT_RE = re.compile(r"[^a-zA-Z0-9]+")


def _tokenize(text: str) -> list[str]:
    """Tokenize like Lucene StandardAnalyzer: lowercase, split on non-alphanum, remove stop words."""
    tokens = _SPLIT_RE.split(text.lower())
    return [t for t in tokens if t and t not in _STOP_WORDS]


def _build_sparse_tfidf(
    docs: list[list[str]], vocab: dict[str, int], idf: np.ndarray
) -> csr_matrix:
    """Build L2-normalized sparse TF-IDF matrix (one row per document)."""
    rows, cols, data = [], [], []
    for i, doc in enumerate(docs):
        tf: dict[int, int] = {}
        for token in doc:
            idx = vocab[token]
            tf[idx] = tf.get(idx, 0) + 1
        for idx, count in tf.items():
            # Lucene 3.x / Mahout uses sqrt(tf) * idf
            w = np.sqrt(count) * idf[idx]
            if w > 0:
                rows.append(i)
                cols.append(idx)
                data.append(w)

    mat = csr_matrix((data, (rows, cols)), shape=(len(docs), len(vocab)))

    # L2 normalize each row
    norms = np.sqrt(mat.multiply(mat).sum(axis=1)).A1
    norms[norms == 0] = 1.0
    # Multiply by inverse norms (sparse diagonal)
    mat = mat.multiply(1.0 / norms[:, np.newaxis])
    return mat.tocsr()


class TfidfCorpus:
    """
    Pre-computed global TF-IDF corpus matching Java COMA's behavior.

    Java's LuceneTFIDFFullyCachedAlternative indexes ALL instances from ALL
    columns of both tables into a single Lucene index, computing IDF globally.
    This class replicates that by pre-tokenizing all instances and computing
    a global vocabulary and IDF vector.
    """

    def __init__(self, all_column_instances: list[list[str]]) -> None:
        # Tokenize all instances from all columns into one flat corpus
        all_docs: list[list[str]] = []
        for column_instances in all_column_instances:
            for v in column_instances:
                tokens = _tokenize(str(v))
                if tokens:
                    all_docs.append(tokens)

        self._vocab: dict[str, int] = {}
        for doc in all_docs:
            for token in doc:
                if token not in self._vocab:
                    self._vocab[token] = len(self._vocab)

        n_docs = len(all_docs)
        vocab_size = len(self._vocab)

        if n_docs == 0 or vocab_size == 0:
            self._idf = np.zeros(0)
            return

        # Compute global document frequencies and IDF
        df = np.zeros(vocab_size)
        for doc in all_docs:
            for token in set(doc):
                df[self._vocab[token]] += 1
        self._idf = np.zeros(vocab_size)
        mask = df > 0
        self._idf[mask] = np.log(n_docs / df[mask])

    def similarity(self, instances1: list[str], instances2: list[str]) -> float:
        """Compute TF-IDF cosine similarity using the global IDF."""
        if not instances1 or not instances2 or len(self._idf) == 0:
            return 0.0

        docs1 = [d for v in instances1 if (d := _tokenize(str(v)))]
        docs2 = [d for v in instances2 if (d := _tokenize(str(v)))]

        if not docs1 or not docs2:
            return 0.0

        vecs1 = _build_sparse_tfidf(docs1, self._vocab, self._idf)
        vecs2 = _build_sparse_tfidf(docs2, self._vocab, self._idf)

        sim_sparse = vecs1 @ vecs2.T

        m, n = len(docs1), len(docs2)
        sum_row_max = float(sim_sparse.max(axis=1).toarray().sum())
        sum_col_max = float(sim_sparse.max(axis=0).toarray().sum())

        return (sum_row_max + sum_col_max) / (m + n)


def tfidf_similarity(instances1: list[str], instances2: list[str]) -> float:
    """
    Compute TF-IDF cosine similarity between two sets of instance values
    using a local (per-pair) corpus. Prefer TfidfCorpus.similarity() for
    accuracy matching Java COMA, which uses a global corpus.
    """
    if not instances1 or not instances2:
        return 0.0

    corpus = TfidfCorpus([instances1, instances2])
    return corpus.similarity(instances1, instances2)
