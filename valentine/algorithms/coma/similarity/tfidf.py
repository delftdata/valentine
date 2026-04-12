from __future__ import annotations

import re
from functools import lru_cache

import nltk
import numpy as np
from nltk.corpus import stopwords
from scipy.sparse import csr_matrix

_SPLIT_RE = re.compile(r"[^a-zA-Z0-9]+")


@lru_cache(maxsize=1)
def _english_stopwords() -> frozenset[str]:
    """Return NLTK English stopwords as a cached frozenset."""
    try:
        return frozenset(stopwords.words("english"))
    except LookupError:
        nltk.download("stopwords", quiet=True)
        return frozenset(stopwords.words("english"))


def _tokenize(text: str) -> list[str]:
    """Tokenize: lowercase, split on non-alphanum, remove stop words."""
    sw = _english_stopwords()
    tokens = _SPLIT_RE.split(text.lower())
    return [t for t in tokens if t and t not in sw]


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

    mat = csr_matrix(
        (np.asarray(data, dtype=np.float32), (rows, cols)),
        shape=(len(docs), len(vocab)),
        dtype=np.float32,
    )

    # L2 normalize each row
    norms = np.sqrt(mat.multiply(mat).sum(axis=1)).A1
    norms[norms == 0] = 1.0
    # Multiply by inverse norms (sparse diagonal)
    mat = mat.multiply((1.0 / norms)[:, np.newaxis].astype(np.float32))
    return mat.tocsr().astype(np.float32)


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
        else:
            # Compute global document frequencies and IDF
            df = np.zeros(vocab_size)
            for doc in all_docs:
                for token in set(doc):
                    df[self._vocab[token]] += 1
            self._idf = np.zeros(vocab_size)
            mask = df > 0
            self._idf[mask] = np.log(n_docs / df[mask])

        # Per-column TF-IDF row cache. Cupid/Coma call ``similarity`` with
        # the same ``instances`` list object repeatedly (once per target
        # column in the cross product), so caching on list identity turns
        # an O(N*M) rebuild of sparse matrices into O(N+M).
        # We store a reference to the list alongside the cached value so
        # that GC cannot reclaim the list and reuse its ``id()``.
        self._column_cache: dict[int, tuple[list, csr_matrix, int]] = {}
        # Per-pair similarity cache. ``InstancesCM`` evaluates both
        # ``InstancesDirect`` and ``InstancesAll`` per element pair, and
        # for flat schemas both extract the same ``elem.instances`` list,
        # so ``similarity`` would otherwise be called twice with identical
        # arguments. Caching on the (id, id) pair collapses that to one
        # computation.
        self._pair_cache: dict[tuple[int, int], float] = {}

    def _vectorise_column(self, instances: list[str]) -> tuple[csr_matrix, int]:
        key = id(instances)
        cached = self._column_cache.get(key)
        if cached is not None:
            # Verify the reference is the same object (not a recycled id)
            ref, vecs, n = cached
            if ref is instances:
                return vecs, n
        docs = [d for v in instances if (d := _tokenize(str(v)))]
        n = len(docs)
        if n == 0:
            vecs = csr_matrix((0, max(len(self._vocab), 1)))
        else:
            vecs = _build_sparse_tfidf(docs, self._vocab, self._idf)
        self._column_cache[key] = (instances, vecs, n)
        return vecs, n

    def similarity(self, instances1: list[str], instances2: list[str]) -> float:
        """Compute TF-IDF cosine similarity using the global IDF."""
        if not instances1 or not instances2 or len(self._idf) == 0:
            return 0.0

        id1, id2 = id(instances1), id(instances2)
        key = (id1, id2) if id1 <= id2 else (id2, id1)
        cached = self._pair_cache.get(key)
        if cached is not None:
            return cached

        vecs1, m = self._vectorise_column(instances1)
        vecs2, n = self._vectorise_column(instances2)

        if m == 0 or n == 0:
            self._pair_cache[key] = 0.0
            return 0.0

        # Densify the (m x n) similarity matrix once and let numpy do the
        # axis maxes. scipy.sparse's csr_matrix.max(axis=...) converts to
        # CSC under the hood and was the dominant cost on Coma's worst
        # cases (~10s/dataset on the NYU benchmark). The dense matrix is
        # bounded by n_instances1 * n_instances2 floats, which stays under
        # a few hundred MB even on the largest columns we benchmark.
        sim_dense = (vecs1 @ vecs2.T).toarray()

        sum_row_max = float(sim_dense.max(axis=1).sum())
        sum_col_max = float(sim_dense.max(axis=0).sum())

        result = (sum_row_max + sum_col_max) / (m + n)
        self._pair_cache[key] = result
        return result


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
