from functools import lru_cache
from itertools import combinations, product

import numpy as np
from rapidfuzz import process
from rapidfuzz.distance import (
    DamerauLevenshtein,
    Hamming,
    Jaro,
    JaroWinkler,
    Levenshtein,
)

from ...data_sources.base_table import BaseTable
from ..base_matcher import BaseMatcher
from ..jaccard_distance import StringDistanceFunction
from ..match import Match

# Map our public StringDistanceFunction enum to the rapidfuzz scorer that
# returns a normalized similarity in [0, 1]. rapidfuzz.process.cdist runs
# the comparison in a C++ inner loop with optional thread-level parallelism.
_SCORER_MAP = {
    StringDistanceFunction.Levenshtein: Levenshtein.normalized_similarity,
    StringDistanceFunction.DamerauLevenshtein: DamerauLevenshtein.normalized_similarity,
    StringDistanceFunction.Hamming: Hamming.normalized_similarity,
    StringDistanceFunction.Jaro: Jaro.normalized_similarity,
    StringDistanceFunction.JaroWinkler: JaroWinkler.normalized_similarity,
}


@lru_cache(maxsize=4)
def _load_sentence_transformer(model_name: str, device: str | None):
    """Lazily load and cache a SentenceTransformer model on a device.

    Importing inside the function keeps ``sentence-transformers`` an
    optional dependency: the rest of this module — and every other
    ``StringDistanceFunction`` value — works without it installed.

    ``device`` is passed straight through to ``SentenceTransformer``.
    ``None`` lets the library auto-pick (typically ``cuda`` if available,
    else ``mps`` on Apple Silicon, else ``cpu``). Pass ``"cpu"``,
    ``"cuda"``, ``"cuda:1"``, or ``"mps"`` to force a specific device.
    The cache is keyed by ``(model_name, device)`` so switching devices
    does not silently reuse a model loaded elsewhere.
    """
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise ImportError(
            "StringDistanceFunction.Embedding requires the 'sentence-transformers' "
            "package. Install it with: pip install 'valentine[embeddings]'"
        ) from exc
    return SentenceTransformer(model_name, device=device)


class JaccardDistanceMatcher(BaseMatcher):
    """Baseline instance-based matcher using Jaccard similarity.

    Columns are compared by Jaccard similarity of their value sets, with
    element equality decided by a configurable string distance function.
    This is a simple but effective baseline for instance-based matching.

    Parameters
    ----------
    threshold_dist : float, optional
        Acceptance threshold above which two string values are considered
        equal under the chosen ``distance_fun``, in ``[0, 1]``
        (default: ``0.8``). Ignored when ``distance_fun`` is
        :attr:`StringDistanceFunction.Exact`. For
        :attr:`StringDistanceFunction.Embedding`, the threshold is
        applied to cosine similarity of sentence-transformer embeddings;
        ~0.7 is a typical operating point.
    distance_fun : StringDistanceFunction, optional
        String similarity function. One of
        :attr:`StringDistanceFunction.Levenshtein` (default),
        :attr:`StringDistanceFunction.DamerauLevenshtein`,
        :attr:`StringDistanceFunction.Hamming`,
        :attr:`StringDistanceFunction.Jaro`,
        :attr:`StringDistanceFunction.JaroWinkler`,
        :attr:`StringDistanceFunction.Exact`, or
        :attr:`StringDistanceFunction.Embedding`.
    process_num : int, optional
        Number of worker threads passed to ``rapidfuzz.process.cdist``
        (must be ``>= 1``, default: ``1``). Earlier versions used a
        ``multiprocessing.Pool``; with rapidfuzz the inner kernel is
        already C++ and parallelises via OpenMP threads, so the pool is
        no longer needed.
    embedding_model : str, optional
        Name of the sentence-transformers model used when
        ``distance_fun=StringDistanceFunction.Embedding`` (default:
        ``"all-MiniLM-L6-v2"``, a 23 MB / 384-dim model that runs well
        on CPU). Ignored for non-embedding distances.
    embedding_device : str or None, optional
        Device to load the embedding model on. Passed straight through
        to ``SentenceTransformer``. ``None`` (the default) lets the
        library auto-detect — usually ``"cuda"`` if a GPU is present,
        ``"mps"`` on Apple Silicon, otherwise ``"cpu"``. Pass
        ``"cpu"``, ``"cuda"``, ``"cuda:1"``, or ``"mps"`` to force a
        specific device. Ignored for non-embedding distances.
    embedding_batch_size : int or None, optional
        Batch size used for the global ``model.encode`` call. ``None``
        (the default) does not pass the kwarg, letting
        sentence-transformers use its own default (``32``). Pass an
        explicit value (e.g. ``128`` or ``256``) to amortise per-call
        overhead when encoding large vocabularies on capable hardware.
        Ignored for non-embedding distances.
    tversky_alpha : float, optional
        Tversky penalty for unmatched values on the *reference* side
        (default: ``1.0``). The pair-similarity reduction is
        ``T(A, B; α, β) = |A∩B| / (|A∩B| + α·|A−B| + β·|B−A|)``,
        symmetrised by computing both ``T(A, B)`` and ``T(B, A)`` and
        taking the max so the matcher remains direction-agnostic. With
        ``α = β = 1.0`` this reduces to Jaccard; with ``α = 1.0,
        β = 0.0`` (or vice versa) it reduces to ``max(|∩|/|A|, |∩|/|B|)``,
        i.e. set containment — the right choice when one column is
        expected to be a subset of the other. Intermediate values trade
        off between these extremes.
    tversky_beta : float, optional
        Tversky penalty for unmatched values on the *other* side
        (default: ``1.0``). See ``tversky_alpha``.
    """

    def __init__(
        self,
        threshold_dist: float = 0.8,
        distance_fun: StringDistanceFunction = StringDistanceFunction.Levenshtein,
        process_num: int = 1,
        embedding_model: str = "all-MiniLM-L6-v2",
        embedding_device: str | None = None,
        embedding_batch_size: int | None = None,
        tversky_alpha: float = 1.0,
        tversky_beta: float = 1.0,
    ):
        self.__threshold_dist = float(threshold_dist)
        self.__process_num = int(process_num)
        self.__distance_function = distance_fun
        self.__embedding_model_name = str(embedding_model)
        self.__embedding_device = embedding_device
        if embedding_batch_size is not None and embedding_batch_size < 1:
            raise ValueError(
                f"embedding_batch_size must be >= 1 or None, got {embedding_batch_size}"
            )
        self.__embedding_batch_size = (
            None if embedding_batch_size is None else int(embedding_batch_size)
        )
        if tversky_alpha < 0.0 or tversky_beta < 0.0:
            raise ValueError(
                f"tversky_alpha and tversky_beta must be >= 0, "
                f"got alpha={tversky_alpha}, beta={tversky_beta}"
            )
        self.__tversky_alpha = float(tversky_alpha)
        self.__tversky_beta = float(tversky_beta)
        if not 0.0 <= self.__threshold_dist <= 1.0:
            raise ValueError(
                f"threshold_dist must be between 0.0 and 1.0, got {self.__threshold_dist}"
            )
        if self.__process_num < 1:
            raise ValueError(f"process_num must be >= 1, got {self.__process_num}")

    def get_matches(self, source_input: BaseTable, target_input: BaseTable) -> dict:
        col_embeddings = self.__build_col_embeddings([source_input, target_input])
        return self.__match_pair(source_input, target_input, col_embeddings)

    def get_matches_batch(self, tables: list[BaseTable]) -> dict:
        """Match all unique table pairs, sharing one global embedding pass.

        For ``StringDistanceFunction.Embedding`` this means each unique
        string across every column of every table is encoded exactly
        once. With other distances the override is equivalent to the
        default ``BaseMatcher.get_matches_batch``.
        """
        col_embeddings = self.__build_col_embeddings(tables)
        matches: dict = {}
        for t1, t2 in combinations(tables, 2):
            matches.update(self.__match_pair(t1, t2, col_embeddings))
        return matches

    def __match_pair(
        self,
        source_input: BaseTable,
        target_input: BaseTable,
        col_embeddings: dict[tuple[str, str], tuple[list[str], np.ndarray]] | None,
    ) -> dict:
        matches: dict = {}
        for combination in self.__get_column_combinations(
            source_input,
            target_input,
            self.__threshold_dist,
            self.__distance_function,
            col_embeddings,
        ):
            matches.update(self.process_jaccard_distance(combination))
        # Remove the pairs with zero similarity
        return {k: v for k, v in matches.items() if v > 0.0}

    def __build_col_embeddings(
        self, tables: list[BaseTable]
    ) -> dict[tuple[str, str], tuple[list[str], np.ndarray]] | None:
        """Encode every column across every table with one batched call.

        Returns a ``(table_name, column_name) -> (values, embeddings)``
        map, or ``None`` when the chosen distance is not embedding-based.

        Two layers of deduplication keep this cheap:

        - **Per-column**: the column's value set is converted to a sorted
          list of unique strings (deterministic for repeated runs).
        - **Global vocabulary**: identical strings appearing in many
          columns are encoded only once, then sliced back out.

        The encode itself is a single ``model.encode`` call with a large
        batch size, which dominates the speedup over per-column encoding.
        """
        if self.__distance_function != StringDistanceFunction.Embedding:
            return None

        # Collect per-column unique values, deterministically ordered.
        col_values: dict[tuple[str, str], list[str]] = {}
        for table in tables:
            for column in table.get_instances_columns():
                key = (table.name, column.name)
                if key in col_values:
                    continue
                col_values[key] = sorted({str(v) for v in column.data})

        # Build a global vocabulary: each unique string is encoded once.
        vocab: dict[str, int] = {}
        for values in col_values.values():
            for v in values:
                if v not in vocab:
                    vocab[v] = len(vocab)

        if not vocab:
            return {key: (values, np.zeros((0, 0), dtype=np.float32)) for key, values in col_values.items()}

        model = _load_sentence_transformer(
            self.__embedding_model_name, self.__embedding_device
        )
        encode_kwargs: dict = {
            "normalize_embeddings": True,
            "show_progress_bar": False,
            "convert_to_numpy": True,
        }
        if self.__embedding_batch_size is not None:
            encode_kwargs["batch_size"] = self.__embedding_batch_size
        all_embeddings = model.encode(list(vocab.keys()), **encode_kwargs).astype(
            np.float32
        )

        dim = all_embeddings.shape[1]
        out: dict[tuple[str, str], tuple[list[str], np.ndarray]] = {}
        for key, values in col_values.items():
            if not values:
                out[key] = (values, np.zeros((0, dim), dtype=np.float32))
                continue
            indices = [vocab[v] for v in values]
            out[key] = (values, all_embeddings[indices])
        return out

    def process_jaccard_distance(self, tup: tuple):
        (
            source_data,
            target_data,
            threshold,
            target_table_name,
            target_column_name,
            source_table_name,
            source_column_name,
            distance_function,
            embeddings,
        ) = tup

        if distance_function == StringDistanceFunction.Embedding:
            sim = self.__embedding_similarity(
                embeddings,
                source_table_name,
                source_column_name,
                target_table_name,
                target_column_name,
                threshold,
            )
            return Match(
                target_table_name,
                target_column_name,
                source_table_name,
                source_column_name,
                sim,
            ).to_dict

        set1 = {str(x) for x in source_data}
        set2 = {str(x) for x in target_data}
        # Iterate over the smaller set as queries: cdist scales with
        # rows x cols, and the row dimension dominates Python-side overhead.
        if len(set1) > len(set2):
            set1, set2 = set2, set1

        if distance_function == StringDistanceFunction.Exact:
            # Exact match is symmetric — both sides see the same intersection.
            inter = len(set1 & set2)
            a_match = b_match = float(inter)
        elif not set1 or not set2:
            a_match = b_match = 0.0
        else:
            scorer = _SCORER_MAP[distance_function]
            queries = list(set1)
            choices = list(set2)
            scores = process.cdist(
                queries,
                choices,
                scorer=scorer,
                score_cutoff=threshold,
                workers=self.__process_num,
            )
            a_match, b_match = self.__directional_counts(scores, threshold)

        sim = self.__aggregate(a_match, b_match, len(set1), len(set2))

        return Match(
            target_table_name,
            target_column_name,
            source_table_name,
            source_column_name,
            sim,
        ).to_dict

    def __embedding_similarity(
        self,
        embeddings: dict[tuple[str, str], tuple[list[str], np.ndarray]],
        source_table_name: str,
        source_column_name: str,
        target_table_name: str,
        target_column_name: str,
        threshold: float,
    ) -> float:
        """Tversky-reduced set similarity using cosine on embeddings.

        Two values are treated as "matched" when their cosine similarity
        is ``>= threshold``. Both directional match counts come from the
        same ``sims`` matrix and are reduced via Tversky.
        """
        src_values, src_emb = embeddings[(source_table_name, source_column_name)]
        tgt_values, tgt_emb = embeddings[(target_table_name, target_column_name)]
        if not src_values or not tgt_values:
            return 0.0
        # Iterate over the smaller side, matching the rapidfuzz branch.
        if len(src_values) > len(tgt_values):
            src_values, tgt_values = tgt_values, src_values
            src_emb, tgt_emb = tgt_emb, src_emb
        # Embeddings are L2-normalised at encode-time, so cosine = dot product.
        sims = src_emb @ tgt_emb.T
        a_match, b_match = self.__directional_counts(sims, threshold)
        return self.__aggregate(a_match, b_match, len(src_values), len(tgt_values))

    @staticmethod
    def __directional_counts(
        scores: np.ndarray, threshold: float
    ) -> tuple[float, float]:
        """Count rows / columns whose best entry is at least ``threshold``.

        ``scores[i, j]`` is the similarity between A's i-th value and B's
        j-th value. The first value is the count of A-side values with at
        least one above-threshold partner; the second is the count on B's
        side.
        """
        hits = scores >= threshold
        return (
            float(np.count_nonzero(hits.any(axis=1))),
            float(np.count_nonzero(hits.any(axis=0))),
        )

    def __aggregate(
        self, a_match: float, b_match: float, a_size: int, b_size: int
    ) -> float:
        """Reduce directional match counts to a similarity score via Tversky.

        Uses the asymmetric Tversky index in both directions and returns
        the larger of the two so the matcher stays direction-agnostic:

            T(A, B; α, β) = a_match / (a_match + α·(|A|−a_match) + β·(|B|−b_match))

        With α = β = 1 this is Jaccard; α = 1, β = 0 (or vice versa)
        recovers ``max(|∩|/|A|, |∩|/|B|)`` containment.
        """
        if a_size == 0 or b_size == 0:
            return 0.0
        alpha, beta = self.__tversky_alpha, self.__tversky_beta
        a_unmatched = max(a_size - a_match, 0.0)
        b_unmatched = max(b_size - b_match, 0.0)
        denom_ab = a_match + alpha * a_unmatched + beta * b_unmatched
        denom_ba = b_match + alpha * b_unmatched + beta * a_unmatched
        t_ab = 0.0 if denom_ab <= 0.0 else a_match / denom_ab
        t_ba = 0.0 if denom_ba <= 0.0 else b_match / denom_ba
        return float(max(t_ab, t_ba))

    @staticmethod
    def __get_column_combinations(
        source_table: BaseTable,
        target_table: BaseTable,
        threshold,
        distance_function: StringDistanceFunction,
        col_embeddings: dict[tuple[str, str], tuple[list[str], np.ndarray]] | None,
    ):
        for source_column, target_column in product(
            source_table.get_instances_columns(), target_table.get_instances_columns()
        ):
            yield (
                source_column.data,
                target_column.data,
                threshold,
                target_table.name,
                target_column.name,
                source_table.name,
                source_column.name,
                distance_function,
                col_embeddings,
            )
