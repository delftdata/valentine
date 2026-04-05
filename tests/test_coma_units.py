"""Unit tests for Coma internal modules."""

from __future__ import annotations

import pandas as pd
import pytest

from valentine.algorithms.coma.combination import (
    average,
    maximum,
    set_average,
    set_highest,
    weighted,
)
from valentine.algorithms.coma.schema import SchemaGraph
from valentine.algorithms.coma.selection import select_both_multiple
from valentine.algorithms.coma.similarity.datatype import datatype_similarity
from valentine.algorithms.coma.similarity.tfidf import TfidfCorpus, tfidf_similarity
from valentine.algorithms.coma.similarity.trigram import trigram_similarity
from valentine.data_sources import DataframeTable

# ---------------------------------------------------------------------------
# combination.py
# ---------------------------------------------------------------------------


class TestAverage:
    def test_empty(self):
        assert average([]) == 0.0

    def test_single(self):
        assert average([0.5]) == 0.5

    def test_multiple(self):
        assert average([0.2, 0.4, 0.6]) == pytest.approx(0.4)


class TestMaximum:
    def test_empty(self):
        assert maximum([]) == 0.0

    def test_values(self):
        assert maximum([0.1, 0.9, 0.5]) == 0.9


class TestWeighted:
    def test_empty_values(self):
        assert weighted([], []) == 0.0

    def test_empty_weights(self):
        assert weighted([1.0], []) == 0.0

    def test_zero_weights(self):
        assert weighted([1.0, 2.0], [0.0, 0.0]) == 0.0

    def test_uniform_weights(self):
        assert weighted([0.2, 0.8], [1.0, 1.0]) == pytest.approx(0.5)

    def test_skewed_weights(self):
        assert weighted([0.0, 1.0], [1.0, 3.0]) == pytest.approx(0.75)


class TestSetAverage:
    def test_empty(self):
        assert set_average([]) == 0.0

    def test_empty_rows(self):
        assert set_average([[]]) == 0.0

    def test_1x1(self):
        assert set_average([[0.8]]) == pytest.approx(0.8)

    def test_2x2(self):
        # Row maxes: 0.9, 0.7; Col maxes: 0.7, 0.9
        # (0.9+0.7+0.7+0.9) / (2+2) = 3.2/4 = 0.8
        assert set_average([[0.9, 0.1], [0.2, 0.7]]) == pytest.approx(0.8)

    def test_rectangular(self):
        # 2x3 matrix
        mat = [[0.3, 0.6, 0.1], [0.8, 0.2, 0.4]]
        # Row maxes: 0.6, 0.8 -> sum=1.4
        # Col maxes: 0.8, 0.6, 0.4 -> sum=1.8
        # (1.4 + 1.8) / (2 + 3) = 3.2/5 = 0.64
        assert set_average(mat) == pytest.approx(0.64)


class TestSetHighest:
    def test_empty(self):
        assert set_highest([]) == 0.0

    def test_values(self):
        assert set_highest([[0.1, 0.3], [0.5, 0.2]]) == 0.5


# ---------------------------------------------------------------------------
# similarity/datatype.py
# ---------------------------------------------------------------------------


class TestDatatypeSimilarity:
    def test_same_type(self):
        assert datatype_similarity("varchar", "varchar") == 1.0
        assert datatype_similarity("int", "int") == 1.0

    def test_compatible(self):
        assert datatype_similarity("int", "float") == 0.5
        assert datatype_similarity("varchar", "date") == 0.3

    def test_incompatible(self):
        assert datatype_similarity("int", "date") == 0.0
        assert datatype_similarity("float", "varchar") == 0.0

    def test_case_insensitive(self):
        assert datatype_similarity("VARCHAR", "varchar") == 1.0
        assert datatype_similarity(" Int ", " FLOAT ") == 0.5

    def test_empty_and_none(self):
        assert datatype_similarity("", "") == 1.0
        assert datatype_similarity(None, None) == 1.0
        assert datatype_similarity("int", "") == 0.0

    def test_unknown_types(self):
        assert datatype_similarity("boolean", "text") == 0.0


# ---------------------------------------------------------------------------
# similarity/trigram.py
# ---------------------------------------------------------------------------


class TestTrigramSimilarity:
    def test_identical(self):
        assert trigram_similarity("hello", "hello") == 1.0

    def test_case_insensitive(self):
        assert trigram_similarity("Hello", "hello") == 1.0

    def test_empty_both(self):
        assert trigram_similarity("", "") == 1.0

    def test_empty_one(self):
        assert trigram_similarity("abc", "") == 0.0
        assert trigram_similarity("", "abc") == 0.0

    def test_short_strings(self):
        # Strings shorter than 3 chars produce a single "trigram" equal to the string
        sim = trigram_similarity("ab", "ab")
        assert sim == 1.0

    def test_short_vs_long(self):
        sim = trigram_similarity("a", "abc")
        assert 0.0 <= sim <= 1.0

    def test_partial_overlap(self):
        sim = trigram_similarity("fname", "first_name")
        assert 0.0 < sim < 1.0

    def test_no_overlap(self):
        sim = trigram_similarity("xyz", "abc")
        assert sim == 0.0


# ---------------------------------------------------------------------------
# similarity/tfidf.py
# ---------------------------------------------------------------------------


class TestTfidfCorpus:
    def test_identical_instances(self):
        corpus = TfidfCorpus([["hello world", "foo bar"], ["hello world", "foo bar"]])
        sim = corpus.similarity(["hello world", "foo bar"], ["hello world", "foo bar"])
        assert sim > 0.9

    def test_disjoint_instances(self):
        corpus = TfidfCorpus([["apple banana"], ["cherry dragonfruit"]])
        sim = corpus.similarity(["apple banana"], ["cherry dragonfruit"])
        assert sim == 0.0

    def test_empty_corpus(self):
        corpus = TfidfCorpus([])
        assert corpus.similarity(["hello"], ["world"]) == 0.0

    def test_empty_instances(self):
        corpus = TfidfCorpus([["hello"]])
        assert corpus.similarity([], ["hello"]) == 0.0
        assert corpus.similarity(["hello"], []) == 0.0

    def test_stop_words_only(self):
        # All stop words get removed — should handle gracefully
        corpus = TfidfCorpus([["the and or"], ["is was"]])
        sim = corpus.similarity(["the and or"], ["is was"])
        assert sim == 0.0


class TestTfidfSimilarityStandalone:
    def test_basic(self):
        # Need enough distinct docs so IDF doesn't zero out shared terms
        sim = tfidf_similarity(
            ["hello world", "foo bar", "baz qux"],
            ["hello world", "foo bar", "baz qux"],
        )
        assert sim > 0.9

    def test_empty(self):
        assert tfidf_similarity([], ["hello"]) == 0.0
        assert tfidf_similarity(["hello"], []) == 0.0


# ---------------------------------------------------------------------------
# schema.py
# ---------------------------------------------------------------------------


class TestSchemaGraph:
    @pytest.fixture
    def graph(self):
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25], "city": ["NYC", "LA"]})
        table = DataframeTable(df, name="people")
        return SchemaGraph.from_table(table)

    def test_root_name(self, graph):
        assert graph.root.name == "people"

    def test_columns_count(self, graph):
        assert len(graph.columns) == 3

    def test_column_accession(self, graph):
        names = {c.accession for c in graph.columns}
        assert names == {"people.name", "people.age", "people.city"}

    def test_instances_loaded(self, graph):
        name_col = next(c for c in graph.columns if c.name == "name")
        assert name_col.instances == ["Alice", "Bob"]

    def test_get_parents_of_root(self, graph):
        assert graph.get_parents(graph.root) == []

    def test_get_parents_of_column(self, graph):
        col = graph.columns[0]
        assert graph.get_parents(col) == [graph.root]

    def test_get_children_of_root(self, graph):
        assert graph.get_children(graph.root) == graph.columns

    def test_get_children_of_column(self, graph):
        assert graph.get_children(graph.columns[0]) == []

    def test_get_siblings(self, graph):
        col = graph.columns[0]
        siblings = graph.get_siblings(col)
        assert col not in siblings
        assert len(siblings) == 2

    def test_get_siblings_of_root(self, graph):
        assert graph.get_siblings(graph.root) == []

    def test_get_leaves_of_root(self, graph):
        assert graph.get_leaves(graph.root) == graph.columns

    def test_get_leaves_of_column(self, graph):
        col = graph.columns[0]
        assert graph.get_leaves(col) == [col]

    def test_get_paths(self, graph):
        paths = graph.get_paths()
        assert len(paths) == 3
        for path in paths:
            assert path[0] is graph.root
            assert path[1] in graph.columns

    def test_null_handling(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, None, "x"]})
        table = DataframeTable(df, name="t")
        graph = SchemaGraph.from_table(table)
        col_a = next(c for c in graph.columns if c.name == "a")
        col_b = next(c for c in graph.columns if c.name == "b")
        # Row 0: a=1, b=None -> row counts, a gets "1"
        # Row 1: a=None, b=None -> skip (no values)
        # Row 2: a=3, b="x" -> row counts
        assert len(col_a.instances) == 2  # "1", "3"
        assert len(col_b.instances) == 1  # "x"


# ---------------------------------------------------------------------------
# selection.py
# ---------------------------------------------------------------------------


class TestSelection:
    @pytest.fixture
    def elements_and_matrix(self):
        e1, e2, e3 = "A", "B", "C"
        t1, t2, t3 = "X", "Y", "Z"
        sources = [e1, e2, e3]
        targets = [t1, t2, t3]
        sim = {
            (e1, t1): 0.9,
            (e1, t2): 0.1,
            (e1, t3): 0.2,
            (e2, t1): 0.1,
            (e2, t2): 0.8,
            (e2, t3): 0.15,
            (e3, t1): 0.2,
            (e3, t2): 0.15,
            (e3, t3): 0.7,
        }
        return sim, sources, targets

    def test_basic_selection(self, elements_and_matrix):
        sim, sources, targets = elements_and_matrix
        result = select_both_multiple(sim, sources, targets, delta=0.5)
        # Best mutual matches should be selected
        assert ("A", "X") in result
        assert ("B", "Y") in result
        assert ("C", "Z") in result

    def test_max_n_limits(self, elements_and_matrix):
        sim, sources, targets = elements_and_matrix
        # With max_n=1, only the best match per element
        strict = select_both_multiple(sim, sources, targets, max_n=1, delta=0.0)
        relaxed = select_both_multiple(sim, sources, targets, max_n=0, delta=0.0)
        assert len(strict) <= len(relaxed)

    def test_threshold_filters(self, elements_and_matrix):
        sim, sources, targets = elements_and_matrix
        result = select_both_multiple(sim, sources, targets, threshold=0.5, delta=0.0)
        for score in result.values():
            assert score >= 0.5

    def test_delta_zero_keeps_all(self, elements_and_matrix):
        sim, sources, targets = elements_and_matrix
        result = select_both_multiple(sim, sources, targets, delta=0.0, max_n=0, threshold=0.0)
        # delta=0 means no delta filtering; all non-zero pairs kept
        assert len(result) == 9

    def test_high_delta_filters(self, elements_and_matrix):
        sim, sources, targets = elements_and_matrix
        tight = select_both_multiple(sim, sources, targets, delta=0.5)
        loose = select_both_multiple(sim, sources, targets, delta=0.01)
        assert len(loose) <= len(tight)

    def test_empty_matrix(self):
        assert select_both_multiple({}, [], [], delta=0.01) == {}
