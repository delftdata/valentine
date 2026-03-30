from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .combination import average, maximum, set_average
from .schema import SchemaElement, SchemaGraph
from .similarity.datatype import datatype_similarity
from .similarity.tfidf import tfidf_similarity
from .similarity.trigram import trigram_similarity

# ---------------------------------------------------------------------------
# RES3: Information extraction functions
# ---------------------------------------------------------------------------


def extract_name(elem: SchemaElement) -> str:
    return elem.name


def extract_datatype(elem: SchemaElement) -> str:
    return elem.data_type


def extract_path(elem: SchemaElement) -> str:
    return elem.accession


def extract_instances_direct(elem: SchemaElement) -> list[str]:
    return elem.instances


def extract_instances_all(elem: SchemaElement) -> list[str]:
    # For flat schemas, all instances = direct instances
    return elem.instances


# ---------------------------------------------------------------------------
# RES2: Context selection functions
# ---------------------------------------------------------------------------


def ctx_selfnode(elem: SchemaElement, _graph: SchemaGraph) -> list[SchemaElement]:
    return [elem]


def ctx_selfpath(elem: SchemaElement, graph: SchemaGraph) -> list[SchemaElement]:
    if elem is graph.root:
        return [elem]
    return [graph.root, elem]


def ctx_leaves(elem: SchemaElement, graph: SchemaGraph) -> list[SchemaElement]:
    return graph.get_leaves(elem)


def ctx_parents(elem: SchemaElement, graph: SchemaGraph) -> list[SchemaElement]:
    return graph.get_parents(elem)


def ctx_siblings(elem: SchemaElement, graph: SchemaGraph) -> list[SchemaElement]:
    return graph.get_siblings(elem)


# ---------------------------------------------------------------------------
# Matcher (RES3 level)
# ---------------------------------------------------------------------------


@dataclass
class Matcher:
    """
    Lowest-level matcher: extracts information (RES3) from two elements
    and computes a similarity score.
    """

    name: str
    extract: Callable[[SchemaElement], str | list[str]]
    similarity_fn: Callable
    set_combination: Callable[[list[float]], float]

    def compute(self, elem1: SchemaElement, elem2: SchemaElement) -> float:
        val1 = self.extract(elem1)
        val2 = self.extract(elem2)
        return self.similarity_fn(val1, val2)


# Predefined matchers
NAME_MATCHER = Matcher("Name", extract_name, trigram_similarity, set_average)
DATATYPE_MATCHER = Matcher("Datatype", extract_datatype, datatype_similarity, set_average)
PATH_MATCHER = Matcher("Path", extract_path, trigram_similarity, set_average)
INSTANCES_DIRECT_MATCHER = Matcher(
    "InstancesDirect", extract_instances_direct, tfidf_similarity, set_average
)
INSTANCES_ALL_MATCHER = Matcher(
    "InstancesAll", extract_instances_all, tfidf_similarity, set_average
)


# ---------------------------------------------------------------------------
# ComplexMatcher (RES2 level)
# ---------------------------------------------------------------------------


@dataclass
class ComplexMatcher:
    """
    Mid-level matcher: selects context elements (RES2), runs inner matchers
    on those context elements, and combines results.

    Inner components can be Matcher or ComplexMatcher instances.
    """

    name: str
    context_selector: Callable[[SchemaElement, SchemaGraph], list[SchemaElement]]
    inner: list[Matcher | ComplexMatcher]
    sim_combination: Callable[[list[float]], float]
    set_combination: Callable[[list[float]], float]

    def compute(
        self,
        elem1: SchemaElement,
        elem2: SchemaElement,
        graph1: SchemaGraph,
        graph2: SchemaGraph,
    ) -> float:
        # Get context elements for each side
        ctx1 = self.context_selector(elem1, graph1)
        ctx2 = self.context_selector(elem2, graph2)

        if not ctx1 or not ctx2:
            return 0.0

        # For each pair of context elements, compute combined inner matcher score
        pair_scores = []
        for c1 in ctx1:
            for c2 in ctx2:
                inner_scores = []
                for m in self.inner:
                    if isinstance(m, ComplexMatcher):
                        score = m.compute(c1, c2, graph1, graph2)
                    else:
                        score = m.compute(c1, c2)
                    inner_scores.append(score)
                pair_scores.append(self.sim_combination(inner_scores))

        return self.set_combination(pair_scores)


# Predefined complex matchers
NAME_CM = ComplexMatcher("NameCM", ctx_selfnode, [NAME_MATCHER], average, set_average)
PATH_CM = ComplexMatcher("PathCM", ctx_selfpath, [PATH_MATCHER], average, set_average)
LEAVES_CM = ComplexMatcher("LeavesCM", ctx_leaves, [NAME_MATCHER], average, set_average)
PARENTS_CM = ComplexMatcher("ParentsCM", ctx_parents, [LEAVES_CM], average, set_average)
SIBLINGS_CM = ComplexMatcher("SiblingsCM", ctx_siblings, [LEAVES_CM], average, set_average)
INSTANCES_CM = ComplexMatcher(
    "InstancesCM",
    ctx_selfnode,
    [INSTANCES_DIRECT_MATCHER, INSTANCES_ALL_MATCHER],
    maximum,
    set_average,
)

# ---------------------------------------------------------------------------
# Strategy configurations
# ---------------------------------------------------------------------------

# COMA_OPT: schema-only matching
COMA_OPT_MATCHERS = [NAME_CM, PATH_CM, LEAVES_CM, PARENTS_CM]

# COMA_OPT_INST: schema + instance matching
COMA_OPT_INST_MATCHERS = [NAME_CM, PATH_CM, INSTANCES_CM, LEAVES_CM, PARENTS_CM]
