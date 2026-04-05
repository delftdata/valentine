from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .combination import average, maximum, set_average
from .schema import SchemaElement, SchemaGraph
from .similarity.datatype import datatype_similarity
from .similarity.tfidf import TfidfCorpus, tfidf_similarity
from .similarity.trigram import trigram_similarity

# ---------------------------------------------------------------------------
# RES3: Information extraction functions
# ---------------------------------------------------------------------------


def extract_name(elem: SchemaElement) -> str:
    return elem.name


def extract_datatype(elem: SchemaElement) -> str:
    return elem.data_type


def extract_path(elem: SchemaElement) -> str:
    # Java's RES3_PATH calls path.toNameString().replace(".", " ")
    # converting "root.column" into "root column" before trigram matching
    return elem.accession.replace(".", " ")


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


def ctx_selfpath(elem: SchemaElement, _graph: SchemaGraph) -> list[SchemaElement]:
    # The element's accession already IS the full path string (e.g. "table.column"),
    # so returning [elem] is equivalent to returning [path] for RES3_PATH extraction.
    return [elem]


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
    set_combination: Callable[[list[list[float]]], float]

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

        # Build similarity matrix [len(ctx1) x len(ctx2)]
        sim_matrix: list[list[float]] = []
        for c1 in ctx1:
            row = []
            for c2 in ctx2:
                inner_scores = []
                for m in self.inner:
                    if isinstance(m, ComplexMatcher):
                        score = m.compute(c1, c2, graph1, graph2)
                    else:
                        score = m.compute(c1, c2)
                    inner_scores.append(score)
                row.append(self.sim_combination(inner_scores))
            sim_matrix.append(row)

        return self.set_combination(sim_matrix)


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

# COMA_OPT_INST: schema + instance matching (local per-pair IDF fallback)
COMA_OPT_INST_MATCHERS = [NAME_CM, PATH_CM, INSTANCES_CM, LEAVES_CM, PARENTS_CM]


def make_instance_matchers(corpus: TfidfCorpus) -> ComplexMatcher:
    """Create an InstancesCM using a pre-built global TF-IDF corpus."""
    inst_direct = Matcher(
        "InstancesDirect", extract_instances_direct, corpus.similarity, set_average
    )
    inst_all = Matcher("InstancesAll", extract_instances_all, corpus.similarity, set_average)
    return ComplexMatcher(
        "InstancesCM", ctx_selfnode, [inst_direct, inst_all], maximum, set_average
    )


def build_matchers(
    corpus: TfidfCorpus | None = None,
    *,
    use_schema: bool = True,
    use_instances: bool = False,
) -> list[ComplexMatcher]:
    """Build the list of complex matchers based on the requested configuration."""
    matchers: list[ComplexMatcher] = []
    if use_schema:
        matchers.extend([NAME_CM, PATH_CM])
    if use_instances and corpus is not None:
        matchers.append(make_instance_matchers(corpus))
    if use_schema:
        matchers.extend([LEAVES_CM, PARENTS_CM])
    return matchers
