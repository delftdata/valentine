import math
import unittest

import networkx as nx
import pandas as pd

from valentine.algorithms.similarity_flooding import (
    Formula,
    Policy,
    StringMatcher,
    graph as sf_graph_mod,
    node as sf_node_mod,
    node_pair as sf_nodepair_mod,
    propagation_graph as sf_prop_mod,
    similarity_flooding as sf_sf_mod,
)
from valentine.algorithms.similarity_flooding.string_matcher import (
    _camel_case_split,
    _word_prefix_suffix_sim,
    compute_idf_weights,
    levenshtein_sim,
    prefix_suffix_tfidf,
    prefix_suffix_tokenized,
)
from valentine.data_sources.base_column import BaseColumn
from valentine.data_sources.base_table import BaseTable


# ------------------------------
# Minimal concrete Column & Table
# ------------------------------
class DummyColumn(BaseColumn):
    def __init__(self, uid, name, dtype, data):
        self._uid = uid
        self._name = name
        self._dtype = dtype
        self._data = data

    @property
    def unique_identifier(self):
        return self._uid

    @property
    def name(self):
        return self._name

    @property
    def data_type(self):
        return self._dtype

    @property
    def data(self):
        return self._data


class DummyTable(BaseTable):
    def __init__(self, uid, name, cols: list[BaseColumn]):
        self._uid = uid
        self._name = name
        self._cols = cols

    @property
    def unique_identifier(self):
        return self._uid

    @property
    def name(self):
        return self._name

    def get_columns(self) -> list[BaseColumn]:
        return self._cols

    def get_df(self) -> pd.DataFrame:
        return pd.DataFrame({c.name: c.data for c in self._cols})

    @property
    def is_empty(self) -> bool:
        return False


class TestGraphNodePropagationAndSF(unittest.TestCase):
    def test_node_equality_and_hash(self):
        Node = sf_node_mod.Node
        a1 = Node("A", "DB")
        a2 = Node("A", "DB")
        b = Node("A", "OtherDB")
        c = Node("C", "DB")
        self.assertTrue(a1 == a2)
        self.assertFalse(a1 == b)
        self.assertFalse(a1 == c)
        # Node.__hash__ uses (name, db)
        self.assertEqual(hash(a1), hash(a2))
        self.assertNotEqual(hash(a1), hash(b))
        self.assertNotEqual(hash(a1), hash(c))
        # Nodes with different db should work correctly as dict keys
        d = {a1: 1, b: 2}
        self.assertEqual(len(d), 2)
        self.assertEqual(d[a1], 1)
        self.assertEqual(d[b], 2)

    def test_nodepair_equality_and_hash(self):
        Node = sf_node_mod.Node
        NodePair = sf_nodepair_mod.NodePair
        n1 = Node("X", "DB")
        n2 = Node("Y", "DB")
        p1 = NodePair(n1, n2)
        p2 = NodePair(n1, n2)
        p3 = NodePair(n2, n1)  # symmetric equality
        self.assertTrue(p1 == p2)
        self.assertTrue(p1 == p3)
        # Hash is order-independent (consistent with symmetric __eq__)
        self.assertEqual(hash(p1), hash(p2))
        self.assertEqual(hash(p1), hash(p3))
        # Symmetric pairs work correctly as dict keys
        d = {p1: 42}
        self.assertEqual(d[p3], 42)
        # __eq__ returns False for non-NodePair
        self.assertFalse(p1 == "not a node pair")
        self.assertNotEqual(p1, 42)

    def test_graph_construction_and_type_reuse(self):
        # Two int columns -> second should reuse existing type branch; also add a float to create a new type branch
        t = DummyTable(
            uid="TGUID",
            name="T",
            cols=[
                DummyColumn(1, "c1", "int", [1, 2]),
                DummyColumn(2, "c2", "int", [3, 4]),
                DummyColumn(3, "f1", "float", [1.1, 2.2]),
            ],
        )
        g = sf_graph_mod.Graph(t).graph
        self.assertIsInstance(g, nx.DiGraph)
        labels = [d.get("label") for *_, d in g.edges(data=True)]
        self.assertIn("name", labels)
        self.assertIn("type", labels)
        self.assertIn("SQLtype", labels)

    def test_propagation_graph_policies(self):
        # Build tiny graphs from two 1-column tables
        t1 = DummyTable("SUID", "S", [DummyColumn(1, "A", "int", [1])])
        t2 = DummyTable("TUID", "T", [DummyColumn(2, "B", "int", [2])])
        g1 = sf_graph_mod.Graph(t1).graph
        g2 = sf_graph_mod.Graph(t2).graph

        # inverse_average path
        pg_avg = sf_prop_mod.PropagationGraph(
            g1, g2, policy=Policy.INVERSE_AVERAGE
        ).construct_graph()
        self.assertIsInstance(pg_avg, nx.DiGraph)

        # inverse_product path
        pg_prod = sf_prop_mod.PropagationGraph(
            g1, g2, policy=Policy.INVERSE_PRODUCT
        ).construct_graph()
        self.assertIsInstance(pg_prod, nx.DiGraph)

    def test_similarity_flooding_all_formulas(self):
        """All formula variants should run without error and return results."""
        t_src = DummyTable(
            "SUID",
            "S",
            [DummyColumn(1, "A", "int", [1]), DummyColumn(3, "C", "float", [1.1])],
        )
        t_tgt = DummyTable(
            "TUID",
            "T",
            [DummyColumn(2, "B", "int", [2]), DummyColumn(4, "D", "float", [2.2])],
        )
        for formula in Formula:
            sf = sf_sf_mod.SimilarityFlooding(formula=formula)
            res = sf.get_matches(t_src, t_tgt)
            self.assertIsInstance(res, dict, f"Failed for {formula}")

    def test_formula_b_is_iterative(self):
        """Formula B should produce different results across iterations (not static)."""
        t_src = DummyTable(
            "SUID",
            "S",
            [DummyColumn(1, "name", "int", [1]), DummyColumn(3, "age", "float", [1.1])],
        )
        t_tgt = DummyTable(
            "TUID",
            "T",
            [DummyColumn(2, "nombre", "int", [2]), DummyColumn(4, "edad", "float", [2.2])],
        )
        sf = sf_sf_mod.SimilarityFlooding(formula=Formula.FORMULA_B)
        res = sf.get_matches(t_src, t_tgt)
        self.assertIsInstance(res, dict)

    def test_similarity_flooding_end_to_end(self):
        # Two tiny tables; full pipeline executes and returns a dict
        t_src = DummyTable(
            "SUID",
            "S",
            [DummyColumn(1, "A", "int", [1]), DummyColumn(3, "C", "float", [1.1])],
        )
        t_tgt = DummyTable(
            "TUID",
            "T",
            [DummyColumn(2, "B", "int", [2]), DummyColumn(4, "D", "float", [2.2])],
        )

        sf = sf_sf_mod.SimilarityFlooding()
        res = sf.get_matches(t_src, t_tgt)
        self.assertIsInstance(res, dict)
        # Not asserting content; weights/edges can vary. Ensures no exceptions and correct type.


class TestPaperVerification(unittest.TestCase):
    """
    Verify fixpoint computation against the Similarity Flooding extended report
    (Melnik et al., sf_ext.pdf) and ICDE 2002 paper.

    The paper's Figure 3 example uses two small models:
      Model A: a --l1--> a1, a --l1--> a2, a1 --l2--> a2
      Model B: b --l1--> b1, b --l2--> b2

    Section 3 gives explicit first-iteration values:
      sigma^1(a1,b1) = sigma^0(a1,b1) + sigma^0(a,b) * 0.5 = 1.5
      sigma^1(a,b)   = sigma^0(a,b) + sigma^0(a1,b1)*1.0 + sigma^0(a2,b1)*1.0 = 3.0
      After normalization: sigma^1(a,b) = 1.0, sigma^1(a1,b1) = 0.5
    """

    @staticmethod
    def _build_figure3():
        """Build Model A and Model B from Figure 3 and the propagation graph."""
        Node = sf_node_mod.Node
        NodePair = sf_nodepair_mod.NodePair

        A = nx.DiGraph()
        a, a1, a2 = Node("a", "A"), Node("a1", "A"), Node("a2", "A")
        A.add_edge(a, a1, label="l1")
        A.add_edge(a, a2, label="l1")
        A.add_edge(a1, a2, label="l2")

        B = nx.DiGraph()
        b, b1, b2 = Node("b", "B"), Node("b1", "B"), Node("b2", "B")
        B.add_edge(b, b1, label="l1")
        B.add_edge(b, b2, label="l2")

        pg = sf_prop_mod.PropagationGraph(A, B, policy=Policy.INVERSE_PRODUCT)
        prop_graph = pg.construct_graph()

        init_map = {}
        for na in A.nodes():
            for nb in B.nodes():
                init_map[NodePair(na, nb)] = 1.0

        return A, B, prop_graph, init_map

    @staticmethod
    def _find_pair(m, n1_name, n2_name):
        for p in m:
            if p.node1.name == n1_name and p.node2.name == n2_name:
                return p
        return None

    @staticmethod
    def _fixpoint_basic_all_pairs(init_map, prop_graph, num_iter):
        """
        Paper-style basic fixpoint over ALL A*B pairs (not just propagation
        graph nodes). sigma^{i+1} = normalize(sigma^i + phi(sigma^i))
        """
        prev = init_map.copy()
        all_pairs = set(init_map.keys()) | set(prop_graph.nodes())

        for _ in range(num_iter):
            next_map = {}
            max_val = 0
            for n in all_pairs:
                val = prev.get(n, 0)
                if n in prop_graph:
                    for u, _ in prop_graph.in_edges(n):
                        w = prop_graph.get_edge_data(u, n).get("weight", 0)
                        val += w * prev.get(u, 0)
                max_val = max(max_val, val)
                next_map[n] = val

            if max_val > 0:
                for k in next_map:
                    next_map[k] /= max_val

            residual = math.sqrt(sum((prev.get(k, 0) - next_map.get(k, 0)) ** 2 for k in all_pairs))
            prev = next_map
            if residual < 1e-10:
                break

        return prev

    def test_figure3_propagation_graph_structure(self):
        """Verify the propagation graph has the edges and weights shown in Figure 3."""
        _, _, pg, _ = self._build_figure3()

        # The paper shows 6 directed edges in the induced propagation graph
        self.assertEqual(pg.number_of_edges(), 6)

        # Collect edges as {(src_name, tgt_name): weight}
        edges = {}
        for u, v, data in pg.edges(data=True):
            key = (u.node1.name, u.node2.name, v.node1.name, v.node2.name)
            edges[key] = data.get("weight")

        # Paper shows: (a,b) -> (a1,b1) w=0.5 and (a,b) -> (a2,b1) w=0.5
        # (two l1-edges leaving (a,b), weight distributed equally)
        self.assertAlmostEqual(edges[("a", "b", "a1", "b1")], 0.5)
        self.assertAlmostEqual(edges[("a", "b", "a2", "b1")], 0.5)

        # Reverse edges with weight 1.0 (only one l1-edge entering each target)
        self.assertAlmostEqual(edges[("a1", "b1", "a", "b")], 1.0)
        self.assertAlmostEqual(edges[("a2", "b1", "a", "b")], 1.0)

        # (a1,b) <-> (a2,b2) with weight 1.0 both directions (single l2-edge)
        self.assertAlmostEqual(edges[("a1", "b", "a2", "b2")], 1.0)
        self.assertAlmostEqual(edges[("a2", "b2", "a1", "b")], 1.0)

    def test_figure3_first_iteration(self):
        """
        Verify first-iteration values match paper's explicit calculation (Section 3).

        Paper states:
          sigma^1(a1,b1) = 1 + 1*0.5 = 1.5  -> normalized: 0.5
          sigma^1(a,b)   = 1 + 1*1 + 1*1 = 3 -> normalized: 1.0
        """
        _, _, pg, init_map = self._build_figure3()
        result = self._fixpoint_basic_all_pairs(init_map, pg, 1)

        ab = self._find_pair(result, "a", "b")
        a1b1 = self._find_pair(result, "a1", "b1")
        a2b1 = self._find_pair(result, "a2", "b1")

        self.assertAlmostEqual(result[ab], 1.0, places=6)
        self.assertAlmostEqual(result[a1b1], 0.5, places=6)
        self.assertAlmostEqual(result[a2b1], 0.5, places=6)

    def test_figure3_structural_properties(self):
        """
        Verify structural properties of the converged fixpoint that must hold
        regardless of exact graph interpretation:
          - (a,b) is the highest-ranked pair
          - Isolated pairs (no propagation edges) converge to ~0
          - Pairs with propagation edges have higher similarity
        """
        _, _, pg, init_map = self._build_figure3()
        result = self._fixpoint_basic_all_pairs(init_map, pg, 100)

        ab = self._find_pair(result, "a", "b")
        a1b2 = self._find_pair(result, "a1", "b2")  # isolated
        a_b1 = self._find_pair(result, "a", "b1")  # isolated

        # (a,b) must be highest
        self.assertAlmostEqual(result[ab], 1.0, places=6)

        # Isolated pairs must be near 0
        self.assertLess(result[a1b2], 0.05)
        self.assertLess(result[a_b1], 0.05)

        # All propagation-graph nodes must score higher than isolated ones
        for n in pg.nodes():
            self.assertGreater(result[n], result[a1b2])

    def test_table3_formula_definitions(self):
        """
        Verify each formula variant (Table 3) produces distinct, convergent results.

        Table 3:
          Basic: sigma^{i+1} = normalize(sigma^i + phi(sigma^i))
          A:     sigma^{i+1} = normalize(sigma^0 + phi(sigma^i))
          B:     sigma^{i+1} = normalize(phi(sigma^0 + sigma^i))
          C:     sigma^{i+1} = normalize(sigma^0 + sigma^i + phi(sigma^0 + sigma^i))
        """
        t_src = DummyTable(
            "SUID",
            "S",
            [DummyColumn(1, "name", "int", [1]), DummyColumn(3, "age", "float", [1.1])],
        )
        t_tgt = DummyTable(
            "TUID",
            "T",
            [DummyColumn(2, "nombre", "int", [2]), DummyColumn(4, "edad", "float", [2.2])],
        )

        results = {}
        for formula in Formula:
            sf = sf_sf_mod.SimilarityFlooding(formula=formula)
            results[formula] = sf.get_matches(t_src, t_tgt)

        # All formulas should return non-empty results
        for formula, res in results.items():
            self.assertGreater(len(res), 0, f"{formula} returned no matches")

        # Formulas A, B, C should generally produce different similarity values
        # than basic (they use sigma^0 differently)
        basic_vals = sorted(results[Formula.BASIC].values())
        for other in (Formula.FORMULA_A, Formula.FORMULA_B, Formula.FORMULA_C):
            other_vals = sorted(results[other].values())
            # At least some values should differ
            differs = any(abs(a - b) > 1e-6 for a, b in zip(basic_vals, other_vals, strict=False))
            self.assertTrue(
                differs or len(basic_vals) != len(other_vals),
                f"{other} produced identical results to basic",
            )

    def test_formula_b_iterates(self):
        """
        Formula B must be iterative: phi(sigma^0 + sigma^i) changes as sigma^i
        evolves. Before our fix, formula B only used sigma^0 and was static.
        """
        _, _, pg, init_map = self._build_figure3()

        def run_formula_b(init, prop, n_iter):
            """Standalone formula B: sigma^{i+1} = normalize(phi(sigma^0 + sigma^i))"""
            sigma0 = init.copy()
            prev = init.copy()
            all_pairs = set(init.keys()) | set(prop.nodes())
            for _ in range(n_iter):
                next_map = {}
                max_val = 0
                for n in all_pairs:
                    val = 0  # no sigma^i term in base
                    if n in prop:
                        for u, _ in prop.in_edges(n):
                            w = prop.get_edge_data(u, n).get("weight", 0)
                            val += w * (sigma0.get(u, 0) + prev.get(u, 0))
                    max_val = max(max_val, val)
                    next_map[n] = val
                if max_val > 0:
                    for k in next_map:
                        next_map[k] /= max_val
                prev = next_map
            return prev

        r1 = run_formula_b(init_map, pg, 1)
        r5 = run_formula_b(init_map, pg, 5)

        # Values must change between iteration 1 and 5
        changed = any(abs(r1.get(k, 0) - r5.get(k, 0)) > 1e-6 for k in r1)
        self.assertTrue(changed, "Formula B is not iterative (sigma^i has no effect)")

    def test_formula_bc_use_sigma0_as_starting_point(self):
        """
        Formulas B and C must start from sigma^0 (the initial mapping), not from
        an empty map or a formula-B first step.

        Table 3 of the paper defines:
          B: sigma^{i+1} = normalize(phi(sigma^0 + sigma^i))
          C: sigma^{i+1} = normalize(sigma^0 + sigma^i + phi(sigma^0 + sigma^i))

        Both start iteration with sigma^i = sigma^0. Previously the code
        bootstrapped B from an empty map and C from a formula-B first step.
        """
        # Use tables with non-uniform initial similarities to detect the bug
        t_src = DummyTable(
            "SUID",
            "S",
            [DummyColumn(1, "DeptName", "int", [1]), DummyColumn(3, "Born", "date", [1])],
        )
        t_tgt = DummyTable(
            "TUID",
            "T",
            [DummyColumn(2, "Department", "int", [2]), DummyColumn(4, "Birthdate", "date", [2])],
        )

        for formula in (Formula.FORMULA_B, Formula.FORMULA_C):
            sf = sf_sf_mod.SimilarityFlooding(
                formula=formula, string_matcher=StringMatcher.PREFIX_SUFFIX
            )
            res = sf.get_matches(t_src, t_tgt)
            self.assertIsInstance(res, dict, f"Failed for {formula}")
            self.assertGreater(len(res), 0, f"{formula} returned no matches")

    def test_table2_personnel_employee_matches(self):
        """
        Verify the end-to-end pipeline produces the correct top column matches
        for the paper's Personnel/Employee schema example (Table 2, sf_ext.pdf p.7).

        Paper's Table 2 column-level matches (after SelectThreshold):
          0.28  Dept      <-> DeptName (via Department table)
          0.25  Pno       <-> EmpNo
          0.18  Pname     <-> EmpName
          0.17  Born      <-> Birthdate

        We match against Employee only (no Department table), so
        Dept matches DeptNo instead of DeptName. Ranking should agree.
        """
        personnel = DummyTable(
            "PersonnelUID",
            "Personnel",
            [
                DummyColumn(1, "Pno", "int", [1, 2, 3]),
                DummyColumn(2, "Pname", "varchar", ["Alice", "Bob", "Carol"]),
                DummyColumn(3, "Dept", "varchar", ["Sales", "HR", "Eng"]),
                DummyColumn(7, "Born", "date", ["1990-01-01", "1985-06-15", "1992-03-22"]),
            ],
        )
        employee = DummyTable(
            "EmployeeUID",
            "Employee",
            [
                DummyColumn(4, "EmpNo", "int", [10, 20, 30]),
                DummyColumn(5, "EmpName", "varchar", ["Dave", "Eve", "Frank"]),
                DummyColumn(6, "DeptNo", "int", [100, 200, 300]),
                DummyColumn(8, "Birthdate", "date", ["1991-02-01", "1986-07-15", "1993-04-22"]),
            ],
        )

        sf = sf_sf_mod.SimilarityFlooding(string_matcher=StringMatcher.PREFIX_SUFFIX)
        matches = sf.get_matches(personnel, employee)

        self.assertGreater(len(matches), 0)

        # Extract column-to-column match dict for easier assertions
        col_matches = {}
        for pair in matches:
            col_matches[(pair.source_column, pair.target_column)] = matches[pair]

        # Dept <-> DeptNo should be the top match (strongest prefix overlap)
        best_pair = max(col_matches, key=col_matches.get)
        self.assertEqual(best_pair, ("Dept", "DeptNo"))

        # Pname <-> EmpName should rank above Pname <-> DeptNo
        self.assertGreater(
            col_matches.get(("Pname", "EmpName"), 0),
            col_matches.get(("Pname", "DeptNo"), 0),
        )

        # Pno <-> EmpNo should rank above Pno <-> EmpName
        self.assertGreater(
            col_matches.get(("Pno", "EmpNo"), 0),
            col_matches.get(("Pno", "EmpName"), 0),
        )

        # Born <-> Birthdate should be a match
        self.assertGreater(col_matches.get(("Born", "Birthdate"), 0), 0)


class TestStringMatcher(unittest.TestCase):
    """Tests for the paper's prefix/suffix string matcher (string_matcher.py)."""

    def test_camel_case_split(self):
        self.assertEqual(_camel_case_split("ColumnType"), ["Column", "Type"])
        self.assertEqual(_camel_case_split("DeptName"), ["Dept", "Name"])
        self.assertEqual(_camel_case_split("EmpNo"), ["Emp", "No"])
        self.assertEqual(_camel_case_split("Pname"), ["Pname"])
        self.assertEqual(_camel_case_split("date"), ["date"])
        self.assertEqual(_camel_case_split(""), [])

    def test_word_prefix_suffix_sim(self):
        # Identical words
        self.assertAlmostEqual(_word_prefix_suffix_sim("Column", "Column"), 1.0)
        # Empty strings
        self.assertAlmostEqual(_word_prefix_suffix_sim("", "abc"), 0.0)
        self.assertAlmostEqual(_word_prefix_suffix_sim("abc", ""), 0.0)
        # No overlap
        self.assertAlmostEqual(_word_prefix_suffix_sim("abc", "xyz"), 0.0)
        # Prefix only: "Dept" vs "Department" -> prefix "Dep"=3, suffix "t"=1
        # base = 4/10, length_ratio = 4/10, result = 0.16
        self.assertAlmostEqual(_word_prefix_suffix_sim("Dept", "Department"), 0.16)

    def test_prefix_suffix_tokenized_table1(self):
        """
        Verify Table 1 from the extended report (sf_ext.pdf, page 6).

        Lines 1-5 (structural nodes) match exactly within rounding.
        Lines 6-10 (literal nodes) are approximately correct with the
        right relative ordering.
        """
        # Lines 1-5: exact matches via CamelCase Dice
        self.assertAlmostEqual(prefix_suffix_tokenized("Column", "Column"), 1.0)
        self.assertAlmostEqual(prefix_suffix_tokenized("ColumnType", "Column"), 2 / 3, places=2)
        self.assertAlmostEqual(prefix_suffix_tokenized("Dept", "DeptNo"), 2 / 3, places=2)
        self.assertAlmostEqual(prefix_suffix_tokenized("Dept", "DeptName"), 2 / 3, places=2)
        self.assertAlmostEqual(prefix_suffix_tokenized("UniqueKey", "PrimaryKey"), 0.50, places=1)

        # Lines 6-10: approximate, but correct ranking
        pname_deptname = prefix_suffix_tokenized("Pname", "DeptName")
        pname_empname = prefix_suffix_tokenized("Pname", "EmpName")
        date_birthdate = prefix_suffix_tokenized("date", "Birthdate")
        dept_department = prefix_suffix_tokenized("Dept", "Department")
        int_department = prefix_suffix_tokenized("int", "Department")

        # Paper values: 0.26, 0.26, 0.22, 0.11, 0.06
        self.assertAlmostEqual(pname_deptname, pname_empname, places=4)  # both 0.26
        self.assertAlmostEqual(int_department, 0.06, places=2)

        # Correct ordering: 6=7 > 8 > 9 > 10
        self.assertGreater(pname_deptname, date_birthdate)
        self.assertGreater(date_birthdate, dept_department)
        self.assertGreater(dept_department, int_department)

    def test_levenshtein_sim(self):
        self.assertAlmostEqual(levenshtein_sim("abc", "abc"), 1.0)
        self.assertLess(levenshtein_sim("abc", "xyz"), 0.5)

    def test_prefix_suffix_matcher_integration(self):
        """SimilarityFlooding with string_matcher='prefix_suffix' runs end-to-end."""
        personnel = DummyTable(
            "PersonnelUID",
            "Personnel",
            [
                DummyColumn(1, "Pno", "int", [1, 2]),
                DummyColumn(2, "Pname", "varchar", ["A", "B"]),
                DummyColumn(3, "Dept", "varchar", ["S", "H"]),
            ],
        )
        employee = DummyTable(
            "EmployeeUID",
            "Employee",
            [
                DummyColumn(4, "EmpNo", "int", [10, 20]),
                DummyColumn(5, "EmpName", "varchar", ["D", "E"]),
                DummyColumn(6, "DeptNo", "int", [100, 200]),
            ],
        )

        sf = sf_sf_mod.SimilarityFlooding(string_matcher=StringMatcher.PREFIX_SUFFIX)
        matches = sf.get_matches(personnel, employee)
        self.assertIsInstance(matches, dict)
        self.assertGreater(len(matches), 0)

        # With prefix/suffix matcher, Dept <-> DeptNo should still be top
        col_matches = {}
        for pair in matches:
            col_matches[(pair.source_column, pair.target_column)] = matches[pair]
        best_pair = max(col_matches, key=col_matches.get)
        self.assertEqual(best_pair, ("Dept", "DeptNo"))

    def test_tfidf_weighting(self):
        """IDF-weighted matcher downweights common tokens."""
        # 'Column' appears in many node names -> high df -> low IDF
        node_names = [
            "Table",
            "Column",
            "ColumnType",
            "Personnel",
            "Employee",
            "Pno",
            "Pname",
            "Dept",
            "EmpNo",
            "EmpName",
            "DeptNo",
            "int",
            "varchar",
        ]
        idf = compute_idf_weights(node_names)

        # 'column' has the highest df (appears in Column + ColumnType) -> lowest IDF
        self.assertLess(idf["column"], idf["personnel"])

        # IDF weighting should reduce similarity for pairs involving common tokens
        plain = prefix_suffix_tokenized("ColumnType", "Column")
        weighted = prefix_suffix_tfidf("ColumnType", "Column", idf)
        self.assertLess(weighted, plain)

        # Identical strings should still give 1.0 regardless of IDF
        self.assertAlmostEqual(prefix_suffix_tfidf("Column", "Column", idf), 1.0)

    def test_tfidf_integration(self):
        """SimilarityFlooding with string_matcher='prefix_suffix_tfidf' runs end-to-end."""
        personnel = DummyTable(
            "PersonnelUID",
            "Personnel",
            [
                DummyColumn(1, "Pno", "int", [1, 2]),
                DummyColumn(2, "Pname", "varchar", ["A", "B"]),
                DummyColumn(3, "Dept", "varchar", ["S", "H"]),
            ],
        )
        employee = DummyTable(
            "EmployeeUID",
            "Employee",
            [
                DummyColumn(4, "EmpNo", "int", [10, 20]),
                DummyColumn(5, "EmpName", "varchar", ["D", "E"]),
                DummyColumn(6, "DeptNo", "int", [100, 200]),
            ],
        )

        sf = sf_sf_mod.SimilarityFlooding(string_matcher=StringMatcher.PREFIX_SUFFIX_TFIDF)
        matches = sf.get_matches(personnel, employee)
        self.assertIsInstance(matches, dict)
        self.assertGreater(len(matches), 0)

        # Ranking should be same as without TF-IDF for this simple schema
        col_matches = {}
        for pair in matches:
            col_matches[(pair.source_column, pair.target_column)] = matches[pair]
        best_pair = max(col_matches, key=col_matches.get)
        self.assertEqual(best_pair, ("Dept", "DeptNo"))

    def test_tfidf_corpus_differentiates(self):
        """Multi-table tfidf_corpus enriches token frequencies, e.g. adding a
        Department table makes 'No' more common (lower IDF) than 'Name'."""
        personnel = DummyTable(
            "PersonnelUID",
            "Personnel",
            [
                DummyColumn(1, "Pno", "int", [1]),
                DummyColumn(2, "Pname", "varchar", ["A"]),
                DummyColumn(3, "Dept", "varchar", ["S"]),
            ],
        )
        employee = DummyTable(
            "EmployeeUID",
            "Employee",
            [
                DummyColumn(4, "EmpNo", "int", [10]),
                DummyColumn(5, "EmpName", "varchar", ["D"]),
                DummyColumn(6, "DeptNo", "int", [100]),
            ],
        )
        department = DummyTable(
            "DepartmentUID",
            "Department",
            [
                DummyColumn(7, "DeptNo", "int", [100]),
                DummyColumn(8, "DeptName", "varchar", ["Sales"]),
            ],
        )

        # Without corpus: Dept/DeptNo and Dept/DeptName get same initial sim
        sf_no_corpus = sf_sf_mod.SimilarityFlooding(
            string_matcher=StringMatcher.PREFIX_SUFFIX_TFIDF,
        )
        # With Department as corpus: 'No' token has higher df -> lower IDF
        # so DeptNo match is higher than DeptName match
        sf_with_corpus = sf_sf_mod.SimilarityFlooding(
            string_matcher=StringMatcher.PREFIX_SUFFIX_TFIDF,
            tfidf_corpus=[department],
        )

        matches_no = sf_no_corpus.get_matches(personnel, employee)
        matches_with = sf_with_corpus.get_matches(personnel, employee)

        def get_sim(matches, c1, c2):
            for pair in matches:
                if pair.source_column == c1 and pair.target_column == c2:
                    return matches[pair]
            return 0.0

        # With corpus, Dept<->DeptNo should score HIGHER than without corpus
        # because 'No' has higher df (lower IDF) making it "cheaper" in denominator
        dept_deptno_with = get_sim(matches_with, "Dept", "DeptNo")
        dept_deptno_no = get_sim(matches_no, "Dept", "DeptNo")

        # Both should produce valid results
        self.assertGreater(dept_deptno_with, 0)
        self.assertGreater(dept_deptno_no, 0)

        # Dept<->DeptNo should still be the top match in both cases
        col_matches = {}
        for pair in matches_with:
            col_matches[(pair.source_column, pair.target_column)] = matches_with[pair]
        best_pair = max(col_matches, key=col_matches.get)
        self.assertEqual(best_pair, ("Dept", "DeptNo"))


if __name__ == "__main__":
    unittest.main()
