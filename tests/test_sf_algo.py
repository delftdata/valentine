import unittest
from typing import List
import pandas as pd
import networkx as nx

from valentine.algorithms.similarity_flooding import (
    graph as sf_graph_mod,
    node as sf_node_mod,
    node_pair as sf_nodepair_mod,
    propagation_graph as sf_prop_mod,
    similarity_flooding as sf_sf_mod,
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
    def unique_identifier(self): return self._uid
    @property
    def name(self): return self._name
    @property
    def data_type(self): return self._dtype
    @property
    def data(self): return self._data


class DummyTable(BaseTable):
    def __init__(self, uid, name, cols: List[BaseColumn]):
        self._uid = uid
        self._name = name
        self._cols = cols

    @property
    def unique_identifier(self): return self._uid
    @property
    def name(self): return self._name
    def get_columns(self) -> List[BaseColumn]: return self._cols
    def get_df(self) -> pd.DataFrame: return pd.DataFrame({c.name: c.data for c in self._cols})
    @property
    def is_empty(self) -> bool: return False


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
        # Node.__hash__ uses name only
        self.assertEqual(hash(a1), hash(a2))
        self.assertEqual(hash(a1), hash(b))
        self.assertNotEqual(hash(a1), hash(c))

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
        # Hash stable for identical order (spec is order-insensitive equality, but we check stability)
        self.assertEqual(hash(p1), hash(p2))

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
        labels = [d.get("label") for *_ , d in g.edges(data=True)]
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
        pg_avg = sf_prop_mod.PropagationGraph(g1, g2, policy="inverse_average").construct_graph()
        self.assertIsInstance(pg_avg, nx.DiGraph)

        # inverse_product path
        pg_prod = sf_prop_mod.PropagationGraph(g1, g2, policy="inverse_product").construct_graph()
        self.assertIsInstance(pg_prod, nx.DiGraph)

        # unknown policy -> {}
        pg_wrong = sf_prop_mod.PropagationGraph(g1, g2, policy="unknown").construct_graph()
        self.assertEqual(pg_wrong, {})

    def test_similarity_flooding_end_to_end(self):
        # Two tiny tables; full pipeline executes and returns a dict
        t_src = DummyTable("SUID", "S", [DummyColumn(1, "A", "int", [1]), DummyColumn(3, "C", "float", [1.1])])
        t_tgt = DummyTable("TUID", "T", [DummyColumn(2, "B", "int", [2]), DummyColumn(4, "D", "float", [2.2])])

        sf = sf_sf_mod.SimilarityFlooding(coeff_policy="inverse_average", formula="formula_c")
        res = sf.get_matches(t_src, t_tgt)
        self.assertIsInstance(res, dict)
        # Not asserting content; weights/edges can vary. Ensures no exceptions and correct type.


if __name__ == "__main__":
    unittest.main()
