import unittest
from types import SimpleNamespace
from typing import List
import math
import pandas as pd
from anytree import LevelOrderIter

from valentine.algorithms.cupid import (
    cupid_model,
    linguistic_matching as cupid_ling,
    structural_similarity as cupid_struct,
    tree_match as cupid_tree,
    schema_element as cupid_elem,
    schema_element_node as cupid_node,  # noqa: F401
    schema_tree as cupid_tree_mod,
)

from valentine.data_sources.base_column import BaseColumn
from valentine.data_sources.base_table import BaseTable


class DummyColumn(BaseColumn):
    def __init__(self, uid, name, dtype, data):
        self._uid, self._name, self._dtype, self._data = uid, name, dtype, data

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
        self._uid, self._name, self._cols = uid, name, cols

    @property
    def unique_identifier(self): return self._uid
    @property
    def name(self): return self._name
    def get_columns(self) -> List[BaseColumn]: return self._cols
    def get_df(self) -> pd.DataFrame: return pd.DataFrame({c.name: c.data for c in self._cols})
    @property
    def is_empty(self) -> bool: return False


# ---- Patch nltk + wordnet so tests run offline ----
def _mock_word_tokenize(s: str):
    s = s.replace(",", " , ").replace("_", " ")
    return s.split()

def _install_nltk_mocks():
    mock_stopwords = SimpleNamespace(words=lambda lang: ["the", "and"])
    mock_wn = SimpleNamespace(
        all_lemma_names=lambda: {"alpha", "beta"},
        synsets=lambda w: [f"{w}_s1", f"{w}_s2"] if w in {"alpha", "beta"} else [],
        wup_similarity=lambda s1, s2: 0.5,
    )
    cupid_ling.nltk = SimpleNamespace(word_tokenize=_mock_word_tokenize)
    cupid_ling.stopwords = mock_stopwords
    cupid_ling.wn = mock_wn


class TestCupidLinguisticStructural(unittest.TestCase):
    def setUp(self):
        _install_nltk_mocks()

    def test_snakecase_and_normalization(self):
        sc = cupid_ling.snakecase_convert("CamelCaseX")
        self.assertEqual(sc, "camel_case_x")

        se = cupid_ling.normalization("HelloWorld, 123 and")
        datas = [t.data for t in se.tokens]
        types = [t.token_type for t in se.tokens]
        self.assertIn("hello", datas)
        self.assertIn("world", datas)
        self.assertIn(",", datas)
        self.assertIn("123", datas)
        self.assertIn(cupid_elem.TokenTypes.SYMBOLS, types)
        self.assertIn(cupid_elem.TokenTypes.NUMBER, types)
        self.assertIn(cupid_elem.TokenTypes.COMMON_WORDS, types)
        self.assertIn(cupid_elem.TokenTypes.CONTENT, types)

    def test_token_type_and_similarity(self):
        t_num = cupid_elem.Token().add_data("3.14")
        t_txt = cupid_elem.Token().add_data("alpha")
        self.assertEqual(cupid_ling.add_token_type(t_num), cupid_elem.TokenTypes.NUMBER)
        self.assertEqual(cupid_ling.add_token_type(t_txt), cupid_elem.TokenTypes.CONTENT)

        a1 = cupid_elem.Token(); a1.data = "alpha"; a1.token_type = cupid_elem.TokenTypes.CONTENT
        b1 = cupid_elem.Token(); b1.data = "beta";  b1.token_type = cupid_elem.TokenTypes.CONTENT

        sim_ab = cupid_ling.name_similarity_tokens([a1], [b1])
        self.assertGreaterEqual(sim_ab, 0.0)

        sim_same = cupid_ling.get_partial_similarity([a1], [a1])
        self.assertEqual(sim_same, 1.0)

    def test_wordnet_and_leven(self):
        self.assertEqual(cupid_ling.compute_similarity_wordnet("alpha", "beta"), 0.5)
        self.assertTrue(math.isnan(cupid_ling.compute_similarity_wordnet("zzz", "beta")))
        lv = cupid_ling.compute_similarity_leven("alpha", "alp")
        self.assertGreaterEqual(lv, 0.0)
        self.assertLessEqual(lv, 1.0)

    def test_data_type_and_compatibility(self):
        def mk(content: str):
            t = cupid_elem.Token(); t.data = content; t.token_type = cupid_elem.TokenTypes.CONTENT; return t
        sim = cupid_ling.data_type_similarity([mk("alpha")], [mk("beta")])
        self.assertGreaterEqual(sim, 0.0)

        comp = cupid_ling.compute_compatibility({"alpha", "beta"})
        self.assertIn("alpha", comp)
        self.assertIn("beta", comp["alpha"])

    def test_name_similarity_elements_and_compute_lsim(self):
        e1 = cupid_elem.SchemaElement("A")
        e2 = cupid_elem.SchemaElement("B")
        for w in ["hello", "world"]:
            t = cupid_elem.Token(); t.data = w; t.token_type = cupid_elem.TokenTypes.CONTENT; e1.add_token(t)
        for w in ["hello", "beta"]:
            t = cupid_elem.Token(); t.data = w; t.token_type = cupid_elem.TokenTypes.CONTENT; e2.add_token(t)
        e1.add_category("alpha"); e2.add_category("beta")

        nse = cupid_ling.name_similarity_elements(e1, e2)
        self.assertGreaterEqual(nse, 0.0)
        lsim = cupid_ling.compute_lsim(e1, e2)
        self.assertGreaterEqual(lsim, 0.0)
        mx = cupid_ling.get_max_ns_category(["alpha"], ["beta"])
        self.assertGreaterEqual(mx, 0.0)

    def test_schema_tree_and_structural_similarity(self):
        st = cupid_tree_mod.SchemaTree("DB__X")
        root = st.get_node("DB__X")
        st.add_node(table_name="T", table_guid="tg", data_type="Table", parent=root)
        tbl = st.get_node("T")
        st.add_node(table_name="T", table_guid="tg", column_name="C1", column_guid="c1", data_type="int", parent=tbl)
        st.add_node(table_name="T", table_guid="tg", column_name="C2", column_guid="c2", data_type="int", parent=tbl)

        st2 = cupid_tree_mod.SchemaTree("DB__Y")
        root2 = st2.get_node("DB__Y")
        st2.add_node(table_name="U", table_guid="ug", data_type="Table", parent=root2)
        tbl2 = st2.get_node("U")
        st2.add_node(table_name="U", table_guid="ug", column_name="D1", column_guid="d1", data_type="int", parent=tbl2)
        st2.add_node(table_name="U", table_guid="ug", column_name="D2", column_guid="d2", data_type="int", parent=tbl2)

        leaves_s = [n.long_name for n in st.get_leaves()]
        leaves_t = [n.long_name for n in st2.get_leaves()]

        # Provide sims for ALL leaf pairs to avoid KeyError inside compute_ssim
        sims = {
            (s, t): {'wsim': 0.0, 'ssim': 0.0, 'lsim': 0.0}
            for s in leaves_s
            for t in leaves_t
        }
        sims[(leaves_s[0], leaves_t[0])]['wsim'] = 1.0
        sims[(leaves_s[0], leaves_t[0])]['ssim'] = 1.0

        ssim = cupid_struct.compute_ssim(tbl, tbl2, sims, th_accept=0.5)
        self.assertFalse(math.isnan(ssim))
        self.assertGreaterEqual(ssim, 0.0)
        self.assertLessEqual(ssim, 1.0)

        cupid_struct.change_structural_similarity(leaves_s, leaves_t, sims, factor=2.0)
        self.assertEqual(sims[(leaves_s[0], leaves_t[0])]['ssim'], 1.0)

    def test_tree_match_helpers_and_mapping(self):
        st = cupid_tree_mod.SchemaTree("DB__A"); root = st.get_node("DB__A")
        st.add_node(table_name="T", table_guid="tg", data_type="Table", parent=root)
        tbl = st.get_node("T")
        st.add_node(table_name="T", table_guid="tg", column_name="C", column_guid="c", data_type="int", parent=tbl)

        st2 = cupid_tree_mod.SchemaTree("DB__B"); root2 = st2.get_node("DB__B")
        st2.add_node(table_name="U", table_guid="ug", data_type="Table", parent=root2)
        tbl2 = st2.get_node("U")
        st2.add_node(table_name="U", table_guid="ug", column_name="D", column_guid="d", data_type="int", parent=tbl2)

        comp = {"int": {"int": 1.0}}
        l_sims = { (st.get_leaves()[0].long_name, st2.get_leaves()[0].long_name): 0.5 }
        sims = cupid_tree.get_sims(st.get_leaves(), st2.get_leaves(), comp, l_sims, leaf_w_struct=0.2)
        self.assertIn((st.get_leaves()[0].long_name, st2.get_leaves()[0].long_name), sims)

        new = cupid_tree.recompute_wsim(st, st2, sims, w_struct=0.6, th_accept=0.14)
        self.assertTrue(new)

        mapped = cupid_tree.mapping_generation_leaves(st, st2, new, th_accept=0.1)
        self.assertIsInstance(mapped, dict)

        # create_output_dict expects a pair of long-name (4-tuples), not the already-mapped keys
        ln_pair = (st.get_leaves()[0].long_name, st2.get_leaves()[0].long_name)
        out = cupid_tree.create_output_dict(ln_pair, 0.6)
        self.assertIsInstance(out, dict)

        # Ensure sims has ALL non-leaf pairs to avoid KeyError in mapping_generation_non_leaves
        max_level_s = st.height - 1
        max_level_t = st2.height - 1
        non_leaves_s = [n.long_name for n in LevelOrderIter(st.root, maxlevel=max_level_s)]
        non_leaves_t = [n.long_name for n in LevelOrderIter(st2.root, maxlevel=max_level_t)]

        for s_ln in non_leaves_s:
            for t_ln in non_leaves_t:
                new.setdefault((s_ln, t_ln), {'wsim': 0.0, 'ssim': 0.0, 'lsim': 0.0})

        # Explicitly ensure the table-table pair exists, then bump wsim
        entry = new.setdefault((tbl.long_name, tbl2.long_name), {'wsim': 0.0, 'ssim': 0.0, 'lsim': 0.0})
        entry['wsim'] = 1.0

        # The function should run and return a list (may be empty depending on structure/thresholds)
        non_leaves = cupid_tree.mapping_generation_non_leaves(st, st2, new, th_accept=0.0)
        self.assertIsInstance(non_leaves, list)

    def test_cupid_model_top_level(self):
        t_src = DummyTable("SUID", "S", [DummyColumn(1, "A", "int", [1])])
        t_tgt = DummyTable("TUID", "T", [DummyColumn(2, "B", "int", [2])])

        def fake_tree_match(st, tt, cats, *args, **kwargs):
            s_leaf = st.get_leaves()[0].long_name
            t_leaf = tt.get_leaves()[0].long_name
            return {(s_leaf, t_leaf): {'wsim': 1.0, 'ssim': 1.0, 'lsim': 0.0}}

        def fake_recompute_wsim(st, tt, sims, *args, **kwargs):
            return sims

        def fake_mapping(st, tt, sims, th):
            key = next(iter(sims.keys()))
            return {((key[1][0], key[1][2]), (key[0][0], key[0][2])): 1.0}

        # Patch both cupid_model (where Cupid resolves names) and cupid_tree (for consistency)
        orig_tm_m, orig_rc_m, orig_map_m = (
            cupid_model.tree_match,
            cupid_model.recompute_wsim,
            cupid_model.mapping_generation_leaves,
        )
        orig_tm, orig_rc, orig_map = (
            cupid_tree.tree_match,
            cupid_tree.recompute_wsim,
            cupid_tree.mapping_generation_leaves,
        )
        try:
            cupid_model.tree_match = fake_tree_match
            cupid_model.recompute_wsim = fake_recompute_wsim
            cupid_model.mapping_generation_leaves = fake_mapping

            cupid_tree.tree_match = fake_tree_match
            cupid_tree.recompute_wsim = fake_recompute_wsim
            cupid_tree.mapping_generation_leaves = fake_mapping

            matcher = cupid_model.Cupid()
            res = matcher.get_matches(t_src, t_tgt)
            self.assertIsInstance(res, dict)
            self.assertTrue(res)
        finally:
            cupid_model.tree_match, cupid_model.recompute_wsim, cupid_model.mapping_generation_leaves = (
                orig_tm_m, orig_rc_m, orig_map_m
            )
            cupid_tree.tree_match, cupid_tree.recompute_wsim, cupid_tree.mapping_generation_leaves = (
                orig_tm, orig_rc, orig_map
            )
