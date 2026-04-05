import math
from itertools import product

from anytree import LevelOrderIter, PostOrderIter

from ..match import Match
from .linguistic_matching import comparison, compute_compatibility, compute_lsim
from .structural_similarity import change_structural_similarity, compute_ssim


def compute_weighted_similarity(s_sim, l_sim, w_struct=0.5):
    return w_struct * s_sim + (1 - w_struct) * l_sim


def get_sims(s_leaves, t_leaves, compatibility_table, l_sims, leaf_w_struct):
    sims = {}
    for s, t in product(s_leaves, t_leaves):
        if s.data_type in compatibility_table and t.data_type in compatibility_table:
            s_sim = compatibility_table[s.data_type][t.data_type] * 0.5
            w_sim = compute_weighted_similarity(
                s_sim, l_sims.get((s.long_name, t.long_name), 0), leaf_w_struct
            )
            sims[(s.long_name, t.long_name)] = {
                "ssim": s_sim,
                "lsim": l_sims.get((s.long_name, t.long_name), 0),
                "wsim": w_sim,
            }
    return sims


def tree_match(
    source_tree,
    target_tree,
    categories,
    leaf_w_struct,
    w_struct,
    th_accept,
    th_high,
    th_low,
    c_inc,
    c_dec,
    th_ns,
    process_num,
):
    compatibility_table = compute_compatibility(categories)
    l_sims = comparison(source_tree, target_tree, compatibility_table, th_ns, process_num)
    s_leaves = source_tree.get_leaves()
    t_leaves = target_tree.get_leaves()
    sims = get_sims(s_leaves, t_leaves, compatibility_table, l_sims, leaf_w_struct)
    s_post_order = list(PostOrderIter(source_tree.root))
    t_post_order = list(PostOrderIter(target_tree.root))
    for s in s_post_order:
        s_name = s.long_name

        if s.is_leaf:
            continue

        for t in t_post_order:
            t_name = t.long_name

            if t.is_leaf:
                continue

            ssim = compute_ssim(s, t, sims, th_accept)

            # the nodes should have a similar number of leaves (within a factor of 2)
            if math.isnan(ssim):
                continue

            if (s.long_name, t.long_name) not in l_sims:
                l_sims[(s.long_name, t.long_name)] = 0

            wsim = compute_weighted_similarity(ssim, l_sims[(s.long_name, t.long_name)], w_struct)
            sims[(s_name, t_name)] = {
                "ssim": ssim,
                "lsim": l_sims[(s.long_name, t.long_name)],
                "wsim": wsim,
            }

            if (s_name, t_name) in sims and sims[(s_name, t_name)]["wsim"] > th_high:
                change_structural_similarity(
                    [n.long_name for n in s.leaves],
                    [n.long_name for n in t.leaves],
                    sims,
                    c_inc,
                    leaf_w_struct,
                )

            if (s_name, t_name) in sims and sims[(s_name, t_name)]["wsim"] < th_low:
                change_structural_similarity(
                    [n.long_name for n in s.leaves],
                    [n.long_name for n in t.leaves],
                    sims,
                    c_dec,
                    leaf_w_struct,
                )
    return sims


def recompute_wsim(source_tree, target_tree, sims, w_struct=0.6, th_accept=0.14):
    s_post_order = list(PostOrderIter(source_tree.root))
    t_post_order = list(PostOrderIter(target_tree.root))

    for s in s_post_order:
        s_name = s.long_name

        if s.is_leaf:
            continue

        for t in t_post_order:
            t_name = t.long_name

            if t.is_leaf:
                continue

            ssim = compute_ssim(s, t, sims, th_accept)

            if math.isnan(ssim):
                continue

            if (s_name, t_name) not in sims:
                lsim = compute_lsim(s, t)
            else:
                lsim = sims[(s_name, t_name)]["lsim"]

            wsim = compute_weighted_similarity(ssim, lsim, w_struct)
            sims[(s_name, t_name)] = {"ssim": ssim, "lsim": lsim, "wsim": wsim}
    return sims


def mapping_generation_leaves(source_tree, target_tree, sims, th_accept) -> dict:
    s_leaves = source_tree.get_leaf_names()
    t_leaves = target_tree.get_leaf_names()
    leave_combinations = list(product(s_leaves, t_leaves))
    matches = {}
    for k, v in sorted(sims.items(), key=lambda item: -item[1]["wsim"]):
        if th_accept <= v["wsim"] and k in leave_combinations:
            matches.update(create_output_dict(k, v["wsim"]))
    return matches


def create_output_dict(match: tuple, similarity) -> dict:
    s, t = match
    s_t_name, _, s_c_name, _ = s
    t_t_name, _, t_c_name, _ = t
    return Match(t_t_name, t_c_name, s_t_name, s_c_name, float(similarity)).to_dict


def mapping_generation_non_leaves(source_tree, target_tree, sims, th_accept=0.14):
    max_level_s = source_tree.height - 1
    max_level_t = target_tree.height - 1

    non_leaves_s = [n.long_name for n in LevelOrderIter(source_tree.root, maxlevel=max_level_s)]
    non_leaves_t = [n.long_name for n in LevelOrderIter(target_tree.root, maxlevel=max_level_t)]

    return [s for s in product(non_leaves_s, non_leaves_t) if sims[s]["wsim"] > th_accept]
