import math
from itertools import product
from anytree import PostOrderIter, LevelOrderIter

from .linguistic_matching import compute_compatibility, comparison, compute_lsim
from .schema_element import SchemaElement
from .structural_similarity import compute_ssim, change_structural_similarity
from ..match import Match


def compute_weighted_similarity(s_sim, l_sim, w_struct=0.5):
    return w_struct * s_sim + (1 - w_struct) * l_sim


def tree_match(source_tree, target_tree, categories, leaf_w_struct, w_struct, th_accept, th_high, th_low, c_inc, c_dec,
               th_ns, parallelism):
    compatibility_table = compute_compatibility(categories)
    l_sims = comparison(source_tree, target_tree, compatibility_table, th_ns, parallelism)
    s_leaves = source_tree.get_leaves()
    t_leaves = target_tree.get_leaves()
    all_leaves = product(s_leaves, t_leaves)
    sims = dict()

    for s, t in all_leaves:
        if s.data_type in compatibility_table and t.data_type in compatibility_table:
            s_sim = compatibility_table[s.data_type][t.data_type]
            w_sim = compute_weighted_similarity(s_sim, l_sims.get((s.long_name, t.long_name), 0), leaf_w_struct)
            sims[(s.long_name, t.long_name)] = {'ssim': s_sim, 'lsim': l_sims.get((s.long_name, t.long_name), 0),
                                                'wsim': w_sim}

    s_post_order = [node for node in PostOrderIter(source_tree.root)]
    t_post_order = [node for node in PostOrderIter(target_tree.root)]

    for s in s_post_order:
        s_name = s.long_name

        if isinstance(s, SchemaElement):
            continue

        for t in t_post_order:
            t_name = t.long_name

            if isinstance(t, SchemaElement):
                continue

            # if the nodes are on the same level
            if s.height == t.height:
                ssim = compute_ssim(s, t, sims, th_accept)

                # the nodes should have a similar number of leaves (within a factor of 2)
                if math.isnan(ssim):
                    continue

                if (s.long_name, t.long_name) not in l_sims:
                    l_sims[(s.long_name, t.long_name)] = 0

                wsim = compute_weighted_similarity(ssim, l_sims[(s.long_name, t.long_name)], w_struct)
                sims[(s_name, t_name)] = {'ssim': ssim, 'lsim': l_sims[(s.long_name, t.long_name)], 'wsim': wsim}

            if (s_name, t_name) in sims and sims[(s_name, t_name)]['wsim'] > th_high:
                change_structural_similarity(list(map(lambda n: n.long_name, s.leaves)),
                                             list(map(lambda n: n.long_name, t.leaves)), sims, c_inc)

            if (s_name, t_name) in sims and sims[(s_name, t_name)]['wsim'] < th_low:
                change_structural_similarity(list(map(lambda n: n.long_name, s.leaves)),
                                             list(map(lambda n: n.long_name, t.leaves)), sims, c_dec)
    return sims


def recompute_wsim(source_tree, target_tree, sims, w_struct=0.6, th_accept=0.14):
    s_post_order = [node for node in PostOrderIter(source_tree.root)]
    t_post_order = [node for node in PostOrderIter(target_tree.root)]

    for s in s_post_order:
        s_name = s.long_name

        if isinstance(s, SchemaElement):
            continue

        for t in t_post_order:
            t_name = t.long_name

            if isinstance(t, SchemaElement):
                continue

            # if the nodes are on the same level and are not leaves
            if s.height == t.height and (s.height > 0 and t.height > 0):
                ssim = compute_ssim(s, t, sims, th_accept)

                if math.isnan(ssim):
                    continue

                if (s_name, t_name) not in sims:
                    lsim = compute_lsim(s.name, t.name)
                else:
                    lsim = sims[(s_name, t_name)]['lsim']

                wsim = compute_weighted_similarity(ssim, lsim, w_struct)
                sims[(s_name, t_name)] = {'ssim': ssim, 'lsim': lsim, 'wsim': wsim}
    return sims


def mapping_generation_leaves(source_guid, target_guid, source_tree, target_tree, sims, th_accept):
    s_leaves = source_tree.get_leaf_names()
    t_leaves = target_tree.get_leaf_names()
    leave_combinations = list(product(s_leaves, t_leaves))
    return [create_output_dict(k, v['wsim'], source_guid, target_guid)
            for k, v in sorted(sims.items(), key=lambda item: -item[1]['wsim'])
            if th_accept <= v['wsim'] and k in leave_combinations]


def create_output_dict(match: tuple, similarity, source_guid, target_guid) -> dict:
    s, t = match
    s_t_name, s_t_guid, s_c_name, s_c_guid = s
    t_t_name, t_t_guid, t_c_name, t_c_guid = t
    return Match(target_guid, t_t_name, t_t_guid, t_c_name, t_c_guid,
                 source_guid, s_t_name, s_t_guid, s_c_name, s_c_guid,
                 float(similarity)).to_dict


def mapping_generation_non_leaves(source_tree, target_tree, sims, th_accept=0.14):
    max_level_s = source_tree.height - 1
    max_level_t = target_tree.height - 1

    non_leaves_s = list(map(lambda n: n.long_name, LevelOrderIter(source_tree.root, maxlevel=max_level_s)))
    non_leaves_t = list(map(lambda n: n.long_name, LevelOrderIter(target_tree.root, maxlevel=max_level_t)))

    return list(filter(lambda s: sims[s]['wsim'] > th_accept, product(non_leaves_s, non_leaves_t)))
