import math
from itertools import product
from operator import getitem

from anytree import PostOrderIter, LevelOrderIter

from cupid.elements import SchemaElement
from cupid.linguistic_matching import name_similarity_elements, normalize, compute_lsim
from cupid.structural_similarity import compute_ssim, change_structural_similarity


def compute_weighted_similarity(ssim, lsim, w_struct=0.5):
    return w_struct * ssim + (1 - w_struct) * lsim


def tree_match(source_tree, target_tree, leaf_w_struct=0.5, w_struct=0.6, th_accept=0.14, th_high=0.15, th_low=0.13,
               c_inc=1.2, c_dec=0.9):

    s_leaves = list(map(lambda n: n.name, source_tree.leaves))
    t_leaves = list(map(lambda n: n.name, target_tree.leaves))
    all_leaves = product(s_leaves, t_leaves)
    sims = dict()

    for s, t in all_leaves:
        # data type compatibility - max = 0.5
        ssim = name_similarity_elements(normalize(s.data_type), normalize(t.data_type))
        lsim = compute_lsim(s, t)
        wsim = compute_weighted_similarity(ssim, lsim, leaf_w_struct)
        sims[(s.long_name, t.long_name)] = {'ssim': ssim, 'lsim': lsim, 'wsim': wsim}

    s_post_order = [node for node in PostOrderIter(source_tree)]
    t_post_order = [node for node in PostOrderIter(target_tree)]

    for s in s_post_order:
        s_name = s.name.long_name

        if type(s.name) is not SchemaElement:
            continue

        for t in t_post_order:
            t_name = t.name.long_name

            if type(t.name) is not SchemaElement:
                continue

            # if the nodes are on the same level
            if s.height == t.height:
                ssim = compute_ssim(s, t, sims, th_accept)

                # the nodes should have a similar number of leaves (within a factor of 2)
                if math.isnan(ssim):
                    continue

                lsim = compute_lsim(s.name, t.name)
                wsim = compute_weighted_similarity(ssim, lsim, w_struct)
                sims[(s_name, t_name)] = {'ssim': ssim, 'lsim': lsim, 'wsim': wsim}

            if (s_name, t_name) in sims and sims[(s_name, t_name)]['wsim'] > th_high:
                change_structural_similarity(list(map(lambda n: n.name.long_name, s.leaves)),
                                             list(map(lambda n: n.name.long_name, t.leaves)), sims, c_inc)

            if (s_name, t_name) in sims and sims[(s_name, t_name)]['wsim'] < th_low:
                change_structural_similarity(list(map(lambda n: n.name.long_name, s.leaves)),
                                             list(map(lambda n: n.name.long_name, t.leaves)), sims, c_dec)
    return sims


def recompute_wsim(source_tree, target_tree, sims, w_struct=0.6, th_accept=0.14):
    s_post_order = [node for node in PostOrderIter(source_tree)]
    t_post_order = [node for node in PostOrderIter(target_tree)]

    for s in s_post_order:
        s_name = s.name.long_name

        if type(s.name) is not SchemaElement:
            continue

        for t in t_post_order:
            t_name = t.name.long_name

            if type(t.name) is not SchemaElement:
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


def mapping_generation_leaves(source_tree, target_tree, sims, th_accept):
    s_leaves = list(map(lambda n: n.name.long_name, source_tree.leaves))
    t_leaves = list(map(lambda n: n.name.long_name, target_tree.leaves))
    all_leaves = product(s_leaves, t_leaves)
    final_mappings = list()

    for pair in all_leaves:
        if sims[pair]['wsim'] > th_accept:
            final_mappings.append(sims[pair])

    return final_mappings


def mapping_generation_non_leaves(source_tree, target_tree, sims, th_accept):
    max_level_s = source_tree.height - 1
    max_level_t = target_tree.height - 1

    non_leaves_s = list(map(lambda n: n.name.long_name, LevelOrderIter(source_tree.root, maxlevel=max_level_s)))
    non_leaves_t = list(map(lambda n: n.name.long_name, LevelOrderIter(target_tree.root, maxlevel=max_level_t)))

    all_leaves = product(non_leaves_s, non_leaves_t)
    final_mappings = list()

    for pair in all_leaves:
        if sims[pair]['wsim'] > th_accept:
            final_mappings.append(sims[pair])

    return final_mappings


def get_matchings(sims, th_accept=0.14):
    sorted_sims = sorted(sims.items(), key=lambda x: getitem(x[1], 'wsim'), reverse=True)
    return list(filter(lambda s: s[1]['wsim'] > th_accept, sorted_sims))


