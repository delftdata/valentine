import math
import time
from itertools import product

from anytree import PostOrderIter, LevelOrderIter

from algorithms.cupid.elements import SchemaElement
from algorithms.cupid.linguistic_matching import name_similarity_elements, normalization, compute_lsim, \
    compute_compatibility, comparison
from algorithms.cupid.structural_similarity import compute_ssim, change_structural_similarity


def compute_weighted_similarity(ssim, lsim, w_struct=0.5):
    return w_struct * ssim + (1 - w_struct) * lsim


def tree_match(source_tree, target_tree, categories, leaf_w_struct, w_struct, th_accept, th_high, th_low, c_inc, c_dec,
               th_ns):

    compatibility_table = compute_compatibility(categories[source_tree.name.initial_name].keys(),
                                                categories[target_tree.name.initial_name].keys())
    lsims = comparison(source_tree, target_tree, categories[source_tree.name.initial_name],
                       categories[target_tree.name.initial_name], compatibility_table, th_ns)

    s_leaves = list(map(lambda n: n.name, source_tree.leaves))
    t_leaves = list(map(lambda n: n.name, target_tree.leaves))
    all_leaves = product(s_leaves, t_leaves)
    sims = dict()

    start = time.time()
    for s, t in all_leaves:
        # data type compatibility: max = 0.5
        ssim = name_similarity_elements(normalization(s.data_type), normalization(t.data_type))
        # lsim = compute_lsim(s, t)
        if (s, t) not in lsims:
            lsims[(s, t)] = 0
        wsim = compute_weighted_similarity(ssim, lsims[(s, t)], leaf_w_struct)
        sims[(s.long_name, t.long_name)] = {'ssim': ssim, 'lsim': lsims[(s, t)], 'wsim': wsim}
    end = time.time()
    print('Leaves computation took: {}'.format(end-start))

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

                # lsim = compute_lsim(s.name, t.name)
                if (s.name, t.name) not in lsims:
                    lsims[(s.name, t.name)] = 0

                wsim = compute_weighted_similarity(ssim, lsims[(s.name, t.name)], w_struct)
                sims[(s_name, t_name)] = {'ssim': ssim, 'lsim': lsims[(s.name, t.name)], 'wsim': wsim}

            if (s_name, t_name) in sims and sims[(s_name, t_name)]['wsim'] > th_high:
                change_structural_similarity(list(map(lambda n: n.name.long_name, s.leaves)),
                                             list(map(lambda n: n.name.long_name, t.leaves)), sims, c_inc)

            if (s_name, t_name) in sims and sims[(s_name, t_name)]['wsim'] < th_low:
                change_structural_similarity(list(map(lambda n: n.name.long_name, s.leaves)),
                                             list(map(lambda n: n.name.long_name, t.leaves)), sims, c_dec)
    end2 = time.time()
    print('Post-order matchings computation took: {}'.format(end2 - end))
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

    leave_combinations = list(product(s_leaves, t_leaves))

    return {k: v['wsim'] for k, v in sorted(sims.items(), key=lambda item: -item[1]['wsim'])
            if th_accept <= v['wsim'] and k in leave_combinations}


def mapping_generation_non_leaves(source_tree, target_tree, sims, th_accept=0.14):
    max_level_s = source_tree.height - 1
    max_level_t = target_tree.height - 1

    non_leaves_s = list(map(lambda n: n.name.long_name, LevelOrderIter(source_tree.root, maxlevel=max_level_s)))
    non_leaves_t = list(map(lambda n: n.name.long_name, LevelOrderIter(target_tree.root, maxlevel=max_level_t)))

    non_leaves_combinations = product(non_leaves_s, non_leaves_t)

    return {k: v['wsim'] for k, v in sorted(sims.items(), key=lambda item: -item[1]['wsim'])
            if th_accept <= v['wsim'] and k in non_leaves_combinations}
