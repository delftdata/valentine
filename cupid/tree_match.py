from itertools import product
from operator import getitem

from anytree import PostOrderIter

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
        ssim = name_similarity_elements(normalize(s.data_type), normalize(t.data_type))
        lsim = compute_lsim(s, t)
        wsim = compute_weighted_similarity(ssim, lsim, leaf_w_struct)
        sims[(s.initial_name, t.initial_name)] = {'ssim': ssim, 'lsim': lsim, 'wsim': wsim}

    s_post_order = [node for node in PostOrderIter(source_tree)]
    t_post_order = [node for node in PostOrderIter(target_tree)]

    for s in s_post_order:
        s_name = s.name.initial_name

        if type(s.name) is not SchemaElement:
            continue

        for t in t_post_order:
            t_name = t.name.initial_name

            if type(t.name) is not SchemaElement:
                continue

            if s.name not in s_leaves or t.name not in t_leaves:
                ssim = compute_ssim(s, t, sims, th_accept)
                lsim = compute_lsim(s.name, t.name)
                wsim = compute_weighted_similarity(ssim, lsim, w_struct)
                sims[(s_name, t_name)] = {'ssim': ssim, 'lsim': lsim, 'wsim': wsim}

            if sims[(s_name, t_name)]['wsim'] > th_high:
                change_structural_similarity(list(map(lambda n: n.name.initial_name, s.leaves)),
                                             list(map(lambda n: n.name.initial_name, t.leaves)), sims, c_inc)

            if sims[(s_name, t_name)]['wsim'] < th_low:
                change_structural_similarity(list(map(lambda n: n.name.initial_name, s.leaves)),
                                             list(map(lambda n: n.name.initial_name, t.leaves)), sims, c_dec)
    return sims


def get_matchings(sims, th_accept=0.14):
    sorted_sims = sorted(sims.items(), key=lambda x: getitem(x[1], 'wsim'), reverse=True)
    return list(map(lambda x: x[0], filter(lambda s: s[1]['wsim'] > th_accept, sorted_sims)))


