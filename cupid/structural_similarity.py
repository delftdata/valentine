from itertools import product


def compute_ssim(node_s, node_t, sims, th_accept=0.5):
    s_leaves = list(map(lambda n: n.name.initial_name, node_s.leaves))
    t_leaves = list(map(lambda n: n.name.initial_name, node_t.leaves))
    all_leaves = product(s_leaves, t_leaves)

    filtered_pairs = [pair for pair in filter(lambda s: sims[s]['wsim'] > th_accept, sims.keys())
                      if pair in all_leaves]

    return len(filtered_pairs) / (len(s_leaves) + len(t_leaves))


def change_structural_similarity(leaves_s, leaves_t, sims, factor):
    all_leaves = product(leaves_s, leaves_t)
    for pair in all_leaves:
        sims[pair]['ssim'] = sims[pair]['ssim'] * factor