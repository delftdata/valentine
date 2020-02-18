from collections import defaultdict
from collections import namedtuple

Leave = namedtuple('Leave', 'id, matching')


class SimpleTrie:

    def __init__(self):
        self._leave = 0
        self.root = dict()
        self.step_dic = defaultdict(int)
        self.summarized_matchings = dict()

    def add_sequences(self, sequences, seq_corresponding_matching):
        self.step_dic["root"] = len(sequences)  # add also the number of sequences
        for seq in sequences:
            current_dict = self.root
            for token in seq:
                current_dict = current_dict.setdefault(token, {})  # another dict as default
                self.step_dic[token] += 1
            self._leave += 1  # increase leave id
            leave = Leave(self._leave, seq_corresponding_matching[str(seq)])  # create leave and assign matchings
            current_dict[self._leave] = leave
        return self.root, self.step_dic

    def _reduce_matchings(self, subtree, output):
        if type(subtree) is Leave:
            for el in subtree.matching:
                output.add(el)
        else:
            for child in subtree.keys():
                if type(child) is not Leave:
                    output = self._reduce_matchings(subtree[child], output)
                elif type(child) is Leave:
                    for el in subtree[child].matching:
                        output.add(el)
        return output

    def _add_matchings(self, subtree, child):
        subtree = subtree[child]
        if type(subtree) is Leave:
            matchings_of_child = subtree.matching
            for el in matchings_of_child:
                self.summarized_matchings[el] = 1  # the child
            return
        matchings = self._reduce_matchings(subtree, set())
        sch, cla = list(matchings)[0]
        new_match = (sch, (cla[0], child))  # child summarizes all the others
        self.summarized_matchings[new_match] = len(matchings)  # the number

    def _add_matchings2(self, subtree, parent):

        matchings = self._reduce_matchings(subtree, set())
        sch, cla = list(matchings)[0]
        new_match = (sch, (cla[0], parent))  # child summarizes all the others
        self.summarized_matchings[new_match] = len(matchings)  # the number

    """
    def cuts(self, current_node, subtree=None, num_seqs=None):
        if subtree is None and num_seqs is None:
            subtree = self.root
            num_seqs = self.step_dic["root"]

        #children = len(subtree.keys())
        children_represented = self.step_dic[current_node]
        ratio_cut = float(children / num_seqs)
        if ratio_cut > 0.5:
            return True
        return False
    """

    def summarize(self, num_seqs):

        def summarize_seq(num_seqs, subtree=None, current_node=None):

            # Choose the max representing child
            max_repr = 0
            chosen_child = None
            for child in subtree.keys():
                represented_seqs = self.step_dic[child]
                if represented_seqs > max_repr:
                    max_repr = represented_seqs
                    chosen_child = child

            # Does the max representing child cuts?
            ratio_cut = float(max_repr / num_seqs)
            if ratio_cut > 0.4:  # if cuts, keep digging
                return summarize_seq(num_seqs, subtree[chosen_child], chosen_child)
            else:  # i then summarize
                matchings = self._reduce_matchings(subtree, set())
                return matchings, current_node

        matchings, cutter = summarize_seq(num_seqs, self.root, "root")
        #sch, cla = list(matchings)[0]
        #new_match = (sch, (cla[0], cutter))
        return matchings, cutter