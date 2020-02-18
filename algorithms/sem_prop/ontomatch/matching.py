from collections import defaultdict


class Matching:

    def __init__(self, db_name, source_name):
        self.db_name = db_name
        self.source_name = source_name
        self.source_level_matchings = defaultdict(lambda: defaultdict(list))
        self.attr_matchings = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    def add_relation_correspondence(self, kr_name, class_name, matching_type):
        self.source_level_matchings[kr_name][class_name].append(matching_type)

    def add_attribute_correspondence(self, attr_name, kr_name, class_name, matching_type):
        self.attr_matchings[attr_name][kr_name][class_name].append(matching_type)

    def __str__(self):
        header = self.db_name + " - " + self.source_name
        relation_matchings = list()
        relation_matchings.append(header)
        if len(self.source_level_matchings.items()) > 0:
            for kr_name, values in self.source_level_matchings.items():
                for class_name, matchings in values.items():
                    line = kr_name + " - " + class_name + " : " + str(matchings)
                    relation_matchings.append(line)
        else:
            line = "0 relation matchings"
            relation_matchings.append(line)
        if len(self.attr_matchings.items()) > 0:
            for attr_name, values in self.attr_matchings.items():
                for kr_name, classes in values.items():
                    for class_name, matchings in classes.items():
                        line = attr_name + " ==>> " + kr_name + " - " + class_name + " : " + str(matchings)
                        relation_matchings.append(line)
        string_repr = '\n'.join(relation_matchings)
        return string_repr

    def get_matchings(self):
        def get_matching_code(ms):
            code_value = 0
            for m in ms:
                code_value += m.value
            return code_value

        matchings = []
        for kr_name, values in self.source_level_matchings.items():
            for class_name, ms in values.items():
                mcode = get_matching_code(ms)
                # match = ((self.db_name, self.source_name, "_"), (kr_name, class_name), mcode)
                match = ((self.db_name, self.source_name, "_"), (kr_name, class_name))
                matchings.append(match)
        for attr_name, values in self.attr_matchings.items():
            for kr_name, classes in values.items():
                for class_name, ms in classes.items():
                    mcode = get_matching_code(ms)
                    # match = ((self.db_name, self.source_name, attr_name), (kr_name, class_name), mcode)
                    match = ((self.db_name, self.source_name, attr_name), (kr_name, class_name))
                    matchings.append(match)
        return matchings

    def print_serial(self):
        relation_matchings = []
        for kr_name, values in self.source_level_matchings.items():
            for class_name, matchings in values.items():
                line = self.db_name + " %%% " + self.source_name + " %%% _ ==>> " + kr_name \
                       + " %%% " + class_name + " %%% " + str(matchings)
                relation_matchings.append(line)
        for attr_name, values in self.attr_matchings.items():
            for kr_name, classes in values.items():
                for class_name, matchings in classes.items():
                    line = self.db_name + " %%% " + self.source_name + " %%% " + attr_name \
                           + " ==>> " + kr_name + " %%% " + class_name + " %%% " + str(matchings)
                    relation_matchings.append(line)
        #string_repr = '\n'.join(relation_matchings)
        return relation_matchings