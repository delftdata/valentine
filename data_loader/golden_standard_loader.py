import json


class GoldenStandardLoader:
    def __init__(self, path):
        self.path = path
        self.expected_matches = set()
        self.size = 0
        self.load_golden_standard()

    def load_golden_standard(self):
        with open(self.path) as json_file:
            mappings: list = json.load(json_file)["matches"]
        for mapping in mappings:
            self.expected_matches.add(
                frozenset(((mapping["source_table"], mapping["source_column"]),
                          (mapping["target_table"], mapping["target_column"]))))
        self.size = len(self.expected_matches)

    def is_in_golden_standard(self, mapping: set):
        return mapping in self.expected_matches
