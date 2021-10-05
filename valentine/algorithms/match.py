class Match:
    """
    Class representing a match of two columns target is the one we want to find the matches of, source an other
    that exists in the database and the similarity between the two.

    NOTE: Use the to_dict method when you want to append a match to a list of matches
    """
    def __init__(self, target_table_name: str, target_column_name: str,
                 source_table_name: str, source_column_name: str,
                 similarity: float):
        self.target_table_name = target_table_name
        self.target_column_name = target_column_name
        self.source_table_name = source_table_name
        self.source_column_name = source_column_name
        self.similarity = similarity

    @property
    def to_dict(self) -> dict:
        return {((self.source_table_name, self.source_column_name),
                 (self.target_table_name, self.target_column_name)): self.similarity}
