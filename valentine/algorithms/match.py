class Match:
    """
    Class representing a match of two columns target is the one we want to find the matches of, source an other
    that exists in the database and the similarity between the two.

    NOTE: Use the to_dict method when you want to append a match to a list of matches
    """
    def __init__(self,
                 target_db_guid: object, target_table_name: str, target_table_guid: object, target_column_name: str,
                 target_column_guid: object,
                 source_db_guid: object, source_table_name: str, source_table_guid: object, source_column_name: str,
                 source_column_guid: object,
                 similarity: float):
        self.target_db_guid = target_db_guid
        self.target_table_name = target_table_name
        self.target_table_guid = target_table_guid
        self.target_column_name = target_column_name
        self.target_column_guid = target_column_guid
        self.source_db_guid = source_db_guid
        self.source_table_name = source_table_name
        self.source_table_guid = source_table_guid
        self.source_column_name = source_column_name
        self.source_column_guid = source_column_guid
        self.similarity = similarity

    @property
    def to_dict(self):
        return {"source": {"db_guid": self.source_db_guid,
                           "tbl_nm":  self.source_table_name, "tbl_guid": self.source_table_guid,
                           "clm_nm": self.source_column_name, "clm_guid": self.source_column_guid},
                "target": {"db_guid": self.target_db_guid,
                           "tbl_nm":  self.target_table_name, "tbl_guid": self.target_table_guid,
                           "clm_nm": self.target_column_name, "clm_guid": self.target_column_guid},
                "sim": self.similarity}
