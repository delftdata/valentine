import subprocess
import os
import csv
import time
from itertools import product
from typing import List, Dict

from ..base_matcher import BaseMatcher
from ..match import Match
from ...data_sources.base_db import BaseDB
from ...data_sources.base_table import BaseTable
from ...utils.utils import create_folder, get_project_root

# FIXME (1) use tmpdir everywhere
# FIXME (2) figure out how to access the jar (licensing etc)
# FIXME (3) figure out a way to specify the Xmx jar parameter


class Coma(BaseMatcher):

    def __init__(self, max_n: int = 0, strategy: str = "COMA_OPT"):
        self.max_n = int(max_n)
        self.strategy = strategy
        self.source_guid = None
        self.target_guid = None

    def get_matches(self, source_input: BaseDB, target_input: BaseDB) -> List[Dict]:
        tmp_folder_path: str = get_project_root() + '/algorithms/coma/tmp_data/'
        create_folder(tmp_folder_path)

        coma_output_path: str = get_project_root() + '/algorithms/coma/coma_output/'
        create_folder(coma_output_path)

        dataset_name: str = source_input.name + "____" + target_input.name
        coma_output_file: str = coma_output_path + '/' + dataset_name + str(self.max_n) + self.strategy + ".txt"

        matches = []

        source_tables = [table for table in source_input.get_tables().values()]
        target_tables = [table for table in target_input.get_tables().values()]

        self.source_guid = source_input.db_belongs_uid if isinstance(source_input, BaseTable) \
            else source_input.unique_identifier
        self.target_guid = target_input.db_belongs_uid if isinstance(target_input, BaseTable) \
            else target_input.unique_identifier

        for s_table, t_table in product(source_tables, target_tables):
            s_f_name, t_f_name = self.write_schema_csv_files(s_table, t_table)
            self.run_coma_jar(s_f_name, t_f_name, coma_output_file)
            raw_output = self.read_coma_output(s_f_name, t_f_name, coma_output_file)
            matches.extend(self.process_coma_output(raw_output, t_table, s_table))
            delete_file(s_f_name)
            delete_file(t_f_name)

        delete_file(coma_output_file)

        return matches

    def run_coma_jar(self, source_table_f_name: str, target_table_f_name: str, coma_output_path):
        jar_path = get_project_root() + '/algorithms/coma/artifact/coma.jar'
        jar_path = os.path.relpath(jar_path, get_project_root())
        source_data = os.path.relpath(source_table_f_name, get_project_root())
        target_data = os.path.relpath(target_table_f_name, get_project_root())
        coma_output_path = os.path.relpath(coma_output_path, get_project_root())
        fh = open("NUL", "w")
        subprocess.call(['java', '-Xmx4000m',  # YOU MIGHT NEED TO INCREASE THE MEMORY HERE WITH BIGGER TABLES
                         '-cp', jar_path,
                         '-DinputFile1=' + source_data,
                         '-DinputFile2=' + target_data,
                         '-DoutputFile=' + coma_output_path,
                         '-DmaxN=' + str(self.max_n),
                         '-Dstrategy=' + self.strategy,
                         'Main'], stdout=fh, stderr=fh)

    def write_schema_csv_files(self, table1: BaseTable, table2: BaseTable):
        f_name1 = self.write_csv_file(table1.name,
                                      list(map(lambda x: x.name, table1.get_columns())))
        f_name2 = self.write_csv_file(table2.name,
                                      list(map(lambda x: x.name, table2.get_columns())))
        return f_name1, f_name2

    def process_coma_output(self, matches, t_table: BaseTable, s_table: BaseTable) -> List:
        if matches is None:
            return []
        formatted_output = []
        t_lookup = t_table.get_guid_column_lookup
        s_lookup = s_table.get_guid_column_lookup
        for match in matches:
            m, similarity = match.split(":")
            m1, m2 = m.split(" <-> ")
            column1 = self.get_column(m2)
            column2 = self.get_column(m1)
            if column1 == "" or column2 == "":
                continue
            formatted_output.append(Match(self.target_guid,
                                          t_table.name, t_table.unique_identifier, column1, t_lookup[column1],
                                          self.source_guid,
                                          s_table.name, s_table.unique_identifier, column2, s_lookup[column2],
                                          float(similarity)).to_dict)
        return formatted_output

    def read_coma_output(self, s_f_name, t_f_name, coma_output_path, retries=0):
        try:
            with open(coma_output_path) as f:
                matches = f.readlines()
            matches = [x.strip() for x in matches]
            matches.pop()
        except FileNotFoundError:
            if retries == 1:
                self.run_coma_jar(s_f_name, t_f_name, coma_output_path)
            elif retries == 3:
                return []
            else:
                time.sleep(1)
            self.read_coma_output(s_f_name, t_f_name, coma_output_path, retries + 1)
        else:
            return matches

    @staticmethod
    def write_csv_file(table_name: str, data: List[str]) -> str:
        f_name: str = correct_file_ending(get_project_root() + '/algorithms/coma/tmp_data/' + table_name)
        with open(f_name, 'w', newline='') as out:
            writer = csv.writer(out)
            writer.writerow(data)
        return f_name

    @staticmethod
    def get_column(match) -> str:
        return ".".join(match.split(".")[1:])
