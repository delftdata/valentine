import subprocess
import os
import tempfile
import time
from typing import AnyStr, Dict, Tuple

from ..base_matcher import BaseMatcher
from ..match import Match
from ...data_sources.base_table import BaseTable
from ...utils.utils import get_project_root


class Coma(BaseMatcher):

    def __init__(self,
                 max_n: int = 0,
                 strategy: str = "COMA_OPT",
                 java_xmx: str = "4096m"):
        self.__max_n = int(max_n)
        self.__strategy = strategy
        self.__java_XmX = java_xmx

    def get_matches(self,
                    source_input: BaseTable,
                    target_input: BaseTable
                    ) -> Dict[Tuple[Tuple[str, str], Tuple[str, str]], float]:

        with tempfile.TemporaryDirectory() as tmp_folder_path:
            s_f_name, t_f_name = self.__write_schema_csv_files(source_input, target_input, tmp_folder_path)
            dataset_name: str = f'{source_input.name}____{target_input.name}{self.__max_n}{self.__strategy}.txt'
            coma_output_file: str = os.path.join(tmp_folder_path, dataset_name)
            self.__run_coma_jar(s_f_name, t_f_name, coma_output_file, tmp_folder_path)
            raw_output = self.__read_coma_output(s_f_name, t_f_name, coma_output_file, tmp_folder_path)
            matches = self.__process_coma_output(raw_output, target_input, source_input)

        return matches

    def __run_coma_jar(self,
                       source_table_f_name: str,
                       target_table_f_name: str,
                       coma_output_path: str,
                       tmp_folder_path: str
                       ) -> None:
        jar_path = os.path.join(get_project_root(), 'algorithms', 'coma', 'artifact', 'coma.jar')
        source_data = os.path.relpath(source_table_f_name, get_project_root())
        target_data = os.path.relpath(target_table_f_name, get_project_root())
        coma_output_path = os.path.relpath(coma_output_path, get_project_root())
        with open(os.path.join(tmp_folder_path, "NUL"), "w") as fh:
            subprocess.call(['java', f'-Xmx{self.__java_XmX}',
                             '-cp', jar_path,
                             '-DinputFile1=' + source_data,
                             '-DinputFile2=' + target_data,
                             '-DoutputFile=' + coma_output_path,
                             '-DmaxN=' + str(self.__max_n),
                             '-Dstrategy=' + self.__strategy,
                             'Main'], stdout=fh, stderr=fh)

    def __write_schema_csv_files(self,
                                 table1: BaseTable,
                                 table2: BaseTable,
                                 tmp_folder_path: str
                                 ) -> Tuple[str, str]:
        f_name1 = self.__write_csv_file(table1, tmp_folder_path)
        f_name2 = self.__write_csv_file(table2, tmp_folder_path)
        return f_name1, f_name2

    def __process_coma_output(self,
                              matches,
                              t_table: BaseTable,
                              s_table: BaseTable
                              ) -> dict:
        if matches is None:
            return {}
        formatted_output = {}
        for match in matches:
            m, similarity = match.split(":")
            m1, m2 = m.split(" <-> ")
            column1 = self.__get_column(m2)
            column2 = self.__get_column(m1)
            if column1 == "" or column2 == "":
                continue
            formatted_output.update(Match(t_table.name, column1,
                                          s_table.name, column2,
                                          float(similarity)).to_dict)
        return formatted_output

    def __read_coma_output(self,
                           s_f_name,
                           t_f_name,
                           coma_output_path,
                           tmp_folder_path,
                           retries=0):
        try:
            with open(coma_output_path) as f:
                matches = f.readlines()
            matches = [x.strip() for x in matches]
            matches.pop()
        except FileNotFoundError:
            if retries == 1:
                self.__run_coma_jar(s_f_name, t_f_name, coma_output_path, tmp_folder_path)
            elif retries == 3:
                return []
            else:
                time.sleep(1)
            self.__read_coma_output(s_f_name, t_f_name, coma_output_path, tmp_folder_path, retries + 1)
        else:
            return matches

    @staticmethod
    def __write_csv_file(table: BaseTable,
                         tmp_folder_path: str
                         ) -> str:
        f_name: AnyStr = os.path.join(tmp_folder_path, table.name + ".csv")
        table.get_df().to_csv(f_name, index=False)
        return f_name

    @staticmethod
    def __get_column(match) -> str:
        return ".".join(match.split(".")[1:])
