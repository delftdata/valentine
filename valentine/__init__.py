import pandas as pd

import valentine.algorithms
import valentine.data_sources

from typing import Iterable, List, Union
from valentine.algorithms.matcher_results import MatcherResults


class NotAValentineMatcher(Exception):
    pass


def validate_matcher(matcher):
    if not isinstance(matcher, valentine.algorithms.BaseMatcher):
        raise NotAValentineMatcher('Please provide a valid matcher')


def valentine_match(df1: pd.DataFrame,
                    df2: pd.DataFrame,
                    matcher: valentine.algorithms.BaseMatcher,
                    df1_name: str = 'table_1',
                    df2_name: str = 'table_2'):

    validate_matcher(matcher)

    table_1 = valentine.data_sources.DataframeTable(df1, name=df1_name)
    table_2 = valentine.data_sources.DataframeTable(df2, name=df2_name)
    matches = matcher.get_matches(table_1, table_2)

    return MatcherResults(matches)


def valentine_match_batch(df_iter_1: Iterable[pd.DataFrame],
                          df_iter_2: Iterable[pd.DataFrame],
                          matcher: valentine.algorithms.BaseMatcher,
                          df_iter_1_names: Union[List[str], None] = None,
                          df_iter_2_names: Union[List[str], None] = None):

    validate_matcher(matcher)

    matches = {}

    for df1_idx, df1 in enumerate(df_iter_1):
        for df2_idx, df2 in enumerate(df_iter_2):
            table_1_name = df_iter_1_names[df1_idx] if df_iter_1_names is not None else f'table_1_{df1_idx}'
            table_2_name = df_iter_2_names[df2_idx] if df_iter_2_names is not None else f'table_2_{df2_idx}'
            table_1 = valentine.data_sources.DataframeTable(df1, name=table_1_name)
            table_2 = valentine.data_sources.DataframeTable(df2, name=table_2_name)
            matches.update(matcher.get_matches(table_1, table_2))

    return MatcherResults(matches)
