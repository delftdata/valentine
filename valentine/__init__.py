import pandas as pd

import valentine.metrics as valentine_metrics
import valentine.algorithms
import valentine.data_sources


class NotAValentineMatcher(Exception):
    pass


def valentine_match(df1: pd.DataFrame,
                    df2: pd.DataFrame,
                    matcher: valentine.algorithms.BaseMatcher,
                    df1_name: str = 'table_1',
                    df2_name: str = 'table_2'):
    if isinstance(matcher, valentine.algorithms.BaseMatcher):
        table_1 = valentine.data_sources.DataframeTable(df1, name=df1_name)
        table_2 = valentine.data_sources.DataframeTable(df2, name=df2_name)
        matches = dict(sorted(matcher.get_matches(table_1, table_2).items(),
                              key=lambda item: item[1], reverse=True))
    else:
        raise NotAValentineMatcher('The method that you selected is not supported by Valentine')

    return matches
