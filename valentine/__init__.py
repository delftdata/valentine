from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

import valentine.algorithms
import valentine.data_sources
from valentine.algorithms.matcher_results import MatcherResults

__all__ = [
    "NotAValentineMatcher",
    "valentine_match",
]


class NotAValentineMatcher(Exception):
    pass


def _validate_matcher(matcher: valentine.algorithms.BaseMatcher) -> None:
    if not isinstance(matcher, valentine.algorithms.BaseMatcher):
        raise NotAValentineMatcher("Please provide a valid matcher")


def valentine_match(
    dfs: Iterable[pd.DataFrame],
    matcher: valentine.algorithms.BaseMatcher,
    df_names: list[str] | None = None,
) -> MatcherResults:
    """Match columns across DataFrames.

    Accepts any iterable of DataFrames (list, generator, tuple, etc.) and
    matches columns across all unique pairs.

    Parameters
    ----------
    dfs : Iterable[pd.DataFrame]
        Two or more DataFrames to match against each other.
    matcher : BaseMatcher
        The matching algorithm to use.
    df_names : list[str] | None
        Optional names for each DataFrame. If not provided, defaults to
        "table_0", "table_1", etc.

    Returns
    -------
    MatcherResults
        Dictionary of column matches sorted by similarity (high to low).

    Raises
    ------
    ValueError
        If fewer than 2 DataFrames are provided, or if ``df_names`` length
        does not match the number of DataFrames.
    NotAValentineMatcher
        If ``matcher`` is not a valid BaseMatcher instance.

    Examples
    --------
    Match two DataFrames:

    >>> matches = valentine_match([df1, df2], Coma())

    Match multiple DataFrames (computes all pairs):

    >>> matches = valentine_match([df1, df2, df3], Coma(), df_names=["a", "b", "c"])
    """
    _validate_matcher(matcher)

    df_list = list(dfs)

    if len(df_list) < 2:
        raise ValueError("At least 2 DataFrames are required")

    if df_names is not None and len(df_names) != len(df_list):
        raise ValueError(
            f"Length of df_names ({len(df_names)}) must match number of DataFrames ({len(df_list)})"
        )

    tables = [
        valentine.data_sources.DataframeTable(
            df, name=df_names[i] if df_names is not None else f"table_{i}"
        )
        for i, df in enumerate(df_list)
    ]

    return MatcherResults(matcher.get_matches_batch(tables))
