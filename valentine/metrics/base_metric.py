"""Provides the base metric class, that can be inherited from to implement
metrics.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ..algorithms.matcher_results import MatcherResults
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, final

# Valid choices for the 1:1 selection algorithm used by Precision /
# Recall / F1Score / PrecisionTopNPercent when ``one_to_one=True``.
OneToOneMethod = Literal["greedy", "hungarian", "mutual_top"]


@dataclass(eq=True, frozen=True)
class Metric(ABC):
    """Base class for a metric. Metrics can be prepared with parameters by
    instantiating them, their application is deferred to a later moment this
    way, which can be implemented by overriding the `apply` method.
    """

    @abstractmethod
    def apply(
        self: Metric,
        matches: MatcherResults,
        ground_truth: list[tuple[str, str]] | list,
        one_to_one_method: OneToOneMethod = "hungarian",
    ) -> dict[str, Any]:
        """Apply the metric to a ``MatcherResults`` instance, given ground truth.

        Parameters
        ----------
        matches : MatcherResults
            The ``MatcherResults`` instance, obtained from ``valentine_match``.
        ground_truth : list
            Expected column matches. Either column-name pairs
            ``[("src_col", "tgt_col"), ...]`` (table names ignored during
            comparison) or full :class:`~valentine.algorithms.ColumnPair`
            instances for table-aware comparison.
        one_to_one_method : {"greedy", "hungarian", "mutual_top"}
            Selection algorithm used when the metric's ``one_to_one``
            field is ``True``. Defaults to ``"hungarian"`` (globally
            optimal). ``"greedy"`` matches the legacy behaviour;
            ``"mutual_top"`` keeps only mutually-confirmed pairs (top-1
            on each side).  Ignored when ``one_to_one`` is ``False`` or
            for metrics that do not apply 1:1 filtering.
        """
        pass

    def name(self: Metric) -> str:
        """The name of the metric, as it appears in the metric results.

        Returns
        -------
        str
            The name of the metric.
        """
        return self.__class__.__name__

    @final
    def return_format(self: Metric, value: Any) -> dict[str, Any]:
        """The return format of the `apply` method.

        Parameters
        ----------
        value : Any
            The metric value or score.

        Returns
        -------
        Dict[str, Any]
            The formatted metric value or score.
        """
        return {self.name(): value}
