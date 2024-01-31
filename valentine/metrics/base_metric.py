"""Provides the base metric class, that can be inherited from to implement
metrics.
"""
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..algorithms.matcher_results import MatcherResults
from abc import ABC, abstractmethod
from typing import Dict, Tuple, List, Any, final
from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Metric(ABC):
    """Base class for a metric. Metrics can be prepared with parameters by
    instantiating them, their application is deferred to a later moment this
    way, which can be implemented by overriding the `apply` method.
    """

    @abstractmethod
    def apply(self: Metric, matches: MatcherResults, ground_truth: List[Tuple[str, str]]) -> Dict[str, Any]:
        """Applies the metric to a `MatcherResults` instance, given ground
        truth.

        Parameters
        ----------
        matches : MatcherResults
            The `MatcherResults` instance, obtained from `valentine_match`.

        ground_truth : List[Tuple[str, str]]
            The ground truth column match pairs, by column name.
            e.g. [("col1_tab_A", "col1_tab_B"), ...etc...]

        Raises
        ------
        NotImplementedError
        Override this method in concrete implementations.
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
    def return_format(self: Metric, value: Any) -> Dict[str, Any]:
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
