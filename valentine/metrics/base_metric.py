"""Provides the base metric class, that can be inherited from to implement
metrics.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ..algorithms.matcher_results import MatcherResults
from abc import ABC, abstractmethod
from collections.abc import Collection
from dataclasses import MISSING, dataclass, fields
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
            comparison) or full `ColumnPair`
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

        Instances constructed with default arguments are named after their
        class (e.g. ``"F1Score"``). Any field that differs from its default
        is appended so that differently configured instances of the same
        metric do not collide in the results, e.g.
        ``"F1Score(one_to_one=False)"``.

        Returns
        -------
        str
            The name of the metric.
        """
        return self.__class__.__name__ + self._non_default_params()

    def _non_default_params(self: Metric, exclude: Collection[str] = ()) -> str:
        """Format the dataclass fields that differ from their defaults.

        Parameters
        ----------
        exclude : Collection[str]
            Field names to leave out, e.g. ones already encoded in the name.

        Returns
        -------
        str
            ``"(field=value, ...)"``, or an empty string if every field has
            its default value.
        """
        params = []
        for f in fields(self):
            if not f.repr or f.name in exclude:
                continue
            value = getattr(self, f.name)
            if f.default is not MISSING:
                default = f.default
            elif f.default_factory is not MISSING:
                default = f.default_factory()
            else:
                default = MISSING
            if default is MISSING or value != default:
                params.append(f"{f.name}={value!r}")
        return f"({', '.join(params)})" if params else ""

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
