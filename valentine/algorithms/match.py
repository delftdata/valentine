from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple


class ColumnPair(NamedTuple):
    """A matched pair of columns from two tables.

    Used as the key in match result dictionaries. Provides named access
    to all four identifiers::

        pair = ColumnPair("orders", "price", "sales", "amount")
        pair.source_table  # "orders"
        pair.source_column  # "price"
        pair.target_table  # "sales"
        pair.target_column  # "amount"
        pair.source  # ("orders", "price")
        pair.target  # ("sales", "amount")
    """

    source_table: str
    source_column: str
    target_table: str
    target_column: str

    @property
    def source(self) -> tuple[str, str]:
        """(source_table, source_column) pair."""
        return (self.source_table, self.source_column)

    @property
    def target(self) -> tuple[str, str]:
        """(target_table, target_column) pair."""
        return (self.target_table, self.target_column)


@dataclass
class Match:
    """Internal helper for building match result entries.

    Algorithms create ``Match`` instances and call ``.to_dict`` to produce
    the ``{ColumnPair: similarity}`` entries that get merged into the
    final result dictionary.
    """

    target_table_name: str
    target_column_name: str
    source_table_name: str
    source_column_name: str
    similarity: float

    @property
    def to_dict(self: Match) -> dict[ColumnPair, float]:
        return {
            ColumnPair(
                source_table=self.source_table_name,
                source_column=self.source_column_name,
                target_table=self.target_table_name,
                target_column=self.target_column_name,
            ): self.similarity
        }
