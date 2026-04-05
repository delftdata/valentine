from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from ...data_sources.base_table import BaseTable

INSTANCES_MAX = 1000


@dataclass(eq=False)
class SchemaElement:
    """A node in the schema graph (either root or column)."""

    name: str
    accession: str  # Dot-separated path, e.g. "table.column"
    data_type: str
    instances: list[str] = field(default_factory=list)


@dataclass
class SchemaGraph:
    """
    Simplified two-level schema graph for DataFrame-based input.

    Structure: root -> [column1, column2, ...]
    """

    root: SchemaElement
    columns: list[SchemaElement]

    @classmethod
    def from_table(cls, table: BaseTable) -> SchemaGraph:
        root = SchemaElement(
            name=table.name,
            accession=table.name,
            data_type="element",
        )

        # Java's InstanceCSVParser reads rows until 1000 rows have at least one
        # non-null value. All columns share the same row window. We replicate
        # this by selecting up to INSTANCES_MAX rows from the DataFrame where
        # at least one column is non-null, then extracting per-column values.
        all_cols = table.get_columns()
        col_instances: dict[str, list[str]] = {col.name: [] for col in all_cols}

        df = table.get_df()
        # Keep only rows where at least one value is non-null and non-empty
        row_count = 0
        for _, row in df.iterrows():
            if row_count >= INSTANCES_MAX:
                break
            has_value = False
            for col in all_cols:
                val = row[col.name]
                if pd.notna(val) and str(val) != "":
                    has_value = True
                    col_instances[col.name].append(str(val))
            if has_value:
                row_count += 1

        columns = []
        for col in all_cols:
            elem = SchemaElement(
                name=col.name,
                accession=f"{table.name}.{col.name}",
                data_type=col.data_type,
                instances=col_instances[col.name],
            )
            columns.append(elem)
        return cls(root=root, columns=columns)

    def get_parents(self, elem: SchemaElement) -> list[SchemaElement]:
        """Return parents of an element. Columns have root as parent; root has none."""
        if elem is self.root:
            return []
        return [self.root]

    def get_children(self, elem: SchemaElement) -> list[SchemaElement]:
        """Return children of an element. Root has columns; columns have none."""
        if elem is self.root:
            return list(self.columns)
        return []

    def get_siblings(self, elem: SchemaElement) -> list[SchemaElement]:
        """Return siblings (same parent, excluding self)."""
        if elem is self.root:
            return []
        return [c for c in self.columns if c is not elem]

    def get_leaves(self, elem: SchemaElement) -> list[SchemaElement]:
        """Return leaf descendants. Root's leaves are all columns; a column is its own leaf."""
        if elem is self.root:
            return list(self.columns)
        return [elem]

    def get_paths(self) -> list[list[SchemaElement]]:
        """Return all root-to-leaf paths. Each path is [root, column]."""
        return [[self.root, col] for col in self.columns]
