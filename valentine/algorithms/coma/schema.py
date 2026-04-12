from __future__ import annotations

from dataclasses import dataclass, field

from ...data_sources.base_table import BaseTable


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

        # Use the instance-sampled columns directly. Each column's
        # ``.data`` property already contains the non-null values as
        # a plain list, so we just stringify them. This avoids any
        # dependency on the frame type (pandas/Polars).
        instance_cols = table.get_instances_columns()
        instance_lookup = {col.name: col.data for col in instance_cols}

        columns = []
        for col in table.get_columns():
            raw = instance_lookup.get(col.name, [])
            instances = [str(v) for v in raw if v is not None and str(v) != ""]
            elem = SchemaElement(
                name=col.name,
                accession=f"{table.name}.{col.name}",
                data_type=col.data_type,
                instances=instances,
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
