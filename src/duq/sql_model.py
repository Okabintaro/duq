"""Definition of a SQL model."""

from dataclasses import dataclass
from pathlib import Path

import sqlglot
from sqlglot import expressions


class ParseError(Exception):
    """Raised when a model definition is not valid."""


class ExpectedSelectError(ParseError):
    """Raised when a model definition is not valid."""

    def __init__(self) -> None:
        super().__init__("Expected a SELECT statement")


@dataclass
class SqlModel:
    """Represents a SQL model."""

    name: str
    tree: sqlglot.Expression
    dependencies: dict[str, expressions.Table]
    source_sql: str

    filepath: Path | None = None

    @classmethod
    def from_str(cls, sql: str, filepath: Path | None = None) -> "SqlModel":
        """Create a SqlModel instance from a file."""
        tree = sqlglot.parse_one(sql)  # pyright: ignore[reportUnknownMemberType]
        if not isinstance(tree, expressions.Select):
            raise ExpectedSelectError

        ctes = list(tree.find_all(expressions.CTE))
        cte_names = {cte.alias for cte in ctes}
        dependencies = {
            tree.name: tree
            for tree in tree.find_all(expressions.Table)
            if tree.name not in cte_names
        }

        model_name = tree.name
        if len(model_name) == 0 and filepath is not None:
            filename = filepath.name.lower()
            if filename.endswith(".sql"):
                model_name = filename[:-4]

        return cls(
            name=model_name,
            dependencies=dependencies,
            source_sql=sql,
            tree=tree,
            filepath=filepath,
        )

    @classmethod
    def from_file(cls, filepath: Path | str) -> "SqlModel":
        """Create a SqlModel instance from a file."""
        if isinstance(filepath, str):
            filepath = Path(filepath)
        return cls.from_str(filepath.read_text(), filepath=filepath)

    def is_source_model(self) -> bool:
        """Return True if the model is a source model."""
        return all(table.name == "" for table in self.dependencies.values())
