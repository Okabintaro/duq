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
    dependencies: list[str]
    sql: str
    tree: sqlglot.Expression

    filepath: Path | None = None

    @classmethod
    def from_str(cls, sql: str) -> "SqlModel":
        """Create a SqlModel instance from a file."""
        tree = sqlglot.parse_one(sql) # pyright: ignore[reportUnknownMemberType]
        if not isinstance(tree, expressions.Select):
            raise ExpectedSelectError

        source_trees = list(tree.find_all(expressions.Table))
        return cls(
            name=tree.name,
            dependencies=[t.name for t in source_trees],
            sql=sql,
            tree=tree,
        )

