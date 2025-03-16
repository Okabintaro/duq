"""Definition of a directed acyclic graph of SQL models."""

from dataclasses import dataclass
from graphlib import TopologicalSorter
from typing import NewType

from duq.sql_model import SqlModel

ModelName = NewType("ModelName", str)


@dataclass
class Dag:
    """A directed acyclic graph of SQL models."""

    graph: dict[ModelName, set[ModelName]]
    """A graph of SQL models identified by their names."""

    models: dict[ModelName, SqlModel]

    @classmethod
    def from_sql_models(cls, models: list[SqlModel]) -> "Dag":
        """Create a DAG from a list of SQL models."""
        graph: dict[ModelName, set[ModelName]] = {}
        for model in models:
            graph[ModelName(model.name)] = {
                ModelName(name) for name in model.dependencies if len(name) > 0
            }
        return cls(graph, models={ModelName(model.name): model for model in models})

    def topo_sort(self) -> list[ModelName]:
        """Return a topological sort of the DAG."""
        sorter = TopologicalSorter(self.graph)
        return list(sorter.static_order())
