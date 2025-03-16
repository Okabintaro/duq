"""Definition of a directed acyclic graph of SQL models."""

from dataclasses import dataclass
from graphlib import TopologicalSorter
from pathlib import Path
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

    def to_script(self, use_views: bool = False) -> str:
        """Generate a sql script that is executable by duckdb."""
        toposort = self.topo_sort()
        script = ""
        for model_name in toposort:
            model = self.models[model_name]
            sql_ctas: str = model.as_ctas(as_view=use_views).sql(
                dialect="duckdb",
                pretty=True,
            )
            script += sql_ctas
            script += ";\n\n"

        return script

    def execute_sequentially(self, db_path: str | Path) -> None:
        """Execute all SQL models in the DAG sequentially."""
        import duckdb

        conn = duckdb.connect(db_path)

        toposort = self.topo_sort()
        for model_name in toposort:
            model = self.models[model_name]
            ctas_sql = model.as_ctas().sql(dialect="duckdb")
            print(f"Executing {model_name}")
            _ = conn.execute(ctas_sql)

    def execute_parallel(self, db_path: str | Path) -> None:
        """Execute all SQL models in the DAG in parallel using a ThreadPoolExecutor.

        This method executes each SQL model in the DAG in topological order,
        using a ThreadPoolExecutor to parallelize the execution.
        For the jaffle shop example this actually didn't improve performance.

        Args:
            db_path: Path to the DuckDB database file.
        """
        from concurrent.futures import Future, ThreadPoolExecutor

        import duckdb

        def execute_node(node: ModelName) -> None:
            """Execute a single node in the DAG."""
            print(f"Starting {node}")
            model = self.models[node]
            ctas_sql = model.as_ctas().sql(dialect="duckdb")
            _ = connection.execute(ctas_sql)
            print(node)

        connection = duckdb.connect(db_path)
        try:
            sorter = TopologicalSorter(self.graph)
            sorter.prepare()

            with ThreadPoolExecutor() as executor:
                futures: dict[ModelName, Future[None]] = {}

                while sorter.is_active():
                    ready_nodes = list(sorter.get_ready())
                    print(f"Submitting {len(ready_nodes)} jobs")
                    for node in ready_nodes:
                        futures[node] = executor.submit(execute_node, node)

                    for node in ready_nodes:
                        try:
                            futures[node].result()  # Wait for the task to complete
                            sorter.done(node)
                        except Exception as e:  # noqa: BLE001
                            print(f"Failed to execute node {node}: {e}")
                            # Still mark as done to allow other nodes to proceed
                            sorter.done(node)
        finally:
            connection.close()
