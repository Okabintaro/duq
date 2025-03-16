"""Cli of duq."""

from pathlib import Path

import click

from duq.dag import Dag
from duq.sql_model import SqlModel

DirPath = click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path)


@click.command()
@click.argument("sql_dir", type=DirPath)
def main(sql_dir: Path) -> None:
    """Process SQL files in the specified directory."""
    files = sql_dir.rglob("*.sql") # TODO: Make glob a parameter?
    models = [SqlModel.from_file(file) for file in files]

    print("All models")
    for model in models:
        print("-", model.name)

    source_models = [m for m in models if m.is_source_model()]
    print("Source models")
    for model in source_models:
        print("-", model.name)

    dag = Dag.from_sql_models(models)
    print("DAG")
    print(dag.graph)
    print("Sorted")
    print(dag.topo_sort())

if __name__ == "__main__":
    main()

