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
    files = sql_dir.rglob("*.sql")  # TODO: Make glob a parameter?
    models = [SqlModel.from_file(file) for file in files]
    dag = Dag.from_sql_models(models)
    dag.execute_parallel("duq.db")


if __name__ == "__main__":
    main()
