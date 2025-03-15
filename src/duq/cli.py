"""Cli of duq."""

from pathlib import Path

import click

DirPath = click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path)


@click.command()
@click.argument("sql_dir", type=DirPath)
def main(sql_dir: Path) -> None:
    """Process SQL files in the specified directory."""
    # Fetch all SQL files in the directory
    files = sql_dir.rglob("*.sql")
    for file in files:
        print(file)

if __name__ == "__main__":
    main()

