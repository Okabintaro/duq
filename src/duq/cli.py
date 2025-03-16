"""Cli of duq."""

from dataclasses import dataclass
from pathlib import Path

import click

from duq.dag import Dag

DirPath = click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path)


@dataclass
class CliConfig:
    """Configuration for the duq CLI."""

    sql_path: Path
    use_views: bool = False


@click.group()
@click.argument("sql_path", type=DirPath)
@click.option(
    "-v",
    "--views",
    type=bool,
    default=False,
    is_flag=True,
    help="Create views instead of tables.",
)
@click.pass_context
def cli(ctx: click.Context, sql_path: Path, views: bool) -> None:
    """Schedule and execute SQL models using duq."""
    ctx.obj = CliConfig(sql_path, use_views=views)


@cli.command()
@click.pass_context
def plan(ctx: click.Context) -> None:
    """Parse and plan the DAG without executing it."""
    cfg: CliConfig = ctx.obj  # pyright: ignore[reportAny]
    dag = Dag.from_sql_dir(cfg.sql_path)
    for model in dag.topo_sort():
        print(model)


@cli.command()
@click.pass_context
def script(ctx: click.Context) -> None:
    """Generate a SQL script that can be executed by DuckDB."""
    cfg: CliConfig = ctx.obj  # pyright: ignore[reportAny]
    dag = Dag.from_sql_dir(cfg.sql_path)
    print(dag.to_script(use_views=cfg.use_views))
    print(cfg)


@cli.command()
@click.argument("db_path", type=str)
@click.option("-p", "--parallel", type=bool, is_flag=True, help="Execute in parallel.")
@click.pass_context
def run(ctx: click.Context, db_path: str, parallel: bool) -> None:
    """Plan and execute the DAG."""
    cfg: CliConfig = ctx.obj  # pyright: ignore[reportAny]
    dag = Dag.from_sql_dir(cfg.sql_path)
    if parallel:
        dag.execute_parallel(db_path, use_views=cfg.use_views)
    else:
        dag.execute_sequentially(db_path, use_views=cfg.use_views)


def main() -> None:
    """Entrypoint for the duq CLI."""
    cli()


if __name__ == "__main__":
    cli()
