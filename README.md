# Duq

Small DuckDB Orchestrator, inspired by [SQLMesh], [yato] and [crabwalk].

## Features

- Parse SQL files into a dag, similar to [yato] and [sqlmesh]
- Execute the dag either sequentially or in parallel
    - [DuckDB supports parallel execution of queries when the database is opened by single process](https://duckdb.org/docs/stable/connect/concurrency.html#concurrency-within-a-single-process)
        - It seems to work, but only seems to gives speedups for io bound queries like network requests
        - I observed no speedup for the jaffle shop example
- Minimal dependencies
    - You don't even need the python duckdb package, since duq can generate a script that is executable by the duckdb cli
        - For the parallel execution, you need to install the duckdb package though
- Clean/[rust like code](https://news.ycombinator.com/item?id=36018621)

## Future Plans / Ideas

- Creating documentation with column level lineage
    - sqlglot already has an [implementation](https://sqlglot.com/sqlglot/lineage.html) which was mentioned in the primer
- Transforming comments into `DESCRIBE TABLE, COLUMN` etc
- Python Models
- Minimal state tracking for backfill

## Motivation

This is a weekend learning project inspired by [yato].
I had fun learning how [sqlglot] works using the [primer] and consider it a very good and useful library for data engineering.

## Usage

```shell
# View the execution plan without running it
duq /path/to/sql/models plan

# Generate a SQL script for DuckDB
duq /path/to/sql/models script

# Execute the SQL models sequentially
duq /path/to/sql/models run /path/to/database.ddb

# Execute the SQL models in parallel
duq /path/to/sql/models run /path/to/database.ddb --parallel

# Create views instead of tables
duq /path/to/sql/models --views run /path/to/database.ddb
```

## Development

I am using [uv], [ruff], [basedpyright] and [pytest] for development.
Take a look at the [justfile](./justfile) for a how to lint and test the code.

## Examples

The examples/jaffle_shop directory referenced in tests is not included in this repository due to its size. You can download it from the yato repository:
https://github.com/Bl3f/yato/tree/main/examples/jaffle_shop


[uv]: https://github.com/astral-sh/uv
[ruff]: https://github.com/astral-sh/ruff
[pytest]: https://github.com/pytest-dev/pytest
[basedpyright]: https://github.com/detachhead/basedpyright
[sqlglot]: https://sqlglot.com/sqlglot.html
[yato]: https://github.com/Bl3f/yato
[SQLMesh]: https://github.com/TobikoData/sqlmesh
[crabwalk]: https://github.com/definite-app/crabwalk
[primer]: https://github.com/tobymao/sqlglot/blob/main/posts/ast_primer.md]
