all: fix check test

fix:
    uv run ruff format src/ tests/
    uv run ruff check --fix src/ tests/

check:
    uv run ruff format --check src/ tests/
    uv run ruff check src/ tests/
    uv run basedpyright src/ tests/

test:
    uv run pytest -v