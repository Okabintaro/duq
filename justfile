all: fix check test

fix:
    ruff format src/ tests/
    ruff check --fix src/ tests/

check:
    ruff check src/ tests/
    basedpyright src/ tests/

test:
    pytest