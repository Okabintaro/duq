
check:
    ruff check src/ tests/
    basedpyright src/ tests/

test:
    pytest