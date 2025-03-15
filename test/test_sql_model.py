"""Test the sql model."""

import pytest

from duq.sql_model import ExpectedSelectError, SqlModel

# ruff: noqa: ANN201

def test_parse_simple():
    """Test parsing a simple SQL model."""
    sql = """
    select
        id as customer_id,
        name as customer_name
    from source;
    """

    model = SqlModel.from_str(sql)
    print(model)

def test_parse_broken():
    """Test parsing a non SELECT model."""
    sql = """
    CREATE TABLE source (
        id INT,
        name VARCHAR(255)
    );
    """

    with pytest.raises(ExpectedSelectError):
        _ = SqlModel.from_str(sql)


test_parse_broken()
