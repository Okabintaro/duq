"""Test the sql model."""


from pathlib import Path

import pytest

from duq.sql_model import ExpectedSelectError, SqlModel


def test_parse_simple():
    """Test parsing a simple SQL model."""
    sql = """
    SELECT *
    FROM read_csv('raw_customers.csv')
    """

    model = SqlModel.from_str(sql, filepath=Path("raw_customers.sql"))
    assert model.name == "raw_customers"
    assert model.dependencies.keys() == {""}
    assert model.source_sql == sql
    assert model.is_source_model()

def test_parse_with_cte():
    """Test parsing a simple SQL model."""
    sql = """
    with source as (
        select *
        from raw_customers
    )
    select *
    from source
    """

    model = SqlModel.from_str(sql, filepath=Path("stg_customers.sql"))
    assert model.name == "stg_customers"
    assert model.dependencies.keys() == {"raw_customers"}

def test_parse_source_model():
    """Test parsing a simple SQL model."""
    sql = """
    select
        id as customer_id,
        name as customer_name
    from source;
    """

    model = SqlModel.from_str(sql, filepath=Path("test.sql"))
    assert model.name == "test"
    assert model.dependencies.keys() == {"source"}
    assert model.source_sql == sql


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
