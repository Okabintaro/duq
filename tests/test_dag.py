"""Tests for the DAG creation."""

from pathlib import Path

import pytest

from duq.dag import Dag, ModelName
from duq.sql_model import SqlModel


def test_simple_dag():
    """Test the creation of a simple DAG."""
    sql_a = "SELECT * FROM read_csv('path/to/file.csv')"
    sql_b = "SELECT *, 'test' as new_col FROM model_a;"
    model_a = SqlModel.from_str(sql_a, filepath=Path("model_a.sql"))
    model_b = SqlModel.from_str(sql_b, filepath=Path("model_b.sql"))
    models = [model_a, model_b]

    assert model_a.is_source_model()
    assert not model_b.is_source_model()

    # Create Dag from the sample models
    dag = Dag.from_sql_models(models)

    # Expected graph structure
    expected_graph: dict[ModelName, set[ModelName]] = {
        ModelName("model_a"): set(),
        ModelName("model_b"): {ModelName("model_a")},
    }

    # Assert the graph matches the expected graph
    assert dag.graph == expected_graph
    assert dag.models == {ModelName("model_a"): model_a, ModelName("model_b"): model_b}

    order = dag.topo_sort()
    assert order == [ModelName("model_a"), ModelName("model_b")]


example_path = Path(__file__).parent.parent / Path("examples/jaffle_shop")


@pytest.mark.skipif(
    not example_path.exists(), reason="Jaffle shop example directory does not exist",
)
def test_jaffle_shop():
    """Test the jaffle shop example."""
    assert example_path.exists()
    files = example_path.rglob("*.sql")
    models = [SqlModel.from_file(file) for file in files]
    assert len(models) > 1
    dag = Dag.from_sql_models(models)
    order = dag.topo_sort()
    print(order)
