"""Test the cli of duq."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from duq.cli import cli

example_path = Path(__file__).parent.parent / "examples" / "jaffle_shop"


@pytest.mark.skipif(
    not example_path.exists(),
    reason="Jaffle shop example directory does not exist",
)
def test_jaffle_shop():
    """Test the jaffle shop example."""
    runner = CliRunner()
    model_path = str(example_path)
    result = runner.invoke(cli, [model_path, "plan"])
    assert result.exit_code == 0
    result = runner.invoke(cli, [model_path, "script"])
    assert result.exit_code == 0
    result = runner.invoke(cli, [model_path, "run", ":memory:"])
    assert result.exit_code == 0
