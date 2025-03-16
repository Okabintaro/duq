"""Test the cli of duq."""
from click.testing import CliRunner

from duq.cli import main


def test_jaffle_shop():
    """Test the jaffle shop example."""
    runner = CliRunner()
    result = runner.invoke(main, ["examples/jaffle_shop"])
    assert result.exit_code == 0
    print(result.output)

test_jaffle_shop()
