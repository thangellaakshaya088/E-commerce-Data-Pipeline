import pytest
import subprocess
import os


DBT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt")


def run_dbt(command: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["dbt"] + command,
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )


@pytest.mark.integration
class TestDbtModels:
    def test_dbt_compile(self):
        result = run_dbt(["compile"])
        assert result.returncode == 0, f"dbt compile failed:\n{result.stderr}"

    def test_dbt_seed(self):
        result = run_dbt(["seed", "--select", "country_codes"])
        assert result.returncode == 0, f"dbt seed failed:\n{result.stderr}"

    def test_dbt_run_staging(self):
        result = run_dbt(["run", "--select", "staging"])
        assert result.returncode == 0, f"dbt run staging failed:\n{result.stderr}"

    def test_dbt_test_staging(self):
        result = run_dbt(["test", "--select", "staging"])
        assert result.returncode == 0, f"dbt test staging failed:\n{result.stderr}"
