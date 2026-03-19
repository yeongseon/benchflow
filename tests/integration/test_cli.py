"""Integration tests for the BenchFlow CLI."""

from __future__ import annotations

import json
import os

import pytest

PSYCOPG_DSN = os.environ.get("BENCHFLOW_TEST_DSN", "")

requires_postgres = pytest.mark.skipif(
    not PSYCOPG_DSN,
    reason="BENCHFLOW_TEST_DSN not set — skipping integration tests",
)


@pytest.fixture(scope="module", autouse=True)
def _check_postgres_reachable():
    """Verify PostgreSQL is available."""
    if not PSYCOPG_DSN:
        pytest.skip("BENCHFLOW_TEST_DSN not set")
    try:
        import psycopg

        conn = psycopg.connect(PSYCOPG_DSN, autocommit=True)
        conn.execute("SELECT 1")
        conn.close()
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")


@requires_postgres
class TestCLISmoke:
    """Verify the CLI commands work end-to-end."""

    def test_bench_run_produces_json(self, tmp_path):
        """bench run should produce a valid JSON result file."""
        from typer.testing import CliRunner

        from benchflow.cli.main import app

        # Create a minimal scenario file with the test DSN
        scenario = {
            "name": "cli-smoke",
            "setup": {
                "queries": ["CREATE TABLE IF NOT EXISTS cli_test (id SERIAL PRIMARY KEY, val INT)"]
            },
            "teardown": {"queries": ["DROP TABLE IF EXISTS cli_test"]},
            "steps": [{"name": "select-1", "query": "SELECT 1"}],
            "load": {"concurrency": 1, "duration": 2, "warmup": {"duration": 1}},
            "experiment": {"iterations": 1, "seed": 42, "pause_between": 0.0},
            "targets": [
                {
                    "name": "psycopg-cli",
                    "stack_id": "python+psycopg",
                    "driver": "psycopg",
                    "dsn": PSYCOPG_DSN,
                }
            ],
        }

        import yaml

        scenario_path = tmp_path / "cli_smoke.yaml"
        scenario_path.write_text(yaml.dump(scenario))

        output_path = tmp_path / "result.json"

        runner = CliRunner()
        result = runner.invoke(app, ["run", str(scenario_path), "-o", str(output_path)])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert output_path.exists()

        with open(output_path) as f:
            data = json.load(f)

        assert data["schema_version"] == 2
        assert data["targets"][0]["steps"][0]["ops"] > 0

    def test_bench_report_produces_html(self, tmp_path):
        """bench report should produce an HTML file from a result JSON."""
        from benchflow.core.runner.runner import run_benchmark
        from benchflow.core.scenario.schema import (
            ExperimentConfig,
            LoadConfig,
            Scenario,
            SetupTeardown,
            Step,
            TargetConfig,
            WarmupConfig,
        )

        scenario = Scenario(
            name="report-smoke",
            steps=[Step(name="select-1", query="SELECT 1")],
            load=LoadConfig(concurrency=1, duration=2, warmup=WarmupConfig(duration=1)),
            experiment=ExperimentConfig(iterations=1, seed=42, pause_between=0.0),
            setup=SetupTeardown(
                queries=["CREATE TABLE IF NOT EXISTS report_test (id SERIAL PRIMARY KEY)"]
            ),
            teardown=SetupTeardown(queries=["DROP TABLE IF EXISTS report_test"]),
            targets=[
                TargetConfig(
                    name="psycopg-report",
                    stack_id="python+psycopg",
                    driver="psycopg",
                    dsn=PSYCOPG_DSN,
                )
            ],
        )

        result = run_benchmark(scenario)
        result_path = tmp_path / "result.json"
        result_path.write_text(result.model_dump_json(indent=2))

        from typer.testing import CliRunner

        from benchflow.cli.main import app

        html_path = tmp_path / "report.html"
        runner = CliRunner()
        cli_result = runner.invoke(app, ["report", str(result_path), "-o", str(html_path)])

        assert cli_result.exit_code == 0, f"CLI failed: {cli_result.output}"
        assert html_path.exists()

        html_content = html_path.read_text()
        assert "<html" in html_content
        assert "report-smoke" in html_content
