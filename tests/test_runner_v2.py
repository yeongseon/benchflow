"""Tests for multi-iteration runner with setup/teardown and time-series."""

from __future__ import annotations

import time
from typing import Any

import pytest

from benchflow.core.runner.runner import run_benchmark, run_target
from benchflow.core.scenario.schema import (
    ExperimentConfig,
    LoadConfig,
    Scenario,
    SetupTeardown,
    Step,
    TargetConfig,
    WarmupConfig,
)
from benchflow.workers.protocol import Worker, WorkerFactory, register_worker


# ---------------------------------------------------------------------------
# Mock worker that tracks setup/teardown/introspect calls
# ---------------------------------------------------------------------------

class InstrumentedWorker(Worker):
    """Worker that records all lifecycle calls for testing."""

    # Class-level tracking (shared across instances)
    setup_queries_executed: list[str] = []
    teardown_queries_executed: list[str] = []
    introspect_called: int = 0

    def __init__(self) -> None:
        self.opened = False
        self.closed = False
        self.exec_count = 0

    @classmethod
    def reset_tracking(cls) -> None:
        cls.setup_queries_executed = []
        cls.teardown_queries_executed = []
        cls.introspect_called = 0

    def setup(self, *, dsn: str, worker_config: dict[str, Any], scenario: Any) -> None:
        pass

    def open(self) -> None:
        self.opened = True

    def execute(self, step: Step) -> None:
        self.exec_count += 1
        time.sleep(0.001)

    def execute_raw(self, query: str) -> None:
        InstrumentedWorker.setup_queries_executed.append(query)

    def introspect(self) -> dict[str, Any]:
        InstrumentedWorker.introspect_called += 1
        return {"server_version": "MockDB 1.0"}

    def close(self) -> None:
        self.closed = True


class InstrumentedWorkerFactory(WorkerFactory):
    def create(self, thread_index: int) -> InstrumentedWorker:
        return InstrumentedWorker()


# Register only once — use a unique stack_id to avoid conflicts
register_worker("mock+instrumented", InstrumentedWorkerFactory)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_worker_tracking():
    InstrumentedWorker.reset_tracking()
    yield
    InstrumentedWorker.reset_tracking()


def _make_scenario(
    iterations: int = 1,
    seed: int | None = None,
    setup_queries: list[str] | None = None,
    teardown_queries: list[str] | None = None,
    duration: int = 1,
) -> Scenario:
    return Scenario(
        name="multi-iter-test",
        steps=[Step(name="test-step", query="SELECT 1")],
        load=LoadConfig(concurrency=1, duration=duration, warmup=WarmupConfig(duration=0)),
        experiment=ExperimentConfig(
            iterations=iterations,
            seed=seed,
            pause_between=0.0,  # No pause in tests
        ),
        setup=SetupTeardown(queries=setup_queries) if setup_queries else None,
        teardown=SetupTeardown(queries=teardown_queries) if teardown_queries else None,
        targets=[
            TargetConfig(
                name="mock",
                stack_id="mock+instrumented",
                driver="mock",
                dsn="mock://localhost",
            )
        ],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMultiIterationRunner:
    def test_single_iteration_backward_compat(self):
        """Single iteration should work like before — targets populated, no iterations list."""
        scenario = _make_scenario(iterations=1)
        result = run_benchmark(scenario)

        assert len(result.targets) == 1
        assert result.targets[0].stack_id == "mock+instrumented"
        assert result.targets[0].steps[0].ops > 0
        assert result.iterations == []  # No iteration data for single runs
        assert result.aggregate == []
        assert result.iterations_requested == 1

    def test_multi_iteration_creates_iterations(self):
        """Multiple iterations should populate iterations list and aggregate."""
        scenario = _make_scenario(iterations=3)
        result = run_benchmark(scenario)

        assert result.iterations_requested == 3
        assert len(result.iterations) == 3

        for i, ir in enumerate(result.iterations):
            assert ir.iteration == i
            assert len(ir.targets) == 1
            assert ir.targets[0].steps[0].ops > 0

        # Top-level targets should be from last iteration
        assert len(result.targets) == 1

        # Aggregate should be computed
        assert len(result.aggregate) == 1
        agg = result.aggregate[0]
        assert agg.stack_id == "mock+instrumented"
        assert agg.iterations_completed == 3
        assert len(agg.steps) == 1
        assert agg.steps[0].step_name == "test-step"
        assert agg.steps[0].ops.mean > 0
        assert agg.steps[0].throughput_ops_s.mean > 0

    def test_seed_control(self):
        """Seed should be recorded in iteration results."""
        scenario = _make_scenario(iterations=2, seed=42)
        result = run_benchmark(scenario)

        assert result.experiment_seed == 42
        assert result.iterations[0].seed == 42  # seed + 0
        assert result.iterations[1].seed == 43  # seed + 1

    def test_iterations_override(self):
        """CLI override should take precedence over scenario config."""
        scenario = _make_scenario(iterations=1)
        result = run_benchmark(scenario, iterations_override=2)

        assert result.iterations_requested == 2
        assert len(result.iterations) == 2

    def test_seed_override(self):
        """CLI seed override should take precedence."""
        scenario = _make_scenario(iterations=2, seed=42)
        result = run_benchmark(scenario, seed_override=999)

        assert result.experiment_seed == 999
        assert result.iterations[0].seed == 999
        assert result.iterations[1].seed == 1000


class TestSetupTeardown:
    def test_setup_queries_executed(self):
        scenario = _make_scenario(
            setup_queries=["CREATE TABLE test (id INT)", "INSERT INTO test VALUES (1)"],
        )
        result = run_benchmark(scenario)

        assert "CREATE TABLE test (id INT)" in InstrumentedWorker.setup_queries_executed
        assert "INSERT INTO test VALUES (1)" in InstrumentedWorker.setup_queries_executed

    def test_teardown_queries_executed(self):
        scenario = _make_scenario(
            teardown_queries=["DROP TABLE IF EXISTS test"],
        )
        result = run_benchmark(scenario)

        assert "DROP TABLE IF EXISTS test" in InstrumentedWorker.setup_queries_executed

    def test_setup_teardown_per_iteration(self):
        """Setup and teardown should run for each iteration."""
        scenario = _make_scenario(
            iterations=3,
            setup_queries=["SETUP_QUERY"],
            teardown_queries=["TEARDOWN_QUERY"],
        )
        result = run_benchmark(scenario)

        # 3 iterations × 1 setup query + 3 iterations × 1 teardown query
        setup_count = InstrumentedWorker.setup_queries_executed.count("SETUP_QUERY")
        teardown_count = InstrumentedWorker.setup_queries_executed.count("TEARDOWN_QUERY")
        assert setup_count == 3
        assert teardown_count == 3


class TestTimeSeries:
    def test_time_series_populated(self):
        """Steps should have time-series data with 1-second windows."""
        scenario = _make_scenario(duration=2)
        result = run_benchmark(scenario)

        step = result.targets[0].steps[0]
        assert len(step.time_series) > 0
        # Should have roughly 2 seconds of data
        assert len(step.time_series) >= 1

        for tw in step.time_series:
            assert tw.second >= 0
            assert tw.ops > 0
            if tw.ops > 0:
                assert tw.p50_ns > 0

    def test_time_series_seconds_ordering(self):
        """Time windows should be in order."""
        scenario = _make_scenario(duration=2)
        result = run_benchmark(scenario)

        step = result.targets[0].steps[0]
        seconds = [tw.second for tw in step.time_series]
        assert seconds == sorted(seconds)


class TestEnvironmentCapture:
    def test_schema_version_2(self):
        scenario = _make_scenario()
        result = run_benchmark(scenario)
        assert result.schema_version == 2

    def test_environment_fields_populated(self):
        scenario = _make_scenario()
        result = run_benchmark(scenario)

        assert result.environment.hostname != ""
        assert result.environment.os != ""
        assert result.environment.cpu_count > 0
        assert result.environment.python_version != ""
