"""Test runner with a mock worker (no DB required)."""

from __future__ import annotations

import time
from typing import Any

from benchflow.core.runner.runner import run_target
from benchflow.core.scenario.schema import (
    LoadConfig,
    Scenario,
    Step,
    TargetConfig,
    WarmupConfig,
)
from benchflow.workers.protocol import Worker, WorkerFactory, register_worker


class MockWorker(Worker):
    def __init__(self) -> None:
        self.opened = False
        self.closed = False
        self.exec_count = 0

    def setup(self, *, dsn: str, worker_config: dict[str, Any], scenario: Scenario) -> None:
        pass

    def open(self) -> None:
        self.opened = True

    def execute(self, step: Step) -> None:
        self.exec_count += 1
        time.sleep(0.001)

    def close(self) -> None:
        self.closed = True


class MockWorkerFactory(WorkerFactory):
    def create(self, thread_index: int) -> MockWorker:
        return MockWorker()


register_worker("mock+test", MockWorkerFactory)


def test_run_target_basic():
    scenario = Scenario(
        name="mock-test",
        steps=[Step(name="mock-step", query="SELECT 1")],
        load=LoadConfig(concurrency=2, duration=2, warmup=WarmupConfig(duration=1)),
    )
    target = TargetConfig(
        name="mock",
        stack_id="mock+test",
        driver="mock",
        dsn="mock://localhost",
    )

    result = run_target(scenario, target)

    assert result.stack_id == "mock+test"
    assert result.status == "ok"
    assert len(result.steps) == 1
    assert result.steps[0].ops > 0
    assert result.steps[0].latency_summary.p50_ns > 0
    assert result.overall is not None
    assert result.duration_s > 0


def test_run_target_concurrency():
    scenario = Scenario(
        name="concurrency-test",
        steps=[Step(name="fast-step", query="SELECT 1")],
        load=LoadConfig(concurrency=4, duration=1, warmup=WarmupConfig(duration=0)),
    )
    target = TargetConfig(
        name="mock",
        stack_id="mock+test",
        driver="mock",
        dsn="mock://localhost",
    )

    result = run_target(scenario, target)
    assert result.steps[0].ops > 0
