import json

from benchflow.core.result import (
    CompareResult,
    ComparisonItem,
    DatabaseInfo,
    LatencySummary,
    RunResult,
    ScenarioRef,
    StackInfo,
    StepResult,
    TargetResult,
    compute_scenario_signature,
)


def _make_summary(p50: float = 1000000, p95: float = 2000000, p99: float = 3000000) -> LatencySummary:
    return LatencySummary(
        min_ns=500000,
        max_ns=5000000,
        mean_ns=1500000,
        stdev_ns=800000,
        p50_ns=p50,
        p95_ns=p95,
        p99_ns=p99,
    )


def _make_run_result(run_id: str = "test-run") -> RunResult:
    return RunResult(
        run_id=run_id,
        db=DatabaseInfo(kind="postgres"),
        scenario=ScenarioRef(
            name="test-scenario",
            signature="abc123",
        ),
        targets=[
            TargetResult(
                stack_id="python+psycopg",
                stack=StackInfo(language="python", driver="psycopg"),
                steps=[
                    StepResult(
                        name="point-select",
                        ops=1000,
                        latency_summary=_make_summary(),
                        throughput_ops_s=100.0,
                    )
                ],
                overall=_make_summary(),
            )
        ],
    )


def test_run_result_serialization(tmp_path):
    result = _make_run_result()
    path = str(tmp_path / "result.json")
    result.save(path)

    loaded = RunResult.load(path)
    assert loaded.run_id == "test-run"
    assert loaded.db.kind == "postgres"
    assert len(loaded.targets) == 1
    assert loaded.targets[0].stack_id == "python+psycopg"
    assert loaded.targets[0].steps[0].ops == 1000


def test_scenario_signature_stability():
    d1 = {"name": "test", "steps": [{"query": "SELECT 1"}]}
    d2 = {"steps": [{"query": "SELECT 1"}], "name": "test"}
    assert compute_scenario_signature(d1) == compute_scenario_signature(d2)


def test_scenario_signature_changes():
    d1 = {"name": "test", "steps": [{"query": "SELECT 1"}]}
    d2 = {"name": "test", "steps": [{"query": "SELECT 2"}]}
    assert compute_scenario_signature(d1) != compute_scenario_signature(d2)


def test_compare_result():
    compare = CompareResult(
        baseline_run_id="run-1",
        contender_run_id="run-2",
        scenario_name="test",
        comparisons=[
            ComparisonItem(
                stack_id="python+psycopg",
                step="point-select",
                baseline=_make_summary(1000000, 2000000, 3000000),
                contender=_make_summary(1200000, 2400000, 3600000),
                p50_ratio=1.2,
                p95_ratio=1.2,
                p99_ratio=1.2,
                throughput_ratio=0.833,
            )
        ],
    )
    data = compare.model_dump()
    assert len(data["comparisons"]) == 1
    assert data["comparisons"][0]["p95_ratio"] == 1.2
