"""Tests for new result models: IterationResult, AggregateResult, TimeWindow, etc."""

from __future__ import annotations

from benchflow.core.result import (
    AggregateMetric,
    AggregateStepResult,
    AggregateTargetResult,
    ConfidenceInterval,
    DatabaseInfo,
    EnvironmentInfo,
    IterationResult,
    LatencySummary,
    RunResult,
    ScenarioRef,
    StackInfo,
    StepResult,
    TargetResult,
    TimeWindow,
)


def _make_summary(
    p50: float = 1000000,
    p95: float = 2000000,
    p99: float = 3000000,
) -> LatencySummary:
    return LatencySummary(
        min_ns=500000,
        max_ns=5000000,
        mean_ns=1500000,
        stdev_ns=800000,
        p50_ns=p50,
        p95_ns=p95,
        p99_ns=p99,
        p999_ns=p99 * 1.1,
        p9999_ns=p99 * 1.2,
    )


class TestLatencySummaryExtended:
    def test_p999_p9999_fields(self):
        ls = _make_summary()
        assert ls.p999_ns > 0
        assert ls.p9999_ns > 0

    def test_backward_compat_defaults(self):
        """Old-style summary without p999/p9999 should default to 0."""
        ls = LatencySummary(
            min_ns=100,
            max_ns=1000,
            mean_ns=500,
            stdev_ns=200,
            p50_ns=500,
            p95_ns=900,
            p99_ns=990,
        )
        assert ls.p999_ns == 0.0
        assert ls.p9999_ns == 0.0


class TestTimeWindow:
    def test_basic(self):
        tw = TimeWindow(second=0, ops=100, errors=2, p50_ns=500.0, p95_ns=900.0, p99_ns=990.0)
        assert tw.second == 0
        assert tw.ops == 100
        assert tw.errors == 2

    def test_defaults(self):
        tw = TimeWindow(second=5, ops=50)
        assert tw.errors == 0
        assert tw.p50_ns == 0.0


class TestIterationResult:
    def test_basic(self):
        ir = IterationResult(
            iteration=0,
            seed=42,
            targets=[
                TargetResult(
                    stack_id="python+psycopg",
                    stack=StackInfo(language="python", driver="psycopg"),
                    steps=[
                        StepResult(
                            name="select",
                            ops=1000,
                            latency_summary=_make_summary(),
                            throughput_ops_s=100.0,
                        )
                    ],
                )
            ],
            duration_s=10.5,
        )
        assert ir.iteration == 0
        assert ir.seed == 42
        assert len(ir.targets) == 1
        assert ir.duration_s == 10.5


class TestAggregateModels:
    def test_confidence_interval(self):
        ci = ConfidenceInterval(low=1.0, high=2.0, confidence=0.95)
        assert ci.low == 1.0
        assert ci.high == 2.0
        assert ci.confidence == 0.95

    def test_aggregate_metric(self):
        am = AggregateMetric(
            mean=100.0,
            stdev=10.0,
            cv=0.1,
            ci=ConfidenceInterval(low=90.0, high=110.0),
        )
        assert am.cv == 0.1

    def test_aggregate_step_result(self):
        ci = ConfidenceInterval(low=0.0, high=0.0)
        am = AggregateMetric(mean=0.0, stdev=0.0, cv=0.0, ci=ci)
        asr = AggregateStepResult(
            step_name="select",
            ops=am,
            throughput_ops_s=am,
            p50_ns=am,
            p95_ns=am,
            p99_ns=am,
        )
        assert asr.step_name == "select"
        assert asr.p999_ns is None

    def test_aggregate_target_result(self):
        atr = AggregateTargetResult(
            stack_id="python+psycopg",
            iterations_completed=5,
        )
        assert atr.iterations_completed == 5
        assert atr.steps == []


class TestRunResultV2:
    def test_schema_version(self):
        rr = RunResult(
            db=DatabaseInfo(kind="postgres"),
            scenario=ScenarioRef(name="test", signature="abc"),
        )
        assert rr.schema_version == 2

    def test_iterations_field(self):
        rr = RunResult(
            db=DatabaseInfo(kind="postgres"),
            scenario=ScenarioRef(name="test", signature="abc"),
            iterations_requested=3,
            experiment_seed=42,
        )
        assert rr.iterations_requested == 3
        assert rr.experiment_seed == 42
        assert rr.iterations == []
        assert rr.aggregate == []

    def test_serialization_roundtrip(self, tmp_path):
        rr = RunResult(
            db=DatabaseInfo(
                kind="postgres",
                server_version="PostgreSQL 16.2",
                server_config={"shared_buffers": "128MB"},
            ),
            scenario=ScenarioRef(name="test", signature="abc"),
            iterations_requested=2,
            experiment_seed=42,
            iterations=[
                IterationResult(
                    iteration=0,
                    seed=42,
                    targets=[
                        TargetResult(
                            stack_id="python+psycopg",
                            stack=StackInfo(language="python", driver="psycopg"),
                            steps=[
                                StepResult(
                                    name="select",
                                    ops=1000,
                                    latency_summary=_make_summary(),
                                    throughput_ops_s=100.0,
                                    time_series=[
                                        TimeWindow(
                                            second=0,
                                            ops=50,
                                            p50_ns=500.0,
                                            p95_ns=900.0,
                                            p99_ns=990.0,
                                        ),
                                        TimeWindow(
                                            second=1,
                                            ops=50,
                                            p50_ns=510.0,
                                            p95_ns=920.0,
                                            p99_ns=995.0,
                                        ),
                                    ],
                                )
                            ],
                        )
                    ],
                ),
            ],
        )

        path = str(tmp_path / "result.json")
        rr.save(path)
        loaded = RunResult.load(path)

        assert loaded.schema_version == 2
        assert loaded.iterations_requested == 2
        assert loaded.experiment_seed == 42
        assert len(loaded.iterations) == 1
        assert loaded.iterations[0].seed == 42
        assert len(loaded.iterations[0].targets[0].steps[0].time_series) == 2
        assert loaded.db.server_config == {"shared_buffers": "128MB"}


class TestEnvironmentInfoDetection:
    def test_detect_cpu_model_doesnt_crash(self):
        """Should return a string or None, never crash."""
        result = EnvironmentInfo.detect_cpu_model()
        assert result is None or isinstance(result, str)

    def test_detect_memory_gb_doesnt_crash(self):
        result = EnvironmentInfo.detect_memory_gb()
        assert result is None or isinstance(result, float)


class TestDatabaseInfoExtended:
    def test_server_config_field(self):
        db = DatabaseInfo(
            kind="postgres",
            server_config={"shared_buffers": "128MB", "work_mem": "4MB"},
        )
        assert db.server_config["shared_buffers"] == "128MB"

    def test_server_config_default_empty(self):
        db = DatabaseInfo(kind="postgres")
        assert db.server_config == {}
