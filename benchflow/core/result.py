"""Result schema — single source of truth for run, compare, and report."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

import benchflow

# ---------------------------------------------------------------------------
# Metadata models
# ---------------------------------------------------------------------------


class BenchFlowInfo(BaseModel):
    """BenchFlow version metadata."""

    version: str = Field(default_factory=lambda: benchflow.__version__)
    git_sha: str | None = None

    @staticmethod
    def detect_git_sha() -> str | None:
        """Best-effort detection of current git SHA."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        return None


class EnvironmentInfo(BaseModel):
    """Host environment captured at run time."""

    hostname: str = Field(default_factory=lambda: platform.node())
    os: str = Field(default_factory=lambda: f"{platform.system()} {platform.release()}")
    cpu_count: int = Field(default_factory=lambda: os.cpu_count() or 0)
    cpu_model: str | None = None
    memory_gb: float | None = None
    python_version: str = Field(default_factory=lambda: sys.version.split()[0])

    @staticmethod
    def detect_cpu_model() -> str | None:
        """Best-effort CPU model detection."""
        try:
            if platform.system() == "Linux":
                with open("/proc/cpuinfo") as f:
                    for line in f:
                        if line.startswith("model name"):
                            return line.split(":", 1)[1].strip()
            elif platform.system() == "Darwin":
                result = subprocess.run(
                    ["sysctl", "-n", "machdep.cpu.brand_string"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    return result.stdout.strip()
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
            pass
        return None

    @staticmethod
    def detect_memory_gb() -> float | None:
        """Best-effort total memory detection in GB."""
        try:
            if platform.system() == "Linux":
                with open("/proc/meminfo") as f:
                    for line in f:
                        if line.startswith("MemTotal"):
                            kb = int(line.split()[1])
                            return round(kb / 1024 / 1024, 1)
            elif platform.system() == "Darwin":
                result = subprocess.run(
                    ["sysctl", "-n", "hw.memsize"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    return round(int(result.stdout.strip()) / 1024 / 1024 / 1024, 1)
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError, ValueError):
            pass
        return None


class DatabaseInfo(BaseModel):
    """Target database metadata."""

    kind: str  # e.g. "postgres", "mysql", "cubrid"
    server_version: str | None = None
    dsn_redacted: str | None = None
    server_config: dict[str, str] = Field(default_factory=dict)


class StackInfo(BaseModel):
    """Describes a DB access stack (language + driver + ORM)."""

    language: str  # e.g. "python"
    driver: str  # e.g. "psycopg", "pgx"
    orm: str | None = None  # e.g. "sqlalchemy", None for raw driver
    versions: dict[str, str] = Field(default_factory=dict)  # package → version


# ---------------------------------------------------------------------------
# Latency and time-series models
# ---------------------------------------------------------------------------


class LatencySummary(BaseModel):
    """Aggregated latency statistics in nanoseconds."""

    min_ns: int
    max_ns: int
    mean_ns: float
    stdev_ns: float
    p50_ns: float
    p95_ns: float
    p99_ns: float
    p999_ns: float = 0.0
    p9999_ns: float = 0.0


class TimeWindow(BaseModel):
    """One second of time-series data within a step execution."""

    second: int  # 0-indexed offset from run start
    ops: int
    errors: int = 0
    p50_ns: float = 0.0
    p95_ns: float = 0.0
    p99_ns: float = 0.0


# ---------------------------------------------------------------------------
# Step / Error / Target results
# ---------------------------------------------------------------------------


class ErrorSample(BaseModel):
    """A sampled error from a step execution."""

    step: str
    message: str
    code: str | None = None


class StepResult(BaseModel):
    """Result of a single scenario step."""

    name: str
    ops: int  # total operations completed
    errors: int = 0
    latency_summary: LatencySummary
    throughput_ops_s: float
    samples_ns: list[int] = Field(default_factory=list)  # bounded reservoir (max 10k)
    time_series: list[TimeWindow] = Field(default_factory=list)


class ErrorInfo(BaseModel):
    """Aggregated error info for a target run."""

    count_total: int = 0
    sample: list[ErrorSample] = Field(default_factory=list)


class TargetResult(BaseModel):
    """Result for one target stack within a run session."""

    stack_id: str  # e.g. "python+psycopg", "python+sqlalchemy"
    stack: StackInfo
    config: dict[str, Any] = Field(default_factory=dict)
    status: str = "ok"  # "ok" | "failed"
    steps: list[StepResult] = Field(default_factory=list)
    overall: LatencySummary | None = None
    errors: ErrorInfo | None = None
    duration_s: float = 0.0  # actual measured duration


# ---------------------------------------------------------------------------
# Iteration and aggregate models (multi-iteration experiment support)
# ---------------------------------------------------------------------------


class IterationResult(BaseModel):
    """Result of a single iteration within a multi-iteration experiment."""

    iteration: int  # 0-indexed
    seed: int | None = None
    targets: list[TargetResult] = Field(default_factory=list)
    duration_s: float = 0.0


class ConfidenceInterval(BaseModel):
    """Bootstrap confidence interval."""

    low: float
    high: float
    confidence: float = 0.95


class AggregateMetric(BaseModel):
    """Cross-iteration aggregate for a single metric."""

    mean: float
    stdev: float
    cv: float  # coefficient of variation (stdev / mean)
    ci: ConfidenceInterval


class AggregateStepResult(BaseModel):
    """Cross-iteration aggregate for a single step."""

    step_name: str
    ops: AggregateMetric
    throughput_ops_s: AggregateMetric
    p50_ns: AggregateMetric
    p95_ns: AggregateMetric
    p99_ns: AggregateMetric
    p999_ns: AggregateMetric | None = None


class AggregateTargetResult(BaseModel):
    """Cross-iteration aggregate for a single target stack."""

    stack_id: str
    iterations_completed: int
    steps: list[AggregateStepResult] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Scenario reference
# ---------------------------------------------------------------------------


class ScenarioRef(BaseModel):
    """Reference to the scenario used in a run."""

    name: str
    signature: str  # hash of scenario content for comparison
    source_path: str | None = None
    parsed: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Top-level run result
# ---------------------------------------------------------------------------


class RunResult(BaseModel):
    """Top-level result model — one file per benchmark session.

    For single-iteration runs (default), `targets` is populated directly
    and `iterations`/`aggregate` remain empty.

    For multi-iteration experiments, each iteration's results go into
    `iterations`, and `aggregate` holds cross-iteration statistics.
    The top-level `targets` holds the LAST iteration's results for
    backward compatibility.
    """

    schema_version: int = 2
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    benchflow: BenchFlowInfo = Field(default_factory=BenchFlowInfo)
    environment: EnvironmentInfo = Field(default_factory=EnvironmentInfo)
    db: DatabaseInfo
    scenario: ScenarioRef

    # Single-iteration / last-iteration results (backward compat)
    targets: list[TargetResult] = Field(default_factory=list)

    # Multi-iteration experiment data
    iterations: list[IterationResult] = Field(default_factory=list)
    aggregate: list[AggregateTargetResult] = Field(default_factory=list)

    # Experiment metadata
    experiment_seed: int | None = None
    iterations_requested: int = 1

    def save(self, path: str) -> None:
        """Persist result to a JSON file."""
        with open(path, "w") as f:
            json.dump(self.model_dump(), f, indent=2, default=str)

    @classmethod
    def load(cls, path: str) -> RunResult:
        """Load result from a JSON file."""
        with open(path) as f:
            data = json.load(f)
        return cls.model_validate(data)


# ---------------------------------------------------------------------------
# Comparison models
# ---------------------------------------------------------------------------


class ComparisonItem(BaseModel):
    """Comparison between two target results for the same step."""

    stack_id: str
    step: str
    baseline: LatencySummary
    contender: LatencySummary
    p50_ratio: float
    p95_ratio: float
    p99_ratio: float
    throughput_ratio: float
    error_delta: int = 0
    # CI-based significance (populated when multi-iteration data available)
    ratio_ci: ConfidenceInterval | None = None
    significant: bool | None = None  # True if CI excludes 1.0


class CompareResult(BaseModel):
    """Result of comparing two benchmark runs."""

    baseline_run_id: str
    contender_run_id: str
    scenario_name: str
    scenario_match: bool = True  # whether signatures matched
    comparisons: list[ComparisonItem] = Field(default_factory=list)


def compute_scenario_signature(scenario_dict: dict[str, Any]) -> str:
    """Compute a stable hash of scenario content for comparison validation."""
    canonical = json.dumps(scenario_dict, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]
