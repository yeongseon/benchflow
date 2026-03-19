"""Scenario schema — YAML-defined benchmark scenarios with Pydantic validation."""

from __future__ import annotations

import random
from typing import Any

from pydantic import BaseModel, Field, field_validator


class ParamGenerator:
    """Generates parameter values from DSL expressions like random_int(1, 100000)."""

    @staticmethod
    def resolve(params: dict[str, Any], rng: random.Random | None = None) -> dict[str, Any]:
        """Resolve parameter expressions to concrete values.

        Args:
            params: Raw parameter dict with DSL expressions.
            rng: Optional seeded Random instance for reproducibility.
                 Falls back to module-level random if not provided.
        """
        _rng = rng or random
        resolved = {}
        for key, value in params.items():
            if isinstance(value, str) and value.startswith("random_int("):
                args = value[len("random_int(") : -1]
                low, high = (int(x.strip()) for x in args.split(","))
                resolved[key] = _rng.randint(low, high)
            elif isinstance(value, str) and value.startswith("random_choice("):
                args = value[len("random_choice(") : -1]
                choices = [x.strip().strip("'\"") for x in args.split(",")]
                resolved[key] = _rng.choice(choices)
            else:
                resolved[key] = value
        return resolved


class Step(BaseModel):
    name: str
    query: str
    params: dict[str, Any] | None = None

    def resolve_params(self, rng: random.Random | None = None) -> dict[str, Any]:
        if self.params is None:
            return {}
        return ParamGenerator.resolve(self.params, rng=rng)


class SetupTeardown(BaseModel):
    queries: list[str] = Field(default_factory=list)


class WarmupConfig(BaseModel):
    duration: int = 5  # seconds


class LoadConfig(BaseModel):
    concurrency: int = 1
    duration: int = 10  # seconds
    warmup: WarmupConfig = Field(default_factory=WarmupConfig)

    @field_validator("concurrency")
    @classmethod
    def concurrency_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("concurrency must be >= 1")
        return v

    @field_validator("duration")
    @classmethod
    def duration_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("duration must be >= 1")
        return v


class TargetConfig(BaseModel):
    name: str  # display name: "psycopg-raw", "sqlalchemy-orm"
    stack_id: str  # stable key: "python+psycopg"
    language: str = "python"
    driver: str  # "psycopg", "sqlalchemy"
    orm: str | None = None
    dsn: str
    worker_config: dict[str, Any] = Field(default_factory=dict)


class ExperimentConfig(BaseModel):
    """Controls multi-iteration experiment behavior."""

    iterations: int = 1
    seed: int | None = None
    pause_between: float = 5.0  # seconds between iterations

    @field_validator("iterations")
    @classmethod
    def iterations_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("iterations must be >= 1")
        return v

    @field_validator("pause_between")
    @classmethod
    def pause_must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("pause_between must be >= 0")
        return v


class Scenario(BaseModel):
    name: str
    description: str | None = None
    setup: SetupTeardown | None = None
    teardown: SetupTeardown | None = None
    steps: list[Step]
    load: LoadConfig = Field(default_factory=LoadConfig)
    experiment: ExperimentConfig = Field(default_factory=ExperimentConfig)
    targets: list[TargetConfig] = Field(default_factory=list)

    @field_validator("steps")
    @classmethod
    def must_have_steps(cls, v: list[Step]) -> list[Step]:
        if not v:
            raise ValueError("scenario must have at least one step")
        return v
