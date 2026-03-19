"""Tests for new schema features: ExperimentConfig, seeded ParamGenerator."""

from __future__ import annotations

import random

import pytest

from benchflow.core.scenario.schema import (
    ExperimentConfig,
    LoadConfig,
    ParamGenerator,
    Scenario,
    Step,
)


class TestExperimentConfig:
    def test_defaults(self):
        config = ExperimentConfig()
        assert config.iterations == 1
        assert config.seed is None
        assert config.pause_between == 5.0

    def test_custom_values(self):
        config = ExperimentConfig(iterations=5, seed=42, pause_between=2.0)
        assert config.iterations == 5
        assert config.seed == 42
        assert config.pause_between == 2.0

    def test_iterations_must_be_positive(self):
        with pytest.raises(ValueError, match="iterations must be >= 1"):
            ExperimentConfig(iterations=0)

    def test_pause_must_be_non_negative(self):
        with pytest.raises(ValueError, match="pause_between must be >= 0"):
            ExperimentConfig(pause_between=-1.0)

    def test_scenario_has_experiment_config(self):
        scenario = Scenario(
            name="test",
            steps=[Step(name="s1", query="SELECT 1")],
        )
        assert scenario.experiment.iterations == 1
        assert scenario.experiment.seed is None

    def test_scenario_with_custom_experiment(self):
        scenario = Scenario(
            name="test",
            steps=[Step(name="s1", query="SELECT 1")],
            experiment=ExperimentConfig(iterations=3, seed=123),
        )
        assert scenario.experiment.iterations == 3
        assert scenario.experiment.seed == 123


class TestSeededParamGenerator:
    def test_resolve_with_rng_reproducible(self):
        params = {"id": "random_int(1, 1000000)"}
        rng1 = random.Random(42)
        rng2 = random.Random(42)
        result1 = ParamGenerator.resolve(params, rng=rng1)
        result2 = ParamGenerator.resolve(params, rng=rng2)
        assert result1["id"] == result2["id"]

    def test_resolve_without_rng_still_works(self):
        params = {"id": "random_int(1, 100)"}
        result = ParamGenerator.resolve(params)
        assert 1 <= result["id"] <= 100

    def test_resolve_choice_with_rng(self):
        params = {"color": "random_choice('red', 'blue', 'green')"}
        rng = random.Random(42)
        result = ParamGenerator.resolve(params, rng=rng)
        assert result["color"] in ("red", "blue", "green")

    def test_step_resolve_params_with_rng(self):
        step = Step(name="test", query="SELECT 1", params={"id": "random_int(1, 10)"})
        rng = random.Random(42)
        resolved = step.resolve_params(rng=rng)
        assert 1 <= resolved["id"] <= 10

    def test_different_seeds_different_results(self):
        """Different seeds should (very likely) produce different values."""
        params = {"id": "random_int(1, 1000000)"}
        rng1 = random.Random(42)
        rng2 = random.Random(999)
        result1 = ParamGenerator.resolve(params, rng=rng1)
        result2 = ParamGenerator.resolve(params, rng=rng2)
        # Technically could be equal, but with range 1-1M it's astronomically unlikely
        assert result1["id"] != result2["id"]
