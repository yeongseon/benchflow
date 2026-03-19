from benchflow.core.scenario.schema import (
    LoadConfig,
    ParamGenerator,
    Scenario,
    Step,
    TargetConfig,
)


def test_param_generator_random_int():
    params = {"id": "random_int(1, 100)"}
    resolved = ParamGenerator.resolve(params)
    assert 1 <= resolved["id"] <= 100


def test_param_generator_static():
    params = {"name": "hello"}
    resolved = ParamGenerator.resolve(params)
    assert resolved["name"] == "hello"


def test_param_generator_random_choice():
    params = {"color": "random_choice('red', 'blue', 'green')"}
    resolved = ParamGenerator.resolve(params)
    assert resolved["color"] in ("red", "blue", "green")


def test_step_resolve_params():
    step = Step(name="test", query="SELECT 1", params={"id": "random_int(1, 10)"})
    resolved = step.resolve_params()
    assert 1 <= resolved["id"] <= 10


def test_step_resolve_params_none():
    step = Step(name="test", query="SELECT 1")
    assert step.resolve_params() == {}


def test_load_config_defaults():
    config = LoadConfig()
    assert config.concurrency == 1
    assert config.duration == 10
    assert config.warmup.duration == 5


def test_scenario_validation():
    scenario = Scenario(
        name="test",
        steps=[Step(name="s1", query="SELECT 1")],
        load=LoadConfig(concurrency=2, duration=5),
        targets=[
            TargetConfig(
                name="test-target",
                stack_id="python+psycopg",
                driver="psycopg",
                dsn="postgresql://localhost/test",
            )
        ],
    )
    assert scenario.name == "test"
    assert len(scenario.steps) == 1
    assert scenario.load.concurrency == 2
    assert len(scenario.targets) == 1


def test_scenario_must_have_steps():
    import pytest

    with pytest.raises(ValueError, match="at least one step"):
        Scenario(name="empty", steps=[])
