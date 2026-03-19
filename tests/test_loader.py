from benchflow.core.scenario.loader import load_scenario


def test_load_basic_scenario():
    scenario = load_scenario("scenarios/basic.yaml")
    assert scenario.name == "basic-select"
    assert len(scenario.steps) == 1
    assert scenario.steps[0].name == "point-select"
    assert scenario.load.concurrency == 4
    assert scenario.load.duration == 10
    assert len(scenario.targets) == 2


def test_load_scenario_file_not_found():
    import pytest

    with pytest.raises(FileNotFoundError):
        load_scenario("nonexistent.yaml")
