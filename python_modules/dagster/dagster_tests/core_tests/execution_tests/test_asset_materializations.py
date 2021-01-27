import pytest
from dagster import (
    AssetMaterialization,
    DagsterInvariantViolationError,
    Output,
    execute_solid,
    solid,
)


def test_output_name():
    @solid
    def my_solid(_):
        yield AssetMaterialization(asset_key=["a"], output_name="result")
        yield Output(1)

    result = execute_solid(my_solid)
    events = result.step_events
    asset_events = [event for event in events if event.event_type_value == "STEP_MATERIALIZATION"]
    assert len(asset_events) == 1
    assert asset_events[0].event_specific_data.output_name == "result"


def test_no_output_name():
    @solid
    def my_solid(_):
        yield AssetMaterialization(asset_key=["a"])
        yield Output(1)

    result = execute_solid(my_solid)
    events = result.step_events
    asset_events = [event for event in events if event.event_type_value == "STEP_MATERIALIZATION"]
    assert len(asset_events) == 1
    assert asset_events[0].event_specific_data.output_name is None


def test_output_name_no_corresponding_output():
    @solid
    def my_solid(_):
        yield AssetMaterialization(asset_key=["a"], output_name="nonexistent_output")
        yield Output(1)

    with pytest.raises(DagsterInvariantViolationError):
        execute_solid(my_solid)
