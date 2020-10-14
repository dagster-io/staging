from dagster import DagsterInstance, execute_pipeline, reexecute_pipeline
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.storage.address_store import AddressStore

from ..pickled_object import model_pipeline


def test_pickled_object():

    instance = DagsterInstance.ephemeral(address_store=AddressStore())

    result = execute_pipeline(model_pipeline, preset="local", instance=instance)

    assert result.success
    assert len(instance.address_store.mapping.keys()) == 3
    _step_output_handle = StepOutputHandle("call_api.compute", "result")
    _address = instance.address_store.mapping[_step_output_handle][0]

    re1_result = reexecute_pipeline(
        model_pipeline,
        result.run_id,
        preset="local",
        instance=instance,
        step_selection=["parse_df.compute"],
    )
    assert re1_result.success
    assert instance.address_store.mapping[_step_output_handle][0] == _address

    re2_result = reexecute_pipeline(
        model_pipeline,
        re1_result.run_id,
        preset="local",
        instance=instance,
        step_selection=["parse_df.compute"],
    )
    assert re2_result.success
    assert instance.address_store.mapping[_step_output_handle][0] == _address
