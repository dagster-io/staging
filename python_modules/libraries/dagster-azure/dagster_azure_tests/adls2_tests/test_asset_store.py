import os

import pytest
from dagster import (
    AssetStoreContext,
    DagsterInstance,
    InputDefinition,
    Int,
    ModeDefinition,
    OutputDefinition,
    PipelineRun,
    lambda_solid,
    pipeline,
)
from dagster.core.events import DagsterEventType
from dagster.core.execution.api import create_execution_plan, execute_plan
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.utils import make_new_run_id
from dagster_azure.adls2.asset_store import PickledObjectADLS2AssetStore, adls2_asset_store
from dagster_azure.adls2.resources import create_adls2_client, create_blob_client


def get_step_output(step_events, step_key, output_name="result"):
    for step_event in step_events:
        if (
            step_event.event_type == DagsterEventType.STEP_OUTPUT
            and step_event.step_key == step_key
            and step_event.step_output_data.output_name == output_name
        ):
            return step_event
    return None


def define_inty_pipeline():
    @lambda_solid(output_def=OutputDefinition(Int, asset_store_key="asset_store"))
    def return_one():
        return 1

    @lambda_solid(
        input_defs=[InputDefinition("num", Int)],
        output_def=OutputDefinition(Int, asset_store_key="asset_store"),
    )
    def add_one(num):
        return num + 1

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"asset_store": adls2_asset_store},)])
    def basic_external_plan_execution():
        add_one(return_one())

    return basic_external_plan_execution


def get_azure_credential():
    try:
        return {"key": os.environ["AZURE_STORAGE_ACCOUNT_KEY"]}
    except KeyError:
        raise Exception("AZURE_STORAGE_ACCOUNT_KEY must be set for asset store tests")


def get_adls2_client(storage_account):
    creds = get_azure_credential()["key"]
    return create_adls2_client(storage_account, creds)


def get_blob_client(storage_account):
    creds = get_azure_credential()["key"]
    return create_blob_client(storage_account, creds)


nettest = pytest.mark.nettest


@nettest
def test_adls2_asset_store_execution(storage_account, file_system):
    pipeline_def = define_inty_pipeline()

    run_config = {
        "resources": {
            "asset_store": {
                "config": {
                    "adls2_file_system": file_system,
                    "storage_account": storage_account,
                    "credential": get_azure_credential(),
                }
            }
        }
    }

    run_id = make_new_run_id()

    execution_plan = create_execution_plan(pipeline_def, run_config=run_config)

    assert execution_plan.get_step_by_key("return_one.compute")

    step_keys = ["return_one.compute"]
    instance = DagsterInstance.ephemeral()
    pipeline_run = PipelineRun(
        pipeline_name=pipeline_def.name, run_id=run_id, run_config=run_config
    )

    return_one_step_events = list(
        execute_plan(
            execution_plan.build_subset_plan(step_keys),
            run_config=run_config,
            pipeline_run=pipeline_run,
            instance=instance,
        )
    )

    assert get_step_output(return_one_step_events, "return_one.compute")

    asset_store = PickledObjectADLS2AssetStore(
        file_system,
        create_adls2_client(storage_account, get_azure_credential()),
        create_blob_client(storage_account, get_azure_credential()),
    )
    step_output_handle = StepOutputHandle("return_one.compute")
    context = AssetStoreContext(
        step_output_handle.step_key,
        step_output_handle.output_name,
        {},
        pipeline_def.name,
        pipeline_def.solid_def_named("return_one"),
        run_id,
    )
    assert asset_store.get_asset(context) == 1

    add_one_step_events = list(
        execute_plan(
            execution_plan.build_subset_plan(["add_one.compute"]),
            run_config=run_config,
            pipeline_run=pipeline_run,
            instance=instance,
        )
    )

    step_output_handle = StepOutputHandle("add_one.compute")
    context = AssetStoreContext(
        step_output_handle.step_key,
        step_output_handle.output_name,
        {},
        pipeline_def.name,
        pipeline_def.solid_def_named("add_one"),
        run_id,
    )

    assert get_step_output(add_one_step_events, "add_one.compute")
    step_output_handle = StepOutputHandle("add_one.compute")
    assert asset_store.get_asset(context) == 2
