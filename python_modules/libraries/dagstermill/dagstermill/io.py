"""We use the IOManager facilities to pass input and output values across the process boundary
to/from the Dagstermill kernel."""

from typing import Any

from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster.core.execution.context.system import (
    InputContext,
    OutputContext,
    SystemStepExecutionContext,
)

from .context import DagstermillRuntimeExecutionContext


def synthetic_output_context(
    solid_context: AbstractComputeExecutionContext,
    step_context: SystemStepExecutionContext,
    output_name: str,
) -> OutputContext:
    step_output = step_context.step.step_output_named(output_name)

    return OutputContext(
        step_key=step_context.step.key,
        name=output_name,
        pipeline_name=solid_context.pipeline_run.pipeline_name,
        run_id=solid_context.run_id,
        metadata=step_output.output_def.metadata,
        # mapping_key would come from DynamicOutput, not clear how we would do that here
        # risk of collision if we map over a dagstermill solid?
        mapping_key=None,
        config=(solid_context.solid_config or {}).get("outputs"),
        solid_def=solid_context.solid_def,
        dagster_type=step_output.output_def.dagster_type,
        log_manager=solid_context.log,
        version=None,
    )


def synthetic_input_context(
    solid_context: AbstractComputeExecutionContext,
    step_context: SystemStepExecutionContext,
    output_name: str,
) -> InputContext:
    step_output = step_context.step.step_output_named(output_name)

    upstream_output = synthetic_output_context(
        solid_context=solid_context, step_context=step_context, output_name=output_name,
    )

    return InputContext(
        name=None,
        pipeline_name=solid_context.pipeline_run.pipeline_name,
        solid_def=None,
        config=(solid_context.solid_config or {}).get("outputs"),
        metadata=step_output.output_def.metadata,
        upstream_output=upstream_output,
        dagster_type=step_output.output_def.dagster_type,
        log_manager=solid_context.log,
        # Rather than construct a synthetic step context (since this is in fact not an input)
        # we pass None
        step_context=None,
    )


def write_notebook_output(
    solid_context: DagstermillRuntimeExecutionContext, output_name: str, value
) -> None:
    output_context = synthetic_output_context(
        solid_context=solid_context,
        step_context=solid_context._step_context,  # pylint: disable=protected-access
        output_name=output_name,
    )
    io_manager_key = solid_context.solid_def.output_def_named(output_name).io_manager_key
    getattr(solid_context.resources, io_manager_key).handle_output(
        context=output_context, obj=value
    )


def read_notebook_output(
    solid_context: AbstractComputeExecutionContext,
    step_context: SystemStepExecutionContext,
    output_name: str,
) -> Any:
    input_context = synthetic_input_context(
        solid_context=solid_context, step_context=step_context, output_name=output_name
    )
    io_manager_key = solid_context.solid_def.output_def_named(output_name).io_manager_key
    return getattr(solid_context.resources, io_manager_key).load_input(context=input_context)
