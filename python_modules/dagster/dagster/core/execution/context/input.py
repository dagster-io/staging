from typing import TYPE_CHECKING, Any, Dict, NamedTuple, Optional

if TYPE_CHECKING:
    from .output import OutputContext
    from dagster.core.definitions import SolidDefinition
    from dagster.core.log_manager import DagsterLogManager
    from dagster.core.types.dagster_type import DagsterType
    from dagster.core.execution.context.system import StepExecutionContext
    from dagster.core.definitions.resource import Resources


class InputContext(NamedTuple):
    """
    The ``context`` object available to the load_input method of :py:class:`RootInputManager`.

    Attributes:
        name (Optional[str]): The name of the input that we're loading.
        pipeline_name (Optional[str]): The name of the pipeline.
        solid_def (Optional[SolidDefinition]): The definition of the solid that's loading the input.
        config (Optional[Any]): The config attached to the input that we're loading.
        metadata (Optional[Dict[str, Any]]): A dict of metadata that is assigned to the
            InputDefinition that we're loading for.
        upstream_output (Optional[OutputContext]): Info about the output that produced the object
            we're loading.
        dagster_type (Optional[DagsterType]): The type of this input.
        log (Optional[DagsterLogManager]): The log manager to use for this input.
        resource_config (Optional[Dict[str, Any]]): The config associated with the resource that
            initializes the RootInputManager.
        resources (Optional[Resources]): The resources required by the resource that initializes the
            input manager. If using the :py:func:`@root_input_manager` decorator, these resources
            correspond to those requested with the `required_resource_keys` parameter.
    """

    name: Optional[str] = None
    pipeline_name: Optional[str] = None
    solid_def: Optional["SolidDefinition"] = None
    config: Optional[Any] = None
    metadata: Optional[Dict[str, Any]] = None
    upstream_output: Optional["OutputContext"] = None
    dagster_type: Optional["DagsterType"] = None
    log: Optional["DagsterLogManager"] = None
    resource_config: Optional[Dict[str, Any]] = None
    resources: Optional["Resources"] = None
    # step_context is only for internal use to punch through to the top level StepExecutionContext.
    step_context: Optional["StepExecutionContext"] = None
