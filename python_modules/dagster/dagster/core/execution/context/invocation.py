# pylint: disable=super-init-not-called
from typing import Any, Dict, Optional

from dagster import check
from dagster.core.definitions.dependency import Solid, SolidHandle
from dagster.core.definitions.mode import ModeDefinition
from dagster.core.definitions.pipeline import PipelineDefinition
from dagster.core.definitions.resource import Resources
from dagster.core.definitions.solid import SolidDefinition
from dagster.core.definitions.step_launcher import StepLauncher
from dagster.core.errors import DagsterInvalidPropertyError
from dagster.core.execution.build_resources import build_resources
from dagster.core.instance import DagsterInstance
from dagster.core.log_manager import DagsterLogManager
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.utils.forked_pdb import ForkedPdb

from .compute import SolidExecutionContext
from .system import StepExecutionContext


def _property_msg(prop_name: str, method_name: str) -> str:
    return (
        f"The {prop_name} {method_name} is not set on the context when a solid is directly invoked."
    )


class DirectSolidExecutionContext(SolidExecutionContext):
    """The ``context`` object available as the first argument to a solid's compute function when
    being invoked directly.
    """

    def __init__(
        self, solid_config: Any, resources: Optional[Resources], instance: Optional[DagsterInstance]
    ):  # pylint: disable=super-init-not-called
        from dagster.core.execution.context_creation_pipeline import initialize_console_manager

        self._solid_config = solid_config
        self._resources = resources
        self._instance = instance
        self._log = initialize_console_manager(None)
        self._pdb: Optional[ForkedPdb] = None

    @property
    def solid_config(self) -> Any:
        return self._solid_config

    @property
    def resources(self) -> Resources:
        if not self._resources:
            raise DagsterInvalidPropertyError(
                "Attempted to access resources on context without providing context."
            )
        return self._resources

    @property
    def pipeline_run(self) -> PipelineRun:
        raise DagsterInvalidPropertyError(_property_msg("pipeline_run", "property"))

    @property
    def instance(self) -> DagsterInstance:
        if not self._instance:
            raise DagsterInvalidPropertyError(
                "Attempted to access instance on context without providing context."
            )
        return self._instance

    @property
    def pdb(self) -> ForkedPdb:
        """dagster.utils.forked_pdb.ForkedPdb: Gives access to pdb debugging from within the solid.

        Example:

        .. code-block:: python

            @solid
            def debug_solid(context):
                context.pdb.set_trace()

        """
        if self._pdb is None:
            self._pdb = ForkedPdb()

        return self._pdb

    @property
    def step_launcher(self) -> Optional[StepLauncher]:
        raise DagsterInvalidPropertyError(_property_msg("step_launcher", "property"))

    @property
    def run_id(self) -> str:
        """str: Hard-coded value to indicate that we are directly invoking solid."""
        return "EPHEMERAL"

    @property
    def run_config(self) -> dict:
        raise DagsterInvalidPropertyError(_property_msg("run_config", "property"))

    @property
    def pipeline_def(self) -> PipelineDefinition:
        raise DagsterInvalidPropertyError(_property_msg("pipeline_def", "property"))

    @property
    def pipeline_name(self) -> str:
        raise DagsterInvalidPropertyError(_property_msg("pipeline_name", "property"))

    @property
    def mode_def(self) -> ModeDefinition:
        raise DagsterInvalidPropertyError(_property_msg("mode_def", "property"))

    @property
    def log(self) -> DagsterLogManager:
        """DagsterLogManager: A console manager constructed for this context."""
        return self._log

    @property
    def solid_handle(self) -> SolidHandle:
        raise DagsterInvalidPropertyError(_property_msg("solid_handle", "property"))

    @property
    def solid(self) -> Solid:
        raise DagsterInvalidPropertyError(_property_msg("solid", "property"))

    @property
    def solid_def(self) -> SolidDefinition:
        raise DagsterInvalidPropertyError(_property_msg("solid_def", "property"))

    def has_tag(self, key: str) -> bool:
        raise DagsterInvalidPropertyError(_property_msg("has_tag", "method"))

    def get_tag(self, key: str) -> str:
        raise DagsterInvalidPropertyError(_property_msg("get_tag", "method"))

    def get_step_execution_context(self) -> StepExecutionContext:
        raise DagsterInvalidPropertyError(_property_msg("get_step_execution_context", "methods"))


class SolidExecutionContextManager(DirectSolidExecutionContext):
    def __init__(
        self, resources: Dict[str, Any], solid_config: Any, instance: Optional[DagsterInstance]
    ):
        from dagster.core.execution.api import ephemeral_instance_if_missing

        self._instance_provided = not instance
        self._instance_cm = ephemeral_instance_if_missing(instance)
        # Pylint can't infer that the ephemeral_instance context manager has an __enter__ method,
        # so ignore lint error
        instance = self._instance_cm.__enter__()  # pylint: disable=no-member

        self._resources_cm = build_resources(resources, instance)
        self._solid_execution_context = DirectSolidExecutionContext(
            solid_config, self._resources_cm, instance
        )
        self._cm_scope_entered = False

    def __enter__(self):
        self._cm_scope_entered = True
        self._resources_cm.__enter__()
        return self._solid_execution_context

    def __exit__(self, *exc):
        self._resources_cm.__exit__(*exc)
        if self._instance_provided:
            self._instance_cm.__exit__(*exc)  # pylint: disable=no-member

    def __del__(self):
        self._resources_cm.__del__()
        if self._instance_provided and not self._cm_scope_entered:
            self._instance_cm.__exit__(None, None, None)  # pylint: disable=no-member

    def __getattr__(self, attr):
        return self._solid_execution_context.__getattribute__(attr)


def get_solid_execution_context(
    resources: Optional[Dict[str, Any]] = None,
    solid_config: Optional[Any] = None,
    instance: Optional[DagsterInstance] = None,
) -> DirectSolidExecutionContext:
    return SolidExecutionContextManager(
        resources=check.opt_dict_param(resources, "resources", key_type=str),
        solid_config=solid_config,
        instance=check.opt_inst_param(instance, "instance", DagsterInstance),
    )
