from collections import namedtuple

from dagster import check
from dagster.core.definitions.pipeline_base import IPipeline
from dagster.core.definitions.resource import IResourceDefinition
from dagster.core.instance import DagsterInstance
from dagster.core.log_manager import DagsterLogManager
from dagster.core.storage.pipeline_run import PipelineRun


def _property_deleted_error_message():
    return (
        "No longer supported on InitExecutor. Use ensure_multiprocess_safe() to "
        "check that this execution is setup to be properly multiprocess."
    )


class InitResourceContext(
    namedtuple(
        "InitResourceContext",
        "pipeline resource_config resource_def pipeline_run log_manager instance",
    )
):
    """Resource-specific initialization context.

    Attributes:
        pipeline (IPipeline): The pipeline currently being executed.
        resource_config (Any): The configuration data provided by the environment config. The schema
            for this data is defined by the ``config_field`` argument to
            :py:class:`ResourceDefinition`.
        resource_def (ResourceDefinition): The definition of the resource currently being
            constructed.
        run_id (str): The id for this run of the pipeline.
        log_manager (DagsterLogManager): The log manager for this run of the pipeline
    """

    def __new__(cls, pipeline, resource_config, resource_def, pipeline_run, log_manager, instance):
        return super(InitResourceContext, cls).__new__(
            cls,
            check.inst_param(pipeline, "pipeline", IPipeline),
            resource_config,
            check.inst_param(resource_def, "resource_def", IResourceDefinition),
            check.inst_param(pipeline_run, "pipeline_run", PipelineRun),
            check.inst_param(log_manager, "log_manager", DagsterLogManager),
            check.inst_param(instance, "instance", DagsterInstance),
        )

    @property
    def run_id(self):
        return self.pipeline_run.run_id

    @property
    def pipeline_def(self):
        return self.pipeline.get_definition()

    @property
    def log(self):
        return self.log_manager

    def replace_config(self, config):
        return InitResourceContext(
            pipeline=self.pipeline,
            resource_config=config,
            resource_def=self.resource_def,
            pipeline_run=self.pipeline_run,
            log_manager=self.log_manager,
            instance=self.instance,
        )

    @property
    def mode_def(self):
        raise NotImplementedError(_property_deleted_error_message())

    @property
    def intermediate_storage_def(self):
        raise NotImplementedError(_property_deleted_error_message())

    @property
    def environment_config(self):
        raise NotImplementedError(_property_deleted_error_message())

    # TODO rename all callsites and deprecate
    @property
    def executor_config(self):
        raise Exception("TODO")
