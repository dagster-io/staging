from collections import namedtuple

from dagster import check
from dagster.core.definitions.pipeline_base import IPipeline
from dagster.core.definitions.resource import ResourceDefinition
from dagster.core.log_manager import DagsterLogManager


class InitResourceContext(
    namedtuple("InitResourceContext", "pipeline resource_config resource_def run_id log_manager")
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

    def __new__(cls, pipeline, resource_config, resource_def, run_id, log_manager):
        return super(InitResourceContext, cls).__new__(
            cls,
            check.inst_param(pipeline, "pipeline", IPipeline),
            resource_config,
            check.inst_param(resource_def, "resource_def", ResourceDefinition),
            check.str_param(run_id, "run_id"),
            check.inst_param(log_manager, "log_manager", DagsterLogManager),
        )

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
            run_id=self.run_id,
            log_manager=self.log_manager,
        )
