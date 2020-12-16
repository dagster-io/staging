from collections import namedtuple

from dagster import check
from dagster.core.definitions.pipeline import PipelineDefinition
from dagster.core.definitions.resource import ResourceDefinition, ScopedResourcesBuilder
from dagster.core.instance import DagsterInstance
from dagster.core.log_manager import DagsterLogManager
from dagster.core.storage.pipeline_run import PipelineRun


class InitResourceContext(
    namedtuple(
        "InitResourceContext",
        "resource_config pipeline_def resource_def pipeline_run log_manager resources instance_for_backwards_compat",
    )
):
    """Resource-specific initialization context.

    Attributes:
        resource_config (Any): The configuration data provided by the environment config. The schema
            for this data is defined by the ``config_field`` argument to
            :py:class:`ResourceDefinition`.
        pipeline_def (PipelineDefinition): The definition of the pipeline currently being executed.
        resource_def (ResourceDefinition): The definition of the resource currently being
            constructed.
        run_id (str): The id for this run of the pipeline.
        log_manager (DagsterLogManager): The log manager for this run of the pipeline
        resources (ScopedResources): The resources that are available to the resource that we are
            initalizing.
    """

    def __new__(
        cls,
        resource_config,
        pipeline_def,
        resource_def,
        pipeline_run,
        log_manager=None,
        resource_instance_dict=None,
        required_resource_keys=None,
        instance_for_backwards_compat=None,
    ):
        check.opt_dict_param(resource_instance_dict, "resource_instance_dict")
        required_resource_keys = check.opt_set_param(
            required_resource_keys, "required_resource_keys"
        )

        scoped_resources_builder = ScopedResourcesBuilder(resource_instance_dict)

        return super(InitResourceContext, cls).__new__(
            cls,
            resource_config,
            check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition),
            check.inst_param(resource_def, "resource_def", ResourceDefinition),
            check.inst_param(pipeline_run, "pipeline_run", PipelineRun),
            check.opt_inst_param(log_manager, "log_manager", DagsterLogManager),
            resources=scoped_resources_builder.build(required_resource_keys),
            # Used internally for adapting intermediate storage defs to resources, for backcompat
            instance_for_backwards_compat=check.opt_inst_param(
                instance_for_backwards_compat, "instance_for_backwards_compat", DagsterInstance
            ),
        )

    @property
    def log(self):
        return self.log_manager

    @property
    def run_id(self):
        return self.pipeline_run.run_id

    def replace_config(self, config):
        return InitResourceContext(
            resource_config=config,
            pipeline_def=self.pipeline_def,
            resource_def=self.resource_def,
            pipeline_run=self.pipeline_run,
            log_manager=self.log_manager,
            instance_for_backwards_compat=self.instance_for_backwards_compat,
        )
