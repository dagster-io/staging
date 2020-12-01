from dagster import check
from dagster.core.definitions import (
    ExecutorDefinition,
    IPipeline,
    IntermediateStorageDefinition,
    ModeDefinition,
)
from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.errors import DagsterSingleProcessOnlyResourceError
from dagster.core.instance import DagsterInstance
from dagster.core.storage.pipeline_run import PipelineRun


def _property_deleted_error_message():
    return (
        "No longer supported on InitExecutor. Use ensure_multiprocess_safe() to "
        "check that this execution is setup to be properly multiprocess."
    )


class InitExecutorContext:
    """Executor-specific initialization context.

    Attributes:
        pipeline (IPipeline): The pipeline to be executed.
        mode_def (ModeDefinition): The mode in which the pipeline is to be executed.
        executor_def (ExecutorDefinition): The definition of the executor currently being
            constructed.
        pipeline_run (PipelineRun): Configuration for this pipeline run.
        environment_config (EnvironmentConfig): The parsed environment configuration for this
            pipeline run.
        executor_config (dict): The parsed config passed to the executor.
        intermediate_storage_def (Optional[IntermediateStorageDefinition]): The intermediate storage definition.
        instance (DagsterInstance): The current instance.
    """

    def __init__(
        self,
        pipeline,
        mode_def,
        executor_def,
        pipeline_run,
        environment_config,
        executor_config,
        instance,
        intermediate_storage_def=None,
    ):
        self.pipeline = check.inst_param(pipeline, "pipeline", IPipeline)
        self.executor_def = check.inst_param(executor_def, "executor_def", ExecutorDefinition)
        self.pipeline_run = check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        self.executor_config = check.dict_param(executor_config, executor_config, key_type=str)
        self._mode_def = check.inst_param(mode_def, "mode_def", ModeDefinition)
        self._intermediate_storage_def = check.opt_inst_param(
            intermediate_storage_def, "intermediate_storage_def", IntermediateStorageDefinition
        )
        self._instance = check.inst_param(instance, "instance", DagsterInstance)
        self._environment_config = environment_config  # make lint be quiet

    @property
    def pipeline_def(self):
        return self.pipeline.get_definition()

    @property
    def instance(self):
        raise NotImplementedError(_property_deleted_error_message())

    @property
    def mode_def(self):
        raise NotImplementedError(_property_deleted_error_message())

    @property
    def intermediate_storage_def(self):
        raise NotImplementedError(_property_deleted_error_message())

    @property
    def environment_config(self):
        raise NotImplementedError(_property_deleted_error_message())

    def ensure_multiprocess_safe(self):
        _check_intra_process_pipeline(self.pipeline)
        _check_non_ephemeral_instance(self._instance)
        _check_persistent_storage_requirement(
            self.pipeline.get_definition(), self._mode_def, self._intermediate_storage_def,
        )


def _check_intra_process_pipeline(pipeline):
    if not isinstance(pipeline, ReconstructablePipeline):
        raise DagsterSingleProcessOnlyResourceError(
            'You have attempted to use an executor that uses multiple processes with the pipeline "{name}" '
            "that is not reconstructable. Pipelines must be loaded in a way that allows dagster to reconstruct "
            "them in a new process. This means: \n"
            "  * using the file, module, or repository.yaml arguments of dagit/dagster-graphql/dagster\n"
            "  * loading the pipeline through the reconstructable() function\n".format(
                name=pipeline.get_definition().name
            )
        )


def _all_outputs_non_mem_asset_stores(pipeline_def, mode_def):
    """Returns true if every output definition in the pipeline uses an asset store that's not
    the mem_asset_store.

    If true, this indicates that it's OK to execute steps in their own processes, because their
    outputs will be available to other processes.
    """
    # pylint: disable=comparison-with-callable
    from dagster.core.storage.asset_store import mem_asset_store

    output_defs = [
        output_def
        for solid_def in pipeline_def.all_solid_defs
        for output_def in solid_def.output_defs
    ]
    for output_def in output_defs:
        if mode_def.resource_defs[output_def.asset_store_key] == mem_asset_store:
            return False

    return True


def _check_persistent_storage_requirement(pipeline_def, mode_def, intermediate_storage_def):
    """We prefer to store outputs with asset stores, but will fall back to intermediate storage
    if an asset store isn't set and will fall back to system storage if neither an asset
    store nor an intermediate storage are set.
    """
    if not (
        _all_outputs_non_mem_asset_stores(pipeline_def, mode_def)
        or (intermediate_storage_def and intermediate_storage_def.is_persistent)
    ):
        raise DagsterSingleProcessOnlyResourceError(
            "You have attempted to use an executor that uses multiple processes, but your pipeline "
            "includes solid outputs that will not be stored somewhere where other processes can"
            "retrieve them. "
            "Please make sure that your pipeline definition includes a ModeDefinition whose "
            'resource_keys assign the "asset_store" key to an AssetStore resource '
            "that stores outputs outside of the process, such as the fs_asset_store."
        )


def _check_non_ephemeral_instance(instance):
    if instance.is_ephemeral:
        raise DagsterSingleProcessOnlyResourceError(
            "You have attempted to use an executor that uses multiple processes with an "
            "ephemeral DagsterInstance. A non-ephemeral instance is needed to coordinate "
            "execution between multiple processes. You can configure your default instance "
            "via $DAGSTER_HOME or ensure a valid one is passed when invoking the python APIs."
        )
