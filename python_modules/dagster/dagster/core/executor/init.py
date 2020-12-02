from dagster import check
from dagster.core.definitions import ExecutorDefinition, IPipeline
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
        executor_def (ExecutorDefinition): The definition of the executor currently being constructed.
        pipeline_run (PipelineRun): Configuration for this pipeline run.
        executor_config (dict): The parsed config passed to the executor.
        instance (DagsterInstance): The current instance.
    """

    def __init__(self, pipeline, executor_def, pipeline_run, executor_config, instance):
        self.pipeline = check.inst_param(pipeline, "pipeline", IPipeline)
        self.executor_def = check.inst_param(executor_def, "executor_def", ExecutorDefinition)
        self.pipeline_run = check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        self.executor_config = check.dict_param(executor_config, executor_config, key_type=str)
        self._instance = check.inst_param(instance, "instance", DagsterInstance)

    @property
    def pipeline_def(self):
        return self.pipeline.get_definition()

    @property
    def instance(self):
        return self._instance

    @property
    def mode_def(self):
        raise NotImplementedError(_property_deleted_error_message())

    @property
    def intermediate_storage_def(self):
        raise NotImplementedError(_property_deleted_error_message())

    @property
    def environment_config(self):
        raise NotImplementedError(_property_deleted_error_message())
