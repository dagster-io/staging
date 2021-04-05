from collections import namedtuple

from dagster import check
from dagster.core.definitions import IPipeline
from dagster.core.instance import DagsterInstance
from dagster.core.storage.pipeline_run import PipelineRun


class InitExecutorContext(
    namedtuple(
        "InitExecutorContext",
        "pipeline pipeline_run executor_config instance",
    )
):
    """Executor-specific initialization context.

    Attributes:
        pipeline (IPipeline): The pipeline to be executed.
        pipeline_run (PipelineRun): Configuration for this pipeline run.
        executor_config (dict): The parsed config passed to the executor.
        instance (DagsterInstance): The current instance.
    """

    def __new__(
        cls,
        pipeline,
        pipeline_run,
        executor_config,
        instance,
    ):
        return super(InitExecutorContext, cls).__new__(
            cls,
            pipeline=check.inst_param(pipeline, "pipeline", IPipeline),
            pipeline_run=check.inst_param(pipeline_run, "pipeline_run", PipelineRun),
            executor_config=check.dict_param(executor_config, executor_config, key_type=str),
            instance=check.inst_param(instance, "instance", DagsterInstance),
        )

    @property
    def pipeline_def(self):
        return self.pipeline.get_definition()
