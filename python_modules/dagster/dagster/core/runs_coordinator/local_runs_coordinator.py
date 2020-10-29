from dagster import DagsterInstance, check
from dagster.core.host_representation import ExternalPipeline
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus

from .base import RunsCoordinator


class LocalRunsCoordinator(RunsCoordinator):
    """Immediately send runs to the run launcher.
    """

    def initialize(self, instance):
        check.inst_param(instance, "instance", DagsterInstance)

    def enqueue_run(self, instance, pipeline_run, external_pipeline):
        check.inst_param(instance, "instance", DagsterInstance)
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        check.inst_param(external_pipeline, "external_pipeline", ExternalPipeline)
        check.invariant(pipeline_run.status == PipelineRunStatus.NOT_STARTED)

        return instance.launch_run(pipeline_run.run_id, external_pipeline)

    def can_terminate(self, run_id):
        raise NotImplementedError()

    def terminate(self, run_id):
        raise NotImplementedError()
