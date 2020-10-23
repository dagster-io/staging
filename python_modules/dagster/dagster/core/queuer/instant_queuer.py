from dagster import DagsterInstance, check
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus
from dagster.utils.external import get_external_pipeline_from_run

from .base import RunQueuer


class InstantRunQueuer(RunQueuer):
    """Immediately send runs to the run launcher.
    """

    def initialize(self, instance):
        check.inst_param(instance, "instance", DagsterInstance)

    def enqueue_run(
        self, instance, pipeline_run,
    ):
        check.inst_param(instance, "instance", DagsterInstance)
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        check.invariant(pipeline_run.status == PipelineRunStatus.QUEUED)

        dequeued_run = pipeline_run.with_status(PipelineRunStatus.NOT_STARTED)
        external_pipeline = get_external_pipeline_from_run(instance, dequeued_run)

        instance.store_run(dequeued_run)
        return instance.launch_run(dequeued_run.run_id, external_pipeline)

    def can_terminate(self, run_id):
        raise NotImplementedError()

    def terminate(self, run_id):
        raise NotImplementedError()
