from dagster import DagsterInstance, check
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.utils.external import get_external_pipeline_from_run

from .base import Queuer


class InstantQueuer(Queuer):
    """Immediately send runs to the run launcher.
    """

    def initialize(self, instance):
        check.inst_param(instance, "instance", DagsterInstance)

    def enqueue_run(self, instance, run):
        check.inst_param(instance, "instance", DagsterInstance)
        check.inst_param(run, "run", PipelineRun)

        external_pipeline = get_external_pipeline_from_run(instance, run)
        return instance.launch_run(run.run_id, external_pipeline)

    def can_terminate(self, run_id):
        raise NotImplementedError()

    def terminate(self, run_id):
        raise NotImplementedError()
