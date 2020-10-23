from dagster import DagsterInstance, check
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.utils.external import get_external_pipeline_from_run

from .base import Queuer


class InstantQueuer(Queuer):
    """Immediately send runs to the run launcher.
    """

    def initialize(self, instance):
        check.inst_param(instance, "instance", DagsterInstance)

    def enqueue_run(
        self,
        instance,
        pipeline_name,
        run_id,
        run_config,
        mode,
        solids_to_execute,
        step_keys_to_execute,
        tags,
        root_run_id,
        parent_run_id,
        pipeline_snapshot,
        execution_plan_snapshot,
        parent_pipeline_snapshot,
        pipeline_origin,
        solid_selection=None,
    ):
        check.inst_param(instance, "instance", DagsterInstance)

        run = instance.create_run(
            pipeline_name,
            run_id,
            run_config,
            mode,
            solids_to_execute,
            step_keys_to_execute,
            tags,
            root_run_id,
            parent_run_id,
            pipeline_snapshot,
            execution_plan_snapshot,
            parent_pipeline_snapshot,
            status=PipelineRunStatus.NOT_STARTED,
            solid_selection=None,
            pipeline_origin=pipeline_origin,
        )

        external_pipeline = get_external_pipeline_from_run(instance, run)
        return instance.launch_run(run.run_id, external_pipeline)

    def can_terminate(self, run_id):
        raise NotImplementedError()

    def terminate(self, run_id):
        raise NotImplementedError()
