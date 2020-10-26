from dagster import DagsterInstance, check
from dagster.core.host_representation import ExternalPipeline
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus
from dagster.serdes import ConfigurableClass, ConfigurableClassData
from dagster.utils.external import get_external_pipeline_from_run

from .base import RunQueuer


class InstantRunQueuer(RunQueuer, ConfigurableClass):
    """Immediately send runs to the run launcher.
    """

    def __init__(self, inst_data=None):
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    def initialize(self, instance):
        check.inst_param(instance, "instance", DagsterInstance)

    def enqueue_run(self, instance, pipeline_run, external_pipeline):
        check.inst_param(instance, "instance", DagsterInstance)
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        check.inst_param(external_pipeline, "external_pipeline", ExternalPipeline)

        check.invariant(pipeline_run.status == PipelineRunStatus.QUEUED)

        dequeued_run = pipeline_run.with_status(PipelineRunStatus.NOT_STARTED)
        instance.store_run(dequeued_run)
        return instance.launch_run(dequeued_run.run_id, external_pipeline)

    def can_terminate(self, run_id):
        raise NotImplementedError()

    def terminate(self, run_id):
        raise NotImplementedError()
