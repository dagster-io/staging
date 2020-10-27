from dagster import DagsterInstance, check
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus
from dagster.serdes import ConfigurableClass, ConfigurableClassData
from dagster.utils.backcompat import experimental

from .base import RunQueuer


class DefaultRunQueuer(RunQueuer, ConfigurableClass):
    """
    Sends runs to the dequeuer process via the run storage. Requires the external process to be
    alive for runs to be launched.
    """

    @experimental
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

    def enqueue_run(
        self, instance, pipeline_run,
    ):
        check.inst_param(instance, "instance", DagsterInstance)
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        check.invariant(pipeline_run.status == PipelineRunStatus.QUEUED)

        # passes to dequeuer
        instance.store_run(pipeline_run)

        return pipeline_run

    def can_terminate(self, run_id):
        raise NotImplementedError()

    def terminate(self, run_id):
        raise NotImplementedError()
