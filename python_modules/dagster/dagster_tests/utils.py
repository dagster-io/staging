from dagster import DagsterInstance, Shape, check
from dagster.core.host_representation import ExternalPipeline
from dagster.core.launcher import RunLauncher
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.serdes import ConfigurableClass


class InMemoryRunLauncher(RunLauncher, ConfigurableClass):
    def __init__(self, inst_data=None):
        self._inst_data = inst_data
        self._queue = []

    def launch_run(self, instance, run, external_pipeline):
        check.inst_param(instance, "instance", DagsterInstance)
        check.inst_param(run, "run", PipelineRun)
        check.inst_param(external_pipeline, "external_pipeline", ExternalPipeline)
        self._queue.append(run)
        return run

    def queue(self):
        return self._queue

    @classmethod
    def config_type(cls):
        return Shape({})

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data,)

    @property
    def inst_data(self):
        return self._inst_data

    def can_terminate(self, run_id):
        return False

    def terminate(self, run_id):
        check.not_implemented("Termintation not supported")
