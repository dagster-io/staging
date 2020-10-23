from abc import ABCMeta, abstractmethod

import six


class RunQueuer(six.with_metaclass(ABCMeta)):
    def initialize(self, instance):
        """
        Perform any initialization that depends on the surrounding DagsterInstance.

        Args:
            instance (DagsterInstance): The instance in which the queuer has been created
        """

    @abstractmethod
    def enqueue_run(self, instance, pipeline_run, external_pipeline):
        """
        Enqueue a new run. The run should not exist in the run storage- it will be added by this
        method. Runs passed to this method should be in QUEUED state.

        Args:
            instance (DagsterInstance): The instance in which the run should be created
            pipeline_run (PipelineRun): The run to enqueue
            external_pipeline (ExternalPipeline): The pipeline to enqueue

        Returns:
            PipelineRun: The queued run
        """

    @abstractmethod
    def can_terminate(self, run_id):
        """
        Can this run_id be terminated
        """

    @abstractmethod
    def terminate(self, run_id):
        """
        Terminates a run.

        Returns False is the process was already terminated. Returns true if
        the process was alive and was successfully terminated
        """

    def dispose(self):
        """
        Do any resource cleanup that should happen when the DagsterInstance is
        cleaning itself up.
        """
