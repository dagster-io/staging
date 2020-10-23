from abc import ABCMeta, abstractmethod

import six


class Queuer(six.with_metaclass(ABCMeta)):
    def initialize(self, instance):
        """
        Perform any initialization that depends on the surrounding DagsterInstance.

        Args:
            instance (DagsterInstance): The instance in which the run has been created.
        """

    @abstractmethod
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
        """Enqueue a new run.
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
