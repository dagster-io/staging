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
    def enqueue_run(self, instance, run):
        """Enqueue a run.
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
