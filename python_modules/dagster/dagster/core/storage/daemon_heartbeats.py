from abc import ABCMeta, abstractmethod

import six


class DaemonHeartBeatStorage(six.with_metaclass(ABCMeta)):
    """
    Holds heartbeats from the Dagster Daemon so that other sstem components can alert when it's not
    alive.

    This is currently mixed into RunStorage, but this is temporary. Once all metadata storages are
    consolidated it can be a peer to run storage.
    """

    @abstractmethod
    def add_daemon_heartbeat(self):
        """Called on a regular interval by the daemon"""

    @abstractmethod
    def daemon_healthy(self):
        """True if the daemon has posted a heartbeat recently"""
