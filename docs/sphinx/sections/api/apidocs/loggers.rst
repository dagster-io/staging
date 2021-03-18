Loggers
=======

Built-in loggers
----------------
.. currentmodule:: dagster.loggers

.. autodata:: colored_console_logger
  :annotation: LoggerDefinition

.. autodata:: json_console_logger
  :annotation: LoggerDefinition

Logging from a solid
--------------------
.. currentmodule:: dagster

.. autoclass:: DagsterLogManager

Defining custom loggers
-----------------------
.. currentmodule:: dagster

.. autodecorator:: logger

.. autoclass:: LoggerDefinition
    :members: configured

.. autoclass:: InitLoggerContext

Monitoring stdout and stderr
----------------------------
.. currentmodule:: dagster.core.storage.compute_log_manager

.. autoclass:: ComputeLogManager
  :members:

.. autoenum:: ComputeIOType

.. autoclass:: ComputeLogFileData

.. autoclass:: ComputeLogSubscription

.. currentmodule:: dagster.core.storage.local_compute_log_manager

.. autoclass:: LocalComputeLogManager

See also: :py:class:`dagster_aws.s3.S3ComputeLogManager`.
