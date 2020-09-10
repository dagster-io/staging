Dask (dagster_dask)
-------------------

The Dagster dask integration serves two separate purposes:

1) Running the orchestration cluster on Dask. This means that the graph of Dagster steps are orchestrated using Dask.

For running dask for the orchestration cluster, see: `Dask deployment guide <https://docs.dagster.io/deploying/dask/>`_.

2) Using Dask as a compute substrate. This does *not* require using Dask for orchestration. One could, for example, use Kubernetes and Celery to run the orchestration cluster and use a Dask cluster to execute your business logic. Use `dask_resource` for this.


Python version support
^^^^^^^^^^^^^^^^^^^^^^
Because Dask has dropped support for Python versions < 3.6, we do not test dagster-dask on
Python 2.7 or 3.5.


.. currentmodule:: dagster_dask

.. autodata:: dask_executor
  :annotation: ExecutorDefinition
