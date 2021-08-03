import yaml
from airflow import DAG
from dagster import execute_pipeline
from dagster.core.test_utils import instance_for_test
from dagster_airflow.operators.docker_operator import DagsterDockerOperator
from dagster_airflow.operators.python_operator import DagsterPythonOperator
from docs_snippets.deploying.airflow.generated_airflow import ENVIRONMENT
from docs_snippets.deploying.airflow.pipeline import hello_cereal_pipeline


def test_original_pipeline():
    with instance_for_test() as instance:
        result = execute_pipeline(
            hello_cereal_pipeline, run_config=yaml.safe_load(ENVIRONMENT), instance=instance
        )
        assert result.success


def test_generated_airflow():
    from docs_snippets.deploying.airflow.generated_airflow import dag, tasks

    assert isinstance(dag, DAG)

    for task in tasks:
        assert isinstance(task, DagsterPythonOperator)


def test_mounted():
    from docs_snippets.deploying.airflow.mounted import dag, steps

    assert isinstance(dag, DAG)

    for task in steps:
        assert isinstance(task, DagsterPythonOperator)


def test_containerized():
    from docs_snippets.deploying.airflow.containerized import dag, steps

    assert isinstance(dag, DAG)

    for task in steps:
        assert isinstance(task, DagsterDockerOperator)
