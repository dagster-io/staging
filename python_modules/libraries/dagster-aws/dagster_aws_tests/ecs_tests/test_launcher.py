# pylint: disable=redefined-outer-name, protected-access
import pytest
from dagster.core.test_utils import instance_for_test
from dagster_aws.ecs import EcsRunLauncher
from dagster_test.test_project import get_test_project_external_pipeline
from dagster_test.test_project.test_pipelines.repo import noop_pipeline


@pytest.fixture
def task_definition(ecs):
    return ecs.register_task_definition(
        family="dagster",
        containerDefinitions=[{"name": "dagster", "image": "dagster:latest"}],
        networkMode="awsvpc",
    )["taskDefinition"]


@pytest.fixture
def task(ecs, task_definition):
    return ecs.run_task(
        taskDefinition=task_definition["family"],
        networkConfiguration={"awsvpcConfiguration": {"subnets": ["subnet-12345"]}},
    )["tasks"][0]


@pytest.fixture
def run_launcher(ecs, task, task_definition, monkeypatch, requests_mock):
    container_uri = "http://metadata_host"
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", container_uri)
    container = task["containers"][0]["name"]
    requests_mock.get(container_uri, json={"Name": container})

    task_uri = container_uri + "/task"
    requests_mock.get(
        task_uri,
        json={
            "Cluster": task["clusterArn"],
            "TaskARN": task["taskArn"],
            "Family": task_definition["family"],
        },
    )

    return EcsRunLauncher(boto3_client=ecs)


@pytest.fixture
def instance(run_launcher, monkeypatch):
    with instance_for_test() as instance:
        # TODO: Remove once EcsRunLauncher is a ConfigurableClass
        monkeypatch.setattr(instance, "_run_launcher", run_launcher)
        run_launcher.register_instance(instance)
        yield instance


@pytest.fixture
def pipeline():
    return noop_pipeline


@pytest.fixture
def external_pipeline(pipeline):
    with get_test_project_external_pipeline(pipeline.name) as external_pipeline:
        yield external_pipeline


@pytest.fixture
def run_id(instance, pipeline):
    return instance.create_run_for_pipeline(pipeline).run_id


def test_launching(ecs, instance, run_id, external_pipeline):
    assert not instance.run_launcher._get_task_arn_by_run_id_tag(run_id)

    instance.launch_run(run_id, external_pipeline)

    task_arn = instance.run_launcher._get_task_arn_by_run_id_tag(run_id)
    task = ecs.describe_tasks(tasks=[task_arn])
    assert "execute_run" in task["tasks"][0]["overrides"]["containerOverrides"][0]["command"]
