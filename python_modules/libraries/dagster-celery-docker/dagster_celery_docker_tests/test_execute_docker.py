# pylint doesn't know about pytest fixtures
# pylint: disable=unused-argument

import base64
import os
from contextlib import contextmanager

import boto3
import docker
import pytest
from dagster import execute_pipeline, file_relative_path, seven
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import (
    instance_for_test_tempdir,
    poll_for_finished_run,
    poll_for_step_start,
)
from dagster.utils import merge_dicts
from dagster.utils.test.postgres_instance import TestPostgresInstance
from dagster.utils.yaml_utils import merge_yamls
from dagster_test.test_project import (
    ReOriginatedExternalPipelineForTest,
    build_and_tag_test_image,
    get_test_project_external_pipeline,
    get_test_project_recon_pipeline,
    test_project_docker_image,
    test_project_environments_path,
)

IS_BUILDKITE = os.getenv("BUILDKITE") is not None


@contextmanager
def postgres_instance(overrides=None):
    with seven.TemporaryDirectory() as temp_dir:
        with TestPostgresInstance.docker_service_up_or_skip(
            file_relative_path(__file__, "docker-compose.yml"), "test-postgres-db-celery-docker",
        ) as pg_conn_string:
            TestPostgresInstance.clean_run_storage(pg_conn_string)
            TestPostgresInstance.clean_event_log_storage(pg_conn_string)
            TestPostgresInstance.clean_schedule_storage(pg_conn_string)
            with instance_for_test_tempdir(
                temp_dir,
                overrides=merge_dicts(
                    {
                        "run_storage": {
                            "module": "dagster_postgres.run_storage.run_storage",
                            "class": "PostgresRunStorage",
                            "config": {"postgres_url": pg_conn_string},
                        },
                        "event_log_storage": {
                            "module": "dagster_postgres.event_log.event_log",
                            "class": "PostgresEventLogStorage",
                            "config": {"postgres_url": pg_conn_string},
                        },
                        "schedule_storage": {
                            "module": "dagster_postgres.schedule_storage.schedule_storage",
                            "class": "PostgresScheduleStorage",
                            "config": {"postgres_url": pg_conn_string},
                        },
                    },
                    overrides if overrides else {},
                ),
            ) as instance:
                yield instance


def _get_or_build_local_image(docker_image):
    try:
        client = docker.from_env()
        client.images.get(docker_image)
        print(  # pylint: disable=print-call
            "Found existing image tagged {image}, skipping image build. To rebuild, first run: "
            "docker rmi {image}".format(image=docker_image)
        )
    except docker.errors.ImageNotFound:
        build_and_tag_test_image(docker_image)


def _get_registry_config():
    ecr_client = boto3.client("ecr", region_name="us-west-1")
    token = ecr_client.get_authorization_token()
    username, password = (
        base64.b64decode(token["authorizationData"][0]["authorizationToken"]).decode().split(":")
    )
    registry = token["authorizationData"][0]["proxyEndpoint"]

    return {
        "url": registry,
        "username": username,
        "password": password,
    }


def test_launch_docker_image_on_pipeline_config():
    # Docker image name to use for launch specified as part of the pipeline origin
    # rather than in the run launcher instance config

    docker_image = test_project_docker_image()
    launcher_config = {
        "env_vars": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",],
        "network": "container:test-postgres-db-celery-docker",
    }

    if IS_BUILDKITE:
        launcher_config["registry"] = _get_registry_config()
    else:
        _get_or_build_local_image(docker_image)

    run_config = merge_yamls(
        [
            os.path.join(test_project_environments_path(), "env.yaml"),
            os.path.join(test_project_environments_path(), "env_s3.yaml"),
        ]
    )

    with postgres_instance(
        overrides={
            "run_launcher": {
                "class": "DockerRunLauncher",
                "module": "dagster_celery_docker",
                "config": launcher_config,
            }
        }
    ) as instance:
        recon_pipeline = get_test_project_recon_pipeline("docker_pipeline", docker_image)
        run = instance.create_run_for_pipeline(
            pipeline_def=recon_pipeline.get_definition(), run_config=run_config,
        )

        external_pipeline = ReOriginatedExternalPipelineForTest(
            get_test_project_external_pipeline("docker_pipeline", container_image=docker_image),
            container_image=docker_image,
        )
        instance.launch_run(run.run_id, external_pipeline)

        poll_for_finished_run(instance, run.run_id, timeout=60)

        assert instance.get_run_by_id(run.run_id).status == PipelineRunStatus.SUCCESS


def _check_event_log_contains(event_log, expected_type_and_message):
    types_and_messages = [(e.dagster_event.event_type_value, e.message) for e in event_log]

    for expected_event_type, expected_message_fragment in expected_type_and_message:
        assert any(
            event_type == expected_event_type and expected_message_fragment in message
            for event_type, message in types_and_messages
        )


@pytest.mark.skip("it broke")
def test_terminate_launched_docker_run():
    docker_image = test_project_docker_image()
    launcher_config = {
        "env_vars": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",],
        "network": "container:test-postgres-db-celery-docker",
    }

    if IS_BUILDKITE:
        launcher_config["registry"] = _get_registry_config()
    else:
        _get_or_build_local_image(docker_image)

    run_config = merge_yamls([os.path.join(test_project_environments_path(), "env_s3.yaml"),])

    with postgres_instance(
        overrides={
            "run_launcher": {
                "class": "DockerRunLauncher",
                "module": "dagster_celery_docker",
                "config": launcher_config,
            }
        }
    ) as instance:
        recon_pipeline = get_test_project_recon_pipeline("hanging_pipeline", docker_image)
        run = instance.create_run_for_pipeline(
            pipeline_def=recon_pipeline.get_definition(), run_config=run_config,
        )

        run_id = run.run_id

        external_pipeline = ReOriginatedExternalPipelineForTest(
            get_test_project_external_pipeline("hanging_pipeline", container_image=docker_image),
            container_image=docker_image,
        )
        instance.launch_run(run_id, external_pipeline)

        poll_for_step_start(instance, run_id)

        assert instance.run_launcher.can_terminate(run_id)
        assert instance.run_launcher.terminate(run_id)

        terminated_pipeline_run = poll_for_finished_run(instance, run_id, timeout=30)
        terminated_pipeline_run = instance.get_run_by_id(run_id)
        assert terminated_pipeline_run.status == PipelineRunStatus.FAILURE

        run_logs = instance.all_logs(run_id)

        _check_event_log_contains(
            run_logs,
            [
                ("ENGINE_EVENT", "Received pipeline termination request"),
                ("STEP_FAILURE", 'Execution of step "hanging_solid.compute" failed.'),
                ("PIPELINE_FAILURE", 'Execution of pipeline "hanging_pipeline" failed.'),
                ("ENGINE_EVENT", "Pipeline execution terminated by interrupt"),
                ("ENGINE_EVENT", "Process for pipeline exited"),
            ],
        )


def test_launch_docker_image_on_instance_config():
    docker_image = test_project_docker_image()
    launcher_config = {
        "env_vars": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",],
        "network": "container:test-postgres-db-celery-docker",
        "image": docker_image,
    }

    if IS_BUILDKITE:
        launcher_config["registry"] = _get_registry_config()
    else:
        _get_or_build_local_image(docker_image)

    run_config = merge_yamls(
        [
            os.path.join(test_project_environments_path(), "env.yaml"),
            os.path.join(test_project_environments_path(), "env_s3.yaml"),
        ]
    )

    with postgres_instance(
        overrides={
            "run_launcher": {
                "class": "DockerRunLauncher",
                "module": "dagster_celery_docker",
                "config": launcher_config,
            }
        }
    ) as instance:
        recon_pipeline = get_test_project_recon_pipeline("docker_pipeline")
        run = instance.create_run_for_pipeline(
            pipeline_def=recon_pipeline.get_definition(), run_config=run_config,
        )

        external_pipeline = ReOriginatedExternalPipelineForTest(
            get_test_project_external_pipeline("docker_pipeline")
        )
        instance.launch_run(run.run_id, external_pipeline)

        poll_for_finished_run(instance, run.run_id, timeout=60)

        assert instance.get_run_by_id(run.run_id).status == PipelineRunStatus.SUCCESS


def test_execute_celery_docker_image_on_executor_config():
    docker_image = test_project_docker_image()
    docker_config = {
        "image": docker_image,
        "env_vars": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",],
        "network": "container:test-postgres-db-celery-docker",
    }

    if IS_BUILDKITE:
        docker_config["registry"] = _get_registry_config()
    else:
        _get_or_build_local_image(docker_image)

    run_config = merge_dicts(
        merge_yamls(
            [
                os.path.join(test_project_environments_path(), "env.yaml"),
                os.path.join(test_project_environments_path(), "env_s3.yaml"),
            ]
        ),
        {
            "execution": {
                "celery-docker": {
                    "config": {
                        "docker": docker_config,
                        "config_source": {"task_always_eager": True},
                    }
                }
            },
        },
    )

    with postgres_instance() as instance:

        result = execute_pipeline(
            get_test_project_recon_pipeline("docker_celery_pipeline"),
            run_config=run_config,
            instance=instance,
        )
        assert result.success


def test_execute_celery_docker_image_on_pipeline_config():
    docker_image = test_project_docker_image()
    docker_config = {
        "env_vars": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",],
        "network": "container:test-postgres-db-celery-docker",
    }

    if IS_BUILDKITE:
        docker_config["registry"] = _get_registry_config()

    else:
        _get_or_build_local_image(docker_image)

    run_config = merge_dicts(
        merge_yamls(
            [
                os.path.join(test_project_environments_path(), "env.yaml"),
                os.path.join(test_project_environments_path(), "env_s3.yaml"),
            ]
        ),
        {
            "execution": {
                "celery-docker": {
                    "config": {
                        "docker": docker_config,
                        "config_source": {"task_always_eager": True},
                    }
                }
            },
        },
    )

    with postgres_instance() as instance:
        result = execute_pipeline(
            get_test_project_recon_pipeline("docker_celery_pipeline", docker_image),
            run_config=run_config,
            instance=instance,
        )
        assert result.success
