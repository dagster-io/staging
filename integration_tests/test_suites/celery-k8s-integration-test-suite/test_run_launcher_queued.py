import os
import sys

import pytest
from dagster_k8s.test import wait_for_job_and_get_raw_logs
from dagster_k8s_test_infra.integration_utils import ReOriginatedExternalPipelineForTest
from dagster_test.test_project import (
    get_test_project_external_pipeline,
    test_project_environments_path,
)

from dagster.core.test_utils import create_run_for_test
from dagster.utils import merge_dicts
from dagster.utils.yaml_utils import merge_yamls


def get_celery_engine_config(dagster_docker_image, job_namespace):
    return {
        'execution': {
            'celery-k8s': {
                'config': {
                    'job_image': dagster_docker_image,
                    'job_namespace': job_namespace,
                    'image_pull_policy': 'Always',
                    'env_config_maps': ['dagster-pipeline-env'],
                }
            }
        },
    }


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info < (3, 5), reason="Very slow on Python 2")
def test_execute_on_celery_k8s(  # pylint: disable=redefined-outer-name
    dagster_docker_image, dagster_instance, helm_namespace_provider
):
    helm_namespace = helm_namespace_provider(run_launcher_queued=True)

    run_config = merge_dicts(
        merge_yamls(
            [
                os.path.join(test_project_environments_path(), 'env.yaml'),
                os.path.join(test_project_environments_path(), 'env_s3.yaml'),
            ]
        ),
        get_celery_engine_config(
            dagster_docker_image=dagster_docker_image, job_namespace=helm_namespace
        ),
    )

    pipeline_name = 'demo_pipeline_celery'
    run = create_run_for_test(
        dagster_instance, pipeline_name=pipeline_name, run_config=run_config, mode='default',
    )

    dagster_instance.launch_run(
        run.run_id,
        ReOriginatedExternalPipelineForTest(get_test_project_external_pipeline(pipeline_name)),
    )

    result = wait_for_job_and_get_raw_logs(
        job_name='dagster-run-%s' % run.run_id, namespace=helm_namespace
    )

    assert 'PIPELINE_SUCCESS' in result, 'no match, result: {}'.format(result)
