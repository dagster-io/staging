# import datetime
# import os
import json
import sys

import kubernetes
# import boto3
import pytest
# from dagster_celery_k8s.launcher import CeleryK8sRunLauncher
from dagster_k8s.test import wait_for_job_and_get_raw_logs

# import time


# from dagster_k8s_test_infra.integration_utils import ReOriginatedGrpcExternalPipelineForTest
# from dagster_test.test_project import (
#     get_test_project_external_pipeline,
#     test_project_environments_path,
# )

# from dagster import DagsterEventType
# from dagster.core.storage.pipeline_run import PipelineRunStatus
# from dagster.core.test_utils import create_run_for_test

# from dagster.utils import merge_dicts

# from dagster.utils.yaml_utils import merge_yamls

# from .test_integration import get_celery_engine_config

# import time


def get_celery_engine_config(job_namespace):
    return {
        'execution': {
            'celery-k8s': {
                'config': {
                    # 'job_image': dagster_docker_image,
                    'job_namespace': job_namespace,
                    'image_pull_policy': 'Always',
                    'env_config_maps': ['dagster-pipeline-env'],
                }
            }
        },
    }


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info < (3, 5), reason="Very slow on Python 2")
def test_execute_on_celery_k8s(  # pylint: disable=redefined-outer-name,unused-argument
    dagster_instance_for_user_deployments, helm_namespace_for_user_deployments,
):
    namespace = helm_namespace_for_user_deployments
    # run_config = merge_dicts(
    #     merge_yamls(
    #         [
    #             os.path.join(test_project_environments_path(), 'env.yaml'),
    #             os.path.join(test_project_environments_path(), 'env_s3.yaml'),
    #         ]
    #     ),
    #     get_celery_engine_config(job_namespace=helm_namespace_for_user_deployments),
    # )

    pipeline_name = 'demo_pipeline_celery'
    # run = create_run_for_test(
    #     dagster_instance_for_user_deployments,
    #     pipeline_name=pipeline_name,
    #     run_config=run_config,
    #     mode='default',
    # )

    # ssh into dagit box and run the command
    core_api = kubernetes.client.CoreV1Api()
    pods = core_api.list_namespaced_pod(namespace=namespace)
    print('pods', pods)  # pylint: disable=print-call
    dagit_pod_list = list(filter(lambda item: 'dagit' in item.metadata.name, pods.items))
    assert len(dagit_pod_list) == 1
    dagit_pod = dagit_pod_list[0]
    dagit_pod_name = dagit_pod.metadata.name
    # for item in pods.items:
    #     if 'dagit' in item.metadata.name:
    #         return item
    # kubectl get pods --namespace dagster-test-f5768b -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance=dagster,component=dagit" -o jsonpath="{.items[0].metadata.name}")
    run_config_dict = {
        'storage': {'s3': {'config': {'s3_bucket': 'dagster-scratch-80542c2'}}},
        'execution': {
            'celery-k8s': {
                'config': {
                    'image_pull_policy': "Always",
                    'env_config_maps': ["dagster-pipeline-env"],
                    'job_namespace': namespace,
                }
            }
        },
        'loggers': {'console': {'config': {'log_level': 'DEBUG'}}},
        'solids': {'multiply_the_word': {'inputs': {'word': 'bar'}, 'config': {'factor': 2}}},
    }

    run_config_json = json.dumps(run_config_dict)
    print('run_config_json', run_config_json)  # pylint: disable=print-call
    exec_command = [
        'dagster',
        'pipeline',
        'launch',
        '--repository',
        'demo_execution_repo',
        '--pipeline',
        pipeline_name,
        '--workspace',
        '/dagster-workspace/workspace.yaml',
        '--location',
        'user-code-deployment-1',
        # '--preset',
        # 'buildkite-test',
        '--config-json',
        run_config_json,
    ]
    print('exec_command', exec_command)  # pylint: disable=print-call
    # from kubernetes.client import configuration

    # kubernetes.config.load_kube_config()
    # configuration.assert_hostname = False
    from kubernetes.stream import stream

    api_response = stream(
        core_api.connect_get_namespaced_pod_exec,
        name=dagit_pod_name,
        namespace=namespace,
        command=exec_command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )
    print('api_response', api_response)  # pylint: disable=print-call
    # dagster_instance_for_user_deployments.launch_run(
    #     run.run_id,
    #     ReOriginatedGrpcExternalPipelineForTest(get_test_project_external_pipeline(pipeline_name)),
    # )
    found = False
    runmaster_job_name = None
    import datetime

    timeout = datetime.timedelta(0, 30)
    start_time = datetime.datetime.now()
    while datetime.datetime.now() < start_time + timeout and not found:
        batch_api = kubernetes.client.BatchV1Api()
        jobs = batch_api.list_namespaced_job(namespace=namespace)
        print('jobs', jobs)  # pylint: disable=print-call
        runmaster_job = list(filter(lambda item: 'dagster-run-' in item.metadata.name, jobs.items))[
            0
        ]
        runmaster_job_name = runmaster_job.metadata.name

    assert found
    result = wait_for_job_and_get_raw_logs(job_name=runmaster_job_name, namespace=namespace)

    assert 'PIPELINE_SUCCESS' in result, 'no match, result: {}'.format(result)
