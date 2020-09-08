from dagster import check

from .integration_utils import image_pull_policy

TEST_CONFIGMAP_NAME = 'test-env-configmap'
TEST_SECRET_NAME = 'test-env-secret'


def create_helm_config(namespace, docker_image, user_deployments=False, run_launcher_queued=False):
    check.str_param(namespace, 'namespace')
    check.str_param(docker_image, 'docker_image')
    check.bool_param(user_deployments, 'user_deployments')
    check.bool_param(run_launcher_queued, 'run_launcher_queued')

    repository, tag = docker_image.split(':')
    pull_policy = image_pull_policy()

    helm_config = {}
    helm_config['dagit'] = dagit_config(repository, tag, pull_policy)
    helm_config['celery'] = celery_config(repository, tag, pull_policy, run_launcher_queued)
    helm_config['scheduler'] = scheduler_config(namespace)

    helm_config['serviceAccount'] = {'name': 'dagit-admin'}

    if user_deployments:
        helm_config['userDeployments'] = user_deployments_config(repository, tag, pull_policy)

    return helm_config


def dagit_config(repository, tag, pull_policy):

    return {
        'image': {'repository': repository, 'tag': tag, 'pullPolicy': pull_policy},
        'env': {'TEST_SET_ENV_VAR': 'test_dagit_env_var'},
        'env_config_maps': [TEST_CONFIGMAP_NAME],
        'env_secrets': [TEST_SECRET_NAME],
        'livenessProbe': {
            'tcpSocket': {'port': 'http'},
            'periodSeconds': 20,
            'failureThreshold': 3,
        },
        'startupProbe': {
            'tcpSocket': {'port': 'http'},
            'failureThreshold': 6,
            'periodSeconds': 10,
        },
    }


def celery_config(repository, tag, pull_policy, run_launcher_queued):
    config = {
        'image': {'repository': repository, 'tag': tag, 'pullPolicy': pull_policy},
        # https://github.com/dagster-io/dagster/issues/2671
        # 'extraWorkerQueues': [{'name': 'extra-queue-1', 'replicaCount': 1},],
        'livenessProbe': {
            'exec': {
                'command': [
                    '/bin/sh',
                    '-c',
                    'celery status -A dagster_celery_k8s.app -b {broker_url} | grep "{HOSTNAME}:.*OK"'.format(
                        broker_url='some_broker_url', HOSTNAME='some_hostname',
                    ),
                ]
            },
            'initialDelaySeconds': 15,
            'periodSeconds': 10,
            'timeoutSeconds': 10,
            'successThreshold': 1,
            'failureThreshold': 3,
        },
    }

    if run_launcher_queued:
        config['runQueuing'] = {'enabled': True, 'defaultQueue': 'dagster-step-jobs'}
        # https://github.com/dagster-io/dagster/issues/2671
        # This shouldn't normally use the same queue
        # config['extraWorkerQueues'] = [{'name': 'dagster-run-launchers', 'replicaCount': 1,}]

    return config


def scheduler_config(namespace):
    return {'k8sEnabled': 'true', 'schedulerNamespace': namespace}


def user_deployments_config(repository, tag, pull_policy):
    return {
        'enabled': True,
        'deployments': [
            {
                'name': 'user-code-deployment-1',
                'image': {'repository': repository, 'tag': tag, 'pullPolicy': pull_policy},
                'dagsterApiGrpcArgs': [
                    '-m',
                    'dagster_test.test_project.test_pipelines.repo',
                    '-a',
                    'define_demo_execution_repo',
                ],
                'port': 3030,
            }
        ],
    }
