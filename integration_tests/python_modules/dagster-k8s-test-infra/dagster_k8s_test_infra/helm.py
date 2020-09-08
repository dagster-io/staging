# pylint: disable=print-call
import base64
import os
import subprocess
import time
from contextlib import contextmanager

import kubernetes
import pytest
import six
import yaml
from dagster_k8s.utils import wait_for_pod
from dagster_test.test_project import test_project_docker_image

from dagster import check
from dagster.utils import git_repository_root

from .helm_values import TEST_CONFIGMAP_NAME, TEST_SECRET_NAME, create_helm_config
from .integration_utils import IS_BUILDKITE, check_output, get_test_namespace


def _helm_namespace_helper(helm_chart_fn, request):
    '''If an existing Helm chart namespace is specified via pytest CLI with the argument
    --existing-helm-namespace, we will use that chart.

    Otherwise, provision a test namespace and install Helm chart into that namespace.

    Yields the Helm chart namespace.
    '''
    existing_helm_namespace = request.config.getoption('--existing-helm-namespace')

    if existing_helm_namespace:
        return existing_helm_namespace

    else:
        # Never bother cleaning up on Buildkite
        if IS_BUILDKITE:
            should_cleanup = False
        # Otherwise, always clean up unless --no-cleanup specified
        else:
            should_cleanup = not request.config.getoption('--no-cleanup')

        with test_namespace(should_cleanup) as namespace:
            with helm_test_resources(namespace, should_cleanup):
                docker_image = test_project_docker_image()
                with helm_chart_fn(namespace, docker_image, should_cleanup):
                    print('Helm chart successfully installed in namespace %s' % namespace)
                    return namespace


@pytest.fixture(scope='session')
def helm_namespace_for_user_deployments(
    cluster_provider, request
):  # pylint: disable=unused-argument, redefined-outer-name
    yield _helm_namespace_helper(helm_chart_provider(user_deployments=True), request)


@pytest.fixture(scope='session')
def helm_namespace(
    cluster_provider, request
):  # pylint: disable=unused-argument, redefined-outer-name
    yield _helm_namespace_helper(helm_chart_provider(), request)


@pytest.fixture(scope='session')
def helm_namespace_provider(
    cluster_provider, request
):  # pylint: disable=unused-argument, redefined-outer-name
    def _helm_namespace(**kwargs):
        return _helm_namespace_helper(helm_chart_provider(**kwargs), request)

    return _helm_namespace


@contextmanager
def test_namespace(should_cleanup=True):
    # Will be something like dagster-test-3fcd70 to avoid ns collisions in shared test environment
    namespace = get_test_namespace()

    print('--- \033[32m:k8s: Creating test namespace %s\033[0m' % namespace)
    kube_api = kubernetes.client.CoreV1Api()

    try:
        print('Creating namespace %s' % namespace)
        kube_namespace = kubernetes.client.V1Namespace(
            metadata=kubernetes.client.V1ObjectMeta(name=namespace)
        )
        kube_api.create_namespace(kube_namespace)
        yield namespace

    finally:
        # Can skip this step as a time saver when we're going to destroy the cluster anyway, e.g.
        # w/ a kind cluster
        if should_cleanup:
            print('Deleting namespace %s' % namespace)
            kube_api.delete_namespace(name=namespace)


@contextmanager
def helm_test_resources(namespace, should_cleanup=True):
    '''Create a couple of resources to test Helm interaction w/ pre-existing resources.
    '''
    check.str_param(namespace, 'namespace')
    check.bool_param(should_cleanup, 'should_cleanup')

    try:
        print(
            'Creating k8s test objects ConfigMap %s and Secret %s'
            % (TEST_CONFIGMAP_NAME, TEST_SECRET_NAME)
        )
        kube_api = kubernetes.client.CoreV1Api()

        configmap = kubernetes.client.V1ConfigMap(
            api_version='v1',
            kind='ConfigMap',
            data={'TEST_ENV_VAR': 'foobar'},
            metadata=kubernetes.client.V1ObjectMeta(name=TEST_CONFIGMAP_NAME),
        )
        kube_api.create_namespaced_config_map(namespace=namespace, body=configmap)

        # Secret values are expected to be base64 encoded
        secret_val = six.ensure_str(base64.b64encode(six.ensure_binary('foobar')))
        secret = kubernetes.client.V1Secret(
            api_version='v1',
            kind='Secret',
            data={'TEST_SECRET_ENV_VAR': secret_val},
            metadata=kubernetes.client.V1ObjectMeta(name=TEST_SECRET_NAME),
        )
        kube_api.create_namespaced_secret(namespace=namespace, body=secret)

        yield

    finally:
        # Can skip this step as a time saver when we're going to destroy the cluster anyway, e.g.
        # w/ a kind cluster
        if should_cleanup:
            kube_api.delete_namespaced_config_map(name=TEST_CONFIGMAP_NAME, namespace=namespace)
            kube_api.delete_namespaced_secret(name=TEST_SECRET_NAME, namespace=namespace)


@contextmanager
def _helm_chart_helper(namespace, should_cleanup, helm_config):
    '''Install dagster-k8s helm chart.
    '''
    check.str_param(namespace, 'namespace')
    check.bool_param(should_cleanup, 'should_cleanup')

    print('--- \033[32m:helm: Installing Helm chart\033[0m')

    try:
        helm_config_yaml = yaml.dump(helm_config, default_flow_style=False)

        dagster_k8s_path = os.path.join(
            git_repository_root(), 'python_modules', 'libraries', 'dagster-k8s'
        )

        helm_cmd = [
            'helm',
            'install',
            '--namespace',
            namespace,
            '-f',
            '-',
            'dagster',
            os.path.join(dagster_k8s_path, 'helm', 'dagster'),
        ]

        print('Running Helm Install: \n', ' '.join(helm_cmd), '\nWith config:\n', helm_config_yaml)

        p = subprocess.Popen(
            helm_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = p.communicate(six.ensure_binary(helm_config_yaml))
        print('Helm install completed with stdout: ', stdout)
        print('Helm install completed with stderr: ', stderr)
        assert p.returncode == 0

        # Wait for Dagit pod to be ready (won't actually stay up w/out js rebuild)
        kube_api = kubernetes.client.CoreV1Api()

        print('Waiting for Dagit pod to be ready...')
        dagit_pod = None
        while dagit_pod is None:
            pods = kube_api.list_namespaced_pod(namespace=namespace)
            pod_names = [p.metadata.name for p in pods.items if 'dagit' in p.metadata.name]
            if pod_names:
                dagit_pod = pod_names[0]
            time.sleep(1)

        # Wait for Celery worker queues to become ready
        print('Waiting for celery workers')
        pods = kubernetes.client.CoreV1Api().list_namespaced_pod(namespace=namespace)
        pod_names = [p.metadata.name for p in pods.items if 'celery-workers' in p.metadata.name]
        for pod_name in pod_names:
            print('Waiting for Celery worker pod %s' % pod_name)
            wait_for_pod(pod_name, namespace=namespace)

        if helm_config.get('userDeployments') and helm_config.get('userDeployments', {}).get(
            'enabled'
        ):
            # Wait for user code deployments to be ready
            print('Waiting for user code deployments')
            pods = kubernetes.client.CoreV1Api().list_namespaced_pod(namespace=namespace)
            pod_names = [
                p.metadata.name for p in pods.items if 'user-code-deployment' in p.metadata.name
            ]
            for pod_name in pod_names:
                print('Waiting for user code deployment pod %s' % pod_name)
                wait_for_pod(pod_name, namespace=namespace)

        yield

    finally:
        # Can skip this step as a time saver when we're going to destroy the cluster anyway, e.g.
        # w/ a kind cluster
        if should_cleanup:
            print('Uninstalling helm chart')
            check_output(
                ['helm', 'uninstall', 'dagster', '--namespace', namespace], cwd=dagster_k8s_path,
            )


def helm_chart_provider(**kwargs):
    @contextmanager
    def _helm_chart(namespace, docker_image, should_cleanup=True):
        check.str_param(namespace, 'namespace')
        check.str_param(docker_image, 'docker_image')
        check.bool_param(should_cleanup, 'should_cleanup')

        helm_config = create_helm_config(namespace, docker_image, **kwargs)

        with _helm_chart_helper(namespace, should_cleanup, helm_config):
            yield

    return _helm_chart
