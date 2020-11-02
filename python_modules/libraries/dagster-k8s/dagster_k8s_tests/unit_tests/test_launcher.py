import json

from dagster.core.test_utils import create_run_for_test, instance_for_test
from dagster.seven import mock
from dagster_k8s import K8sRunLauncher
from dagster_k8s.job import UserDefinedDagsterK8sConfig
from dagster_test.test_project import get_test_project_external_pipeline, test_project_docker_image


def test_user_defined_k8s_config_in_run_tags(tmp_path):
    # Construct a K8s run launcher in a fake k8s environment.
    # [TODO(Bob)]: Find a better way to fake the kube config file.
    # Option 1: Use a temporary file (like below), but refactor this into a reusable fixture.
    # Option 2: ???
    kube_path = tmp_path / ".kube"
    kube_path.mkdir()
    kube_config_path = kube_path / "config"
    kube_config_path.write_text(
        """
apiVersion: v1
kind: Config

current-context: fake-context
contexts:
  - context:
      cluster: fake-cluster
    name: fake-context
clusters:
  - cluster: {}
    name: fake-cluster
"""
    )

    mock_k8s_client_batch_api = mock.MagicMock()
    k8s_run_launcher = K8sRunLauncher(
        service_account_name="dagit-admin",
        instance_config_map="dagster-instance",
        postgres_password_secret="dagster-postgresql-secret",
        dagster_home="/opt/dagster/dagster_home",
        job_image=test_project_docker_image(),
        load_incluster_config=False,
        kubeconfig_file=str(kube_config_path),
        k8s_client_batch_api=mock_k8s_client_batch_api,
    )

    # Construct Dagster run tags with user defined k8s config.
    expected_resources = {
        "requests": {"cpu": "250m", "memory": "64Mi"},
        "limits": {"cpu": "500m", "memory": "2560Mi"},
    }
    user_defined_k8s_config = UserDefinedDagsterK8sConfig(
        container_config={"resources": expected_resources},
    )
    user_defined_k8s_config_json = json.dumps(user_defined_k8s_config.to_dict())
    tags = {"dagster-k8s/config": user_defined_k8s_config_json}

    # Launch the run in a fake Dagster instance.
    with instance_for_test() as instance:
        pipeline_name = "demo_pipeline"
        run = create_run_for_test(instance, pipeline_name=pipeline_name, tags=tags)
        external_pipeline = get_test_project_external_pipeline(pipeline_name)
        k8s_run_launcher.initialize(instance)
        k8s_run_launcher.launch_run(None, run, external_pipeline)

    # Check that user defined k8s config was passed down to the k8s job.
    mock_method_calls = mock_k8s_client_batch_api.method_calls
    assert len(mock_method_calls) > 0
    method_name, _args, kwargs = mock_method_calls[0]
    assert method_name == "create_namespaced_job"
    job_resources = kwargs["body"].spec.template.spec.containers[0].resources
    assert job_resources == expected_resources
