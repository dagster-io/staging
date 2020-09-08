# from dagster_k8s_test_infra.integration_utils import get_test_namespace
# from dagster_k8s_test_infra.helm import (
#     helm_namespace,
#     helm_namespace_for_user_deployments,
#     helm_namespace_provider,
# )
# from dagster_k8s_test_infra.cluster import (  # isort:skip
#     dagster_instance,
#     dagster_instance_for_user_deployments,
#     define_cluster_provider_fixture,
# )

# cluster_provider = define_cluster_provider_fixture()


# # def pytest_addoption(parser):
# #     # We catch the ValueError to support cases where we are loading multiple test suites, e.g., in
# #     # the VSCode test explorer. When pytest tries to add an option twice, we get, e.g.
# #     #
# #     #    ValueError: option names {'--cluster-provider'} already added

# #     # Use kind or some other cluster provider?
# #     try:
# #         parser.addoption('--cluster-provider', action='store', default='kind')
# #     except ValueError:
# #         pass

# #     # Specify an existing kind cluster name to use
# #     try:
# #         parser.addoption('--kind-cluster', action='store')
# #     except ValueError:
# #         pass

# #     # Keep resources around after tests are done
# #     try:
# #         parser.addoption('--no-cleanup', action='store_true', default=False)
# #     except ValueError:
# #         pass

# #     # Use existing Helm chart/namespace
# #     try:
# #         parser.addoption('--existing-helm-namespace', action='store')
# #     except ValueError:
# #         pass


# # def test_get_test_namespace():
# #     assert isinstance(get_test_namespace(), str)


# # def test_get_helm_namespace(helm_namespace):
# #     assert isinstance(helm_namespace, str)
