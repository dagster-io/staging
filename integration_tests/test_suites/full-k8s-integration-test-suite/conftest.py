from dagster_k8s_test_infra.cluster import define_cluster_provider_fixture

cluster_provider = define_cluster_provider_fixture(
    additional_kind_images=["docker.io/bitnami/rabbitmq", "docker.io/bitnami/postgresql"]
)
