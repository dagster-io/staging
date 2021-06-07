from dagster import InitResourceContext, resource
from google.cloud import dataproc_v1 as dataproc  # type: ignore[attr-defined]


@resource(config_schema={"region": str})
def dataproc_cluster_client(init_context: InitResourceContext):
    region = init_context.resource_config["region"]
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    return cluster_client


@resource(config_schema={"region": str})
def dataproc_job_client(init_context: InitResourceContext):
    region = init_context.resource_config["region"]
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    return job_client
