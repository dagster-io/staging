from dagster_databricks.databricks import wait_for_run_to_complete

from dagster import InputDefinition, Nothing, OutputDefinition, Permissive, solid


def create_databricks_job_solid(
    name,
    num_inputs=1,
    num_outputs=1,
    description=None,
    required_resource_keys=frozenset(["databricks_connection"]),
):
    """
    Creates a solid that launches a databricks job.

    As config, the solid accepts a blob of the form described in Databricks' job API:
    https://docs.databricks.com/dev-tools/api/latest/jobs.html.
    """
    input_defs = [InputDefinition("input_" + str(i), Nothing) for i in range(num_inputs)]
    output_defs = [OutputDefinition("output_" + str(i), Nothing) for i in range(num_outputs)]

    @solid(
        name=name,
        description=description,
        config_schema={"job": Permissive(), "poll_interval_sec": int, "wait_time_sec": int},
        input_defs=input_defs,
        output_defs=output_defs,
        required_resource_keys=required_resource_keys,
    )
    def databricks_solid(context):
        job_config = context.config["job_config"]
        databricks_conn = context.resources.databricks_connection
        run_id = databricks_conn.jobs.submit_run(job_config)
        wait_for_run_to_complete(
            databricks_conn,
            context.log,
            run_id,
            context.config["poll_interval_sec"],
            context.config["max_wait_time_sec"],
        )

    return databricks_solid
