from databricks_api import DatabricksAPI

from dagster import Field, StringSource, resource


@resource(
    config_schema={
        "databricks_host": Field(
            StringSource,
            is_required=True,
            description="Databricks host, e.g. uksouth.azuredatabricks.com",
        ),
        "databricks_token": Field(
            StringSource, is_required=True, description="Databricks access token",
        ),
    }
)
def databricks_connection(init_context):
    return DatabricksAPI(
        host=init_context.config["databricks_host"], token=init_context.config["databricks_token"]
    )
