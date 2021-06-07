import json

from dagster import StringSource, resource
from google.cloud import bigquery  # type: ignore[attr-defined]
from google.oauth2 import service_account
from pandas import DataFrame


class BigQueryMockQueryJob:
    def __init__(self, rows):
        self._rows = rows

    def __getitem__(self, key):
        return self._rows[key]

    def result(self):
        return self

    def to_dataframe(self):
        return DataFrame(data=self._rows)


class BigQueryMockResource:
    def query(self, q):
        if "SELECT COUNT(*) FROM" in q:
            return BigQueryMockQueryJob(rows=[(200,)])
        return BigQueryMockQueryJob(rows=[])


@resource
def bigquery_mock_resource(_):
    return BigQueryMockResource()


@resource(
    config_schema={"credentials_json": StringSource},
    description="Dagster resource for connecting to BigQuery",
)
def bigquery_resource(context):
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(context.resource_config["credentials_json"]),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
