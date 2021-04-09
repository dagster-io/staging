from typing import List

import google.auth
from dagster import root_input_manager
from google.cloud import bigquery, bigquery_storage  # type: ignore[attr-defined]
from pandas import DataFrame


@root_input_manager
def gcp_to_pandas_loader(context):
    return gcp_table_to_pandas(context.metadata["table"], context.metadata["columns"])


def gcp_table_to_pandas(table_name: str, columns: List[str]) -> DataFrame:
    credentials, your_project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    bqclient = bigquery.Client(credentials=credentials, project=your_project_id)
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)

    query_string = f"""
    SELECT {', '.join(columns)} FROM `{table_name}`
    """
    dataframe = bqclient.query(query_string).result().to_dataframe(bqstorage_client=bqstorageclient)

    return dataframe
