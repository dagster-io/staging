from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.test.utils import execute_dagster_graphql, infer_pipeline_selector

from .graphql_context_test_suite import GraphQLContextVariant, make_graphql_context_test_suite

GET_ASSET_KEY_QUERY = """
{
    assetsOrError {
        __typename
        ...on AssetConnection {
            nodes {
                key {
                    path
                }
            }
        }
    }
}
"""

GET_ASSET_MATERIALIZATION = """
    query AssetQuery($assetKey: AssetKeyInput!) {
        assetOrError(assetKey: $assetKey) {
            ... on Asset {
                assetMaterializations(limit: 1) {
                    materializationEvent {
                        materialization {
                            label
                        }
                    }
                }
            }
        }
    }
"""

GET_ASSET_RUNS = """
    query AssetRunsQuery($assetKey: AssetKeyInput!) {
        assetOrError(assetKey: $assetKey) {
            ... on Asset {
                runs {
                    runId
                }
            }
        }
    }
"""

GET_ASSET_RUNS_INDEXED = """
    query AssetRunsQuery($assetKey: AssetKeyInput!, $cursor: String!, $limit: Int!) {
        assetOrError(assetKey: $assetKey) {
            ... on Asset {
                runs(cursor: $cursor, limit: $limit) {
                    runId
                }
            }
        }
    }
"""


class TestAssetAwareEventLog(
    make_graphql_context_test_suite(
        context_variants=[
            GraphQLContextVariant.in_memory_instance_in_process_env(),
            GraphQLContextVariant.asset_aware_sqlite_instance_in_process_env(),
            GraphQLContextVariant.postgres_with_sync_run_launcher_in_process_env(),
        ]
    )
):
    def test_get_all_asset_keys(self, graphql_context, snapshot):
        selector = infer_pipeline_selector(graphql_context, "multi_asset_pipeline")
        result = execute_dagster_graphql(
            graphql_context,
            LAUNCH_PIPELINE_EXECUTION_MUTATION,
            variables={"executionParams": {"selector": selector, "mode": "default"}},
        )
        assert result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"

        result = execute_dagster_graphql(graphql_context, GET_ASSET_KEY_QUERY)
        assert result.data
        assert result.data["assetsOrError"]
        assert result.data["assetsOrError"]["nodes"]

        # sort by materialization asset key to keep list order is consistent for snapshot
        result.data["assetsOrError"]["nodes"].sort(key=lambda e: e["key"]["path"][0])

        snapshot.assert_match(result.data)

    def test_get_asset_key_materialization(self, graphql_context, snapshot):
        selector = infer_pipeline_selector(graphql_context, "single_asset_pipeline")
        result = execute_dagster_graphql(
            graphql_context,
            LAUNCH_PIPELINE_EXECUTION_MUTATION,
            variables={"executionParams": {"selector": selector, "mode": "default"}},
        )
        assert result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
        result = execute_dagster_graphql(
            graphql_context, GET_ASSET_MATERIALIZATION, variables={"assetKey": {"path": ["a"]}}
        )
        assert result.data
        snapshot.assert_match(result.data)

    def test_get_asset_runs(self, graphql_context):
        single_selector = infer_pipeline_selector(graphql_context, "single_asset_pipeline")
        multi_selector = infer_pipeline_selector(graphql_context, "multi_asset_pipeline")

        result = execute_dagster_graphql(
            graphql_context, GET_ASSET_RUNS, variables={"assetKey": {"path": ["a"]}}
        )
        assert result.data
        fetched_runs = [run["runId"] for run in result.data["assetOrError"]["runs"]]
        assert len(fetched_runs) == 0

        result = execute_dagster_graphql(
            graphql_context,
            LAUNCH_PIPELINE_EXECUTION_MUTATION,
            variables={"executionParams": {"selector": single_selector, "mode": "default"}},
        )
        assert result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
        single_run_id = result.data["launchPipelineExecution"]["run"]["runId"]

        result = execute_dagster_graphql(
            graphql_context,
            LAUNCH_PIPELINE_EXECUTION_MUTATION,
            variables={"executionParams": {"selector": multi_selector, "mode": "default"}},
        )
        assert result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
        multi_run_id = result.data["launchPipelineExecution"]["run"]["runId"]

        result = execute_dagster_graphql(
            graphql_context, GET_ASSET_RUNS, variables={"assetKey": {"path": ["a"]}}
        )
        assert result.data
        fetched_runs = [run["runId"] for run in result.data["assetOrError"]["runs"]]
        assert len(fetched_runs) == 2
        assert multi_run_id in fetched_runs
        assert single_run_id in fetched_runs

    def test_get_asset_runs_indexed(self, graphql_context):
        selector = infer_pipeline_selector(graphql_context, "single_asset_pipeline")

        run_ids = []
        for _ in range(3):
            result = execute_dagster_graphql(
                graphql_context,
                LAUNCH_PIPELINE_EXECUTION_MUTATION,
                variables={"executionParams": {"selector": selector, "mode": "default"}},
            )
            assert (
                result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
            )
            run_ids.append(result.data["launchPipelineExecution"]["run"]["runId"])

        assert len(run_ids) == 3
        result = execute_dagster_graphql(
            graphql_context,
            GET_ASSET_RUNS_INDEXED,
            variables={"assetKey": {"path": ["a"]}, "cursor": run_ids[1], "limit": 1},
        )
        assert result.data
        fetched_runs = [run["runId"] for run in result.data["assetOrError"]["runs"]]
        assert len(fetched_runs) == 1
        assert run_ids[1] in fetched_runs
