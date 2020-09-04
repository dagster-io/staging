# This doesn't really below under "decorators", so need to find a more reasonable place for it
# On spin-up, will evaluate whether to trigger another pipeline run
# Takes a function should_execute_pipeline_fn that returns whether or not a new pipeline should
# be launched

from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.implementation.context import DagsterGraphQLContext
from dagster_graphql.schema import create_schema
from dagster_graphql.test.utils import execute_dagster_graphql, infer_pipeline_selector
from graphql import graphql

from dagster import DagsterInstance, resource
from dagster.cli.workspace.workspace import Workspace

# from dagster_graphql.test.utils import infer_pipeline_selector


def cross_dag_resource_factory(should_execute_pipeline_fn, run_config_data_fn, workspace_fn):
    @resource()
    def cross_dag_resource(init_context):
        print('init_context', type(init_context))
        should_execute = should_execute_pipeline_fn()
        # context.log.info('cross_dag_resource should_execute_pipeline_fn returned ', should_execute)
        if not should_execute:
            return

        graphql_context = DagsterGraphQLContext(
            workspace=workspace_fn(), instance=DagsterInstance.get(),
        )

        # recon_repo = ReconstructableRepository.for_file(
        #     file_relative_path(__file__, "../../../dagster-test/dagster_test/toys/hammer.py"),
        #     "hammer_pipeline",
        # )
        # instance = DagsterInstance.local_temp()

        # context = DagsterGraphQLContext(
        #     workspace=Workspace(
        #         [RepositoryLocationHandle.create_in_process_location(recon_repo.pointer)]
        #     ),
        #     instance=instance,
        # )

        # selector = infer_pipeline_selector(context, "hammer_pipeline")

        # executor = SyncExecutor()

        # variables = {
        #     "executionParams": {
        #         "runConfigData": {
        #             "storage": {"filesystem": {}},
        #             "execution": {"dask": {"config": {"cluster": {"local": {}}}}},
        #         },
        #         "selector": selector,
        #         "mode": "default",
        #     }
        # }
        # context = DagsterGraphQLContext(
        #     workspace=Workspace(
        #         [RepositoryLocationHandle.create_in_process_location(recon_repo.pointer)]
        #     ),
        #     instance=instance,
        # )
        selector = infer_pipeline_selector(graphql_context, "csv_hello_world")
        # selector = {
        #     'repositoryLocationName': 'test',
        #     'repositoryName': 'test_repo',
        #     'pipelineName': 'csv_hello_world',
        #     'solidSelection': None,
        # }
        result = execute_dagster_graphql(
            graphql_context,
            LAUNCH_PIPELINE_EXECUTION_MUTATION,
            variables={
                "executionParams": {
                    "selector": selector,
                    "runConfigData": run_config_data_fn(),
                    "mode": "default",
                }
            },
        )

        # variables = {
        #     'executionParams': {
        #         'selector': {
        #             'repositoryLocationName': 'test',
        #             'repositoryName': 'test_repo',
        #             'pipelineName': 'csv_hello_world',
        #             'solidSelection': None,
        #         },
        #         'runConfigData': {
        #             'solids': {
        #                 'sum_solid': {
        #                     'inputs': {
        #                         'num': '/Users/catherinewu/dagster/python_modules/dagster-graphql/dagster_graphql_tests/graphql/../data/num.csv'
        #                     }
        #                 }
        #             }
        #         },
        #         'mode': 'default',
        #     }
        # }

        # result = graphql(
        #     create_schema(),
        #     query=LAUNCH_PIPELINE_EXECUTION_MUTATION,
        #     context_value=graphql_context,
        #     variable_values={
        #         "executionParams": {
        #             "selector": selector,
        #             "runConfigData": run_config_data_fn(),
        #             "mode": "default",
        #         }
        #     },
        #     allow_subscriptions=True,
        #     return_promise=False,
        # )
        # context.log.info(str(result))
        return result

    return cross_dag_resource
