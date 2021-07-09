from functools import partial
from typing import Dict, Union

from dagit.templates.playground import TEMPLATE
from dagster import DagsterInstance
from dagster import __version__ as dagster_version
from dagster.cli.workspace.cli_target import get_workspace_process_context_from_kwargs
from dagster.core.workspace.context import WorkspaceProcessContext
from dagster_graphql import __version__ as dagster_graphql_version
from dagster_graphql.schema import create_schema
from graphene import Schema
from graphql.error import format_error as format_graphql_error
from starlette import status
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.routing import Route

from .version import __version__


async def dagit_info_endpoint(_request):
    return JSONResponse(
        {
            "dagit_version": __version__,
            "dagster_version": dagster_version,
            "dagster_graphql_version": dagster_graphql_version,
        }
    )


async def graphql_http_endpoint(
    schema: Schema,
    process_context: WorkspaceProcessContext,
    request: Request,
):
    """
    fork of starlette GraphQLApp to allow for
        * our context type (crucial)
        * our GraphiQL playground (could change)
    """

    if request.method == "GET":
        # render graphiql
        if "text/html" in request.headers.get("Accept", ""):
            # need to handle app_path_prefix here
            text = TEMPLATE.replace("{{ app_path_prefix }}", "")
            return HTMLResponse(text)

        data: Union[Dict[str, str], QueryParams] = request.query_params

    elif request.method == "POST":
        content_type = request.headers.get("Content-Type", "")

        if "application/json" in content_type:
            data = await request.json()
        elif "application/graphql" in content_type:
            body = await request.body()
            text = body.decode()
            data = {"query": text}
        elif "query" in request.query_params:
            data = request.query_params
        else:
            return PlainTextResponse(
                "Unsupported Media Type",
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            )

    else:
        return PlainTextResponse(
            "Method Not Allowed", status_code=status.HTTP_405_METHOD_NOT_ALLOWED
        )

    if "query" not in data:
        return PlainTextResponse(
            "No GraphQL query found in the request",
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    query = data["query"]
    variables = data.get("variables")
    operation_name = data.get("operationName")

    # context manager? scoping?
    context = process_context.create_request_context()

    result = await run_in_threadpool(  # threadpool = aio event loop
        schema.execute,
        query,
        variables=variables,
        operation_name=operation_name,
        context=context,
    )

    error_data = [format_graphql_error(err) for err in result.errors] if result.errors else None
    response_data = {"data": result.data}
    if error_data:
        response_data["errors"] = error_data
    status_code = status.HTTP_400_BAD_REQUEST if result.errors else status.HTTP_200_OK

    return JSONResponse(response_data, status_code=status_code)


def create_app(process_context: WorkspaceProcessContext, debug: bool):
    graphql_schema = create_schema()

    return Starlette(
        debug=debug,
        routes=[
            Route(
                "/graphql",
                partial(graphql_http_endpoint, graphql_schema, process_context),
                name="graphql-http",
                methods=["GET", "POST"],
            ),
            Route("/dagit_info", dagit_info_endpoint),
        ],
    )


def default_app():
    instance = DagsterInstance.get()
    process_context = get_workspace_process_context_from_kwargs(
        instance=instance,
        version=__version__,
        kwargs={},
    )
    return create_app(process_context, debug=False)
