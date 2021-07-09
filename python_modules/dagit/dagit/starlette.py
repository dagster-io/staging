from asyncio import Queue, create_task
from functools import partial
from os import path
from typing import Any, AsyncGenerator, Dict, List, Union

from dagit.templates.playground import TEMPLATE
from dagster import DagsterInstance
from dagster import __version__ as dagster_version
from dagster.cli.workspace.cli_target import get_workspace_process_context_from_kwargs
from dagster.core.workspace.context import WorkspaceProcessContext
from dagster_graphql import __version__ as dagster_graphql_version
from dagster_graphql.schema import create_schema
from graphene import Schema
from graphql.error import GraphQLError
from graphql.error import format_error as format_graphql_error
from rx import Observable
from starlette import status
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.types import Receive, Scope, Send
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from .version import __version__

ROOT_ADDRESS_STATIC_RESOURCES = [
    "/manifest.json",
    "/favicon.ico",
    "/favicon.png",
    "/asset-manifest.json",
    "/robots.txt",
    "/favicon_failed.ico",
    "/favicon_pending.ico",
    "/favicon_success.ico",
]

# https://github.com/apollographql/subscriptions-transport-ws/blob/master/PROTOCOL.md
GQL_CONNECTION_INIT = "connection_init"
GQL_CONNECTION_ACK = "connection_ack"
GQL_CONNECTION_ERROR = "connection_error"
GQL_CONNECTION_TERMINATE = "connection_terminate"
GQL_CONNECTION_KEEP_ALIVE = "ka"
GQL_START = "start"
GQL_DATA = "data"
GQL_ERROR = "error"
GQL_COMPLETE = "complete"
GQL_STOP = "stop"


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
    app_path_prefix: str,
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
            text = TEMPLATE.replace("{{ app_path_prefix }}", app_path_prefix)
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


async def graphql_ws_endpoint(
    schema: Schema,
    process_context: WorkspaceProcessContext,
    scope: Scope,
    receive: Receive,
    send: Send,
):

    websocket = WebSocket(scope=scope, receive=receive, send=send)

    subscriptions = {}
    tasks = {}

    await websocket.accept(subprotocol="graphql-ws")

    try:
        while (
            websocket.client_state != WebSocketState.DISCONNECTED
            and websocket.application_state != WebSocketState.DISCONNECTED
        ):
            message = await websocket.receive_json()
            operation_id = message.get("id")
            message_type = message.get("type")

            if message_type == GQL_CONNECTION_INIT:
                await websocket.send_json({"type": GQL_CONNECTION_ACK})

            elif message_type == GQL_CONNECTION_TERMINATE:
                await websocket.close()
            elif message_type == GQL_START:
                try:
                    data = message["payload"]
                    query = data["query"]
                    variables = data.get("variables")
                    operation_name = data.get("operation_name")

                    # correct scoping?
                    request_context = process_context.create_request_context()
                    async_result = schema.execute(
                        query,
                        variables=variables,
                        operation_name=operation_name,
                        context=request_context,
                        allow_subscriptions=True,
                    )
                except GraphQLError as error:
                    # Syntax errors can cause errors early on but bubble up before
                    # being converted to an `ExecutionResult` so we need to handle
                    # them here.
                    payload = format_graphql_error(error)
                    await _send_message(websocket, GQL_ERROR, payload, operation_id)
                    continue

                # Errors -- such as those caused by or an invalid subscription field
                # being specified in the query -- can cause this to fail in a bad
                # way. In addition to the stack trace in the server logs, to the
                # client it appears as though the connection was severed for no
                # reason.
                if not isinstance(async_result, Observable):
                    assert async_result.errors is not None
                    payload = format_graphql_error(async_result.errors[0])
                    await _send_message(websocket, GQL_ERROR, payload, operation_id)
                    continue

                async_result = _obs_to_agen(async_result)

                subscriptions[operation_id] = async_result

                tasks[operation_id] = create_task(
                    handle_async_results(async_result, operation_id, websocket)
                )
            elif message_type == GQL_STOP:
                if operation_id not in subscriptions:
                    return

                await subscriptions[operation_id].aclose()
                tasks[operation_id].cancel()
                del tasks[operation_id]
                del subscriptions[operation_id]
    except WebSocketDisconnect:  # pragma: no cover
        pass
    finally:
        for operation_id in subscriptions:
            await subscriptions[operation_id].aclose()
            tasks[operation_id].cancel()


async def handle_async_results(results: AsyncGenerator, operation_id: str, websocket: WebSocket):
    try:
        async for result in results:
            payload = {"data": result.data}

            if result.errors:
                payload["errors"] = [format_graphql_error(err) for err in result.errors]

            await _send_message(websocket, GQL_DATA, payload, operation_id)
    except Exception as error:  # pylint: disable=broad-except
        if not isinstance(error, GraphQLError):
            # original_error in later versions of graphql?
            # error = GraphQLError(str(error), original_error=error)
            error = GraphQLError(str(error))

        await _send_message(
            websocket,
            GQL_DATA,
            {"data": None, "errors": [format_graphql_error(error)]},
            operation_id,
        )

    if (
        websocket.client_state != WebSocketState.DISCONNECTED
        and websocket.application_state != WebSocketState.DISCONNECTED
    ):
        await _send_message(websocket, GQL_COMPLETE, None, operation_id)


async def _send_message(
    websocket: WebSocket,
    type_: str,
    payload: Any,
    operation_id: str,
) -> None:
    data = {"type": type_, "id": operation_id}

    if payload is not None:
        data["payload"] = payload

    return await websocket.send_json(data)


async def _obs_to_agen(obs: Observable):
    """
    Convert Observable to async generator for back compat.

    Should be removed and subscriptions rewritten.
    """
    queue: Queue = Queue()
    obs.subscribe(on_next=queue.put_nowait)
    while True:
        i = await queue.get()
        yield i


def index_endpoint(
    base_dir: str,
    app_path_prefix: str,
    _request: Request,
):
    """
    Serves root html
    """
    index_path = path.join(base_dir, "./webapp/build/index.html")

    try:
        with open(index_path) as f:
            rendered_template = f.read()
            return HTMLResponse(
                rendered_template.replace('href="/', f'href="{app_path_prefix}/')
                .replace('src="/', f'src="{app_path_prefix}/')
                .replace("__PATH_PREFIX__", app_path_prefix)
            )
    except FileNotFoundError:
        raise Exception(
            """Can't find webapp files. Probably webapp isn't built. If you are using
            dagit, then probably it's a corrupted installation or a bug. However, if you are
            developing dagit locally, your problem can be fixed as follows:

            cd ./python_modules/
            make rebuild_dagit"""
        )


def create_root_static_endpoints(base_dir: str) -> List[Route]:
    def _static_file(file_path):
        return Route(
            file_path,
            lambda _: FileResponse(path=path.join(base_dir, f"./webapp/build{file_path}")),
        )

    return [_static_file(f) for f in ROOT_ADDRESS_STATIC_RESOURCES]


def create_app(process_context: WorkspaceProcessContext, app_path_prefix: str, debug: bool):
    base_dir = path.dirname(__file__)
    graphql_schema = create_schema()

    bound_index_endpoint = partial(index_endpoint, base_dir, app_path_prefix)

    return Starlette(
        debug=debug,
        routes=[
            Route(
                "/graphql",
                partial(graphql_http_endpoint, graphql_schema, process_context, app_path_prefix),
                name="graphql-http",
                methods=["GET", "POST"],
            ),
            WebSocketRoute(
                "/graphql",
                partial(graphql_ws_endpoint, graphql_schema, process_context),
                name="graphql-ws",
            ),
            Route("/dagit_info", dagit_info_endpoint),
            # static resources addressed at /static/
            Mount(
                "/static",
                StaticFiles(directory=path.join(base_dir, "./webapp/build/static")),
                name="static",
            ),
            # static resources addressed at /vendor/
            Mount(
                "/vendor",
                StaticFiles(directory=path.join(base_dir, "./webapp/build/vendor")),
                name="vendor",
            ),
            # specific static resources addressed at /
            *create_root_static_endpoints(base_dir),
            Route("/{path:path}", bound_index_endpoint),
            Route("/", bound_index_endpoint),
        ],
    )


def default_app():
    instance = DagsterInstance.get()
    process_context = get_workspace_process_context_from_kwargs(
        instance=instance,
        version=__version__,
        kwargs={},
    )
    return create_app(process_context, app_path_prefix="", debug=False)
