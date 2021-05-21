import asyncio
import json
import os
import typing

from dagit.templates.playground import TEMPLATE
from dagster import DagsterInstance
from dagster.cli.workspace import get_workspace_from_kwargs
from dagster.cli.workspace.context import WorkspaceProcessContext
from dagster_graphql.schema import create_schema
from graphql.error import GraphQLError
from graphql.error import format_error as format_graphql_error
from rx import Observable
from starlette import status
from starlette.applications import Starlette
from starlette.background import BackgroundTasks
from starlette.concurrency import run_in_threadpool
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.types import Receive, Scope, Send
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

GRAPHQL_WS = "graphql-ws"
WS_PROTOCOL = GRAPHQL_WS

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


class DagsterGraphQLApp:
    """
    Mix of starlettes's GraphQLApp

    https://github.com/encode/starlette/blob/master/starlette/graphql.py

    and strawberry-graphql ASGI app which has support for websockets

    https://github.com/strawberry-graphql/strawberry/blob/main/strawberry/asgi/__init__.py
    """

    def __init__(
        self,
        process_context,
        app_path_prefix="",
        graphiql: bool = True,
    ) -> None:
        self.schema = create_schema()
        self.process_context = process_context
        self.graphiql = graphiql
        self.app_path_prefix = app_path_prefix

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            await self.handle_http(scope=scope, receive=receive, send=send)
        elif scope["type"] == "websocket":
            await self.handle_websocket(scope=scope, receive=receive, send=send)
        else:
            raise ValueError("Unknown scope type: %r" % (scope["type"],))

    async def handle_http(self, scope: Scope, receive: Receive, send: Send):
        request = Request(scope=scope, receive=receive)
        response = await self.handle_graphql(request)
        await response(scope, receive, send)

    async def handle_graphql(self, request: Request) -> Response:
        if request.method in ("GET", "HEAD"):
            if "text/html" in request.headers.get("Accept", ""):
                if not self.graphiql:
                    return PlainTextResponse("Not Found", status_code=status.HTTP_404_NOT_FOUND)
                return await self.handle_graphiql(request)

            data = request.query_params

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

        try:
            query = data["query"]
            variables = data.get("variables")
            operation_name = data.get("operationName")
        except KeyError:
            return PlainTextResponse(
                "No GraphQL query found in the request",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        background = BackgroundTasks()

        # context = {"request": request, "background": background}
        context = self.process_context.create_request_context()

        result = await run_in_threadpool(
            self.schema.execute,
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

        return JSONResponse(response_data, status_code=status_code, background=background)

    async def handle_graphiql(self, request: Request) -> Response:
        text = TEMPLATE.replace("{{ app_path_prefix }}", self.app_path_prefix)
        return HTMLResponse(text)

    async def handle_websocket(self, scope: Scope, receive: Receive, send: Send):
        websocket = WebSocket(scope=scope, receive=receive, send=send)

        subscriptions = {}
        tasks = {}
        print("ohk kay")
        await websocket.accept(subprotocol=GRAPHQL_WS)
        print("whaoahahoh kay")
        try:
            while (
                websocket.client_state != WebSocketState.DISCONNECTED
                and websocket.application_state != WebSocketState.DISCONNECTED
            ):
                print("wait for msg")
                message = await websocket.receive_json()
                print("got", message)
                operation_id = message.get("id")
                message_type = message.get("type")

                if message_type == GQL_CONNECTION_INIT:
                    await websocket.send_json({"type": GQL_CONNECTION_ACK})

                    # if self.keep_alive:
                    #     self._keep_alive_task = asyncio.create_task(
                    #         self.handle_keep_alive(websocket)
                    #     )
                elif message_type == GQL_CONNECTION_TERMINATE:
                    await websocket.close()
                elif message_type == GQL_START:
                    try:
                        print(operation_id)
                        async_result = await self.start_subscription(
                            message.get("payload"), operation_id, websocket
                        )
                        print(async_result)
                    except GraphQLError as error:
                        # Syntax errors can cause errors early on but bubble up before
                        # being converted to an `ExecutionResult` so we need to handle
                        # them here.
                        payload = format_graphql_error(error)
                        await self._send_message(websocket, GQL_ERROR, payload, operation_id)
                        continue

                    # Errors -- such as those caused by or an invalid subscription field
                    # being specified in the query -- can cause this to fail in a bad
                    # way. In addition to the stack trace in the server logs, to the
                    # client it appears as though the connection was severed for no
                    # reason.
                    if not isinstance(async_result, Observable):
                        assert async_result.errors is not None
                        payload = format_graphql_error(async_result.errors[0])
                        await self._send_message(websocket, GQL_ERROR, payload, operation_id)
                        continue

                    async_result = obs_to_agen(async_result)

                    subscriptions[operation_id] = async_result

                    tasks[operation_id] = asyncio.create_task(
                        self.handle_async_results(async_result, operation_id, websocket)
                    )
                elif message_type == GQL_STOP:  # pragma: no cover
                    if operation_id not in subscriptions:
                        return

                    await subscriptions[operation_id].aclose()
                    tasks[operation_id].cancel()
                    del tasks[operation_id]
                    del subscriptions[operation_id]
        except WebSocketDisconnect as e:  # pragma: no cover
            import traceback

            traceback.print_exc()
            pass
        finally:
            # if self._keep_alive_task:
            #     self._keep_alive_task.cancel()

            for operation_id in subscriptions:
                await subscriptions[operation_id].aclose()
                tasks[operation_id].cancel()

    async def start_subscription(self, data, operation_id: str, websocket: WebSocket):
        query = data["query"]
        variables = data.get("variables")
        operation_name = data.get("operation_name")

        # if self.debug:
        #     pretty_print_graphql_operation(operation_name, query, variables)

        context = self.process_context.create_request_context()

        return self.schema.execute(
            query,
            variables=variables,
            operation_name=operation_name,
            context=context,
            allow_subscriptions=True,
        )

    async def handle_async_results(
        self, results: typing.AsyncGenerator, operation_id: str, websocket: WebSocket
    ):
        try:
            async for result in results:
                payload = {"data": result.data}

                if result.errors:
                    payload["errors"] = [format_graphql_error(err) for err in result.errors]

                await self._send_message(websocket, GQL_DATA, payload, operation_id)
        except Exception as error:
            if not isinstance(error, GraphQLError):
                error = GraphQLError(str(error), original_error=error)

            await self._send_message(
                websocket,
                GQL_DATA,
                {"data": None, "errors": [format_graphql_error(error)]},
                operation_id,
            )

        if (
            websocket.client_state != WebSocketState.DISCONNECTED
            and websocket.application_state != WebSocketState.DISCONNECTED
        ):
            await self._send_message(websocket, GQL_COMPLETE, None, operation_id)

    async def _send_message(
        self,
        websocket: WebSocket,
        type_: str,
        payload: typing.Any = None,
        operation_id: str = None,
    ) -> None:
        data = {"type": type_}

        if operation_id is not None:
            data["id"] = operation_id

        if payload is not None:
            data["payload"] = payload

        return await websocket.send_json(data)


async def obs_to_agen(obs: Observable):
    """
    convert Observable to async generator

    credit: https://blog.oakbits.com/rxpy-and-asyncio.html
    """
    queue = asyncio.Queue()
    obs.subscribe(on_next=queue.put_nowait)
    while True:
        i = await queue.get()
        yield i

    # def on_next(i):
    #     queue.put_nowait(i)

    # disposable = obs.pipe(ops.materialize()).subscribe(
    #     on_next=on_next, scheduler=AsyncIOScheduler(loop=loop)
    # )

    # while True:
    #     i = await queue.get()
    #     if isinstance(i, OnNext):
    #         yield i.value
    #         queue.task_done()
    #     elif isinstance(i, OnError):
    #         disposable.dispose()
    #         raise (Exception(i.value))
    #     else:
    #         disposable.dispose()
    #         break


async def homepage(request):
    return JSONResponse({"hello": "world"})


instance = DagsterInstance.get()
workspace = get_workspace_from_kwargs({})
process_context = WorkspaceProcessContext(
    instance=instance,
    workspace=workspace,
)
schema = create_schema()
app_path_prefix = ""

target_dir = os.path.dirname(__file__)
index_path = os.path.join(target_dir, "./webapp/build/index.html")


def index_view(request):  # pylint: disable=unused-argument
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


app = Starlette(
    debug=True,
    routes=[
        Route("/graphql", DagsterGraphQLApp(process_context=process_context), name="graphql-http"),
        WebSocketRoute(
            "/graphql", DagsterGraphQLApp(process_context=process_context), name="graphql-ws"
        ),
        Route("/", index_view),
        Route("/{path:path}", index_view),
        Mount(
            "/",
            StaticFiles(directory=os.path.join(target_dir, "./webapp/build")),
            name="static",
        ),
    ],
    middleware=[
        Middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])
    ],
)
