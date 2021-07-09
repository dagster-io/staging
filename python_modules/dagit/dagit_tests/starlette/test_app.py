from dagit.starlette import (
    GQL_CONNECTION_ACK,
    GQL_CONNECTION_INIT,
    GQL_CONNECTION_TERMINATE,
    GQL_DATA,
    GQL_ERROR,
    GQL_START,
    GRAPHQL_WS,
    ROOT_ADDRESS_STATIC_RESOURCES,
)
from dagster import execute_pipeline, pipeline
from starlette.testclient import TestClient

EVENT_LOG_SUBSCRIPTION = """
    subscription PipelineRunLogsSubscription($runId: ID!) {
        pipelineRunLogs(runId: $runId) {
            __typename
        }
    }
"""


def test_dagit_info(empty_app):
    client = TestClient(empty_app)
    response = client.get("/dagit_info")
    assert response.status_code == 200
    assert response.json() == {
        "dagit_version": "dev",
        "dagster_version": "dev",
        "dagster_graphql_version": "dev",
    }


def test_static_resources(empty_app):
    client = TestClient(empty_app)

    # make sure we did not fallback to the index html
    # for static resources at /
    for address in ROOT_ADDRESS_STATIC_RESOURCES:
        response = client.get(address)
        assert response.status_code == 200, response.text
        assert response.headers["content-type"] != "text/html"

    response = client.get("/vendor/graphql-playground/middleware.js")
    assert response.status_code == 200, response.text
    assert response.headers["content-type"] != "application/js"


def test_graphql_get(empty_app):
    client = TestClient(empty_app)
    response = client.get(
        "/graphql?query={__typename}",
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"data": {"__typename": "Query"}}


def test_graphql_post(empty_app):
    client = TestClient(empty_app)
    response = client.post(
        "/graphql?query={__typename}",
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"data": {"__typename": "Query"}}

    response = client.post(
        "/graphql",
        json={"query": "{__typename}"},
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"data": {"__typename": "Query"}}


def test_graphql_ws_error(empty_app):
    # wtf pylint
    # pylint: disable=not-context-manager
    with TestClient(empty_app).websocket_connect("/graphql", GRAPHQL_WS) as ws:
        ws.send_json({"type": GQL_CONNECTION_INIT})
        ws.send_json(
            {
                "type": GQL_START,
                "id": "1",
                "payload": {"query": "subscription { oops }"},
            }
        )

        response = ws.receive_json()
        assert response["type"] == GQL_CONNECTION_ACK

        response = ws.receive_json()

        assert response["id"] == "1"
        assert response["type"] == GQL_ERROR


def test_graphql_ws_success(instance, empty_app):
    @pipeline
    def _test():
        pass

    result = execute_pipeline(_test, instance=instance)
    run_id = result.run_id
    # wtf pylint
    # pylint: disable=not-context-manager
    with TestClient(empty_app).websocket_connect("/graphql", GRAPHQL_WS) as ws:
        ws.send_json({"type": GQL_CONNECTION_INIT})
        ws.send_json(
            {
                "type": GQL_START,
                "id": "1",
                "payload": {"query": EVENT_LOG_SUBSCRIPTION, "variables": {"runId": run_id}},
            }
        )

        response = ws.receive_json()
        assert response["type"] == GQL_CONNECTION_ACK

        response = ws.receive_json()
        assert response["id"] == "1"
        assert response["type"] == GQL_DATA
