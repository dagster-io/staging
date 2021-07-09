from starlette.testclient import TestClient


def test_dagit_info(empty_app):
    client = TestClient(empty_app)
    response = client.get("/dagit_info")
    assert response.status_code == 200
    assert response.json() == {
        "dagit_version": "dev",
        "dagster_version": "dev",
        "dagster_graphql_version": "dev",
    }


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
