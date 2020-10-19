from dagster.core.errors import DagsterError


class DagsterGraphQLClientError(DagsterError):
    """Indicates that some error has occurred when executing a GraphQL query against
    dagster-graphql"""
