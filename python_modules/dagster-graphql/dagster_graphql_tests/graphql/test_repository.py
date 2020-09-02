from dagster_graphql.test.utils import execute_dagster_graphql, infer_repository_selector


def repository_query():
    return """
    query RepositoryQuery($repositorySelector: RepositorySelector!) {
        repositoryOrError(repositorySelector: $repositorySelector) {
           ... on Repository {
                id
                origin {
                    __typename
                    ... on PythonRepositoryOrigin {
                        executablePath
                        codePointer {
                            description
                            metadata {
                                key
                                value
                            }
                        }
                    }
                    ... on GrpcRepositoryOrigin {
                        grpcUrl
                    }
                }
                location {
                    name
                    isReloadSupported
                    environmentPath
                }
            }
        }
    }
    """


def test_query_all_solids(graphql_context, snapshot):
    selector = infer_repository_selector(graphql_context)
    result = execute_dagster_graphql(
        graphql_context, repository_query(), variables={"repositorySelector": selector}
    )
    snapshot.assert_match(result.data)
