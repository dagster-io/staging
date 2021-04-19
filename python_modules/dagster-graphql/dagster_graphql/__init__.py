from dagster.core.utils import check_dagster_package_version
from dagster_graphql.client import DagsterGraphQLClient

from .client import DagsterGraphQLClient
from .version import __version__

check_dagster_package_version("dagster-graphql", __version__)

__all__ = ["DagsterGraphQLClient"]
