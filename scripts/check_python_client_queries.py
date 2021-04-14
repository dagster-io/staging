import os
from datetime import datetime
from typing import Dict

import click
import dagster_graphql
import dagster_graphql_tests
from dagster_graphql.client import client_queries


def get_queries() -> Dict[str, str]:
    """Helper function to index the graphql client's queries

    Returns:
        Dict[str, str]: dictionary - key is variable (query) name
            the value is the query string
    """
    res_dict: Dict[str, str] = {}
    for name in dir(client_queries):
        obj = getattr(client_queries, name)
        if isinstance(obj, str) and not name.startswith("__"):
            res_dict[name] = obj
    return res_dict


@click.command()
@click.option(
    "--auto-fix-issues",
    "-af",
    default=False,
    type=click.BOOL,
    help="Whether the script should automatically attempt to resolve python client backcompat setup issues. "
    + "This option is meant to be used manually from the command line. Disabled by default.",
)
def check_python_client_queries_in_backcompat_directory(auto_fix_issues: bool):
    """This script checks whether backcompatability test setup is completed for the Dagster Python GraphQL Client.
    This is useful as a reminder when new queries or added or changes are made to existing queries in
    use by the client.
    """
    current_queries_dict = get_queries()

    legacy_query_history_dir = (
        dagster_graphql_tests.__path__[0] + "/graphql/client_backcompat/queries"
    )
    legacy_queries = frozenset(next(os.walk(legacy_query_history_dir))[1])
    query_directories_present = {
        query_name: query_name in legacy_queries for query_name in current_queries_dict
    }
    missing_query_history_subdirs = [
        query_name
        for (query_name, query_present) in query_directories_present.items()
        if not query_present
    ]
    if missing_query_history_subdirs:
        if auto_fix_issues:
            for query_name in missing_query_history_subdirs:
                query_dir = os.path.join(legacy_query_history_dir, query_name)
                click.echo(
                    f"Couldn't find query history subdirectory for query {query_name}, so making a new one"
                    + f"\n\t at {query_dir}"
                )
                os.mkdir(query_dir)
        else:
            raise Exception(
                "Missing some query history (sub)directories:"
                f"\n\t{missing_query_history_subdirs}"
                + f"\n\t at {legacy_query_history_dir}"
                + "\n\t Please run this script again (on the command line) with `--auto-fix-issues=true`"
                + " enabled or manually resolve these issues"
            )
    for query_name in query_directories_present:
        query_dir = os.path.join(legacy_query_history_dir, query_name)
        query_is_present = False
        for filename in os.listdir(query_dir):
            file_path = os.path.join(query_dir, filename)
            with open(file_path, "r") as f:
                old_query = f.read()
                if old_query == current_queries_dict[query_name]:
                    query_is_present = True
                    break
        if not query_is_present:
            if auto_fix_issues:
                query_filename = (
                    "-".join([dagster_graphql.__version__, datetime.today().strftime("%Y_%m_%d")])
                    + ".graphql"
                )
                query_full_file_path = os.path.join(query_dir, query_filename)
                click.echo(
                    f"Couldn't find a matching query for dagster_graphql.client.client_queries.{query_name}"
                    + f", so writing the query to a file: {query_full_file_path}"
                )
                with open(query_full_file_path, "w") as f:
                    f.write(current_queries_dict[query_name])
            else:
                raise Exception(
                    f"The query dagster_graphql.client.client_queries.{query_name} "
                    + "is not present in the backcompatability history "
                    + f"directory {legacy_query_history_dir} "
                    + "\n\t Please run this script again (on the command line) with `--auto-fix-issues=true`"
                    + " enabled or manually resolve these issues"
                )
    click.echo("All GraphQL Python Client backcompatability setup complete!")


if __name__ == "__main__":
    check_python_client_queries_in_backcompat_directory()  # pylint: disable=E1120
