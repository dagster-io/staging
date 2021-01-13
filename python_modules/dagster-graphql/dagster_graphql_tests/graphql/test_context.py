from dagster import lambda_solid, pipeline, repository
from dagster.core.test_utils import instance_for_test
from dagster_graphql.implementation.context import ProcessContext
from dagster_graphql.test.utils import define_out_of_process_workspace


def get_repo():
    """
    This is a repo that changes name very time it's loaded
    """

    @lambda_solid
    def solid_A():
        pass

    @pipeline
    def pipe():
        solid_A()

    import random
    import string

    @repository(name="".join(random.choice(string.ascii_lowercase) for i in range(10)))
    def my_repo():
        return [pipe]

    return my_repo


def test_reload_on_process_context():
    with instance_for_test() as instance:
        with define_out_of_process_workspace(__file__, "get_repo") as workspace:
            # Create a process context
            process_context = ProcessContext(workspace=workspace, instance=instance)
            assert len(process_context.repository_locations) == 1

            # Save the repository name
            repository_location = process_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            repo_name = repo.name

            # Reload the location and save the new repository name
            process_context.reload_repository_location(repository_location.name)
            repository_location = process_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            new_repo_name = repo.name

            # Check that the repository has changed
            assert repo_name != new_repo_name


def test_reload_on_request_context():
    with instance_for_test() as instance:
        with define_out_of_process_workspace(__file__, "get_repo") as workspace:
            # Create a process context
            process_context = ProcessContext(workspace=workspace, instance=instance)
            assert len(process_context.repository_locations) == 1

            # Save the repository name
            repository_location = process_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            repo_name = repo.name

            # Create a request context from the process context
            request_context = process_context.create_request_context()

            # Reload the location and save the new repository name
            process_context.reload_repository_location(repository_location.name)
            repository_location = process_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            new_repo_name = repo.name

            # Save the repository name from the request context
            repository_location = request_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            request_context_repo_name = repo.name

            # Check that the repository has changed
            assert repo_name != new_repo_name

            # Check that the repository name is still the same on the request context,
            # confirming that the old repository location is still running
            assert repo_name == request_context_repo_name


def test_reload_on_request_context_2():
    # This is the similar to the test `test_reload_on_request_context`,
    # but calls reload from the request_context instead of on the process_context

    with instance_for_test() as instance:
        with define_out_of_process_workspace(__file__, "get_repo") as workspace:
            # Create a process context
            process_context = ProcessContext(workspace=workspace, instance=instance)
            assert len(process_context.repository_locations) == 1

            # Save the repository name
            repository_location = process_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            repo_name = repo.name

            # Create a request context from the process context
            request_context = process_context.create_request_context()

            # Reload the location from the request context
            new_request_context = request_context.reload_repository_location(
                repository_location.name
            )

            # Save the repository name from the:
            #   - Old request context
            #   - New request context
            #   - Process context
            repository_location = process_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            new_repo_name_process_context = repo.name

            repository_location = new_request_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            new_request_context_repo_name = repo.name

            repository_location = request_context.repository_locations[0]
            repo = list(repository_location.get_repositories().values())[0]
            request_context_repo_name = repo.name

            assert repo_name == request_context_repo_name
            assert request_context_repo_name != new_request_context_repo_name
            assert new_repo_name_process_context == new_request_context_repo_name
