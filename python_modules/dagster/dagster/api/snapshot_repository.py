from dagster import check
from dagster.core.code_pointer import CodePointer
from dagster.core.host_representation import (
    ExternalRepository,
    ExternalRepositoryData,
    RepositoryHandle,
    RepositoryLocationHandle,
)
from dagster.core.origin import RepositoryGrpcServerOrigin, RepositoryPythonOrigin

from .list_repositories import sync_list_repositories, sync_list_repositories_grpc
from .utils import execute_unary_api_cli_command


def sync_get_external_repositories(repository_location_handle):
    check.inst_param(
        repository_location_handle, 'repository_location_handle', RepositoryLocationHandle,
    )

    loadable_target_origin = repository_location_handle.loadable_target_origin

    response = sync_list_repositories(
        executable_path=loadable_target_origin.executable_path,
        python_file=loadable_target_origin.python_file,
        module_name=loadable_target_origin.module_name,
        working_directory=loadable_target_origin.working_directory,
        attribute=loadable_target_origin.attribute,
    )

    repository_code_pointer_dict = {}

    if loadable_target_origin.python_file:
        repository_code_pointer_dict = {
            lrs.repository_name: CodePointer.from_python_file(
                loadable_target_origin.python_file,
                lrs.attribute,
                loadable_target_origin.working_directory,
            )
            for lrs in response.repository_symbols
        }
    elif repository_location_handle.use_python_package:
        repository_code_pointer_dict = {
            lrs.repository_name: CodePointer.from_python_package(
                loadable_target_origin.module_name, lrs.attribute
            )
            for lrs in response.repository_symbols
        }
    else:
        repository_code_pointer_dict = {
            lrs.repository_name: CodePointer.from_module(
                loadable_target_origin.module_name, lrs.attribute
            )
            for lrs in response.repository_symbols
        }

    repos = []

    for _, pointer in repository_code_pointer_dict.items():
        external_repository_data = check.inst(
            execute_unary_api_cli_command(
                loadable_target_origin.executable_path,
                'repository',
                RepositoryPythonOrigin(loadable_target_origin.executable_path, pointer),
            ),
            ExternalRepositoryData,
        )
        repos.append(
            ExternalRepository(
                external_repository_data,
                RepositoryHandle(
                    repository_name=external_repository_data.name,
                    repository_location_handle=repository_location_handle,
                ),
            )
        )

    return repos


def sync_get_external_repositories_grpc(api_client, repository_location_handle):
    check.inst_param(
        repository_location_handle, 'repository_location_handle', RepositoryLocationHandle
    )

    list_repositories_response = sync_list_repositories_grpc(repository_location_handle.client)

    repos = []
    for symbol in list_repositories_response.repository_symbols:
        repository_name = symbol.repository_name
        external_repository_data = check.inst(
            api_client.external_repository(
                repository_grpc_server_origin=RepositoryGrpcServerOrigin(
                    repository_location_handle.host,
                    repository_location_handle.port,
                    repository_location_handle.socket,
                    repository_name,
                )
            ),
            ExternalRepositoryData,
        )
        repos.append(
            ExternalRepository(
                external_repository_data,
                RepositoryHandle(
                    repository_name=external_repository_data.name,
                    repository_location_handle=repository_location_handle,
                ),
            )
        )
    return repos
