import inspect
import os
from typing import Optional

from dagster import check
from dagster.cli.pipeline import create_external_pipeline_run
from dagster.cli.workspace.autodiscovery import (
    LoadableTarget,
    loadable_targets_from_python_file,
    loadable_targets_from_python_module,
    loadable_targets_from_python_package,
)
from dagster.cli.workspace.cli_target import (
    get_external_pipeline_from_external_repo,
    get_external_repository_from_repo_location,
)
from dagster.cli.workspace.load import location_origins_from_yaml_paths
from dagster.core.code_pointer import CodePointer
from dagster.core.definitions.reconstructable import (
    ReconstructableRepository,
    load_def_in_module,
    load_def_in_package,
    load_def_in_python_file,
)
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.instance import DagsterInstance


def _find_closest_workspace(path: str) -> Optional[str]:
    searching_for = "workspace.yaml"
    last_root = path
    current_root = path
    found_path = None
    while found_path is None and current_root:
        pruned = False
        for root, dirs, files in os.walk(current_root):
            if not pruned:
                try:
                    # Remove the part of the tree we already searched
                    del dirs[dirs.index(os.path.basename(last_root))]
                    pruned = True
                except ValueError:
                    pass
            if searching_for in files:
                # found the file, stop
                found_path = os.path.join(root, searching_for)
                break
        # Otherwise, pop up a level, search again
        last_root = current_root
        current_root = os.path.dirname(last_root)

    return found_path


def _get_location_with_pipeline(workspace_path, pipeline_name):
    location_origins = location_origins_from_yaml_paths([workspace_path])
    for location_origin in location_origins:
        pointer = get_code_pointer_for_origin(location_origin.loadable_target_origin)
        repo = ReconstructableRepository(pointer)
        if pipeline_name in repo.get_definition().pipeline_names:
            return location_origin, repo.get_definition().name

    raise DagsterInvariantViolationError(f"Could not find {pipeline_name} in workspace.")


def launch(pipeline_def, mode=None, run_config=None):
    with DagsterInstance.get() as dagster_instance:
        python_file = inspect.getmodule(pipeline_def).__file__
        workspace_path = _find_closest_workspace(python_file)
        location_containing_pipeline, repo_name = _get_location_with_pipeline(
            workspace_path, pipeline_def.name
        )
        with location_containing_pipeline.create_handle() as handle:
            repo_location = handle.create_location()
            external_repo = get_external_repository_from_repo_location(repo_location, repo_name)
            external_pipeline = get_external_pipeline_from_external_repo(
                external_repo, pipeline_def.name
            )
            pipeline_run = create_external_pipeline_run(
                instance=dagster_instance,
                repo_location=repo_location,
                external_repo=external_repo,
                external_pipeline=external_pipeline,
                run_config=run_config,
                mode=mode,
                preset=None,
                tags=None,
                solid_selection=None,
                run_id=None,
            )

            return dagster_instance.submit_run(pipeline_run.run_id, external_pipeline)


def get_loadable_targets(loadable_target_origin):

    if loadable_target_origin.python_file:
        return (
            [
                LoadableTarget(
                    loadable_target_origin.attribute,
                    load_def_in_python_file(
                        loadable_target_origin.python_file,
                        loadable_target_origin.attribute,
                        loadable_target_origin.working_directory,
                    ),
                )
            ]
            if loadable_target_origin.attribute
            else loadable_targets_from_python_file(
                loadable_target_origin.python_file, loadable_target_origin.working_directory
            )
        )
    elif loadable_target_origin.module_name:
        return (
            [
                LoadableTarget(
                    loadable_target_origin.attribute,
                    load_def_in_module(
                        loadable_target_origin.module_name, loadable_target_origin.attribute
                    ),
                )
            ]
            if loadable_target_origin.attribute
            else loadable_targets_from_python_module(loadable_target_origin.module_name)
        )
    elif loadable_target_origin.package_name:
        return (
            [
                LoadableTarget(
                    loadable_target_origin.attribute,
                    load_def_in_package(
                        loadable_target_origin.package_name, loadable_target_origin.attribute
                    ),
                )
            ]
            if loadable_target_origin.attribute
            else loadable_targets_from_python_package(loadable_target_origin.package_name)
        )
    else:
        check.failed("invalid")


def get_code_pointer_for_origin(loadable_target_origin):
    loadable_targets = get_loadable_targets(loadable_target_origin)
    for loadable_target in loadable_targets:
        if loadable_target_origin.python_file:
            return CodePointer.from_python_file(
                loadable_target_origin.python_file,
                loadable_target.attribute,
                loadable_target_origin.working_directory,
            )
        elif loadable_target_origin.package_name:
            return CodePointer.from_python_package(
                loadable_target_origin.package_name,
                loadable_target.attribute,
            )
        else:
            return CodePointer.from_module(
                loadable_target_origin.module_name,
                loadable_target.attribute,
            )
