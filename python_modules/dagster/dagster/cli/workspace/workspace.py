import os
import sys
import warnings
from collections import OrderedDict, namedtuple

from dagster import check
from dagster.core.code_pointer import CodePointer
from dagster.core.definitions.reconstructable import (
    ReconstructablePipeline,
    ReconstructableRepository,
    load_def_in_module,
    load_def_in_package,
    load_def_in_python_file,
)
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.host_representation import RepositoryLocationOrigin
from dagster.utils.error import serializable_error_info_from_exc_info


class WorkspaceSnapshot(
    namedtuple("WorkspaceSnapshot", "location_origin_dict location_error_dict")
):
    """
    This class is request-scoped object that stores a reference to all the locaiton origins and errors
    that were on a `Workspace`.

    This object is needed because a workspace and handles/errors on that workspace can be updated
    (for example, from a thread on the process context). If a request is accessing a repository location
    at the same time the repository location was being cleaned up, we would run into errors.
    """

    def __new__(cls, location_origin_dict, _location_error_dict):
        return super(WorkspaceSnapshot, cls).__new__(
            cls, location_origin_dict, _location_error_dict
        )

    def is_reload_supported(self, location_name):
        return self.location_origin_dict[location_name].is_reload_supported

    @property
    def repository_location_names(self):
        return list(self.location_origin_dict.keys())

    @property
    def repository_location_errors(self):
        return list(self.location_error_dict.values())

    def has_repository_location_error(self, location_name):
        check.str_param(location_name, "location_name")
        return location_name in self.location_error_dict

    def get_repository_location_error(self, location_name):
        check.str_param(location_name, "location_name")
        return self.location_error_dict[location_name]


class Workspace:
    def __init__(self, workspace_load_target):

        from .cli_target import WorkspaceLoadTarget

        self._workspace_load_target = check.opt_inst_param(
            workspace_load_target, "workspace_load_target", WorkspaceLoadTarget
        )

        repository_location_origins = (
            self._workspace_load_target.create_origins() if self._workspace_load_target else []
        )

        self._location_origin_dict = OrderedDict()
        check.list_param(
            repository_location_origins,
            "repository_location_origins",
            of_type=RepositoryLocationOrigin,
        )

        self._location_handle_dict = {}
        self._location_error_dict = {}
        for origin in repository_location_origins:
            check.invariant(
                self._location_origin_dict.get(origin.location_name) is None,
                'Cannot have multiple locations with the same name, got multiple "{name}"'.format(
                    name=origin.location_name,
                ),
            )

            self._location_origin_dict[origin.location_name] = origin
            self._load_handle(origin.location_name)

    # Can be overidden in subclasses that need different logic for loading repository
    # locations from origins
    def create_handle_from_origin(self, origin):
        return origin.create_handle()

    def _load_handle(self, location_name):
        existing_handle = self._location_handle_dict.get(location_name)
        if existing_handle:
            # We don't clean up here anymore because we want these to last while being
            # used in other requests
            # existing_handle.cleanup()
            del self._location_handle_dict[location_name]

        if self._location_error_dict.get(location_name):
            del self._location_error_dict[location_name]

        origin = self._location_origin_dict[location_name]
        try:
            handle = self.create_handle_from_origin(origin)
            self._location_handle_dict[location_name] = handle
        except Exception:  # pylint: disable=broad-except
            error_info = serializable_error_info_from_exc_info(sys.exc_info())
            self._location_error_dict[location_name] = error_info
            warnings.warn(
                "Error loading repository location {location_name}:{error_string}".format(
                    location_name=location_name, error_string=error_info.to_string()
                )
            )

    def create_snapshot(self):
        return WorkspaceSnapshot(
            self._location_origin_dict.copy(), self._location_error_dict.copy()
        )

    @property
    def repository_location_handles(self):
        return list(self._location_handle_dict.values())

    def has_repository_location_handle(self, location_name):
        check.str_param(location_name, "location_name")
        return location_name in self._location_handle_dict

    def get_repository_location_handle(self, location_name):
        check.str_param(location_name, "location_name")
        return self._location_handle_dict[location_name]

    def has_repository_location_error(self, location_name):
        check.str_param(location_name, "location_name")
        return location_name in self._location_error_dict

    def reload_repository_location(self, location_name):
        self._load_handle(location_name)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        for handle in self.repository_location_handles:
            handle.cleanup()

    @staticmethod
    def reconstructable_from_workspace(
        workspace_path: str, pipeline_name: str
    ) -> ReconstructablePipeline:
        from .load import location_origins_from_yaml_paths

        if not os.path.exists(workspace_path):
            raise DagsterInvariantViolationError(
                f"Provided path '{workspace_path}' does not exist."
            )
        for location_origin in location_origins_from_yaml_paths([workspace_path]):
            pointer = get_code_pointer_for_origin(location_origin.loadable_target_origin)
            repo = ReconstructableRepository(pointer)
            if pipeline_name in repo.get_definition().pipeline_names:
                return repo.get_reconstructable_pipeline(pipeline_name)
        raise DagsterInvariantViolationError(f"No pipeline found in workspace `{workspace_path}`.")


def get_loadable_targets(loadable_target_origin):
    from .autodiscovery import (
        LoadableTarget,
        loadable_targets_from_python_file,
        loadable_targets_from_python_module,
        loadable_targets_from_python_package,
    )

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
