import inspect
import os
import sys
import warnings
from collections import OrderedDict, namedtuple
from contextlib import contextmanager
from typing import Optional

from dagster import check
from dagster.core.definitions.reconstructable import repository_def_from_target_def
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.host_representation import RepositoryLocationOrigin
from dagster.grpc.utils import get_loadable_targets
from dagster.utils.error import serializable_error_info_from_exc_info


def _find_workspace_in_path(path):
    workspace_path = os.path.join(path, "workspace.yaml")
    if os.path.exists(workspace_path):
        return workspace_path
    else:
        parent = os.path.dirname(path)
        if parent == path:
            return None  # reached root of filesystem
        return _find_workspace_in_path(parent)


def _get_pipeline_def_from_location(repo_location, repo_name, pipeline_name):
    found_repos = []
    loadable_target_origin = repo_location.location_handle.loadable_target_origin
    loadable_targets = get_loadable_targets(
        loadable_target_origin.python_file,
        loadable_target_origin.module_name,
        loadable_target_origin.package_name,
        loadable_target_origin.working_directory,
        loadable_target_origin.attribute,
    )
    for target in loadable_targets:
        repo_def = repository_def_from_target_def(target.target_definition)
        found_repos.append(repo_def.name)
        if repo_def.name == repo_name and pipeline_name in repo_def.pipeline_names:
            return repo_def.get_pipeline(pipeline_name)
    raise DagsterInvariantViolationError(
        f"Could not find repository named {repo_name} and pipeline named {pipeline_name} in "
        f"location. Repositories found: {found_repos}"
    )


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
    def get():
        """Search up the directory tree from callsite for workspace.yaml"""
        from .cli_target import WorkspaceFileTarget

        # get previous frame in stack (loc where this fxn was called)
        frame = inspect.stack()[1]
        callsite_file = frame[0].f_code.co_filename
        workspace_path = _find_workspace_in_path(os.path.dirname(callsite_file))
        return Workspace(workspace_load_target=WorkspaceFileTarget(paths=[workspace_path]))

    @contextmanager
    def find_external_target(self, target: str):
        all_pipeline_names = []
        for origin in self._location_origin_dict.values():
            with self.create_handle_from_origin(origin) as handle:
                repo_location = handle.create_location()
                for repo_name, external_repository in repo_location.get_repositories().items():
                    external_pipelines = {
                        ep.name: ep for ep in external_repository.get_all_external_pipelines()
                    }
                    all_pipeline_names += list(external_pipelines.keys())
                    if target in external_pipelines:
                        pipeline_def = _get_pipeline_def_from_location(
                            repo_location, repo_name, target
                        )
                        yield (
                            pipeline_def,
                            external_pipelines[target],
                            external_repository,
                            repo_location,
                        )
                        return
        all_pipelines_as_str = ", ".join(all_pipeline_names)
        raise DagsterInvariantViolationError(
            f"Could not find {target} in workspace. Pipelines found: {all_pipelines_as_str}"
        )
