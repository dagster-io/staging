from contextlib import contextmanager

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from dagster import check
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.host_representation.handle import (
    ManagedGrpcPythonEnvRepositoryLocationHandle,
    PythonEnvRepositoryLocationHandle,
    RepositoryLocationHandle,
)


def ensure_watchable_handle(location_handle):
    if not (
        isinstance(location_handle, PythonEnvRepositoryLocationHandle)
        or isinstance(location_handle, ManagedGrpcPythonEnvRepositoryLocationHandle)
    ):

        # Should never get here
        raise DagsterInvariantViolationError(
            "Attempting to watch location handle {}, but it is unwatchable.".format(
                location_handle.location_name
            )
        )


def ensure_watchable_handles(location_handles):
    check.list_param(location_handles, "locatoin_handles", RepositoryLocationHandle)
    for handle in location_handles:
        ensure_watchable_handle(handle)


class CustomHandler(PatternMatchingEventHandler):
    def __init__(
        self,
        context,
        location_name,
        # TODO: Make these user configurable.
        patterns=None,
        ignore_patterns=None,
        ignore_directories=False,
        case_sensitive=False,
    ):
        super(CustomHandler, self).__init__(
            patterns=patterns,
            ignore_patterns=ignore_patterns,
            ignore_directories=ignore_directories,
            case_sensitive=case_sensitive,
        )
        self._context = context
        self._location_name = location_name

    def on_any_event(self, _event):
        print("Reloading location {}".format(self._location_name))  # pylint: disable=print-call
        self._context.reload_repository_location(self._location_name)


def get_watch_patterns_from_location_handle(location_handle):
    check.inst_param(location_handle, "location_handle", RepositoryLocationHandle)
    ensure_watchable_handle(location_handle)

    loadable_target_origin = location_handle.loadable_target_origin

    # If a python file is specified, only watch for changes in the python file.
    # Eventually, a user will be able to configure this.
    if loadable_target_origin.python_file:
        return (
            ["*{}".format(loadable_target_origin.python_file)],
            loadable_target_origin.working_directory,
        )

    return None, loadable_target_origin.working_directory


@contextmanager
def watchdog_thread(context, handles_to_watch):
    if not handles_to_watch:
        yield
        return

    ensure_watchable_handles(handles_to_watch)

    observer = Observer()
    for handle in handles_to_watch:
        patterns, watch_directory = get_watch_patterns_from_location_handle(handle)
        print(  # pylint: disable=print-call
            "Watching location {} for changes at {}".format(handle.location_name, patterns)
        )
        event_handler = CustomHandler(context, handle.location_name, patterns=patterns)
        observer.schedule(event_handler, watch_directory, recursive=True)

    try:
        observer.start()
        yield
    finally:
        observer.stop()
        observer.join()
