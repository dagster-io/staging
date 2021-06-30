from enum import Enum
from typing import Dict

from dagster.core.workspace.context import IWorkspaceProcessContext


class Permission(str, Enum):
    """
    This enum subclasses str and Enum so that we can serialize it when calling json.dumps
    on a dictionary with Permission keys.
    """

    LAUNCH_PIPELINE_EXECUTION = "LAUNCH_PIPELINE_EXECUTION"
    LAUNCH_PIPELINE_REEXECUTION = "LAUNCH_PIPELINE_REEXECUTION"
    RECONCILE_SCHEDULER_STATE = "RECONCILE_SCHEDULER_STATE"
    START_SCHEDULE = "START_SCHEDULE"
    STOP_RUNNING_SCHEDULE = "STOP_RUNNING_SCHEDULE"
    START_SENSOR = "START_SENSOR"
    STOP_SENSOR = "STOP_SENSOR"
    TERMINATE_PIPELINE_EXECUTION = "TERMINATE_PIPELINE_EXECUTION"
    DELETE_PIPELINE_RUN = "DELETE_PIPELINE_RUN"
    RELOAD_REPOSITORY_LOCATION = "RELOAD_REPOSITORY_LOCATION"
    RELOAD_WORKSPACE = "RELOAD_WORKSPACE"
    WIPE_ASSETS = "WIPE_ASSETS"
    LAUNCH_PARTITION_BACKFILL = "LAUNCH_PARTITION_BACKFILL"
    CANCEL_PARTITION_BACKFILL = "CANCEL_PARTITION_BACKFILL"


VIEWER_PERMISSIONS = {
    Permission.LAUNCH_PIPELINE_EXECUTION: False,
    Permission.LAUNCH_PIPELINE_REEXECUTION: False,
    Permission.RECONCILE_SCHEDULER_STATE: False,
    Permission.START_SCHEDULE: False,
    Permission.STOP_RUNNING_SCHEDULE: False,
    Permission.START_SENSOR: False,
    Permission.STOP_SENSOR: False,
    Permission.TERMINATE_PIPELINE_EXECUTION: False,
    Permission.DELETE_PIPELINE_RUN: False,
    Permission.RELOAD_REPOSITORY_LOCATION: False,
    Permission.RELOAD_WORKSPACE: False,
    Permission.WIPE_ASSETS: False,
    Permission.LAUNCH_PARTITION_BACKFILL: False,
    Permission.CANCEL_PARTITION_BACKFILL: False,
}

EDITOR_PERMISSIONS = {
    Permission.LAUNCH_PIPELINE_EXECUTION: False,
    Permission.LAUNCH_PIPELINE_REEXECUTION: False,
    Permission.RECONCILE_SCHEDULER_STATE: False,
    Permission.START_SCHEDULE: False,
    Permission.STOP_RUNNING_SCHEDULE: False,
    Permission.START_SENSOR: False,
    Permission.STOP_SENSOR: False,
    Permission.TERMINATE_PIPELINE_EXECUTION: False,
    Permission.DELETE_PIPELINE_RUN: False,
    Permission.RELOAD_REPOSITORY_LOCATION: False,
    Permission.RELOAD_WORKSPACE: False,
    Permission.WIPE_ASSETS: False,
    Permission.LAUNCH_PARTITION_BACKFILL: False,
    Permission.CANCEL_PARTITION_BACKFILL: False,
}


def get_user_permissions(context: IWorkspaceProcessContext) -> Dict[Permission, bool]:
    if context.read_only:
        return VIEWER_PERMISSIONS
    else:
        return EDITOR_PERMISSIONS
