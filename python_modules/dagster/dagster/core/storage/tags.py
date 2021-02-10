from enum import Enum

from dagster import check

SYSTEM_TAG_PREFIX = "dagster/"
HIDDEN_TAG_PREFIX = ".dagster/"

SCHEDULE_NAME_TAG = f"{SYSTEM_TAG_PREFIX}schedule_name"

SENSOR_NAME_TAG = f"{SYSTEM_TAG_PREFIX}sensor_name"

BACKFILL_ID_TAG = f"{SYSTEM_TAG_PREFIX}backfill"

PARTITION_NAME_TAG = f"{SYSTEM_TAG_PREFIX}partition"

PARTITION_SET_TAG = f"{SYSTEM_TAG_PREFIX}partition_set"

PARENT_RUN_ID_TAG = f"{SYSTEM_TAG_PREFIX}parent_run_id"

ROOT_RUN_ID_TAG = f"{SYSTEM_TAG_PREFIX}root_run_id"

RESUME_RETRY_TAG = f"{SYSTEM_TAG_PREFIX}is_resume_retry"

MEMOIZED_RUN_TAG = f"{SYSTEM_TAG_PREFIX}is_memoized_run"

STEP_SELECTION_TAG = f"{SYSTEM_TAG_PREFIX}step_selection"

SOLID_SELECTION_TAG = f"{SYSTEM_TAG_PREFIX}solid_selection"

PRESET_NAME_TAG = f"{SYSTEM_TAG_PREFIX}preset_name"

GRPC_INFO_TAG = f"{HIDDEN_TAG_PREFIX}grpc_info"

SCHEDULED_EXECUTION_TIME_TAG = f"{HIDDEN_TAG_PREFIX}scheduled_execution_time"

RUN_KEY_TAG = f"{SYSTEM_TAG_PREFIX}run_key"

PRIORITY_TAG = f"{SYSTEM_TAG_PREFIX}priority"


class TagType(Enum):
    # Custom tag provided by a user
    USER_PROVIDED = "USER_PROVIDED"

    # Tags used by Dagster to manage execution that should be surfaced to users.
    SYSTEM = "SYSTEM"

    # Metadata used by Dagster for execution but isn't useful for users to see.
    # For example, metadata about the gRPC server that executed a run.
    HIDDEN = "HIDDEN"


def get_tag_type(tag):
    if tag.startswith(SYSTEM_TAG_PREFIX):
        return TagType.SYSTEM
    elif tag.startswith(HIDDEN_TAG_PREFIX):
        return TagType.HIDDEN
    else:
        return TagType.USER_PROVIDED


def check_tags(obj, name):
    check.opt_dict_param(obj, name, key_type=str, value_type=str)

    for tag in obj.keys():
        check.invariant(
            not tag.startswith(SYSTEM_TAG_PREFIX),
            desc=f"User attempted to set tag with reserved system prefix: {tag}",
        )
