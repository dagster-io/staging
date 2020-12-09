from enum import Enum

from dagster import check

SYSTEM_TAG_PREFIX = "dagster/"
HIDDEN_TAG_PREFIX = ".dagster/"

SCHEDULE_NAME_TAG = "{prefix}schedule_name".format(prefix=SYSTEM_TAG_PREFIX)

SENSOR_NAME_TAG = "{prefix}sensor_name".format(prefix=SYSTEM_TAG_PREFIX)

BACKFILL_ID_TAG = "{prefix}backfill".format(prefix=SYSTEM_TAG_PREFIX)

PARTITION_NAME_TAG = "{prefix}partition".format(prefix=SYSTEM_TAG_PREFIX)

PARTITION_SET_TAG = "{prefix}partition_set".format(prefix=SYSTEM_TAG_PREFIX)

PARENT_RUN_ID_TAG = "{prefix}parent_run_id".format(prefix=SYSTEM_TAG_PREFIX)

ROOT_RUN_ID_TAG = "{prefix}root_run_id".format(prefix=SYSTEM_TAG_PREFIX)

RESUME_RETRY_TAG = "{prefix}is_resume_retry".format(prefix=SYSTEM_TAG_PREFIX)

MEMOIZED_RUN_TAG = "{prefix}is_memoized_run".format(prefix=SYSTEM_TAG_PREFIX)

STEP_SELECTION_TAG = "{prefix}step_selection".format(prefix=SYSTEM_TAG_PREFIX)

SOLID_SELECTION_TAG = "{prefix}solid_selection".format(prefix=SYSTEM_TAG_PREFIX)

PRESET_NAME_TAG = "{prefix}preset_name".format(prefix=SYSTEM_TAG_PREFIX)

GRPC_INFO_TAG = "{prefix}grpc_info".format(prefix=HIDDEN_TAG_PREFIX)

SCHEDULED_EXECUTION_TIME_TAG = "{prefix}scheduled_execution_time".format(prefix=HIDDEN_TAG_PREFIX)

RUN_KEY_TAG = "{prefix}run_key".format(prefix=SYSTEM_TAG_PREFIX)

PRIORITY_TAG = "{prefix}priority".format(prefix=SYSTEM_TAG_PREFIX)

TICK_ID_TAG = f"{HIDDEN_TAG_PREFIX}tick_id"


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
            desc="User attempted to set tag with reserved system prefix: {tag}".format(tag=tag),
        )


def build_tick_id_tag(tick_id, timestamp):
    check.str_param(tick_id, "tick_id")
    check.float_param(timestamp, "timestamp")
    return f"{tick_id}:{str(timestamp)}"


def parse_tick_id_tag(tick_id_tag_value):
    check.str_param(tick_id_tag_value, "tick_id_tag_value")
    parts = tick_id_tag_value.split(":")
    if len(parts) != 2:
        return tick_id_tag_value, None
    tick_id, timestamp_str = parts
    try:
        return tick_id, float(timestamp_str)
    except TypeError:
        return tick_id, None
