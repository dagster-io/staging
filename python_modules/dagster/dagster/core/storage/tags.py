from enum import Enum

from dagster import check

SYSTEM_TAG_PREFIX = 'dagster/'

SCHEDULE_NAME_TAG = '{prefix}schedule_name'.format(prefix=SYSTEM_TAG_PREFIX)

BACKFILL_ID_TAG = '{prefix}backfill'.format(prefix=SYSTEM_TAG_PREFIX)

PARTITION_NAME_TAG = '{prefix}partition'.format(prefix=SYSTEM_TAG_PREFIX)

PARTITION_SET_TAG = '{prefix}partition_set'.format(prefix=SYSTEM_TAG_PREFIX)

PARENT_RUN_ID_TAG = '{prefix}parent_run_id'.format(prefix=SYSTEM_TAG_PREFIX)

ROOT_RUN_ID_TAG = '{prefix}root_run_id'.format(prefix=SYSTEM_TAG_PREFIX)

RESUME_RETRY_TAG = '{prefix}is_resume_retry'.format(prefix=SYSTEM_TAG_PREFIX)

STEP_SELECTION_TAG = '{prefix}step_selection'.format(prefix=SYSTEM_TAG_PREFIX)

SOLID_SELECTION_TAG = '{prefix}solid_selection'.format(prefix=SYSTEM_TAG_PREFIX)

PRESET_NAME_TAG = '{prefix}preset_name'.format(prefix=SYSTEM_TAG_PREFIX)


INTERNAL_TAG_PREFIX = '.dagster/'


class TagType(Enum):
    SYSTEM = 'SYSTEM'
    INTERNAL = 'INTERNAL'
    CUSTOM = 'CUSTOM'


def get_tag_type(tag):
    if tag.startswith(SYSTEM_TAG_PREFIX):
        return TagType.SYSTEM
    elif tag.startswith(INTERNAL_TAG_PREFIX):
        return TagType.INTERNAL
    else:
        return TagType.CUSTOM


def check_tags(obj, name):
    check.opt_dict_param(obj, name, key_type=str, value_type=str)

    for tag in obj.keys():
        check.invariant(
            not tag.startswith(SYSTEM_TAG_PREFIX),
            desc='User attempted to set tag with reserved system prefix: {tag}'.format(tag=tag),
        )
