from collections import namedtuple

from dagster import check


class StepKey(namedtuple("_StepKey", "key")):
    def __new__(cls, key):
        return super(StepKey, cls).__new__(cls, key=check.str_param(key, "key"),)

    def __str__(self):
        return self.key

    @classmethod
    def from_string(cls, string):
        return cls(string)


def check_step_key_param(step_key):
    if isinstance(step_key, str):
        step_key = StepKey.from_string(step_key)

    return check.inst_param(step_key, "step_key", StepKey)


def check_opt_step_key_param(step_key):
    if isinstance(step_key, str):
        step_key = StepKey.from_string(step_key)

    return check.opt_inst_param(step_key, "step_key", StepKey)


def check_opt_step_key_str_param(step_key):
    if isinstance(step_key, StepKey):
        return str(step_key)

    return check.opt_str_param(step_key, "step_key")
