from collections import namedtuple

from dagster import check


class StepHandle(namedtuple("_StepHandle", "key")):
    def __new__(cls, key):
        return super(StepHandle, cls).__new__(cls, key=check.str_param(key, "key"),)

    @classmethod
    def from_key(cls, string):
        return cls(string)

    @staticmethod
    def coerce(value):
        check.inst_param(value, "value", (str, StepHandle))
        if isinstance(value, StepHandle):
            return value

        return StepHandle.from_key(value)


# def check_step_handle_param(handle_or_key):
#     if isinstance(handle_or_key, str):
#         handle = StepHandle.from_key(handle_or_key)
#     else:
#         handle = handle_or_key

#     return


# def check_opt_step_handle_param(step_key):
#     if isinstance(step_key, str):
#         step_key = StepHandle.from_handle(step_key)

#     return check.opt_inst_param(step_key, "step_key", StepHandle)


# def check_opt_step_key_param(step_key):
#     if isinstance(step_key, StepHandle):
#         return step_key.key

#     return check.opt_str_param(step_key, "step_key")
