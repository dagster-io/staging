import re
from collections import namedtuple

from dagster import check
from dagster.core.definitions.dependency import SolidHandle


class IStepHandle:
    """marker interface"""


class StepHandle(namedtuple("_StepHandle", "solid_handle"), IStepHandle):
    def __new__(cls, solid_handle):
        return super(StepHandle, cls).__new__(
            cls, solid_handle=check.inst_param(solid_handle, "solid_handle", SolidHandle),
        )

    def to_key(self):
        return f"{self.solid_handle.to_string()}.compute"

    @staticmethod
    def from_key(string):

        unresolved_match = re.match(r"(.*)\[\?\]\.compute", string)
        if unresolved_match:
            return UnresolvedStepHandle(SolidHandle.from_string(unresolved_match.group(1)))

        resolved_match = re.match(r"(.*)\[(.*)\]\.compute", string)
        if resolved_match:
            return MappedStepHandle(
                SolidHandle.from_string(resolved_match.group(1)), resolved_match.group(2)
            )

        plain_match = re.match(r"(.*)\.compute", string)
        if plain_match:
            return StepHandle(SolidHandle.from_string(plain_match.group(1)))

        check.failed(f"Could not parse step key to handle: {string}")

    # return

    # @staticmethod
    # def coerce(value):
    #     check.inst_param(value, "value", (str, StepHandle))
    #     if isinstance(value, StepHandle):
    #         return value

    #     return StepHandle.from_key(value)


class UnresolvedStepHandle(namedtuple("_UnresolvedStepHandle", "solid_handle"), IStepHandle):
    def __new__(cls, solid_handle):
        return super(UnresolvedStepHandle, cls).__new__(
            cls, solid_handle=check.inst_param(solid_handle, "solid_handle", SolidHandle),
        )

    def to_key(self):
        return f"{self.solid_handle.to_string()}[?].compute"

    def resolve(self, map_key):
        return MappedStepHandle(self.solid_handle, map_key)


class MappedStepHandle(namedtuple("_ResolvedStepHandle", "solid_handle mapping_key"), IStepHandle):
    def __new__(cls, solid_handle, mapping_key):
        return super(MappedStepHandle, cls).__new__(
            cls,
            solid_handle=check.inst_param(solid_handle, "solid_handle", SolidHandle),
            mapping_key=check.str_param(mapping_key, "mapping_key"),
        )

    def to_key(self):
        return f"{self.solid_handle.to_string()}[{self.mapping_key}].compute"

    @property
    def unresolved_form(self):
        return UnresolvedStepHandle(solid_handle=self.solid_handle)
