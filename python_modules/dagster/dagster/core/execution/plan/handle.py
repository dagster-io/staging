import re
from collections import namedtuple

from dagster import check
from dagster.core.definitions.dependency import SolidHandle


class StepHandle(namedtuple("_StepHandle", "solid_handle")):
    def __new__(cls, solid_handle):
        return super(StepHandle, cls).__new__(
            cls, solid_handle=check.inst_param(solid_handle, "solid_handle", SolidHandle),
        )

    def to_key(self):
        return f"{self.solid_handle.to_string()}.compute"

    @staticmethod
    def from_key(string):

        plain_match = re.match(r"(.*)\.compute", string)
        if plain_match:
            return StepHandle(SolidHandle.from_string(plain_match.group(1)))

        check.failed(f"Could not parse step key to handle: {string}")
