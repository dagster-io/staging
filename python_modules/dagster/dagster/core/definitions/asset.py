from collections import namedtuple

from dagster import check
from dagster.core.errors import DagsterInvalidObjectForAsset
from dagster.core.execution.plan.objects import StepInput, StepOutputHandle


class Asset(namedtuple("_Asset", "key value step_input step_output_handle version")):
    """Defines an asset that is loaded to or generated from a solid's compute function.

    Args:
        key (str): the name of the asset. An instance can have multiple assets with the same key
            but different values and versions
        value (any): the inline value of the asset or its reference (e.g. path to the external
            storage).
        step_input (StepInput): the step input that this asset is loaded into.
        step_output_handle (StepOutputHandle): the handle of the step output which generates this
            asset.
        version (Optional[str]): a version of this asset.

    """

    def __new__(cls, key, value, step_input=None, step_output_handle=None, version=None):
        return super(Asset, cls).__new__(
            cls,
            key=check.str_param(key, "key"),
            value=value,
            step_input=check.opt_inst_param(step_input, "step_input", StepInput),
            step_output_handle=check.opt_inst_param(
                step_output_handle, "step_output_handle", StepOutputHandle
            ),
            version=check.opt_str_param(version, "version"),
        )

    @staticmethod
    def get_key(step_key, obj):
        if isinstance(obj, StepOutputHandle):
            return "{step_key}-{name}".format(step_key=step_key, name=obj.output_name)
        if isinstance(obj, StepInput):
            return "{step_key}-{name}".format(step_key=step_key, name=obj.name)

        raise DagsterInvalidObjectForAsset

    @staticmethod
    def from_step_input(value, step_key, step_input):
        check.str_param(step_key, "step_key")
        check.inst_param(step_input, "step_input", StepInput)
        key = Asset.get_key(step_key, step_input)
        return Asset(key=key, value=value, step_input=step_input)

    @staticmethod
    def from_step_output_handle(value, step_output_handle):
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        key = Asset.get_key(step_output_handle.step_key, step_output_handle)
        return Asset(key=key, value=value, step_output_handle=step_output_handle)
