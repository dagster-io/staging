from typing import NamedTuple, Optional, Union

from dagster.core.definitions import AssetMaterialization, Materialization, OutputDefinition
from dagster.serdes import whitelist_for_serdes

from .objects import TypeCheckData


class StepOutput(NamedTuple):
    """Holds the information for an ExecutionStep to process its outputs"""

    output_def: OutputDefinition
    should_materialize: Optional[bool] = None

    @property
    def name(self):
        return self.output_def.name


@whitelist_for_serdes
class StepOutputHandle(NamedTuple):
    """A reference to a specific output that has or will occur within the scope of an execution"""

    step_key: str
    output_name: str
    mapping_key: Optional[str] = None


@whitelist_for_serdes
class StepOutputData(NamedTuple):
    """Serializable payload of information for the result of processing a step output"""

    step_output_handle: StepOutputHandle
    intermediate_materialization: Optional[Union[AssetMaterialization, Materialization]] = None
    type_check_data: Optional[TypeCheckData] = None
    version: Optional[str] = None

    @property
    def output_name(self):
        return self.step_output_handle.output_name

    @property
    def mapping_key(self):
        return self.step_output_handle.mapping_key
