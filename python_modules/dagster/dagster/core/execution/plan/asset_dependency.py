from abc import ABC, abstractmethod
from collections import namedtuple
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Sequence, Set, Union

from dagster import DagsterInvariantViolationError, ModeDefinition, OutputDefinition, check
from dagster.core.events import AssetKey, AssetLineageInfo
from dagster.core.execution.plan.inputs import (
    StepInput,
    UnresolvedCollectStepInput,
    UnresolvedMappedStepInput,
)
from dagster.core.execution.plan.outputs import StepOutput
from dagster.core.execution.plan.step import (
    ExecutionStep,
    UnresolvedCollectExecutionStep,
    UnresolvedMappedExecutionStep,
)
from dagster.serdes import whitelist_for_serdes

if TYPE_CHECKING:
    from dagster.core.execution.plan.plan import ExecutionPlan
    from dagster.core.definitions.pipeline import PipelineDefinition


class OutputDefinitionContext(
    namedtuple(
        "_OutputDefinitionContext",
        "mode step_key name metadata",
    )
):
    def __new__(
        cls,
        mode: str,
        step_key: str,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        return super(OutputDefinitionContext, cls).__new__(
            cls,
            mode=check.opt_str_param(mode, "mode"),
            step_key=check.str_param(step_key, "step_key"),
            name=check.str_param(name, "name"),
            metadata=check.opt_dict_param(metadata, "metadata"),
        )


class AssetNodeHandle(ABC):
    @abstractmethod
    def normalized(self):
        pass


@whitelist_for_serdes
class AssetOutputHandle(
    NamedTuple(
        "_AssetOutputHandle", [("step_key", str), ("name", str), ("mapping_key", Optional[str])]
    ),
    AssetNodeHandle,
):
    def __new__(cls, step_key: str, name: str, mapping_key: str = None):
        return super(AssetOutputHandle, cls).__new__(cls, step_key, name, mapping_key)

    def normalized(self):
        return AssetOutputHandle(self.step_key, self.name)


@whitelist_for_serdes
class AssetInputHandle(
    NamedTuple("_AssetOutputHandle", [("step_key", str), ("name", str)]),
    AssetNodeHandle,
):
    def __new__(cls, step_key: str, name: str):
        return super(AssetInputHandle, cls).__new__(cls, step_key, name)

    def normalized(self):
        return self


@whitelist_for_serdes
class RuntimeAssetHandle(
    NamedTuple("_RuntimeAssetHandle", [("asset_key", AssetKey)]),
    AssetNodeHandle,
):
    def __new__(cls, asset_key: AssetKey):
        return super(RuntimeAssetHandle, cls).__new__(cls, asset_key)


class AssetDependencyGraph:
    def __init__(
        self,
        execution_plan: "ExecutionPlan",
        pipeline_def: "PipelineDefintion",
        mode_def: ModeDefinition,
    ):
        self.execution_plan = execution_plan
        self.pipeline_def = pipeline_def
        self.mode_def = mode_def
        self._asset_key_map: Dict[AssetNodeHandle, List[AssetKey]] = {}
        self._asset_partitions_map: Dict[AssetNodeHandle, Set[str]] = (
            self.execution_plan.known_state.asset_partitions_map
            if self.execution_plan.known_state
            else {}
        )
        self._asset_parents_map: Dict[AssetNodeHandle, List[AssetNodeHandle]] = {}
        self._build()

    def _get_output_def_context(
        self, step_key, output_def: OutputDefinition
    ) -> OutputDefinitionContext:
        return OutputDefinitionContext(
            mode=self.mode_def.name,
            step_key=step_key,
            name=output_def.name,
            metadata=output_def.metadata,
        )

    def _build(self):
        for step in self.execution_plan.get_all_steps_in_topo_order():
            if isinstance(step, ExecutionStep):
                self._add_step(step)
            elif isinstance(step, (UnresolvedCollectExecutionStep, UnresolvedMappedExecutionStep)):
                self._add_step(step)
            else:
                check.failed("weird step thing")

    def _add_step(
        self,
        step: Union[ExecutionStep, UnresolvedCollectExecutionStep, UnresolvedMappedExecutionStep],
    ):
        for step_input in step.step_inputs:
            self._add_input(step, step_input)
        for step_output in step.step_outputs:
            self._add_output(step, step_output)

    def update_step(self, step: ExecutionStep):
        self._add_step(step)

    def _add_output(
        self,
        step: Union[ExecutionStep, UnresolvedCollectExecutionStep, UnresolvedMappedExecutionStep],
        step_output: StepOutput,
        mapping_key: Optional[str] = None,
    ):

        # assume all inputs are parents of all outputs (this logic is easy to change now)
        parent_node_handles = [
            AssetInputHandle(step.key, step_input.name) for step_input in step.step_inputs
        ]
        node_handle = AssetOutputHandle(step.key, step_output.name, mapping_key)

        output_def = self.pipeline_def.get_solid(step.solid_handle).output_def_named(
            step_output.name
        )
        # see if the output def defines an asset key
        output_def_context = self._get_output_def_context(step.key, output_def)
        output_def_asset_key = output_def.get_asset_key(output_def_context)

        # see if the io manager def defines an asset key
        io_manager_def = self.mode_def.resource_defs[output_def.io_manager_key]
        io_manager_asset_key = io_manager_def.get_output_asset_key(output_def_context)

        # can't have both!
        if output_def_asset_key and io_manager_asset_key:
            raise DagsterInvariantViolationError("TODO: message")

        output_asset_key = output_def_asset_key or io_manager_asset_key

        # an output can have at most one direct asset key produced
        self._add_node(
            node_handle,
            parent_node_handles,
            direct_asset_keys=[output_asset_key] if output_asset_key else [],
        )

    def _add_input(
        self,
        step: Union[ExecutionStep, UnresolvedCollectExecutionStep, UnresolvedMappedExecutionStep],
        step_input: Union[StepInput, UnresolvedMappedStepInput, UnresolvedCollectStepInput],
    ):
        node_handle = AssetInputHandle(step.key, step_input.name)
        if isinstance(step_input, StepInput):
            upstream_output_handles = step_input.get_step_output_handle_dependencies()
        elif isinstance(step_input, (UnresolvedCollectStepInput, UnresolvedMappedStepInput)):
            upstream_output_handles = step_input.get_step_output_handle_deps_with_placeholders()
        else:
            check.failed("TODO - weird thing happened")

        parent_node_handles = [
            AssetOutputHandle(
                output_handle.step_key, output_handle.output_name, output_handle.mapping_key
            )
            for output_handle in upstream_output_handles
        ]

        self._add_node(node_handle, parent_node_handles, [])

    def add_runtime_asset(self, asset_key: AssetKey, parent_asset_lineage: List[AssetLineageInfo]):

        pass

    def _add_node(
        self,
        node_handle: AssetNodeHandle,
        parent_handles: Sequence[AssetNodeHandle],
        direct_asset_keys: List[AssetKey],
    ):
        asset_parents = []

        for parent in parent_handles:
            # check if the parent directly reads/writes any asset key
            if self._asset_key_map[parent.normalized()]:
                asset_parents.append(parent)
            # otherwise just inherit the parents
            else:
                asset_parents.extend(self._asset_parents_map[parent])

        self._asset_parents_map[node_handle] = asset_parents
        self._asset_key_map[node_handle] = direct_asset_keys

    def _get_parent_assets(self, node_handle: AssetNodeHandle) -> List[AssetKey]:

        parent_assets = []
        for asset_node_handle in self._asset_parents_map[node_handle]:
            parent_assets.extend(self._asset_key_map[asset_node_handle])
        return parent_assets

    def _get_parent_lineage(self, node_handle: AssetNodeHandle) -> List[AssetLineageInfo]:
        parent_lineage = []
        for asset_node_handle in self._asset_parents_map[node_handle]:
            asset_keys = self._asset_key_map[asset_node_handle.normalized()]
            for asset_key in asset_keys:
                parent_lineage.append(
                    AssetLineageInfo(
                        asset_key,
                        self._asset_partitions_map.get(asset_node_handle, {}).get(asset_key),
                    )
                )
        return parent_lineage

    def _add_asset_partition(
        self, node_handle: AssetNodeHandle, asset_key: AssetKey, partition: str
    ):
        if not partition:
            return
        if (
            node_handle in self._asset_partitions_map
            and asset_key in self._asset_partitions_map[node_handle]
        ):
            self._asset_partitions_map[node_handle][asset_key].add(partition)
        else:
            self._asset_partitions_map[node_handle] = {asset_key: set([partition])}

    def add_output_asset_partition(
        self,
        step_key: str,
        output_name: str,
        mapping_key: Optional[str],
        asset_key: AssetKey,
        partition: str,
    ):
        self._add_asset_partition(
            AssetOutputHandle(step_key, output_name, mapping_key), asset_key, partition
        )

    def get_output_asset(self, step_key: str, output_name: str) -> Optional[AssetKey]:
        assets = self._asset_key_map[AssetOutputHandle(step_key, output_name)]
        if len(assets) > 1:
            check.failed("TODO")
        elif len(assets) == 1:
            return assets[0]
        return None

    def get_output_parent_assets(self, step_key: str, output_name: str) -> List[AssetKey]:
        return self._get_parent_assets(AssetOutputHandle(step_key, output_name))

    def get_output_asset_lineage(self, step_key: str, output_name: str) -> List[AssetLineageInfo]:
        return self._get_parent_lineage(AssetOutputHandle(step_key, output_name))
