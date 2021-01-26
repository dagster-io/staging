import graphene
from dagster import check
from dagster.core.definitions import SolidHandle as DagsterSolidHandle
from dagster.core.host_representation import RepresentedPipeline
from dagster.core.snap import CompositeSolidDefSnap, DependencyStructureIndex, SolidDefSnap

from .config_types import ConfigTypeField
from .dagster_types import DagsterType, to_dagster_type
from .metadata import MetadataItemDefinition
from .util import non_null_list


def build_solid_definition(represented_pipeline, solid_def_name):
    check.inst_param(represented_pipeline, "represented_pipeline", RepresentedPipeline)
    check.str_param(solid_def_name, "solid_def_name")

    solid_def_snap = represented_pipeline.get_solid_def_snap(solid_def_name)

    if isinstance(solid_def_snap, SolidDefSnap):
        return SolidDefinition(represented_pipeline, solid_def_snap.name)

    if isinstance(solid_def_snap, CompositeSolidDefSnap):
        return CompositeSolidDefinition(represented_pipeline, solid_def_snap.name)

    check.failed("Unknown solid definition type {type}".format(type=type(solid_def_snap)))


class _ArgNotPresentSentinel:
    pass


class InputDefinition(graphene.ObjectType):
    solid_definition = graphene.NonNull(lambda: SolidDefinition)
    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    type = graphene.NonNull(DagsterType)

    def __init__(self, represented_pipeline, solid_def_name, input_def_name):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        check.str_param(solid_def_name, "solid_def_name")
        check.str_param(input_def_name, "input_def_name")
        solid_def_snap = self._represented_pipeline.get_solid_def_snap(solid_def_name)
        self._input_def_snap = solid_def_snap.get_input_snap(input_def_name)
        super(InputDefinition, self).__init__(
            name=self._input_def_snap.name, description=self._input_def_snap.description,
        )

    def resolve_type(self, _graphene_info):
        return to_dagster_type(
            self._represented_pipeline.pipeline_snapshot, self._input_def_snap.dagster_type_key
        )

    def resolve_solid_definition(self, _):
        return build_solid_definition(
            self._represented_pipeline, self._solid_def_snap.name  # pylint: disable=no-member
        )


class OutputDefinition(graphene.ObjectType):
    solid_definition = graphene.NonNull(lambda: SolidDefinition)
    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    type = graphene.NonNull(DagsterType)

    def __init__(self, represented_pipeline, solid_def_name, output_def_name):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        check.str_param(solid_def_name, "solid_def_name")
        check.str_param(output_def_name, "output_def_name")

        self._solid_def_snap = represented_pipeline.get_solid_def_snap(solid_def_name)
        self._output_def_snap = self._solid_def_snap.get_output_snap(output_def_name)

        super().__init__(
            name=self._output_def_snap.name, description=self._output_def_snap.description,
        )

    def resolve_type(self, _):
        return to_dagster_type(
            self._represented_pipeline.pipeline_snapshot, self._output_def_snap.dagster_type_key,
        )

    def resolve_solid_definition(self, _):
        return build_solid_definition(self._represented_pipeline, self._solid_def_snap.name)


class Input(graphene.ObjectType):
    solid = graphene.NonNull(lambda: Solid)
    definition = graphene.NonNull(InputDefinition)
    depends_on = non_null_list(lambda: Output)

    def __init__(self, represented_pipeline, current_dep_structure, solid_name, input_name):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        self._current_dep_structure = check.inst_param(
            current_dep_structure, "current_dep_structure", DependencyStructureIndex
        )
        self._solid_name = check.str_param(solid_name, "solid_name")
        self._input_name = check.str_param(input_name, "input_name")
        self._solid_invocation_snap = current_dep_structure.get_invocation(solid_name)
        self._solid_def_snap = represented_pipeline.get_solid_def_snap(
            self._solid_invocation_snap.solid_def_name
        )
        self._input_def_snap = self._solid_def_snap.get_input_snap(input_name)

        super().__init__()

    def resolve_solid(self, _):
        return Solid(self._represented_pipeline, self._solid_name, self._current_dep_structure)

    def resolve_definition(self, _):
        return InputDefinition(
            self._represented_pipeline, self._solid_def_snap.name, self._input_def_snap.name,
        )

    def resolve_depends_on(self, _):
        return [
            Output(
                self._represented_pipeline,
                self._current_dep_structure,
                output_handle_snap.solid_name,
                output_handle_snap.output_name,
            )
            for output_handle_snap in self._current_dep_structure.get_upstream_outputs(
                self._solid_name, self._input_name
            )
        ]


class Output(graphene.ObjectType):
    solid = graphene.NonNull(lambda: Solid)
    definition = graphene.NonNull(OutputDefinition)
    depended_by = non_null_list(Input)

    def __init__(self, represented_pipeline, current_dep_structure, solid_name, output_name):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        self._current_dep_structure = check.inst_param(
            current_dep_structure, "current_dep_structure", DependencyStructureIndex
        )
        self._solid_name = check.str_param(solid_name, "solid_name")
        self._output_name = check.str_param(output_name, "output_name")
        self._solid_invocation_snap = current_dep_structure.get_invocation(solid_name)
        self._solid_def_snap = represented_pipeline.get_solid_def_snap(
            self._solid_invocation_snap.solid_def_name
        )
        self._output_def_snap = self._solid_def_snap.get_output_snap(output_name)
        super().__init__()

    def resolve_solid(self, _):
        return Solid(self._represented_pipeline, self._solid_name, self._current_dep_structure)

    def resolve_definition(self, _):
        return OutputDefinition(
            self._represented_pipeline, self._solid_def_snap.name, self._output_name,
        )

    def resolve_depended_by(self, _):
        return [
            Input(
                self._represented_pipeline,
                self._current_dep_structure,
                input_handle_snap.solid_name,
                input_handle_snap.input_name,
            )
            for input_handle_snap in self._current_dep_structure.get_downstream_inputs(
                self._solid_name, self._output_def_snap.name
            )
        ]


class InputMapping(graphene.ObjectType):
    mapped_input = graphene.NonNull(Input)
    definition = graphene.NonNull(InputDefinition)

    def __init__(
        self, represented_pipeline, current_dep_index, solid_def_name, input_name,
    ):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        self._current_dep_index = check.inst_param(
            current_dep_index, "current_dep_index", DependencyStructureIndex
        )
        self._solid_def_snap = represented_pipeline.get_solid_def_snap(solid_def_name)
        self._input_mapping_snap = self._solid_def_snap.get_input_mapping_snap(input_name)
        super().__init__()

    def resolve_mapped_input(self, _):
        return Input(
            self._represented_pipeline,
            self._current_dep_index,
            self._input_mapping_snap.mapped_solid_name,
            self._input_mapping_snap.mapped_input_name,
        )

    def resolve_definition(self, _):
        return InputDefinition(
            self._represented_pipeline,
            self._solid_def_snap.name,
            self._input_mapping_snap.external_input_name,
        )


class OutputMapping(graphene.ObjectType):
    mapped_output = graphene.NonNull(Output)
    definition = graphene.NonNull(OutputDefinition)

    def __init__(
        self, represented_pipeline, current_dep_index, solid_def_name, output_name,
    ):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        self._current_dep_index = check.inst_param(
            current_dep_index, "current_dep_index", DependencyStructureIndex
        )
        self._solid_def_snap = represented_pipeline.get_solid_def_snap(solid_def_name)
        self._output_mapping_snap = self._solid_def_snap.get_output_mapping_snap(output_name)
        super().__init__()

    def resolve_mapped_output(self, _):
        return Output(
            self._represented_pipeline,
            self._current_dep_index,
            self._output_mapping_snap.mapped_solid_name,
            self._output_mapping_snap.mapped_output_name,
        )

    def resolve_definition(self, _):
        return OutputDefinition(
            self._represented_pipeline,
            self._solid_def_snap.name,
            self._output_mapping_snap.external_output_name,
        )


class ResourceRequirement(graphene.ObjectType):
    resource_key = graphene.NonNull(graphene.String)

    def __init__(self, resource_key):
        self.resource_key = resource_key


def build_solids(represented_pipeline, current_dep_index):
    check.inst_param(represented_pipeline, "represented_pipeline", RepresentedPipeline)
    return sorted(
        [
            Solid(represented_pipeline, solid_name, current_dep_index)
            for solid_name in current_dep_index.solid_invocation_names
        ],
        key=lambda solid: solid.name,
    )


def build_solid_handles(represented_pipeline, current_dep_index, parent=None):
    check.inst_param(represented_pipeline, "represented_pipeline", RepresentedPipeline)
    check.opt_inst_param(parent, "parent", SolidHandle)
    all_handle = []
    for solid_invocation in current_dep_index.solid_invocations:
        solid_name, solid_def_name = solid_invocation.solid_name, solid_invocation.solid_def_name
        handle = SolidHandle(
            solid=Solid(represented_pipeline, solid_name, current_dep_index),
            handle=SolidHandle(solid_name, parent.handleID if parent else None),
            parent=parent if parent else None,
        )
        solid_def_snap = represented_pipeline.get_solid_def_snap(solid_def_name)
        if isinstance(solid_def_snap, CompositeSolidDefSnap):
            all_handle += build_solid_handles(
                represented_pipeline,
                represented_pipeline.get_dep_structure_index(solid_def_name),
                handle,
            )

        all_handle.append(handle)

    return all_handle


class ISolidDefinition(graphene.Interface):
    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    metadata = non_null_list(MetadataItemDefinition)
    input_definitions = non_null_list(InputDefinition)
    output_definitions = non_null_list(OutputDefinition)
    required_resources = non_null_list(ResourceRequirement)


class ISolidDefinitionMixin:
    def __init__(self, represented_pipeline, solid_def_name):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        check.str_param(solid_def_name, "solid_def_name")
        self._solid_def_snap = represented_pipeline.get_solid_def_snap(solid_def_name)

    def resolve_metadata(self, graphene_info):
        return [
            MetadataItemDefinition(key=item[0], value=item[1])
            for item in self._solid_def_snap.tags.items()
        ]

    @property
    def solid_def_name(self):
        return self._solid_def_snap.name

    def resolve_input_definitions(self, _):
        return [
            InputDefinition(self._represented_pipeline, self.solid_def_name, input_def_snap.name)
            for input_def_snap in self._solid_def_snap.input_def_snaps
        ]

    def resolve_output_definitions(self, _):
        return [
            OutputDefinition(self._represented_pipeline, self.solid_def_name, output_def_snap.name)
            for output_def_snap in self._solid_def_snap.output_def_snaps
        ]

    def resolve_required_resources(self, graphene_info):
        return [ResourceRequirement(key) for key in self._solid_def_snap.required_resource_keys]


class SolidDefinition(graphene.ObjectType, ISolidDefinitionMixin):
    class Meta:
        interfaces = (ISolidDefinition,)

    config_field = graphene.Field(ConfigTypeField)

    def __init__(self, represented_pipeline, solid_def_name):
        check.inst_param(represented_pipeline, "represented_pipeline", RepresentedPipeline)
        self._solid_def_snap = represented_pipeline.get_solid_def_snap(solid_def_name)
        super().__init__(name=solid_def_name, description=self._solid_def_snap.description)
        ISolidDefinitionMixin.__init__(self, represented_pipeline, solid_def_name)

    def resolve_config_field(self, _):
        return (
            ConfigTypeField(
                config_schema_snapshot=self._represented_pipeline.config_schema_snapshot,
                field_snap=self._solid_def_snap.config_field_snap,
            )
            if self._solid_def_snap.config_field_snap
            else None
        )


class Solid(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    definition = graphene.NonNull(ISolidDefinition)
    inputs = non_null_list(Input)
    outputs = non_null_list(Output)

    def __init__(self, represented_pipeline, solid_name, current_dep_structure):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )

        check.str_param(solid_name, "solid_name")
        self._current_dep_structure = check.inst_param(
            current_dep_structure, "current_dep_structure", DependencyStructureIndex
        )
        self._solid_invocation_snap = current_dep_structure.get_invocation(solid_name)
        self._solid_def_snap = represented_pipeline.get_solid_def_snap(
            self._solid_invocation_snap.solid_def_name
        )
        super().__init__(name=solid_name)

    def get_solid_definition_name(self):
        return self._solid_def_snap.name

    def get_solid_definition(self):
        return build_solid_definition(self._represented_pipeline, self._solid_def_snap.name)

    def resolve_definition(self, _):
        return self.get_solid_definition()

    def resolve_inputs(self, _):
        return [
            Input(
                self._represented_pipeline,
                self._current_dep_structure,
                self._solid_invocation_snap.solid_name,
                input_def_snap.name,
            )
            for input_def_snap in self._solid_def_snap.input_def_snaps
        ]

    def resolve_outputs(self, _):
        return [
            Output(
                self._represented_pipeline,
                self._current_dep_structure,
                self._solid_invocation_snap.solid_name,
                output_def_snap.name,
            )
            for output_def_snap in self._solid_def_snap.output_def_snaps
        ]


class SolidContainer(graphene.Interface):
    solids = non_null_list(Solid)


class CompositeSolidDefinition(graphene.ObjectType, ISolidDefinitionMixin):
    class Meta:
        interfaces = (ISolidDefinition, SolidContainer)

    solids = non_null_list("Solid")
    input_mappings = non_null_list("InputMapping")
    output_mappings = non_null_list("OutputMapping")

    def __init__(self, represented_pipeline, solid_def_name):
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        self._solid_def_snap = represented_pipeline.get_solid_def_snap(solid_def_name)
        self._comp_solid_dep_index = represented_pipeline.get_dep_structure_index(solid_def_name)
        super().__init__(name=solid_def_name, description=self._solid_def_snap.description)
        ISolidDefinitionMixin.__init__(self, represented_pipeline, solid_def_name)

    def resolve_solids(self, _graphene_info):
        return build_solids(self._represented_pipeline, self._comp_solid_dep_index)

    def resolve_output_mappings(self, _):
        return [
            OutputMapping(
                self._represented_pipeline,
                self._comp_solid_dep_index,
                self._solid_def_snap.name,
                output_def_snap.name,
            )
            for output_def_snap in self._solid_def_snap.output_def_snaps
        ]

    def resolve_input_mappings(self, _):
        return [
            InputMapping(
                self._represented_pipeline,
                self._comp_solid_dep_index,
                self._solid_def_snap.name,
                input_def_snap.name,
            )
            for input_def_snap in self._solid_def_snap.input_def_snaps
        ]


class SolidHandle(graphene.ObjectType):
    handleID = graphene.NonNull(graphene.String)
    solid = graphene.NonNull(Solid)
    parent = graphene.Field(lambda: SolidHandle)

    def __init__(self, handle, solid, parent):
        # FIXME this is obviously wrong
        self.handleID = check.inst_param(handle, "handle", DagsterSolidHandle)
        self.solid = check.inst_param(solid, "solid", Solid)
        self.parent = check.opt_inst_param(parent, "parent", DagsterSolidHandle)
