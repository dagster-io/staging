from collections import OrderedDict

from dagster import check
from dagster.core.errors import DagsterInvalidDefinitionError
from dagster.core.types.dagster_type import DagsterTypeKind

from .i_solid_definition import ISolidDefinition
from .input import InputDefinition, InputMapping
from .output import OutputDefinition, OutputMapping
from .solid_container import IContainSolids, create_execution_structure, validate_dependency_dict


# pylint: disable=abstract-method
class GraphDefinition(ISolidDefinition, IContainSolids):
    def __init__(self, name, solid_defs, dependencies, input_mappings, output_mappings, **kwargs):
        self._solid_defs = check.list_param(solid_defs, "solid_defs", of_type=ISolidDefinition)
        self._dependencies = validate_dependency_dict(dependencies)
        self._dependency_structure, self._solid_dict = create_execution_structure(
            solid_defs, self._dependencies, container_definition=self
        )

        # List[InputMapping]
        self._input_mappings, input_defs = _validate_in_mappings(
            check.opt_list_param(input_mappings, "input_mappings"), self._solid_dict, name
        )
        # List[OutputMapping]
        self._output_mappings = _validate_out_mappings(
            check.opt_list_param(output_mappings, "output_mappings"), self._solid_dict, name
        )

        super(GraphDefinition, self).__init__(
            name=name,
            input_defs=input_defs,
            output_defs=[output_mapping.definition for output_mapping in self._output_mappings],
            **kwargs
        )

    @property
    def input_mappings(self):
        return self._input_mappings

    @property
    def output_mappings(self):
        return self._output_mappings

    def get_input_mapping(self, input_name):
        check.str_param(input_name, "input_name")
        for mapping in self._input_mappings:
            if mapping.definition.name == input_name:
                return mapping
        return None

    def mapped_input(self, solid_name, input_name):
        check.str_param(solid_name, "solid_name")
        check.str_param(input_name, "input_name")

        for mapping in self._input_mappings:
            if mapping.solid_name == solid_name and mapping.input_name == input_name:
                return mapping
        return None

    def get_output_mapping(self, output_name):
        check.str_param(output_name, "output_name")
        for mapping in self._output_mappings:
            if mapping.definition.name == output_name:
                return mapping
        return None

    def default_value_for_input(self, input_name):
        check.str_param(input_name, "input_name")

        # base case
        if self.input_def_named(input_name).has_default_value:
            return self.input_def_named(input_name).default_value

        mapping = self.get_input_mapping(input_name)
        check.invariant(mapping, "Can only resolve inputs for valid input names")
        mapped_solid = self.solid_named(mapping.solid_name)

        return mapped_solid.definition.default_value_for_input(mapping.input_name)

    def input_has_default(self, input_name):
        check.str_param(input_name, "input_name")

        # base case
        if self.input_def_named(input_name).has_default_value:
            return True

        mapping = self.get_input_mapping(input_name)
        check.invariant(mapping, "Can only resolve inputs for valid input names")
        mapped_solid = self.solid_named(mapping.solid_name)

        return mapped_solid.definition.input_has_default(mapping.input_name)

    @property
    def required_resource_keys(self):
        required_resource_keys = set()
        for solid in self.solids:
            required_resource_keys.update(solid.definition.required_resource_keys)
        return frozenset(required_resource_keys)

    @property
    def has_config_entry(self):
        has_child_solid_config = any([solid.definition.has_config_entry for solid in self.solids])
        return (
            self.has_config_mapping
            or has_child_solid_config
            or self.has_configurable_inputs
            or self.has_configurable_outputs
        )

    @property
    def has_config_mapping(self):
        raise NotImplementedError()


def _validate_in_mappings(input_mappings, solid_dict, name, class_name="CompositeSolid"):
    input_def_dict = OrderedDict()
    for mapping in input_mappings:
        if isinstance(mapping, InputMapping):
            if input_def_dict.get(mapping.definition.name):
                if input_def_dict[mapping.definition.name] != mapping.definition:
                    raise DagsterInvalidDefinitionError(
                        "In {class_name} {name} multiple input mappings with same "
                        "definition name but different definitions".format(
                            name=name, class_name=class_name
                        ),
                    )
            else:
                input_def_dict[mapping.definition.name] = mapping.definition

            target_solid = solid_dict.get(mapping.solid_name)
            if target_solid is None:
                raise DagsterInvalidDefinitionError(
                    "In {class_name} '{name}' input mapping references solid "
                    "'{solid_name}' which it does not contain.".format(
                        name=name, solid_name=mapping.solid_name, class_name=class_name
                    )
                )
            if not target_solid.has_input(mapping.input_name):
                raise DagsterInvalidDefinitionError(
                    "In {class_name} '{name}' input mapping to solid '{mapping.solid_name}' "
                    "which contains no input named '{mapping.input_name}'".format(
                        name=name, mapping=mapping, class_name=class_name
                    )
                )

            target_input = target_solid.input_def_named(mapping.input_name)
            if target_input.dagster_type != mapping.definition.dagster_type:
                raise DagsterInvalidDefinitionError(
                    "In {class_name} '{name}' input "
                    "'{mapping.definition.name}' of type {mapping.definition.dagster_type.display_name} maps to "
                    "{mapping.solid_name}.{mapping.input_name} of different type "
                    "{target_input.dagster_type.display_name}. InputMapping source and "
                    "destination must have the same type.".format(
                        mapping=mapping, name=name, target_input=target_input, class_name=class_name
                    )
                )

        elif isinstance(mapping, InputDefinition):
            raise DagsterInvalidDefinitionError(
                "In {class_name} '{name}' you passed an InputDefinition "
                "named '{input_name}' directly in to input_mappings. Return "
                "an InputMapping by calling mapping_to on the InputDefinition.".format(
                    name=name, input_name=mapping.name, class_name=class_name
                )
            )
        else:
            raise DagsterInvalidDefinitionError(
                "In {class_name} '{name}' received unexpected type '{type}' in input_mappings. "
                "Provide an OutputMapping using InputDefinition(...).mapping_to(...)".format(
                    type=type(mapping), name=name, class_name=class_name
                )
            )

    return input_mappings, input_def_dict.values()


def _validate_out_mappings(output_mappings, solid_dict, name, class_name="CompositeSolid"):
    for mapping in output_mappings:
        if isinstance(mapping, OutputMapping):

            target_solid = solid_dict.get(mapping.solid_name)
            if target_solid is None:
                raise DagsterInvalidDefinitionError(
                    "In {class_name} '{name}' output mapping references solid "
                    "'{solid_name}' which it does not contain.".format(
                        name=name, solid_name=mapping.solid_name, class_name=class_name
                    )
                )
            if not target_solid.has_output(mapping.output_name):
                raise DagsterInvalidDefinitionError(
                    "In {class_name} {name} output mapping from solid '{mapping.solid_name}' "
                    "which contains no output named '{mapping.output_name}'".format(
                        name=name, mapping=mapping, class_name=class_name
                    )
                )

            target_output = target_solid.output_def_named(mapping.output_name)

            if mapping.definition.dagster_type.kind != DagsterTypeKind.ANY and (
                target_output.dagster_type != mapping.definition.dagster_type
            ):
                raise DagsterInvalidDefinitionError(
                    "In {class_name} '{name}' output "
                    "'{mapping.definition.name}' of type {mapping.definition.dagster_type.display_name} "
                    "maps from {mapping.solid_name}.{mapping.output_name} of different type "
                    "{target_output.dagster_type.display_name}. OutputMapping source "
                    "and destination must have the same type.".format(
                        class_name=class_name,
                        mapping=mapping,
                        name=name,
                        target_output=target_output,
                    )
                )

        elif isinstance(mapping, OutputDefinition):
            raise DagsterInvalidDefinitionError(
                "You passed an OutputDefinition named '{output_name}' directly "
                "in to output_mappings. Return an OutputMapping by calling "
                "mapping_from on the OutputDefinition.".format(output_name=mapping.name)
            )
        else:
            raise DagsterInvalidDefinitionError(
                "Received unexpected type '{type}' in output_mappings. "
                "Provide an OutputMapping using OutputDefinition(...).mapping_from(...)".format(
                    type=type(mapping)
                )
            )
    return output_mappings
