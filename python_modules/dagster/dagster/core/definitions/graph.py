from dagster import check

from .i_solid_definition import ISolidDefinition
from .solid_container import IContainSolids


# pylint: disable=abstract-method
class GraphDefinition(ISolidDefinition, IContainSolids):
    def __init__(self, input_mappings, output_mappings, **kwargs):
        self._input_mappings = input_mappings
        self._output_mappings = output_mappings
        super(GraphDefinition, self).__init__(**kwargs)

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
