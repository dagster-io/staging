from collections import namedtuple
from enum import Enum

from dagster import check
from dagster.core.definitions import (
    AssetMaterialization,
    HookDefinition,
    Materialization,
    SolidHandle,
)
from dagster.core.definitions.events import EventMetadataEntry
from dagster.core.types.dagster_type import DagsterType
from dagster.serdes import whitelist_for_serdes
from dagster.utils import frozentags, merge_dicts
from dagster.utils.error import SerializableErrorInfo


@whitelist_for_serdes
class StepOutputHandle(namedtuple('_StepOutputHandle', 'step_key output_name special_index')):
    @staticmethod
    def from_step(step, output_name='result', special_index=None):
        check.inst_param(step, 'step', ExecutionStep)

        return StepOutputHandle(step.key, output_name, special_index)

    def __new__(cls, step_key, output_name='result', special_index=None):
        return super(StepOutputHandle, cls).__new__(
            cls,
            step_key=check.str_param(step_key, 'step_key'),
            output_name=check.str_param(output_name, 'output_name'),
            special_index=check.opt_str_param(special_index, 'special_index'),
        )

    @property
    def special_form(self):
        check.invariant(self.special_index, 'special_form only valid if special index present')
        return SpecialStepOutputHandle(self.step_key, self.output_name)

    @property
    def special_tag(self):
        check.invariant(self.special_index, 'special_tag only valid if special index present')
        return '[{}:{}:{}]'.format(self.step_key, self.output_name, self.special_index)


@whitelist_for_serdes
class SpecialStepOutputHandle(namedtuple('_SpecialStepOutputHandle', 'step_key output_name')):
    def __new__(cls, step_key, output_name='result'):
        return super(SpecialStepOutputHandle, cls).__new__(
            cls,
            step_key=check.str_param(step_key, 'step_key'),
            output_name=check.str_param(output_name, 'output_name'),
        )


@whitelist_for_serdes
class UnresolvedStepOutputHandle(
    namedtuple('_UnresolvedStepOutputHandle', 'unresolved_key output_name special_upstream')
):
    def __new__(cls, unresolved_key, output_name, special_upstream):
        return super(UnresolvedStepOutputHandle, cls).__new__(
            cls,
            unresolved_key=check.str_param(unresolved_key, 'unresolved_key'),
            output_name=check.str_param(output_name, 'output_name'),
            special_upstream=special_upstream,
        )

    def resolve(self, special_output_handle):
        return StepOutputHandle(
            step_key=self.unresolved_key.replace('[?]', special_output_handle.special_tag),
            output_name=self.output_name,
        )


@whitelist_for_serdes
class StepInputData(namedtuple('_StepInputData', 'input_name type_check_data')):
    def __new__(cls, input_name, type_check_data):
        return super(StepInputData, cls).__new__(
            cls,
            input_name=check.str_param(input_name, 'input_name'),
            type_check_data=check.opt_inst_param(type_check_data, 'type_check_data', TypeCheckData),
        )


@whitelist_for_serdes
class TypeCheckData(namedtuple('_TypeCheckData', 'success label description metadata_entries')):
    def __new__(cls, success, label, description=None, metadata_entries=None):
        return super(TypeCheckData, cls).__new__(
            cls,
            success=check.bool_param(success, 'success'),
            label=check.str_param(label, 'label'),
            description=check.opt_str_param(description, 'description'),
            metadata_entries=check.opt_list_param(
                metadata_entries, metadata_entries, of_type=EventMetadataEntry
            ),
        )


@whitelist_for_serdes
class UserFailureData(namedtuple('_UserFailureData', 'label description metadata_entries')):
    def __new__(cls, label, description=None, metadata_entries=None):
        return super(UserFailureData, cls).__new__(
            cls,
            label=check.str_param(label, 'label'),
            description=check.opt_str_param(description, 'description'),
            metadata_entries=check.opt_list_param(
                metadata_entries, metadata_entries, of_type=EventMetadataEntry
            ),
        )


@whitelist_for_serdes
class StepOutputData(
    namedtuple('_StepOutputData', 'step_output_handle intermediate_materialization type_check_data')
):
    def __new__(cls, step_output_handle, intermediate_materialization=None, type_check_data=None):
        return super(StepOutputData, cls).__new__(
            cls,
            step_output_handle=check.inst_param(
                step_output_handle, 'step_output_handle', StepOutputHandle
            ),
            intermediate_materialization=check.opt_inst_param(
                intermediate_materialization,
                'intermediate_materialization',
                (AssetMaterialization, Materialization),
            ),
            type_check_data=check.opt_inst_param(type_check_data, 'type_check_data', TypeCheckData),
        )

    @property
    def output_name(self):
        return self.step_output_handle.output_name

    @property
    def special_index(self):
        return self.step_output_handle.special_index


@whitelist_for_serdes
class StepFailureData(namedtuple('_StepFailureData', 'error user_failure_data')):
    def __new__(cls, error, user_failure_data):
        return super(StepFailureData, cls).__new__(
            cls,
            error=check.opt_inst_param(error, 'error', SerializableErrorInfo),
            user_failure_data=check.opt_inst_param(
                user_failure_data, 'user_failure_data', UserFailureData
            ),
        )


@whitelist_for_serdes
class StepRetryData(namedtuple('_StepRetryData', 'error seconds_to_wait')):
    def __new__(cls, error, seconds_to_wait=None):
        return super(StepRetryData, cls).__new__(
            cls,
            error=check.opt_inst_param(error, 'error', SerializableErrorInfo),
            seconds_to_wait=check.opt_int_param(seconds_to_wait, 'seconds_to_wait'),
        )


@whitelist_for_serdes
class StepSuccessData(namedtuple('_StepSuccessData', 'duration_ms')):
    def __new__(cls, duration_ms):
        return super(StepSuccessData, cls).__new__(
            cls, duration_ms=check.float_param(duration_ms, 'duration_ms')
        )


@whitelist_for_serdes
class StepKind(Enum):
    COMPUTE = 'COMPUTE'


class StepInputSourceType(Enum):
    SINGLE_OUTPUT = 'SINGLE_OUTPUT'
    MULTIPLE_OUTPUTS = 'MULTIPLE_OUTPUTS'
    CONFIG = 'CONFIG'
    DEFAULT_VALUE = 'DEFAULT_VALUE'


class StepInput(
    namedtuple('_StepInput', 'name dagster_type source_type source_handles config_data')
):
    def __new__(
        cls, name, dagster_type=None, source_type=None, source_handles=None, config_data=None,
    ):
        return super(StepInput, cls).__new__(
            cls,
            name=check.str_param(name, 'name'),
            dagster_type=check.inst_param(dagster_type, 'dagster_type', DagsterType),
            source_type=check.inst_param(source_type, 'source_type', StepInputSourceType),
            source_handles=check.opt_list_param(
                source_handles, 'source_handles', of_type=StepOutputHandle
            ),
            config_data=config_data,  # can be any type
        )

    @property
    def is_from_output(self):
        return (
            self.source_type == StepInputSourceType.SINGLE_OUTPUT
            or self.source_type == StepInputSourceType.MULTIPLE_OUTPUTS
        )

    @property
    def is_from_single_output(self):
        return self.source_type == StepInputSourceType.SINGLE_OUTPUT

    @property
    def is_from_config(self):
        return self.source_type == StepInputSourceType.CONFIG

    @property
    def is_from_default_value(self):
        return self.source_type == StepInputSourceType.DEFAULT_VALUE

    @property
    def is_from_multiple_outputs(self):
        return self.source_type == StepInputSourceType.MULTIPLE_OUTPUTS

    @property
    def dependency_keys(self):
        return {handle.step_key for handle in self.source_handles}


class SpecialStepInput(namedtuple('_StepInput', 'name dagster_type special_handles')):
    def __new__(cls, name, dagster_type, special_handles):
        return super(SpecialStepInput, cls).__new__(
            cls,
            name=check.str_param(name, 'name'),
            dagster_type=check.inst_param(dagster_type, 'dagster_type', DagsterType),
            special_handles=check.list_param(
                special_handles, 'special_handles', SpecialStepOutputHandle
            ),
        )

    @property
    def dependency_keys(self):
        return {h.step_key for h in self.special_handles}

    # @property
    # def special_handles(self):  # hacks
    #     return []

    # @property
    # def source_handles(self):
    #     return []

    @property
    def source_type(self):  # hacks
        return None

    def resolve(self, output_handle):
        return StepInput(
            name=self.name,
            dagster_type=self.dagster_type,
            source_type=StepInputSourceType.SINGLE_OUTPUT,
            source_handles=[output_handle],
        )


class UnresolvedStepInput(namedtuple('_StepInput', 'name dagster_type source_handle')):
    def __new__(cls, name, dagster_type, source_handle):
        return super(UnresolvedStepInput, cls).__new__(
            cls,
            name=check.str_param(name, 'name'),
            dagster_type=check.inst_param(dagster_type, 'dagster_type', DagsterType),
            source_handle=source_handle,
        )

    @property
    def special_handles(self):
        return self.source_handle.special_upstream

    @property
    def dependency_keys(self):
        return {self.source_handle.unresolved_key}

    @property
    def source_type(self):  # hacks
        return None

    def resolve(self, output_handle):
        return StepInput(
            name=self.name,
            dagster_type=self.dagster_type,
            source_type=StepInputSourceType.SINGLE_OUTPUT,
            source_handles=[self.source_handle.resolve(output_handle)],
        )


class StepOutput(namedtuple('_StepOutput', 'name dagster_type optional should_materialize')):
    def __new__(
        cls, name, dagster_type=None, optional=None, should_materialize=None,
    ):
        return super(StepOutput, cls).__new__(
            cls,
            name=check.str_param(name, 'name'),
            optional=check.bool_param(optional, 'optional'),
            should_materialize=check.bool_param(should_materialize, 'should_materialize'),
            dagster_type=check.inst_param(dagster_type, 'dagster_type', DagsterType),
        )


class ExecutionStep(
    namedtuple(
        '_ExecutionStep',
        (
            'pipeline_name key_tag step_inputs step_input_dict step_outputs step_output_dict '
            'compute_fn kind solid_handle logging_tags tags hook_defs'
        ),
    )
):
    def __new__(
        cls,
        pipeline_name,
        key_tag,
        step_inputs,
        step_outputs,
        compute_fn,
        kind,
        solid_handle,
        solid,
        logging_tags=None,
        tags=None,
        hook_defs=None,
    ):
        key_tag = check.opt_str_param(key_tag, 'key_tag', '')
        return super(ExecutionStep, cls).__new__(
            cls,
            pipeline_name=check.str_param(pipeline_name, 'pipeline_name'),
            key_tag=key_tag,
            step_inputs=check.list_param(step_inputs, 'step_inputs', of_type=StepInput),
            step_input_dict={si.name: si for si in step_inputs},
            step_outputs=check.list_param(step_outputs, 'step_outputs', of_type=StepOutput),
            step_output_dict={so.name: so for so in step_outputs},
            compute_fn=check.callable_param(compute_fn, 'compute_fn'),
            kind=check.inst_param(kind, 'kind', StepKind),
            solid_handle=check.inst_param(solid_handle, 'solid_handle', SolidHandle),
            logging_tags=merge_dicts(
                {
                    'step_key': str(solid_handle) + key_tag + '.compute',
                    'pipeline': pipeline_name,
                    'solid': solid_handle.name,
                    'solid_definition': solid.definition.name,
                },
                check.opt_dict_param(logging_tags, 'logging_tags'),
            ),
            tags=check.opt_inst_param(tags, 'tags', frozentags),
            hook_defs=check.opt_set_param(hook_defs, 'hook_defs', of_type=HookDefinition),
        )

    @property
    def key(self):
        return str(self.solid_handle) + self.key_tag + '.compute'

    @property
    def solid_name(self):
        return self.solid_handle.name

    def has_step_output(self, name):
        check.str_param(name, 'name')
        return name in self.step_output_dict

    def step_output_named(self, name):
        check.str_param(name, 'name')
        return self.step_output_dict[name]

    def has_step_input(self, name):
        check.str_param(name, 'name')
        return name in self.step_input_dict

    def step_input_named(self, name):
        check.str_param(name, 'name')
        return self.step_input_dict[name]


class UnresolvedExecutionStep(
    namedtuple('_UnresolvedExecutionStep', 'pipeline_name solid step_inputs solid_handle',)
):
    def __new__(cls, pipeline_name, solid, step_inputs, solid_handle):
        return super(UnresolvedExecutionStep, cls).__new__(
            cls,
            pipeline_name=pipeline_name,
            solid=solid,
            step_inputs=step_inputs,
            solid_handle=solid_handle,
        )

    @property
    def key(self):
        return "{}[?].compute".format(self.solid_handle.to_string())

    def get_special_upstream(self):
        handles = []
        for s in self.step_inputs:
            if not isinstance(s, StepInput):
                handles += s.special_handles

        return handles

    @property
    def step_outputs(self):  # hack to deal with required resource key grabbing
        return []

    @property
    def hook_defs(self):  # hack hack hack
        return []

    @property
    def logging_tags(self):  # hack hack hack
        return {
            'step_key': str(self.solid_handle) + self.key,  # + '.compute',
            'pipeline': self.pipeline_name,
            'solid': self.solid_handle.name,
            'solid_definition': self.solid.definition.name,
        }

    def step_output_named(self, name):
        check.str_param(name, 'name')
        step_outputs = [
            StepOutput(
                name=name,
                dagster_type=output_def.dagster_type,
                optional=output_def.optional,
                should_materialize=False,
            )
            for name, output_def in self.solid.definition.output_dict.items()
        ]
        print('step_outputs', step_outputs)
        print('name', name)
        step_output_dict = {so.name: so for so in step_outputs}
        print('step_output_dict', step_output_dict)
        return step_output_dict[name]

    def resolve(self, handle_map):
        from .compute import _execute_core_compute

        execution_steps = []
        # only support one for now
        step_input = self.step_inputs[0]
        special_handle = step_input.special_handles[0]
        step_output_handles = handle_map[special_handle]
        solid = self.solid
        for step_output_handle in step_output_handles:
            resolved_step_input = step_input.resolve(step_output_handle)

            execution_steps.append(
                ExecutionStep(
                    pipeline_name=self.pipeline_name,
                    key_tag=step_output_handle.special_tag,
                    step_inputs=[resolved_step_input],
                    step_outputs=[
                        StepOutput(
                            name=name,
                            dagster_type=output_def.dagster_type,
                            optional=output_def.optional,
                            should_materialize=False,
                        )
                        for name, output_def in solid.definition.output_dict.items()
                    ],
                    compute_fn=lambda step_context, inputs: _execute_core_compute(
                        step_context.for_compute(), inputs, solid.definition.compute_fn
                    ),
                    kind=StepKind.COMPUTE,
                    solid_handle=self.solid_handle,
                    solid=solid,
                    tags=solid.tags,
                    hook_defs=solid.hook_defs,
                )
            )

        return execution_steps
