from abc import ABCMeta, abstractmethod, abstractproperty

import six

from dagster import check
from dagster.core.definitions.address import Address
from dagster.core.definitions.events import ObjectStoreOperation, ObjectStoreOperationType
from dagster.core.errors import (
    DagsterAddressIOError,
    DagsterTypeLoadingError,
    DagsterTypeMaterializationError,
    user_code_error_boundary,
)
from dagster.core.execution.context.system import SystemExecutionContext
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.types.dagster_type import DagsterType, resolve_dagster_type
from dagster.utils.backcompat import experimental_arg_warning

from .object_store import FilesystemObjectStore, InMemoryObjectStore, ObjectStore
from .type_storage import TypeStoragePluginRegistry


class IntermediateStorage(six.with_metaclass(ABCMeta)):  # pylint: disable=no-init
    @abstractmethod
    def get_intermediate(self, context, dagster_type=None, step_output_handle=None, address=None):
        pass

    @abstractmethod
    def set_intermediate(
        self, context, dagster_type=None, step_output_handle=None, value=None, address=None
    ):
        pass

    @abstractmethod
    def has_intermediate(self, context, step_output_handle):
        pass

    @abstractmethod
    def copy_intermediate_from_run(self, context, run_id, step_output_handle):
        pass

    @abstractproperty
    def is_persistent(self):
        pass

    def all_inputs_covered(self, context, step):
        return len(self.uncovered_inputs(context, step)) == 0

    def uncovered_inputs(self, context, step):
        from dagster.core.execution.plan.objects import ExecutionStep

        check.inst_param(step, "step", ExecutionStep)
        uncovered_inputs = []
        for step_input in step.step_inputs:

            if step_input.is_from_single_output:
                for source_handle in step_input.source_handles:
                    if not self.has_intermediate(context, source_handle):
                        uncovered_inputs.append(source_handle)

            elif step_input.is_from_multiple_outputs:
                missing_source_handles = [
                    source_handle
                    for source_handle in step_input.source_handles
                    if not self.has_intermediate(context, source_handle)
                ]
                # only report as uncovered if all are missing from a multi-dep input
                if len(missing_source_handles) == len(step_input.source_handles):
                    uncovered_inputs = uncovered_inputs + missing_source_handles

        return uncovered_inputs


class ObjectStoreIntermediateStorage(IntermediateStorage):
    def __init__(
        self,
        object_store,
        root_for_run_id,
        run_id,
        type_storage_plugin_registry,
        external_intermediates=None,
    ):
        self.root_for_run_id = check.callable_param(root_for_run_id, "root_for_run_id")
        self.run_id = check.str_param(run_id, "run_id")
        self.object_store = check.inst_param(object_store, "object_store", ObjectStore)
        self.type_storage_plugin_registry = check.inst_param(
            type_storage_plugin_registry, "type_storage_plugin_registry", TypeStoragePluginRegistry
        )
        self.external_intermediates = check.opt_dict_param(
            external_intermediates,
            "external_intermediates",
            key_type=StepOutputHandle,
            value_type=Address,
        )

    def _get_paths(self, step_output_handle):
        return ["intermediates", step_output_handle.step_key, step_output_handle.output_name]

    def get_intermediate_object(self, dagster_type, step_output_handle, address=None):
        check.inst_param(dagster_type, "dagster_type", DagsterType)
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)

        # if the address of the object is provided, we use the address directly; otherwise the
        # system will construct the key from step_output_handle
        if address:
            check.inst_param(address, "address", Address)
            key = address.path
        else:
            paths = self._get_paths(step_output_handle)
            check.param_invariant(len(paths) > 0, "paths")
            key = self.object_store.key_for_paths([self.root] + paths)

        try:
            return self.object_store.get_object(
                key, serialization_strategy=dagster_type.serialization_strategy
            )
        except IOError as e:
            raise DagsterAddressIOError(str(e))

    def get_intermediate(self, context, dagster_type=None, step_output_handle=None, address=None):
        dagster_type = resolve_dagster_type(dagster_type)
        check.opt_inst_param(context, "context", SystemExecutionContext)
        check.inst_param(dagster_type, "dagster_type", DagsterType)
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        if address:
            experimental_arg_warning("address", "ObjectStoreIntermediateStorage.get_intermediate")
        else:
            # if address is provided, the intermediate could be stored outside so we skip the check
            check.invariant(self.has_intermediate(context, step_output_handle))

        # if the intermediate was stored externally
        if address is None:
            address = self.external_intermediates.get(step_output_handle)

        if address and address.config_value and dagster_type.loader:
            with user_code_error_boundary(
                DagsterTypeLoadingError,
                msg_fn=lambda: (
                    "Error occurred during input loading:"
                    "input name: '{output_name}'"
                    "step key: '{key}'"
                    "solid invocation: '{solid}'"
                    "solid definition: '{solid_def}'"
                ).format(
                    output_name=step_output_handle.output_name,  # FIXME input name instead
                    key=context.step.key,
                    solid_def=context.solid_def.name,
                    solid=context.solid.name,
                ),
            ):
                value = dagster_type.loader.construct_from_config_value(
                    context, address.config_value
                )
            # yield "get external object" operation event for cross-run intermediate storage
            return ObjectStoreOperation(
                op=ObjectStoreOperationType.GET_EXTERNAL_OBJECT,
                key=address.key,
                obj=value,
                address=address,
                step_output_handle=step_output_handle,
            )

        # START: to deprecate https://github.com/dagster-io/dagster/issues/3043
        if self.type_storage_plugin_registry.is_registered(dagster_type):
            return self.type_storage_plugin_registry.get(dagster_type.name).get_intermediate_object(
                self, context, dagster_type, step_output_handle
            )
        elif dagster_type.name is None:
            self.type_storage_plugin_registry.check_for_unsupported_composite_overrides(
                dagster_type
            )
        # END: to deprecate

        return self.get_intermediate_object(dagster_type, step_output_handle, address)

    def set_intermediate_object(self, dagster_type, step_output_handle, value, address=None):
        check.inst_param(dagster_type, "dagster_type", DagsterType)
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)

        # if the address of the object is provided, we use the address directly; otherwise the
        # system will construct the key from step_output_handle
        if address and address.path:
            check.inst_param(address, "address", Address)
            key = address.path
        else:
            paths = self._get_paths(step_output_handle)
            check.param_invariant(len(paths) > 0, "paths")
            key = self.object_store.key_for_paths([self.root] + paths)

        try:
            return self.object_store.set_object(
                key, value, serialization_strategy=dagster_type.serialization_strategy
            )
        except (IOError, OSError) as e:
            raise DagsterAddressIOError(str(e))

    def _set_external_intermediate(self, context, dagster_type, step_output_handle, value, address):
        with user_code_error_boundary(
            DagsterTypeMaterializationError,
            msg_fn=lambda: (
                "Error occurred during output materialization:"
                "output name: '{output_name}'"
                "step key: '{key}'"
                "solid invocation: '{solid}'"
                "solid definition: '{solid_def}'"
            ).format(
                output_name=step_output_handle.output_name,
                key=context.step.key,
                solid_def=context.solid_def.name,
                solid=context.solid.name,
            ),
        ):
            materializations = dagster_type.materializer.materialize_runtime_values(
                context, address.config_value, value
            )
        self.external_intermediates[step_output_handle] = address
        for materialization in materializations:
            yield materialization

        # yield "set external object" operation event for cross-run intermediate storage
        yield ObjectStoreOperation(
            op=ObjectStoreOperationType.SET_EXTERNAL_OBJECT,
            key=address.key,
            address=address,
            step_output_handle=step_output_handle,
        )

    def set_intermediate(
        self, context, dagster_type=None, step_output_handle=None, value=None, address=None,
    ):
        dagster_type = resolve_dagster_type(dagster_type)
        check.opt_inst_param(context, "context", SystemExecutionContext)
        check.inst_param(dagster_type, "dagster_type", DagsterType)
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        if address:
            experimental_arg_warning("address", "ObjectStoreIntermediateStorage.set_intermediate")
            check.inst_param(address, "address", Address)

        if self.has_intermediate(context, step_output_handle):
            context.log.warning(
                "Replacing existing intermediate for %s.%s"
                % (step_output_handle.step_key, step_output_handle.output_name)
            )

        # load to some external address
        if address and address.config_value and dagster_type.materializer:
            return self._set_external_intermediate(
                context, dagster_type, step_output_handle, value, address
            )

        # skip if the intermediate has already been set by type materializer
        if step_output_handle in self.external_intermediates:
            context.log.info(
                (
                    "{step_output_handle} has already been materialized by DagsterTypeMaterializer. "
                    "Skip object store."
                ).format(step_output_handle=step_output_handle)
            )
            return

        # START: to deprecate https://github.com/dagster-io/dagster/issues/3043
        if self.type_storage_plugin_registry.is_registered(dagster_type):
            return self.type_storage_plugin_registry.get(dagster_type.name).set_intermediate_object(
                self, context, dagster_type, step_output_handle, value
            )
        elif dagster_type.name is None:
            self.type_storage_plugin_registry.check_for_unsupported_composite_overrides(
                dagster_type
            )
        # END: to deprecate

        return self.set_intermediate_object(dagster_type, step_output_handle, value, address)

    def has_intermediate(self, context, step_output_handle):
        check.opt_inst_param(context, "context", SystemExecutionContext)
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        paths = self._get_paths(step_output_handle)
        check.list_param(paths, "paths", of_type=str)
        check.param_invariant(len(paths) > 0, "paths")

        key = self.object_store.key_for_paths([self.root] + paths)
        return (
            self.object_store.has_object(key) or step_output_handle in self.external_intermediates
        )

    def rm_intermediate(self, context, step_output_handle):
        check.opt_inst_param(context, "context", SystemExecutionContext)
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        paths = self._get_paths(step_output_handle)
        check.param_invariant(len(paths) > 0, "paths")
        key = self.object_store.key_for_paths([self.root] + paths)
        return self.object_store.rm_object(key)

    def copy_intermediate_from_run(self, context, run_id, step_output_handle):
        check.opt_inst_param(context, "context", SystemExecutionContext)
        check.str_param(run_id, "run_id")
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        paths = self._get_paths(step_output_handle)

        src = self.object_store.key_for_paths([self.root_for_run_id(run_id)] + paths)
        dst = self.object_store.key_for_paths([self.root] + paths)

        return self.object_store.cp_object(src, dst)

    def uri_for_paths(self, paths, protocol=None):
        check.list_param(paths, "paths", of_type=str)
        check.param_invariant(len(paths) > 0, "paths")
        key = self.key_for_paths(paths)
        return self.object_store.uri_for_key(key, protocol)

    def key_for_paths(self, paths):
        return self.object_store.key_for_paths([self.root] + paths)

    @property
    def is_persistent(self):
        if isinstance(self.object_store, InMemoryObjectStore):
            return False
        return True

    @property
    def root(self):
        return self.root_for_run_id(self.run_id)


def build_in_mem_intermediates_storage(run_id, type_storage_plugin_registry=None):
    return ObjectStoreIntermediateStorage(
        InMemoryObjectStore(),
        lambda _: "",
        run_id,
        type_storage_plugin_registry
        if type_storage_plugin_registry
        else TypeStoragePluginRegistry(types_to_register=[]),
    )


def build_fs_intermediate_storage(
    root_for_run_id, run_id, type_storage_plugin_registry=None, external_intermediates=None
):
    return ObjectStoreIntermediateStorage(
        FilesystemObjectStore(),
        root_for_run_id,
        run_id,
        type_storage_plugin_registry
        if type_storage_plugin_registry
        else TypeStoragePluginRegistry(types_to_register=[]),
        external_intermediates,
    )
