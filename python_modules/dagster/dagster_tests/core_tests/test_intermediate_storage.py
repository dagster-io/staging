import os

import pytest

from dagster import Bool, Int, List, Optional, String, check
from dagster.core.definitions.events import ObjectStoreOperationType
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.instance import DagsterInstance
from dagster.core.storage.intermediates_manager import build_fs_intermediate_storage
from dagster.core.storage.type_storage import TypeStoragePlugin, TypeStoragePluginRegistry
from dagster.core.types.dagster_type import String as RuntimeString
from dagster.core.types.dagster_type import create_any_type, resolve_dagster_type
from dagster.core.types.marshal import SerializationStrategy
from dagster.core.utils import make_new_run_id
from dagster.utils import mkdir_p
from dagster.utils.test import yield_empty_pipeline_context


class UppercaseSerializationStrategy(SerializationStrategy):  # pylint: disable=no-init
    def serialize(self, value, write_file_obj):
        return write_file_obj.write(bytes(value.upper().encode('utf-8')))

    def deserialize(self, read_file_obj):
        return read_file_obj.read().decode('utf-8').lower()


class FancyStringFilesystemTypeStoragePlugin(TypeStoragePlugin):  # pylint:disable=no-init
    @classmethod
    def compatible_with_storage_def(cls, _):
        # Not needed for these tests
        raise NotImplementedError()

    @classmethod
    def set_object(cls, intermediate_store, obj, context, dagster_type, paths):
        paths.append(obj)
        mkdir_p(os.path.join(intermediate_store.root, *paths))

    @classmethod
    def get_object(cls, intermediate_store, context, dagster_type, paths):
        return os.listdir(os.path.join(intermediate_store.root, *paths))[0]


LowercaseString = create_any_type(
    'LowercaseString', serialization_strategy=UppercaseSerializationStrategy('uppercase')
)


def define_intermediates_manager(type_storage_plugin_registry=None):
    run_id = make_new_run_id()
    instance = DagsterInstance.ephemeral()
    intermediates_manager = build_fs_intermediate_storage(
        instance.intermediates_directory,
        run_id=run_id,
        type_storage_plugin_registry=type_storage_plugin_registry,
    )
    return run_id, instance, intermediates_manager


def test_file_system_intermediate_storage():
    _, _, intermediates_manager = define_intermediates_manager()

    assert (
        intermediates_manager.set_intermediate(
            None, Int, StepOutputHandle('return_one.compute'), 21
        ).op
        == ObjectStoreOperationType.SET_OBJECT
    )

    assert (
        intermediates_manager.rm_intermediate(None, StepOutputHandle('return_one.compute')).op
        == ObjectStoreOperationType.RM_OBJECT
    )

    assert (
        intermediates_manager.set_intermediate(
            None, Int, StepOutputHandle('return_one.compute'), 42
        ).op
        == ObjectStoreOperationType.SET_OBJECT
    )

    assert (
        intermediates_manager.get_intermediate(
            None, Int, StepOutputHandle('return_one.compute')
        ).obj
        == 42
    )


def test_file_system_intermediate_storage_composite_types():
    _, _, intermediates_manager = define_intermediates_manager()

    assert intermediates_manager.set_intermediate(
        None, List[Bool], StepOutputHandle('return_true_lst.compute'), [True]
    )

    assert intermediates_manager.has_intermediate(None, StepOutputHandle('return_true_lst.compute'))

    assert intermediates_manager.get_intermediate(
        None, List[Bool], StepOutputHandle('return_true_lst.compute')
    ).obj == [True]


def test_file_system_intermediate_storage_with_custom_serializer():
    run_id, instance, intermediates_manager = define_intermediates_manager()
    with yield_empty_pipeline_context(run_id=run_id, instance=instance) as context:
        intermediates_manager.set_intermediate(
            context, LowercaseString, StepOutputHandle('a.b'), 'bar'
        )

        with open(
            os.path.join(intermediates_manager.root, 'intermediates', 'a.b', 'result'), 'rb'
        ) as fd:
            assert fd.read().decode('utf-8') == 'BAR'

        assert intermediates_manager.has_intermediate(context, StepOutputHandle('a.b'))
        assert (
            intermediates_manager.get_intermediate(
                context, LowercaseString, StepOutputHandle('a.b')
            ).obj
            == 'bar'
        )


def test_file_system_intermediate_storage_composite_types_with_custom_serializer_for_inner_type():
    run_id, instance, intermediates_manager = define_intermediates_manager()

    with yield_empty_pipeline_context(run_id=run_id, instance=instance) as context:

        intermediates_manager.set_intermediate(
            context, resolve_dagster_type(List[LowercaseString]), StepOutputHandle('baz'), ['list']
        )
        assert intermediates_manager.has_intermediate(context, StepOutputHandle('baz'))
        assert intermediates_manager.get_intermediate(
            context, resolve_dagster_type(List[Bool]), StepOutputHandle('baz')
        ).obj == ['list']


def test_file_system_intermediate_storage_with_type_storage_plugin():
    run_id, instance, intermediates_manager = define_intermediates_manager(
        type_storage_plugin_registry=TypeStoragePluginRegistry(
            [(RuntimeString, FancyStringFilesystemTypeStoragePlugin)]
        ),
    )

    with yield_empty_pipeline_context(run_id=run_id, instance=instance) as context:
        try:
            intermediates_manager.set_intermediate(
                context, RuntimeString, StepOutputHandle('obj_name'), 'hello'
            )

            assert intermediates_manager.has_intermediate(context, StepOutputHandle('obj_name'))
            assert (
                intermediates_manager.get_intermediate(
                    context, RuntimeString, StepOutputHandle('obj_name')
                )
                == 'hello'
            )

        finally:
            intermediates_manager.rm_intermediate(context, StepOutputHandle('obj_name'))


def test_file_system_intermediate_storage_with_composite_type_storage_plugin():
    run_id, _, intermediates_manager = define_intermediates_manager(
        type_storage_plugin_registry=TypeStoragePluginRegistry(
            [(RuntimeString, FancyStringFilesystemTypeStoragePlugin)]
        ),
    )

    with yield_empty_pipeline_context(run_id=run_id) as context:
        with pytest.raises(check.NotImplementedCheckError):
            intermediates_manager.set_intermediate(
                context, resolve_dagster_type(List[String]), StepOutputHandle('obj_name'), ['hello']
            )

    with yield_empty_pipeline_context(run_id=run_id) as context:
        with pytest.raises(check.NotImplementedCheckError):
            intermediates_manager.set_intermediate(
                context,
                resolve_dagster_type(Optional[String]),
                StepOutputHandle('obj_name'),
                ['hello'],
            )

    with yield_empty_pipeline_context(run_id=run_id) as context:
        with pytest.raises(check.NotImplementedCheckError):
            intermediates_manager.set_intermediate(
                context,
                resolve_dagster_type(List[Optional[String]]),
                StepOutputHandle('obj_name'),
                ['hello'],
            )

    with yield_empty_pipeline_context(run_id=run_id) as context:
        with pytest.raises(check.NotImplementedCheckError):
            intermediates_manager.set_intermediate(
                context,
                resolve_dagster_type(Optional[List[String]]),
                StepOutputHandle('obj_name'),
                ['hello'],
            )
