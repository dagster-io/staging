import os

from dagster import seven
from dagster.core.definitions import pipeline, solid
from dagster.core.definitions.mode import ModeDefinition
from dagster.core.execution.api import execute_pipeline
from dagster.core.storage.object_manager_backcompat import (
    object_manager_from_serialization_strategy,
)
from dagster.core.types.marshal import SerializationStrategy


def test_serialization_strategy_to_object_manager_def():
    called = {}

    class MySerializationStrategy(SerializationStrategy):  # pylint: disable=no-init
        def __init__(self, name="pickle"):
            super(MySerializationStrategy, self).__init__(name)

        def serialize(self, value, write_file_obj):
            called[write_file_obj.name] = value

        def deserialize(self, read_file_obj):
            return read_file_obj

    with seven.TemporaryDirectory() as tmpdir_path:

        @solid
        def return_one(_):
            return 1

        @pipeline(
            mode_defs=[
                ModeDefinition(
                    resource_defs={
                        "object_manager": object_manager_from_serialization_strategy(
                            MySerializationStrategy(), tmpdir_path
                        )
                    }
                )
            ]
        )
        def foo():
            return_one()

        result = execute_pipeline(foo)
        assert called[os.path.join(tmpdir_path, result.run_id, "return_one", "result")] == 1
