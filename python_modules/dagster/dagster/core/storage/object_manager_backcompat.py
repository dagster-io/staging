import os

from dagster import check
from dagster.core.types.marshal import SerializationStrategy
from dagster.utils import mkdir_p

from .object_manager import ObjectManager, object_manager


class SerializationStrategyAdapter(ObjectManager):
    def __init__(self, serialization_strategy, base_dir):
        self.serialization_strategy = check.inst_param(
            serialization_strategy, "serialization_strategy", SerializationStrategy
        )
        self.base_dir = check.opt_str_param(base_dir, "base_dir")
        self.write_mode = serialization_strategy.write_mode
        self.read_mode = serialization_strategy.read_mode

    def _get_path(self, context):
        keys = context.get_run_scoped_output_identifier()
        return os.path.join(self.base_dir, *keys)

    def handle_output(self, context, obj):
        write_path = self._get_path(context)
        mkdir_p(os.path.dirname(write_path))
        return self.serialization_strategy.serialize_to_file(value=obj, write_path=write_path)

    def load_input(self, context):
        read_path = self._get_path(context.upstream_contxt)
        return self.serialization_strategy.deserialize_from_file(read_path=read_path)


def object_manager_from_serialization_strategy(serialization_strategy, base_dir="."):
    """Define an :py:class:`ObjectManagerDefinition` from an existing :py:class:`SerializationStrategy`.

    This method is used to adapt an existing user-defined serialization strategy to a object manager
    resource, for example:

    ```

    my_object_manager_def = object_manager_from_serialization_strategy(
        MySerializationStrategy(), base_dir
    )

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"object_manager": my_object_manager_def})])
    def my_pipeline():
        ...

    ```

    Args:
        serialization_strategy ([SerializationStrategy]): The serialization strategy to convert
        base_dir (Optional[str]): base directory where all the step outputs which use this object
            manager will be stored in.

    Returns:
        ObjectManagerDefinition
    """

    check.inst_param(serialization_strategy, "serialization_strategy", SerializationStrategy)
    base_dir = check.opt_str_param(base_dir, "base_dir")

    @object_manager
    def _object_manager(_):
        return SerializationStrategyAdapter(serialization_strategy, base_dir)

    return _object_manager
