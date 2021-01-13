from abc import ABC, abstractmethod


class InputManager(ABC):
    """RootInputManagers are used to load the inputs to solids at the root of a pipeline.

    The easiest way to define an RootInputManager is with the :py:decorator:`root_input_manager` decorator.
    """

    @abstractmethod
    def load_input(self, context):
        """The user-defined read method that loads data given its metadata.

        Args:
            context (InputContext): The context of the step output that produces this asset.

        Returns:
            Any: The data object.
        """
