from functools import update_wrapper
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Set, Union, cast

from dagster import check
from dagster.core.errors import DagsterInvalidDefinitionError

from ...decorator_utils import split_function_parameters, validate_decorated_fn_positionals
from ..events import HookExecutionResult
from ..hook import PipelineHookDefinition

if TYPE_CHECKING:
    from dagster.core.execution.context.system import HookContext
    from dagster.core.events import DagsterEvent


class _PipelineHook:
    def __init__(
        self, name: Optional[str] = None, required_resource_keys: Optional[Set[str]] = None
    ):
        self.name = check.opt_str_param(name, "name")
        self.required_resource_keys = check.opt_set_param(
            required_resource_keys, "required_resource_keys"
        )

    def __call__(
        self, fn: Callable[["HookContext", List["DagsterEvent"]], Any]
    ) -> PipelineHookDefinition:

        check.callable_param(fn, "fn")

        if not self.name:
            self.name = fn.__name__

        expected_positionals = ["context", "event"]
        fn_positionals, _ = split_function_parameters(fn, expected_positionals)
        missing_positional = validate_decorated_fn_positionals(fn_positionals, expected_positionals)
        if missing_positional:
            raise DagsterInvalidDefinitionError(
                "'{hook_name}' decorated function does not have required positional "
                "parameter '{missing_param}'. Hook functions should only have keyword arguments "
                "that match input names and a first positional parameter named 'context' and "
                "a second positional parameter named 'event'.".format(
                    hook_name=fn.__name__, missing_param=missing_positional
                )
            )

        hook_def = PipelineHookDefinition(
            name=self.name, hook_fn=fn, required_resource_keys=self.required_resource_keys,
        )
        update_wrapper(cast(Callable[..., Any], hook_def), fn)
        return hook_def


def pipeline_hook(
    name: Union[Optional[str], Callable[..., Any]] = None,
    required_resource_keys: Optional[Set[str]] = None,
) -> Union[PipelineHookDefinition, _PipelineHook]:
    """TODO"""
    # This case is for when decorator is used bare, without arguments.
    # e.g. @event_list_hook versus @event_list_hook()
    if callable(name):
        check.invariant(required_resource_keys is None)
        return _PipelineHook()(name)

    return _PipelineHook(name=name, required_resource_keys=required_resource_keys)


def pipeline_failure_hook(
    name: Optional[str] = None, required_resource_keys: Optional[Set[str]] = None
) -> Union[
    Union[PipelineHookDefinition, _PipelineHook],
    Callable[[Callable[["HookContext"], Any]], Union[PipelineHookDefinition, _PipelineHook]],
]:
    """Create a hook on the pipeline failure event with the specified parameters from the decorated function.

    Args:
        name (Optional[str]): The name of this hook.
        required_resource_keys (Optional[Set[str]]): Keys for the resources required by the
            hook.

    Examples:

        .. code-block:: python

            @pipeline_failure_hook(required_resource_keys={'slack'})
            def slack_message_on_pipeline_failure(context):
                message = 'pipeline {} failed'.format(context.pipeline.name)
                context.resources.slack.send_message(message)

            @pipeline_failure_hook
            def do_something_on_pipeline_failure(context):
                do_something()


    """

    def wrapper(fn: Callable[["HookContext"], Any]) -> Union[PipelineHookDefinition, _PipelineHook]:
        check.callable_param(fn, "fn")

        expected_positionals = ["context"]
        fn_positionals, _ = split_function_parameters(fn, expected_positionals)
        missing_positional = validate_decorated_fn_positionals(fn_positionals, expected_positionals)
        if missing_positional:
            raise DagsterInvalidDefinitionError(
                "@pipeline_failure_hook '{hook_name}' decorated function does not have required positional "
                "parameter '{missing_param}'. Hook functions should only have keyword arguments "
                "that match input names and a first positional parameter named 'context'.".format(
                    hook_name=fn.__name__, missing_param=missing_positional
                )
            )

        if name is None or callable(name):
            _name = fn.__name__
        else:
            _name = name

        @pipeline_hook(_name, required_resource_keys)
        def _pipeline_failure_hook(
            context: "HookContext", event: "DagsterEvent"
        ) -> HookExecutionResult:
            # TODO: is passing single event right?
            if event.is_pipeline_event and event.is_failure:
                fn(context, event)
                return HookExecutionResult(hook_name=_name, is_skipped=False)

            # hook is skipped when fn didn't run
            return HookExecutionResult(hook_name=_name, is_skipped=True)

        return _pipeline_failure_hook

    # This case is for when decorator is used bare, without arguments, i.e. @failure_hook
    if callable(name):
        check.invariant(required_resource_keys is None)
        return wrapper(name)

    return wrapper
