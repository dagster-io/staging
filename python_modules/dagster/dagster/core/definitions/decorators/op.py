from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Union

from dagster import check

from ....seven.typing import get_origin
from ....utils.backcompat import experimental_decorator
from ...errors import DagsterInvariantViolationError
from ..inference import infer_output_props
from ..input import In, InputDefinition
from ..output import Out, OutputDefinition
from ..policy import RetryPolicy
from ..solid import SolidDefinition
from .solid import _Solid


class _Op:
    def __init__(
        self,
        name: Optional[str] = None,
        input_defs: Optional[Sequence[InputDefinition]] = None,
        output_defs: Optional[Sequence[OutputDefinition]] = None,
        description: Optional[str] = None,
        required_resource_keys: Optional[Set[str]] = None,
        config_schema: Optional[Union[Any, Dict[str, Any]]] = None,
        tags: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None,
        decorator_takes_context: Optional[bool] = True,
        retry_policy: Optional[RetryPolicy] = None,
        ins: Optional[Dict[str, In]] = None,
        out: Optional[Union[Out, Dict[str, Out]]] = None,
    ):
        self.name = check.opt_str_param(name, "name")
        self.input_defs = input_defs
        self.output_defs = output_defs
        self.decorator_takes_context = check.bool_param(
            decorator_takes_context, "decorator_takes_context"
        )

        self.description = check.opt_str_param(description, "description")

        # these will be checked within SolidDefinition
        self.required_resource_keys = required_resource_keys
        self.tags = tags
        self.version = version
        self.retry_policy = retry_policy

        # config will be checked within SolidDefinition
        self.config_schema = config_schema

        self.ins = check.opt_dict_param(ins, "ins", key_type=str)
        self.out = out

    def __call__(self, fn: Callable[..., Any]) -> SolidDefinition:
        if self.input_defs is not None and self.ins is not None:
            check.failed("Values cannot be provided for both the 'input_defs' and 'ins' arguments")

        if self.output_defs is not None and self.out is not None:
            check.failed("Values cannot be provided for both the 'output_defs' and 'out' arguments")

        inferred_out = infer_output_props(fn)

        input_defs = [inp.to_definition(name) for name, inp in self.ins.items()]

        final_output_defs: Optional[Sequence[OutputDefinition]] = None
        if self.out:
            check.inst_param(self.out, "out", (Out, dict))

            if isinstance(self.out, Out):
                final_output_defs = [self.out.to_definition(inferred_out.annotation, name=None)]
            else:
                final_output_defs = []
                # If only a single entry has been provided to the out dict, then slurp the
                # annotation into the entry.
                if len(self.out) == 1:
                    name = list(self.out.keys())[0]
                    out = list(self.out.values())[0]
                    final_output_defs.append(out.to_definition(inferred_out.annotation, name))
                else:
                    # Introspection on type annotations is experimental, so checking
                    # metaclass is the best we can do.
                    if inferred_out.annotation and not get_origin(inferred_out.annotation) == tuple:
                        raise DagsterInvariantViolationError(
                            "Expected Tuple annotation for multiple outputs, but received non-tuple annotation."
                        )
                    if inferred_out.annotation and not len(inferred_out.annotation.__args__) == len(
                        self.out
                    ):
                        raise DagsterInvariantViolationError(
                            "Expected Tuple annotation to have number of entries matching the "
                            f"number of outputs for more than one output. Expected {len(self.out)} "
                            f"outputs but annotation has {len(inferred_out.annotation.__args__)}."
                        )
                    for idx, (name, out) in enumerate(self.out.items()):
                        annotation_type = (
                            inferred_out.annotation.__args__[idx]
                            if inferred_out.annotation
                            else None
                        )
                        final_output_defs.append(out.to_definition(annotation_type, name=name))
        else:
            final_output_defs = self.output_defs

        return _Solid(
            name=self.name,
            input_defs=self.input_defs or input_defs,
            output_defs=final_output_defs,
            description=self.description,
            required_resource_keys=self.required_resource_keys,
            config_schema=self.config_schema,
            tags=self.tags,
            version=self.version,
            decorator_takes_context=self.decorator_takes_context,
            retry_policy=self.retry_policy,
        )(fn)


@experimental_decorator
def op(
    name: Union[Callable[..., Any], Optional[str]] = None,
    description: Optional[str] = None,
    input_defs: Optional[List[InputDefinition]] = None,
    output_defs: Optional[List[OutputDefinition]] = None,
    config_schema: Optional[Union[Any, Dict[str, Any]]] = None,
    required_resource_keys: Optional[Set[str]] = None,
    tags: Optional[Dict[str, Any]] = None,
    version: Optional[str] = None,
    retry_policy: Optional[RetryPolicy] = None,
    ins: Optional[Dict[str, In]] = None,
    out: Optional[Union[Out, Dict[str, Out]]] = None,
) -> Union[_Op, SolidDefinition]:
    """Op is an experimental replacement for solid, intended to decrease verbosity of core API.

    Args:
        name (Optional[str]): Name of the op. Must be unique within any
            :py:class:`GraphDefinition` using the op.
        description (Optional[str]): Human-readable description of this op. If not provided, and
            the decorated function has a docstring, that docstring will be used as the description.
        ins (Optional[Dict[str, In]]):
            Information about the inputs to the op. Information provided here will be combined
            with what can be inferred from the function signature, with these explicit ins
            taking precedence.
        outs (Optional[Union[Out, Dict[str, Out]]]):
            Information about the output of the op. Information provided here will be combined with
            what can be inferred from the return type signature if there is only one OutputDefinition
            and the function does not use yield.
        input_defs (Optional[List[InputDefinition]]):
            Backcompat with solid decorator.
        output_defs (Optional[List[OutputDefinition]]):
            Backcompat with solid decorator.
        config_schema (Optional[ConfigSchema): The schema for the config. If set, Dagster will check
            that config provided for the op matches this schema and fail if it does not. If not
            set, Dagster will accept any config provided for the op.
        required_resource_keys (Optional[Set[str]]): Set of resource handles required by this op.
        tags (Optional[Dict[str, Any]]): Arbitrary metadata for the op. Frameworks may
            expect and require certain metadata to be attached to an op. Users should generally
            not set metadata directly. Values that are not strings will be json encoded and must meet
            the criteria that `json.loads(json.dumps(value)) == value`.
        version (Optional[str]): (Experimental) The version of the op's compute_fn. Two ops should have
            the same version if and only if they deterministically produce the same outputs when
            provided the same inputs.
        retry_policy (Optional[RetryPolicy]): The retry policy for this op.

    Examples:

        .. code-block:: python

            @op
            def hello_world():
                print('hello')

            @op
            def hello_world():
                return {'foo': 'bar'}

            @op
            def hello_world():
                return Output(value={'foo': 'bar'})

            @op
            def hello_world():
                yield Output(value={'foo': 'bar'})

            @op
            def hello_world(foo):
                return foo

            @solid(
                ins={"foo": In(dagster_type=str)},
                out=Out(str)
            )
            def hello_world(foo: str) -> str:
                # explicitly type and name inputs and outputs
                return foo

            @solid
            def hello_world(foo: str) -> str:
                # same as above inferred from signature
                return foo

            @solid
            def hello_world(context, foo):
                context.log.info('log something')
                return foo

            @solid(
                config_schema={'str_value' : Field(str)}
            )
            def hello_world(context, foo):
                # context.solid_config is a dictionary with 'str_value' key
                return foo + context.solid_config['str_value']

    """
    # This case is for when decorator is used bare, without arguments. e.g. @op versus @op()
    if callable(name):
        check.invariant(input_defs is None)
        check.invariant(output_defs is None)
        check.invariant(description is None)
        check.invariant(config_schema is None)
        check.invariant(required_resource_keys is None)
        check.invariant(tags is None)
        check.invariant(version is None)

        return _Op()(name)

    return _Op(
        name=name,
        description=description,
        input_defs=input_defs,
        output_defs=output_defs,
        config_schema=config_schema,
        required_resource_keys=required_resource_keys,
        tags=tags,
        version=version,
        retry_policy=retry_policy,
        ins=ins,
        out=out,
    )
