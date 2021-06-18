from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Union

from dagster import check
from dagster.utils.backcompat import experimental_decorator

from ..inference import infer_input_props, infer_output_props
from ..input import In, InputDefinition
from ..output import MultiOut, Out, OutputDefinition
from ..policy import RetryPolicy
from ..solid import SolidDefinition
from ..utils import NoValueSentinel
from .solid import DecoratedSolidFunction, _Solid


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
        ins: Optional[Union[List[In], Dict[str, In]]] = None,
        out: Optional[Union[Out, MultiOut]] = None,
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

        self.ins = ins
        self.out = out

    def __call__(self, fn: Callable[..., Any]) -> SolidDefinition:
        if self.input_defs is not None and self.ins is not None:
            check.failed("Values cannot be provided for both the 'input_defs' and 'ins' arguments")

        if self.output_defs is not None and self.out is not None:
            check.failed("Values cannot be provided for both the 'output_defs' and 'out' arguments")

        if isinstance(self.ins, dict):
            if any([inp.name is not None for inp in self.ins.values()]):
                check.failed(
                    "Cannot provide name to In if providing dictionary of ins. "
                    "The In will take on the dict key as the name."
                )
            ins = [
                In(
                    name=key,
                    dagster_type=inp.dagster_type,
                    description=inp.description,
                    default_value=inp.default_value,
                    root_manager_key=inp.root_manager_key,
                    metadata=inp.metadata,
                    asset_key=inp.asset_key,  # pylint: disable=protected-access
                    asset_partitions=inp.asset_partitions,  # pylint: disable=protected-access
                )
                for key, inp in self.ins.items()
            ]
        else:
            ins = self.ins or []

        inferred_in = infer_input_props(fn, DecoratedSolidFunction(fn).has_context_arg())
        inferred_out = infer_output_props(fn)

        input_defs = []
        for inp, inferred_props in zip(ins, inferred_in):
            dagster_type = (
                inp.dagster_type
                if inp.dagster_type is not NoValueSentinel
                else inferred_props.annotation
            )
            input_defs.append(
                InputDefinition(
                    name=inp.name,
                    dagster_type=dagster_type,
                    description=inp.description,
                    default_value=inp.default_value,
                    root_manager_key=inp.root_manager_key,
                    metadata=inp.metadata,
                    asset_key=inp.asset_key,
                    asset_partitions=inp.asset_partitions,
                )
            )

        final_output_defs: Optional[Sequence[OutputDefinition]] = None
        if self.out:
            check.inst_param(self.out, "out", (Out, MultiOut))

            if isinstance(self.out, Out):
                dagster_type = (
                    inferred_out.annotation if self.out._type_not_set else self.out.dagster_type
                )
                final_output_defs = [
                    OutputDefinition(
                        dagster_type=dagster_type,
                        name=self.out.name,
                        description=self.out.description,
                        is_required=self.out.is_required,
                        io_manager_key=self.out.io_manager_key,
                        metadata=self.out.metadata,
                        asset_key=self.out._asset_key_fn,
                        asset_partitions=self.out._asset_partitions_fn,
                    )
                ]
            elif isinstance(self.out, MultiOut):
                final_output_defs = []
                for idx, out in enumerate(self.out.outs):
                    annotation_type = (
                        inferred_out.annotation.__args__[idx] if inferred_out.annotation else None
                    )
                    final_output_defs.append(
                        OutputDefinition(
                            dagster_type=annotation_type,
                            name=out.name,
                            description=out.description,
                            is_required=out.is_required,
                            io_manager_key=out.io_manager_key,
                            metadata=out.metadata,
                            asset_key=out._asset_key_fn,
                            asset_partitions=out._asset_partitions_fn,
                        )
                    )
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
    ins: Optional[Union[List[In], Dict[str, In]]] = None,
    out: Optional[Union[Out, MultiOut]] = None,
) -> Union[_Op, SolidDefinition]:
    """Op is an experimental replacement for solid, intended to decrease verbosity of core API."""
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
