from functools import update_wrapper

from dagster import check

from ..input import InputDefinition
from ..output import OutputDefinition
from ..pipeline import GraphDefinition


class _Graph:
    def __init__(
        self, name=None, description=None, tags=None, input_defs=None, output_defs=None,
    ):
        self.name = check.opt_str_param(name, "name")
        self.description = check.opt_str_param(description, "description")
        self.tags = check.opt_dict_param(tags, "tags")
        self.input_defs = check.opt_nullable_list_param(
            input_defs, "input_defs", of_type=InputDefinition
        )
        self.did_pass_outputs = output_defs is not None
        self.output_defs = check.opt_nullable_list_param(
            output_defs, "output_defs", of_type=OutputDefinition
        )

    def __call__(self, fn):
        check.callable_param(fn, "fn")

        if not self.name:
            self.name = fn.__name__

        from dagster.core.definitions.decorators.composite_solid import do_composition

        (
            input_mappings,
            output_mappings,
            dependencies,
            solid_defs,
            config_mapping,
            positional_inputs,
        ) = do_composition(
            decorator_name="@graph",
            graph_name=self.name,
            fn=fn,
            provided_input_defs=self.input_defs,
            provided_output_defs=self.output_defs,
            ignore_output_from_composition_fn=False,
            config_schema=None,
            config_fn=None,
        )

        pipeline_def = GraphDefinition(
            name=self.name,
            dependencies=dependencies,
            node_defs=solid_defs,
            description=self.description,
            tags=self.tags,
            input_mappings=input_mappings,
            output_mappings=output_mappings,
            config_mapping=config_mapping,
            positional_inputs=positional_inputs,
        )
        update_wrapper(pipeline_def, fn)
        return pipeline_def


def graph(
    name=None, description=None, tags=None, input_defs=None, output_defs=None,
):
    """Create a graph with the specified parameters from the decorated composition function.

    Using this decorator allows you to build up a dependency graph by writing a
    function that invokes solids (or other graphs) and passes the output to subsequent invocations.

    Args:
        name (Optional[str]): The name of the pipeline. Must be unique within any
            :py:class:`RepositoryDefinition` containing the pipeline.
        description (Optional[str]): A human-readable description of the pipeline.
        tags (Optional[Dict[str, Any]]): Arbitrary metadata for any execution run of the pipeline.
            Values that are not strings will be json encoded and must meet the criteria that
            `json.loads(json.dumps(value)) == value`.  These tag values may be overwritten by tag
            values provided at invocation time.
    """
    if callable(name):
        check.invariant(description is None)
        return _Graph()(name)

    return _Graph(
        name=name,
        description=description,
        tags=tags,
        input_defs=input_defs,
        output_defs=output_defs,
    )
