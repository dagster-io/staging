from typing import cast

from dagster import (
    Failure,
    Field,
    IOManager,
    ResourceDefinition,
    RetryRequested,
    graph,
    io_manager,
    op,
)
from dagster.core.definitions.utils import config_from_pkg_resources
from dagster.utils import segfault


class ExampleException(Exception):
    pass


class ErrorableIOManager(IOManager):
    def __init__(self, throw_input, throw_output):
        self._values = {}
        self._throw_input = throw_input
        self._throw_output = throw_output

    def handle_output(self, context, obj):
        if self._throw_output:
            raise ExampleException("throwing up trying to handle output")

        keys = tuple(context.get_run_scoped_output_identifier())
        self._values[keys] = obj

    def load_input(self, context):
        if self._throw_input:
            raise ExampleException("throwing up trying to load input")

        keys = tuple(context.upstream_output.get_run_scoped_output_identifier())
        return self._values[keys]


@io_manager(
    config_schema={
        "throw_in_load_input": Field(bool, is_required=False, default_value=False),
        "throw_in_handle_output": Field(bool, is_required=False, default_value=False),
    }
)
def errorable_io_manager(init_context):
    return ErrorableIOManager(
        init_context.resource_config["throw_in_load_input"],
        init_context.resource_config["throw_in_handle_output"],
    )


class ErrorableResource:
    pass


def resource_init(init_context):
    if init_context.resource_config["throw_on_resource_init"]:
        raise Exception("throwing from in resource_fn")
    return ErrorableResource()


def define_errorable_resource():
    return ResourceDefinition(
        resource_fn=resource_init,
        config_schema={
            "throw_on_resource_init": Field(bool, is_required=False, default_value=False)
        },
    )


solid_throw_config = {
    "throw_in_solid": Field(bool, is_required=False, default_value=False),
    "failure_in_solid": Field(bool, is_required=False, default_value=False),
    "crash_in_solid": Field(bool, is_required=False, default_value=False),
    "return_wrong_type": Field(bool, is_required=False, default_value=False),
    "request_retry": Field(bool, is_required=False, default_value=False),
}


def _act_on_config(solid_config):
    if solid_config["crash_in_solid"]:
        segfault()
    if solid_config["failure_in_solid"]:
        try:
            raise ExampleException("sample cause exception")
        except ExampleException as e:
            raise Failure(
                description="I'm a Failure",
                metadata={
                    "metadata_label": "I am metadata text",
                },
            ) from e
    elif solid_config["throw_in_solid"]:
        raise ExampleException("I threw up")
    elif solid_config["request_retry"]:
        raise RetryRequested()


@op(
    config_schema=solid_throw_config,
    required_resource_keys={"errorable_resource"},
)
def emit_num(context) -> int:
    _act_on_config(context.solid_config)

    if context.solid_config["return_wrong_type"]:
        return cast(int, "wow")

    return 13


@op(
    config_schema=solid_throw_config,
    required_resource_keys={"errorable_resource"},
)
def num_to_str(context, num: int) -> str:
    _act_on_config(context.solid_config)

    if context.solid_config["return_wrong_type"]:
        return cast(str, num + num)

    return str(num)


@op(
    config_schema=solid_throw_config,
    required_resource_keys={"errorable_resource"},
)
def str_to_num(context, string: str) -> int:
    _act_on_config(context.solid_config)

    if context.solid_config["return_wrong_type"]:
        return cast(int, string + string)

    return int(string)


@graph(
    description=(
        "Demo pipeline that enables configurable types of errors thrown during pipeline execution, "
        "including solid execution errors, type errors, and resource initialization errors."
    )
)
def error_monster_graph():
    start = emit_num.alias("start")()
    middle = num_to_str.alias("middle")(num=start)
    str_to_num.alias("end")(string=middle)


error_monster_job = error_monster_graph.to_job(
    name="error_monster_job",
    resource_defs={
        "errorable_resource": define_errorable_resource(),
        "io_manager": errorable_io_manager,
    },
    default_config=config_from_pkg_resources([("dagster_test.toys.environments", "error.yaml")]),
)
