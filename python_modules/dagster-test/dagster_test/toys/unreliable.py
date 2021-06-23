from random import random

from dagster import Field, graph, op

DEFAULT_EXCEPTION_RATE = 0.3


@op
def unreliable_start() -> int:
    return 1


@op(config_schema={"rate": Field(float, is_required=False, default_value=DEFAULT_EXCEPTION_RATE)})
def unreliable(context, num: int) -> int:
    if random() < context.solid_config["rate"]:
        raise Exception("blah")

    return num


@graph(description="Demo pipeline of chained solids that fail with a configurable probability.")
def unreliable_graph():
    one = unreliable.alias("one")
    two = unreliable.alias("two")
    three = unreliable.alias("three")
    four = unreliable.alias("four")
    five = unreliable.alias("five")
    six = unreliable.alias("six")
    seven = unreliable.alias("seven")
    seven(six(five(four(three(two(one(unreliable_start())))))))


unreliable_job = unreliable_graph.to_job(name="unreliable_job")
