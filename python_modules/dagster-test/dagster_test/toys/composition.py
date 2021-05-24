from typing import List

from dagster import solid
from dagster.core.definitions.decorators.graph import graph


@solid
def emit_one() -> int:
    return 1


@solid
def add(_, numbers: List[int]) -> int:
    return sum(numbers)


@solid
def div_two(_, num: float) -> float:
    return num / 2


@graph
def emit_two() -> int:
    return add([emit_one(), emit_one()])


@graph
def add_four(num: int) -> int:
    return add([emit_two(), emit_two(), num])


@graph
def div_four(num: float) -> float:
    return div_two(num=div_two(num))


@solid
def int_to_float(_, num: int) -> float:
    return float(num)


@graph(description="Demo pipeline that makes use of composite solids.")
def composition():
    div_four(int_to_float(add_four()))  # pylint: disable=no-value-for-parameter
