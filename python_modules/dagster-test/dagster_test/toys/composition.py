from typing import List

from dagster import InputDefinition, OutputDefinition, graph, op


@op
def emit_one() -> int:
    return 1


@op
def add(numbers: List[int]) -> int:
    return sum(numbers)


@op
def div_two(num: float) -> float:
    return num / 2


@graph
def emit_two() -> int:
    return add([emit_one(), emit_one()])


@graph(input_defs=[InputDefinition("num", int)], output_defs=[OutputDefinition(int)])
def add_four(num: int) -> int:
    return add([emit_two(), emit_two(), num])


@graph
def div_four(num: float) -> float:
    return div_two(num=div_two(num))


@op
def int_to_float(num) -> float:
    return float(num)


@graph(description="Demo pipeline that makes use of composition of graph.")
def composition_graph():
    div_four(int_to_float(add_four()))
