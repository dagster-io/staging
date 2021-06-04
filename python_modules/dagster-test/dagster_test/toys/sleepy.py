# pylint:disable=no-member
from time import sleep
from typing import Iterator, List

from dagster import (
    ConfigMapping,
    Field,
    InputDefinition,
    Int,
    List,
    Output,
    OutputDefinition,
    solid,
)
from dagster.core.definitions.decorators.graph import graph


@solid
def sleeper(context, units: List[int]) -> int:
    tot = 0
    for sec in units:
        context.log.info("Sleeping for {} seconds".format(sec))
        sleep(sec)
        tot += sec

    return tot


@solid(
    config_schema=[int],
    output_defs=[
        OutputDefinition(List[Int], "out_1"),
        OutputDefinition(List[Int], "out_2"),
        OutputDefinition(List[Int], "out_3"),
        OutputDefinition(List[Int], "out_4"),
    ],
)
def giver(context) -> Iterator[Output]:
    units = context.solid_config
    queues: List[List[int]] = [[], [], [], []]
    for i, sec in enumerate(units):
        queues[i % 4].append(sec)

    yield Output(queues[0], "out_1")
    yield Output(queues[1], "out_2")
    yield Output(queues[2], "out_3")
    yield Output(queues[3], "out_4")


@solid
def total(in_1: int, in_2: int, in_3: int, in_4: int) -> int:
    return in_1 + in_2 + in_3 + in_4


@graph(
    description=(
        "Demo diamond-shaped pipeline that has four-path parallel structure of solids.  Execute "
        "with the `multi` preset to take advantage of multi-process parallelism."
    ),
)
def sleepy():
    giver_res = giver()

    total(
        in_1=sleeper(units=giver_res.out_1),
        in_2=sleeper(units=giver_res.out_2),
        in_3=sleeper(units=giver_res.out_3),
        in_4=sleeper(units=giver_res.out_4),
    )


def _config(cfg):
    return {
        "intermediate_storage": {"filesystem": {}},
        "execution": {"multiprocess": {}},
        "solids": {"giver": {"config": cfg["sleeps"]}},
    }


sleepy_pipeline = sleepy.to_job(
    config_mapping=ConfigMapping(
        config_schema={"sleeps": Field([int], is_required=False, default_value=[1, 1, 1, 1])},
        config_fn=_config,
    )
)
