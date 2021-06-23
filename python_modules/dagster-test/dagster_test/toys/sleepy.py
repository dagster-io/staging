# pylint:disable=no-member
from time import sleep
from typing import Iterator, List

from dagster import ConfigMapping, MultiOut, Out, Output, OutputDefinition, graph, op


@op
def sleeper(context, units: List[int]) -> int:
    tot = 0
    for sec in units:
        context.log.info(f"Sleeping for {sec} seconds")
        sleep(sec)
        tot += sec

    return tot


@op(
    config_schema=[int],
    out=MultiOut(
        outs=[
            OutputDefinition(List[int], "out_1"),
            OutputDefinition(List[int], "out_2"),
            OutputDefinition(List[int], "out_3"),
            OutputDefinition(List[int], "out_4"),
        ]
    ),
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


@op(
    config_schema={"fail": bool},
    out=Out(int, is_required=False),
)
def total(context, in_1, in_2, in_3, in_4):
    result = in_1 + in_2 + in_3 + in_4
    if context.solid_config["fail"]:
        yield Output(result, "result")
    # skip the failing solid
    context.log.info(str(result))


@op
def will_fail(i):
    raise Exception(i)


@graph(
    description=(
        "Demo diamond-shaped pipeline that has four-path parallel structure of solids. Execute "
        "with the `multi` preset to take advantage of multi-process parallelism."
    ),
)
def sleepy():
    giver_res = giver()

    will_fail(
        total(
            in_1=sleeper(units=giver_res.out_1),
            in_2=sleeper(units=giver_res.out_2),
            in_3=sleeper(units=giver_res.out_3),
            in_4=sleeper(units=giver_res.out_4),
        )
    )


def _config(cfg):
    return {
        "intermediate_storage": {"filesystem": {}},
        "execution": {"multiprocess": {}},
        "solids": {
            "giver": {"config": cfg["sleeps"]},
            "total": {"config": {"fail": cfg["fail"]}},
        },
    }


sleepy_job = sleepy.to_job(
    name="sleepy_job",
    config_mapping=ConfigMapping(
        config_schema={
            "sleeps": [int],
            "fail": bool,
        },
        config_fn=_config,
    ),
    default_config={"sleeps": [1, 1, 1, 1], "fail": False},
)
