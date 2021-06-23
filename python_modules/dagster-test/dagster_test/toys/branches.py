from time import sleep

from dagster import Field, MultiOut, Out, Output, graph, op


@op(
    config_schema={"sleep_secs": Field([int], is_required=False, default_value=[0, 0])},
    out=MultiOut(outs=[Out(dagster_type=int, name="out_1"), Out(dagster_type=int, name="out_2")]),
)
def root(context):
    sleep_secs = context.solid_config["sleep_secs"]
    yield Output(sleep_secs[0], "out_1")
    yield Output(sleep_secs[1], "out_2")


@op
def branch_solid(context, sec):
    if sec < 0:
        sleep(-sec)
        raise Exception("fail")
    context.log.info(f"Sleeping for {sec} seconds")
    sleep(sec)
    return sec


def branch(name, arg, solid_num):
    out = arg
    for i in range(solid_num):
        out = branch_solid.alias(f"{name}_{i}")(out)

    return out


@graph(description="Demo fork-shaped pipeline that has two-path parallel structure of ops.")
def branch_graph():
    out_1, out_2 = root()
    branch("branch_1", out_1, 3)
    branch("branch_2", out_2, 5)


sleep_failed_branch_job = branch_graph.to_job(
    name="sleep_failed_branch_job",
    default_config={
        "intermediate_storage": {"filesystem": {}},
        "execution": {"multiprocess": {}},
        "solids": {"root": {"config": {"sleep_secs": [-10, 30]}}},
    },
)

sleep_branch_job = branch_graph.to_job(
    name="sleep_branch_job",
    default_config={
        "intermediate_storage": {"filesystem": {}},
        "execution": {"multiprocess": {}},
        "solids": {"root": {"config": {"sleep_secs": [0, 10]}}},
    },
)
