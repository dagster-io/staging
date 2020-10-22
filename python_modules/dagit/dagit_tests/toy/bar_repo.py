import string

from dagster import PartitionSetDefinition, ScheduleDefinition, pipeline, repository, solid
from dagster.core.definitions import solid


@solid
def do_something(_):
    return 1


@solid
def do_input(_, x):
    return x


@pipeline(name="foo")
def foo_pipeline():
    do_input(do_something())


def define_foo_pipeline():
    return foo_pipeline


@pipeline(name="baz", description="Not much tbh")
def baz_pipeline():
    do_input()


def define_bar_schedules():
    return {
        "foo_schedule": ScheduleDefinition(
            "foo_schedule", cron_schedule="* * * * *", pipeline_name="test_pipeline", run_config={},
        )
    }


def define_baz_partitions():
    return {
        "baz_partitions": PartitionSetDefinition(
            name="baz_partitions",
            pipeline_name="baz",
            partition_fn=lambda: string.ascii_lowercase,
            run_config_fn_for_partition=lambda partition: {
                "solids": {"do_input": {"inputs": {"x": {"value": partition.value}}}}
            },
        )
    }


@repository
def bar():
    return {
        "pipelines": {"foo": foo_pipeline, "baz": baz_pipeline},
        "schedules": define_bar_schedules(),
        "partition_sets": define_baz_partitions(),
    }
